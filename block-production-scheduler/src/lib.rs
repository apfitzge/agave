#![cfg(unix)]
//! External transaction scheduler implementation for Agave scheduler bindings.

use {
    agave_scheduling_utils::{
        cost_pacer::CostPacer,
        handshake::{ClientHandshakeError, ClientSession, client},
        thread_aware_account_locks::ThreadAwareAccountLocks,
    },
    progress_tracker::SchedulerState,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

mod check_response;
mod config;
mod execution_response;
mod in_flight;
mod progress_tracker;
mod resolved_transaction;
mod scheduling;
mod state_container;
mod tpu_ingress;
mod transaction;

pub use config::{ConfigError, SchedulerConfig};

const MAX_TPU_PACKETS_PER_ITERATION: usize = 1024;
const MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION: usize =
    MAX_TPU_PACKETS_PER_ITERATION / transaction::MAX_PACKETS_PER_CHECK_BATCH;
const MAX_EXECUTION_RESPONSE_BATCHES_PER_ITERATION: usize =
    MAX_TPU_PACKETS_PER_ITERATION / transaction::MAX_PACKETS_PER_EXEC_BATCH;

/// Connect to Agave's scheduler bindings service and run until `exit` is set.
pub fn run(config: SchedulerConfig, exit: Arc<AtomicBool>) -> Result<(), SchedulerError> {
    config.validate()?;
    if exit.load(Ordering::Relaxed) {
        return Ok(());
    }

    let session = client::connect(
        &config.bindings_ipc,
        config.client_logon(),
        config.handshake_timeout,
    )?;
    run_session(config, session, exit);

    Ok(())
}

fn run_session(config: SchedulerConfig, session: ClientSession, exit: Arc<AtomicBool>) {
    let mut scheduler = Scheduler::new(config, session);

    while !exit.load(Ordering::Relaxed) {
        scheduler.run_iteration(Instant::now());
        thread::yield_now();
    }
}

struct Scheduler {
    session: ClientSession,
    state: SchedulerState,
    transaction_state: state_container::StateContainer,
    account_locks: ThreadAwareAccountLocks,
    in_flight: in_flight::InFlightTracker,
    scheduling_scratch: scheduling::SchedulingScratch,
    cost_pacer: Option<(u64, CostPacer)>,
}

impl Scheduler {
    fn new(config: SchedulerConfig, session: ClientSession) -> Self {
        Self {
            session,
            state: SchedulerState::new(),
            transaction_state: state_container::StateContainer::new(
                config.transaction_state_capacity,
            ),
            account_locks: ThreadAwareAccountLocks::new(config.execution_worker_count),
            in_flight: in_flight::InFlightTracker::new(
                config.execution_worker_count,
                config.pack_to_worker_capacity,
            ),
            scheduling_scratch: scheduling::SchedulingScratch::new(
                config.transaction_state_capacity,
                config.execution_worker_count,
            ),
            cost_pacer: None,
        }
    }

    fn run_iteration(&mut self, now: Instant) {
        self.drain_progress();
        self.drain_check_responses();
        self.drain_execution_responses();
        self.schedule(now);
        self.ingest_tpu();
    }

    fn drain_progress(&mut self) {
        progress_tracker::drain_progress(&mut self.session.progress_tracker, &mut self.state);
    }

    fn drain_check_responses(&mut self) {
        let check_sanitize_config = resolved_transaction::sanitize_config(
            self.state
                .feature_set()
                .snapshot()
                .limit_instruction_accounts,
        );
        check_response::drain_check_responses(
            &self.session.check_worker_to_pack,
            &self.session.allocators[0],
            &check_sanitize_config,
            self.state.reserved_account_keys(),
            &mut self.transaction_state,
            MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION,
        );
    }

    fn drain_execution_responses(&mut self) {
        execution_response::drain_execution_responses(
            &mut self.session.workers,
            &self.session.allocators[0],
            &mut self.transaction_state,
            &mut self.account_locks,
            &mut self.in_flight,
            MAX_EXECUTION_RESPONSE_BATCHES_PER_ITERATION,
        );
    }

    fn schedule(&mut self, now: Instant) {
        if self.state.can_process_transactions() {
            let current_slot = self.state.current_slot();
            let needs_new_cost_pacer = match self.cost_pacer.as_ref() {
                Some((slot, _)) => *slot != current_slot,
                None => true,
            };
            if needs_new_cost_pacer {
                let fill_time = (self.state.target_bank_time_ms() != 0)
                    .then(|| Duration::from_millis(u64::from(self.state.target_bank_time_ms())));
                self.cost_pacer = Some((
                    current_slot,
                    CostPacer::new(self.state.initial_remaining_cost_units(), now, fill_time),
                ));
            }
            let (_, cost_pacer) = self
                .cost_pacer
                .as_ref()
                .expect("leader-ready scheduler state initializes the cost pacer");
            let consumed_cost_units = self
                .state
                .initial_remaining_cost_units()
                .saturating_sub(self.state.remaining_cost_units());
            scheduling::schedule(
                &mut self.session.workers,
                &self.session.allocators[0],
                &mut self.transaction_state,
                &mut self.account_locks,
                &mut self.in_flight,
                &mut self.scheduling_scratch,
                current_slot,
                cost_pacer.scheduling_budget(&now, consumed_cost_units),
                self.state.target_scheduled_cus(),
            );
        } else {
            self.cost_pacer = None;
        }
    }

    fn ingest_tpu(&mut self) {
        tpu_ingress::drain_tpu(
            &mut self.session.tpu_to_pack,
            &self.session.pack_to_check_worker,
            &self.session.allocators[0],
            &self.state,
            MAX_TPU_PACKETS_PER_ITERATION,
            |_, _| true,
        );
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("invalid scheduler config: {0}")]
    Config(#[from] ConfigError),
    #[error("scheduler bindings handshake failed: {0}")]
    Handshake(#[from] ClientHandshakeError),
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_scheduling_utils::handshake::{client, server::Server},
        std::sync::atomic::AtomicBool,
    };

    #[test]
    fn exits_without_connecting_when_already_signaled() {
        let exit = Arc::new(AtomicBool::new(true));

        run(SchedulerConfig::new("/does/not/exist"), exit).unwrap();
    }

    #[test]
    fn returns_typed_config_error() {
        let mut config = SchedulerConfig::new("/does/not/exist");
        config.execution_worker_count = 0;

        let error = run(config, Arc::new(AtomicBool::new(false))).unwrap_err();
        assert!(matches!(
            error,
            SchedulerError::Config(ConfigError::ExecutionWorkerCount { count: 0 })
        ));
    }

    #[test]
    fn exits_without_running_when_already_signaled() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (_agave_session, files) = Server::setup_session(logon).unwrap();
        let client_session = client::setup_session(&logon, files).unwrap();

        run_session(config, client_session, Arc::new(AtomicBool::new(true)));
    }
}
