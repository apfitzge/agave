#![cfg(unix)]
//! External transaction scheduler implementation for Agave scheduler bindings.

use {
    agave_scheduling_utils::{
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
    },
};

mod check_response;
mod config;
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

/// Connect to Agave's scheduler bindings service and run until `exit` is set.
pub fn run(config: SchedulerConfig, exit: Arc<AtomicBool>) -> Result<(), SchedulerError> {
    config.validate()?;
    if exit.load(Ordering::Relaxed) {
        return Ok(());
    }

    let mut session = client::connect(
        &config.bindings_ipc,
        config.client_logon(),
        config.handshake_timeout,
    )?;
    let mut state = SchedulerState::new();
    let mut transaction_state =
        state_container::StateContainer::new(config.transaction_state_capacity);
    let mut account_locks = ThreadAwareAccountLocks::new(config.execution_worker_count);
    let mut in_flight = in_flight::InFlightTracker::new(
        config.execution_worker_count,
        config.pack_to_worker_capacity,
    );
    let mut scheduling_scratch = scheduling::SchedulingScratch::new(
        config.transaction_state_capacity,
        config.execution_worker_count,
    );

    while !exit.load(Ordering::Relaxed) {
        progress_tracker::drain_progress(&mut session.progress_tracker, &mut state);
        let check_sanitize_config = resolved_transaction::sanitize_config(
            state.feature_set().snapshot().limit_instruction_accounts,
        );
        let ClientSession {
            allocators,
            tpu_to_pack,
            pack_to_check_worker,
            check_worker_to_pack,
            workers,
            ..
        } = &mut session;
        check_response::drain_check_responses(
            check_worker_to_pack,
            &allocators[0],
            &check_sanitize_config,
            state.reserved_account_keys(),
            &mut transaction_state,
            MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION,
        );
        if state.can_process_transactions() {
            scheduling::schedule(
                workers,
                &allocators[0],
                &mut transaction_state,
                &mut account_locks,
                &mut in_flight,
                &mut scheduling_scratch,
                state.current_slot(),
                state.remaining_cost_units(),
                state.target_scheduled_cus(),
            );
        }
        tpu_ingress::drain_tpu(
            tpu_to_pack,
            pack_to_check_worker,
            &allocators[0],
            &state,
            MAX_TPU_PACKETS_PER_ITERATION,
            |_, _| true,
        );
        thread::yield_now();
    }

    Ok(())
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
        super::*, agave_scheduling_utils::handshake::server::Server, std::sync::atomic::AtomicBool,
        tempfile::NamedTempFile,
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
    fn connects_then_exits() {
        let ipc = NamedTempFile::new().unwrap();
        std::fs::remove_file(ipc.path()).unwrap();
        let mut server = Server::new(ipc.path()).unwrap();
        let mut config = SchedulerConfig::new(ipc.path());
        config.allocator_size = 64 * 1024 * 1024;

        let exit = Arc::new(AtomicBool::new(false));
        let runner_exit = Arc::clone(&exit);
        let runner = thread::spawn(move || run(config, runner_exit));

        let _session = server.accept().unwrap();
        exit.store(true, Ordering::Relaxed);
        runner.join().unwrap().unwrap();
    }
}
