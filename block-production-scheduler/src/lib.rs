#![cfg(unix)]
//! External transaction scheduler implementation for Agave scheduler bindings.

use {
    agave_scheduling_utils::{
        cost_pacer::CostPacer,
        handshake::{ClientHandshakeError, ClientSession, client},
        thread_aware_account_locks::ThreadAwareAccountLocks,
    },
    log::info,
    progress_tracker::SchedulerState,
    std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
};

mod check_response;
mod config;
mod execution_response;
mod in_flight;
mod progress_tracker;
mod recheck;
mod resolved_transaction;
mod scheduling;
mod state_container;
mod tpu_ingress;
mod transaction;

pub use config::{ConfigError, SchedulerConfig};

const MAX_TPU_PACKETS_PER_ITERATION: usize = 256;
const MAX_CHECK_RESPONSE_PACKETS_PER_ITERATION: usize = 512;
const MAX_SCHEDULED_TRANSACTIONS_PER_ITERATION: usize = 1024;
const MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION: usize =
    MAX_CHECK_RESPONSE_PACKETS_PER_ITERATION / transaction::MAX_PACKETS_PER_CHECK_BATCH;
const MAX_EXECUTION_RESPONSE_BATCHES_PER_ITERATION: usize =
    MAX_SCHEDULED_TRANSACTIONS_PER_ITERATION / transaction::MAX_PACKETS_PER_EXEC_BATCH;
const PACING_NON_FILL_TIME: Duration = Duration::from_millis(50);

struct SchedulerStats {
    slots: BTreeMap<u64, SlotStats>,
}

#[derive(Default)]
struct SlotStats {
    tpu_popped: u64,
    check_sent: u64,
    check_received: u64,
    enqueued: u64,
    priority_evictions: u64,
    scheduled: u64,
    completed: u64,
    recorded: u64,
    cost_retries: u64,
    bank_retries: u64,
    scheduler_total_delta_ns: u64,
    scheduler_max_delta_ns: u64,
    scheduler_delta_count: u64,
}

impl SchedulerStats {
    fn new() -> Self {
        Self {
            slots: BTreeMap::new(),
        }
    }

    fn record_check_responses(&mut self, slot: u64, stats: check_response::CheckResponseStats) {
        let stats_for_slot = self.slot_mut(slot);
        stats_for_slot.check_received = stats_for_slot.check_received.wrapping_add(stats.received);
        stats_for_slot.enqueued = stats_for_slot.enqueued.wrapping_add(stats.enqueued);
        stats_for_slot.priority_evictions = stats_for_slot
            .priority_evictions
            .wrapping_add(stats.priority_evictions);
    }

    fn record_tpu_ingress(&mut self, slot: u64, stats: tpu_ingress::TpuIngressStats) {
        let stats_for_slot = self.slot_mut(slot);
        stats_for_slot.tpu_popped = stats_for_slot.tpu_popped.wrapping_add(stats.popped);
        stats_for_slot.check_sent = stats_for_slot.check_sent.wrapping_add(stats.check_sent);
    }

    fn record_scheduled_transactions(&mut self, slot: u64, scheduled_transactions: usize) {
        let stats_for_slot = self.slot_mut(slot);
        stats_for_slot.scheduled = stats_for_slot
            .scheduled
            .wrapping_add(scheduled_transactions as u64);
    }

    fn record_execution_responses(
        &mut self,
        slot: u64,
        stats: execution_response::ExecutionResponseStats,
    ) {
        let stats_for_slot = self.slot_mut(slot);
        stats_for_slot.completed = stats_for_slot
            .completed
            .wrapping_add(stats.completed_transactions);
        stats_for_slot.recorded = stats_for_slot
            .recorded
            .wrapping_add(stats.recorded_transactions);
        stats_for_slot.cost_retries = stats_for_slot
            .cost_retries
            .wrapping_add(stats.cost_limit_retries);
        stats_for_slot.bank_retries = stats_for_slot
            .bank_retries
            .wrapping_add(stats.slot_boundary_retries);
    }

    fn record_scheduler_delta(&mut self, slot: u64, delta: Duration) {
        let stats_for_slot = self.slot_mut(slot);
        let delta_ns = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        stats_for_slot.scheduler_total_delta_ns = stats_for_slot
            .scheduler_total_delta_ns
            .wrapping_add(delta_ns);
        stats_for_slot.scheduler_max_delta_ns = stats_for_slot.scheduler_max_delta_ns.max(delta_ns);
        stats_for_slot.scheduler_delta_count = stats_for_slot.scheduler_delta_count.wrapping_add(1);
    }

    fn report_completed_slots(
        &mut self,
        current_slot: u64,
        in_flight: &in_flight::InFlightTracker,
    ) {
        if !in_flight.is_empty() {
            return;
        }

        while self
            .slots
            .first_key_value()
            .is_some_and(|(slot, _)| *slot < current_slot)
        {
            let (slot, stats) = self
                .slots
                .pop_first()
                .expect("first key was present immediately before removal");
            info!(
                "scheduler_slot={slot} tpu_popped={} check_sent={} check_received={} enqueued={} \
                 priority_evictions={} scheduled={} completed={} recorded={} cost_retries={} \
                 bank_retries={} scheduler_avg_delta_ns={} scheduler_max_delta_ns={}",
                stats.tpu_popped,
                stats.check_sent,
                stats.check_received,
                stats.enqueued,
                stats.priority_evictions,
                stats.scheduled,
                stats.completed,
                stats.recorded,
                stats.cost_retries,
                stats.bank_retries,
                stats.scheduler_avg_delta_ns(),
                stats.scheduler_max_delta_ns,
            );
        }
    }

    fn slot_mut(&mut self, slot: u64) -> &mut SlotStats {
        self.slots.entry(slot).or_default()
    }
}

impl SlotStats {
    fn scheduler_avg_delta_ns(&self) -> u64 {
        if self.scheduler_delta_count == 0 {
            return 0;
        }

        #[allow(clippy::arithmetic_side_effects)]
        {
            self.scheduler_total_delta_ns / self.scheduler_delta_count
        }
    }
}

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
    }
}

struct Scheduler {
    session: ClientSession,
    state: SchedulerState,
    transaction_state: state_container::StateContainer,
    account_locks: ThreadAwareAccountLocks,
    in_flight: in_flight::InFlightTracker,
    scheduling_scratch: scheduling::SchedulingScratch,
    recheck_scratch: recheck::RecheckScratch,
    cost_pacer: Option<(u64, CostPacer)>,
    stats: SchedulerStats,
    previous_iteration_time: Instant,
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
            recheck_scratch: recheck::RecheckScratch::new(),
            cost_pacer: None,
            stats: SchedulerStats::new(),
            previous_iteration_time: Instant::now(),
        }
    }

    fn run_iteration(&mut self, now: Instant) {
        let iteration_time = now.saturating_duration_since(self.previous_iteration_time);
        self.previous_iteration_time = now;
        self.drain_progress(now);
        self.stats
            .record_scheduler_delta(self.state.current_slot(), iteration_time);
        self.drain_check_responses();
        self.drain_execution_responses();
        self.stats
            .report_completed_slots(self.state.current_slot(), &self.in_flight);
        self.schedule(now);
        self.recheck_transactions();
        self.ingest_tpu();
    }

    fn drain_progress(&mut self, now: Instant) {
        if !progress_tracker::drain_progress(&mut self.session.progress_tracker, &mut self.state) {
            return;
        }

        self.update_cost_pacer(now);
    }

    fn drain_check_responses(&mut self) {
        let check_sanitize_config = resolved_transaction::sanitize_config(
            self.state
                .feature_set()
                .snapshot()
                .limit_instruction_accounts,
        );
        let stats = check_response::drain_check_responses(
            &self.session.check_worker_to_pack,
            &self.session.allocators[0],
            &check_sanitize_config,
            self.state.reserved_account_keys(),
            &mut self.transaction_state,
            MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION,
        );
        self.stats
            .record_check_responses(self.state.current_slot(), stats);
    }

    fn drain_execution_responses(&mut self) {
        let fallback_slot = self
            .in_flight
            .scheduling_slot()
            .unwrap_or(self.state.current_slot());
        let stats = &mut self.stats;
        execution_response::drain_execution_responses(
            &mut self.session.workers,
            &self.session.allocators[0],
            &mut self.transaction_state,
            &mut self.account_locks,
            &mut self.in_flight,
            MAX_EXECUTION_RESPONSE_BATCHES_PER_ITERATION,
            |execution_slot, stats_for_response| {
                stats.record_execution_responses(
                    execution_slot.unwrap_or(fallback_slot),
                    stats_for_response,
                );
            },
        );
    }

    fn schedule(&mut self, now: Instant) {
        if self.state.can_process_transactions() {
            let current_slot = self.state.current_slot();
            let consumed_cost_units = self
                .state
                .initial_remaining_cost_units()
                .saturating_sub(self.state.remaining_cost_units());
            let pacing_budget = self.cost_pacer.as_ref().map_or(0, |(_, cost_pacer)| {
                cost_pacer.scheduling_budget(&now, consumed_cost_units)
            });
            let scheduled_transactions = scheduling::schedule(
                &mut self.session.workers,
                &self.session.allocators[0],
                &mut self.transaction_state,
                &mut self.account_locks,
                &mut self.in_flight,
                &mut self.scheduling_scratch,
                current_slot,
                pacing_budget,
                self.state.target_scheduled_cus(),
                MAX_SCHEDULED_TRANSACTIONS_PER_ITERATION,
            );
            self.stats
                .record_scheduled_transactions(current_slot, scheduled_transactions);
        }
    }

    fn update_cost_pacer(&mut self, now: Instant) {
        if !self.state.can_process_transactions() {
            self.cost_pacer = None;
            return;
        }

        let current_slot = self.state.current_slot();
        let needs_new_cost_pacer = match self.cost_pacer.as_ref() {
            Some((slot, _)) => *slot != current_slot,
            None => true,
        };
        if needs_new_cost_pacer {
            let fill_time = (self.state.target_bank_time_ms() != 0).then(|| {
                Duration::from_millis(u64::from(self.state.target_bank_time_ms()))
                    .saturating_sub(PACING_NON_FILL_TIME)
            });
            let detection_time = now
                .checked_sub(self.state.initial_bank_elapsed_time())
                .unwrap_or(now);
            self.cost_pacer = Some((
                current_slot,
                CostPacer::new(
                    self.state.initial_remaining_cost_units(),
                    detection_time,
                    fill_time,
                ),
            ));
        }
    }

    fn ingest_tpu(&mut self) {
        let minimum_priority = self.transaction_state.check_worker_minimum_priority();
        let stats = tpu_ingress::drain_tpu(
            &mut self.session.tpu_to_pack,
            &self.session.pack_to_check_worker,
            &self.session.allocators[0],
            &self.state,
            MAX_TPU_PACKETS_PER_ITERATION,
            minimum_priority,
        );
        self.stats
            .record_tpu_ingress(self.state.current_slot(), stats);
    }

    fn recheck_transactions(&mut self) {
        if self.state.is_leader() {
            return;
        }
        recheck::send_rechecks(
            &self.session.pack_to_check_worker,
            &self.session.allocators[0],
            &mut self.transaction_state,
            &mut self.recheck_scratch,
            recheck::MAX_RECHECK_PACKETS_PER_ITERATION,
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
        agave_scheduler_bindings::{
            CheckWorkerToPackMessage, ExecutionWorkerToPackMessage, LEADER_READY, ProgressMessage,
            SharablePubkeys, TpuToPackMessage, processed_codes,
            worker_message_types::{
                CheckResponse, ExecutionResponse, fee_payer_balance_flags, resolve_flags,
                scheduling_details_flags, status_check_flags,
            },
        },
        agave_scheduling_utils::{
            handshake::{client, server::Server},
            responses_region::{execution_responses_from_iter, resolve_responses_from_iter},
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
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

    fn leader_ready_progress() -> ProgressMessage {
        ProgressMessage {
            leader_state: LEADER_READY,
            current_slot_progress: 0,
            epoch: 0,
            current_slot: 1,
            next_leader_slot: 10,
            leader_range_end: 13,
            remaining_cost_units: 48_000_000,
            latest_blockhash: [0; 32],
            scheduler_features: 0,
            target_bank_time_ms: 0,
        }
    }

    fn valid_check_response() -> CheckResponse {
        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: status_check_flags::REQUESTED | status_check_flags::PERFORMED,
            fee_payer_balance_flags: fee_payer_balance_flags::REQUESTED
                | fee_payer_balance_flags::PERFORMED,
            resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            scheduling_details_flags: scheduling_details_flags::REQUESTED
                | scheduling_details_flags::PERFORMED,
            included_slot: 0,
            transaction_fee: 0,
            prioritization_fee: 0,
            estimated_cost_units: 0,
            balance_slot: 0,
            fee_payer_balance: 0,
            resolution_slot: 0,
            min_alt_deactivation_slot: u64::MAX,
            resolved_pubkeys: SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            },
        }
    }

    fn transaction_bytes() -> Vec<u8> {
        let payer = Keypair::new();
        let message = Message::new(
            &[system_instruction::transfer(
                &payer.pubkey(),
                &Pubkey::new_from_array([1; 32]),
                1,
            )],
            Some(&payer.pubkey()),
        );
        wincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&payer],
            message,
            Hash::default(),
        )))
        .unwrap()
    }

    #[test]
    fn runs_a_transaction_from_tpu_through_execution() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.execution_worker_count = 1;
        config.check_worker_count = 1;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &agave_session.tpu_to_pack.allocator;
        let mut scheduler = Scheduler::new(config, client_session);

        agave_session
            .progress_tracker
            .try_write(leader_ready_progress())
            .unwrap();
        agave_session.progress_tracker.commit();

        let bytes = transaction_bytes();
        let transaction = allocator.allocate(bytes.len().try_into().unwrap()).unwrap();
        // SAFETY: `transaction` is a fresh allocation with room for `bytes`.
        unsafe {
            transaction.copy_from_nonoverlapping(
                core::ptr::NonNull::new(bytes.as_ptr().cast_mut()).unwrap(),
                bytes.len(),
            );
        }
        // SAFETY: `transaction` was allocated by this allocator immediately above.
        let offset = unsafe { allocator.offset(transaction) };
        agave_session
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction: agave_scheduler_bindings::SharableTransactionRegion {
                    offset,
                    length: bytes.len().try_into().unwrap(),
                },
                flags: 0,
                src_addr: [0; 16],
            })
            .unwrap();
        agave_session.tpu_to_pack.producer.commit();

        scheduler.run_iteration(Instant::now());
        let check = agave_session.check_workers[0]
            .pack_to_check_worker
            .try_read()
            .unwrap();
        let responses =
            resolve_responses_from_iter(allocator, std::iter::once(valid_check_response()))
                .unwrap();
        agave_session.check_workers[0]
            .check_worker_to_pack
            .try_write(CheckWorkerToPackMessage {
                batch: check.batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();

        scheduler.run_iteration(Instant::now());
        assert_eq!(scheduler.transaction_state.buffer_len(), 1);
        assert_eq!(scheduler.transaction_state.len(), 0);
        let batch = {
            let worker = &mut agave_session.workers[0];
            worker.pack_to_worker.sync();
            let batch = worker.pack_to_worker.try_read().unwrap().batch;
            worker.pack_to_worker.finalize();
            batch
        };
        let responses = execution_responses_from_iter(
            allocator,
            std::iter::once(ExecutionResponse {
                execution_slot: 1,
                not_included_reason: 0,
                cost_units: 0,
                fee_payer_balance: 0,
            }),
        )
        .unwrap();
        agave_session.workers[0]
            .worker_to_pack
            .try_write(ExecutionWorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
        agave_session.workers[0].worker_to_pack.commit();

        scheduler.run_iteration(Instant::now());
        assert_eq!(scheduler.transaction_state.buffer_len(), 0);
    }
}
