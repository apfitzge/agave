use {
    crate::{LoadTestHarness, TransactionInjector},
    agave_banking_stage_ingress_types::SchedulerPriorityFloor,
    agave_scheduler_bindings::{SharableTransactionRegion, TpuToPackMessage, tpu_message_flags},
    agave_scheduling_utils::handshake::{AgaveSession, AgaveTpuToPackSession},
    agave_votor_messages::migration::MigrationStatus,
    crossbeam_channel::{Receiver, bounded, never},
    solana_clock::DEFAULT_HASHES_PER_TICK,
    solana_core::{
        banking_stage::{
            BankingStage, BankingStageHandle,
            check_worker::ExternalCheckWorker,
            committer::Committer,
            consume_worker::{ConsumeWorkerMetrics, external::ExternalWorker},
            consumer::Consumer,
            progress_tracker,
            transaction_scheduler::scheduler_controller::SchedulerConfig as GreedySchedulerConfig,
        },
        banking_trace::{BankingPacketSender, BankingTracer},
        validator::BlockProductionMethod,
    },
    solana_epoch_schedule::EpochSchedule,
    solana_fee_calculator::FeeRateGovernor,
    solana_leader_schedule::SlotLeader,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_perf::packet::{BytesPacket, Meta, PACKET_DATA_SIZE, PacketBatch, bytes::Bytes},
    solana_poh::{
        poh_controller::PohController,
        poh_recorder::{PohRecorder, WorkingBankEntryOrMarker},
        poh_service::{DEFAULT_HASHES_PER_BATCH, DEFAULT_PINNED_CPU_CORE, PohService},
        record_channels::record_channels,
        transaction_recorder::TransactionRecorder,
    },
    solana_poh_config::PohConfig,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{bootstrap_validator_stake_lamports, create_genesis_config_with_leader},
        vote_sender_types::ReplayVoteReceiver,
    },
    solana_signer::Signer,
    std::{
        num::NonZeroUsize,
        ptr,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
    tempfile::TempDir,
    thiserror::Error,
};

const DEFAULT_SLOT_DURATION: Duration = Duration::from_millis(400);
const ROTATION_RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);
const LAMPORTS_PER_SIGNATURE: u64 = 5_000;

/// Configuration for a [`Harness`].
#[derive(Debug, Clone, Copy)]
pub struct HarnessConfig {
    /// The target duration of each leader slot.
    pub slot_duration: Duration,
    /// The normal PoH producer's hash count per tick.
    pub hashes_per_tick: u64,
    /// Whether every bank's cost tracker limits should be set to `u64::MAX`.
    pub unlimited_cost_limits: bool,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            slot_duration: DEFAULT_SLOT_DURATION,
            hashes_per_tick: DEFAULT_HASHES_PER_TICK,
            unlimited_cost_limits: false,
        }
    }
}

/// Errors returned while starting the PoH-backed load-test harness.
#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("slot duration must be greater than zero")]
    ZeroSlotDuration,
    #[error("slot duration is too short for the configured ticks per slot")]
    SlotDurationTooShort,
    #[error("slot duration is outside the supported range")]
    SlotDurationOutOfRange,
    #[error("failed to create temporary ledger: {0}")]
    TemporaryLedger(#[source] std::io::Error),
    #[error("failed to open temporary blockstore: {0}")]
    Blockstore(#[source] solana_ledger::blockstore::BlockstoreError),
    #[error("PoH service disconnected while installing a bank")]
    PohServiceDisconnected,
}

/// Errors returned by [`TpuInjector`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TpuInjectorError {
    #[error("transaction is too large for the shared-memory allocator")]
    TransactionTooLarge,
    #[error("shared-memory allocator is full")]
    AllocatorFull,
    #[error("TPU-to-scheduler queue is full")]
    QueueFull,
}

/// Direct producer for the external scheduler's TPU queue.
///
/// Call [`Self::sync`] before filling a batch, [`Self::try_push`] for each transaction, then
/// [`Self::commit`] to make the batch visible to the scheduler.
pub struct TpuInjector {
    allocator: rts_alloc::Allocator,
    producer: shaq::spsc::Producer<TpuToPackMessage>,
}

impl TpuInjector {
    /// Synchronize queue offsets and reclaim remote frees before preparing a batch.
    pub fn sync(&mut self) {
        self.allocator.clean_remote_frees();
        self.producer.sync();
    }

    /// Copy a serialized transaction into shared memory and enqueue it for the scheduler.
    ///
    /// The caller must call [`Self::commit`] after one or more successful pushes.
    pub fn try_push(
        &mut self,
        transaction: &[u8],
        flags: u8,
        src_addr: [u8; 16],
    ) -> Result<(), TpuInjectorError> {
        let length =
            u32::try_from(transaction.len()).map_err(|_| TpuInjectorError::TransactionTooLarge)?;
        let allocation = self
            .allocator
            .allocate(length)
            .ok_or(TpuInjectorError::AllocatorFull)?;

        // SAFETY: `allocation` is a fresh allocation of exactly `transaction.len()` bytes and
        // the source slice is readable for that same length. The ranges do not overlap.
        unsafe {
            ptr::copy_nonoverlapping(transaction.as_ptr(), allocation.as_ptr(), transaction.len());
        }
        // SAFETY: `allocation` came from this allocator immediately above.
        let offset = unsafe { self.allocator.offset(allocation) };
        let message = TpuToPackMessage {
            transaction: SharableTransactionRegion { offset, length },
            flags,
            src_addr,
        };

        if self.producer.try_write(message).is_err() {
            // SAFETY: this allocation has not been published, so this injector still owns it.
            unsafe { self.allocator.free(allocation) };
            return Err(TpuInjectorError::QueueFull);
        }

        Ok(())
    }

    /// Publish all transactions queued since the preceding [`Self::sync`].
    pub fn commit(&mut self) {
        self.producer.commit();
    }
}

impl TransactionInjector for TpuInjector {
    type Error = TpuInjectorError;

    fn sync(&mut self) {
        Self::sync(self);
    }

    fn try_push_transaction(&mut self, transaction: &[u8]) -> Result<bool, Self::Error> {
        match Self::try_push(self, transaction, tpu_message_flags::NONE, [0; 16]) {
            Ok(()) => Ok(true),
            Err(Self::Error::AllocatorFull | Self::Error::QueueFull) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn commit(&mut self) -> Result<(), Self::Error> {
        Self::commit(self);
        Ok(())
    }
}

struct PohServices {
    exit: Arc<AtomicBool>,
    bank_forks: Arc<RwLock<BankForks>>,
    rotation_thread: Option<JoinHandle<()>>,
    entry_drainer: Option<JoinHandle<()>>,
    poh_service: Option<PohService>,
    _ledger: TempDir,
}

struct SchedulerSetup {
    transaction_recorder: TransactionRecorder,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    clear_bank_receiver: Receiver<bool>,
    poh_controller: PohController,
    leader: SlotLeader,
    ticks_per_slot: u64,
    unlimited_cost_limits: bool,
}

impl PohServices {
    fn start(
        config: HarnessConfig,
        exit: Arc<AtomicBool>,
        setup: impl FnOnce(&Arc<Bank>),
    ) -> Result<(Self, SchedulerSetup), HarnessError> {
        if config.slot_duration.is_zero() {
            return Err(HarnessError::ZeroSlotDuration);
        }

        let validator_pubkey = Pubkey::new_unique();
        let mut genesis_config_info = create_genesis_config_with_leader(
            1_000_000_000,
            &validator_pubkey,
            bootstrap_validator_stake_lamports(),
        );
        genesis_config_info.genesis_config.fee_rate_governor =
            FeeRateGovernor::new(LAMPORTS_PER_SIGNATURE, 0);
        genesis_config_info.genesis_config.epoch_schedule = EpochSchedule::without_warmup();
        let ticks_per_slot = genesis_config_info.genesis_config.ticks_per_slot;
        let target_tick_duration = target_tick_duration(config.slot_duration, ticks_per_slot)?;
        genesis_config_info.genesis_config.poh_config = PohConfig {
            hashes_per_tick: Some(config.hashes_per_tick),
            target_tick_duration,
            target_tick_count: None,
        };

        let leader = SlotLeader {
            id: genesis_config_info.validator_pubkey,
            vote_address: genesis_config_info.voting_keypair.pubkey(),
        };
        let (root_bank, bank_forks) =
            Bank::new_with_bank_forks_for_tests(&genesis_config_info.genesis_config);
        let initial_bank = {
            let child = Bank::new_from_parent(root_bank.clone(), leader, 1);
            bank_forks.write().unwrap().insert(child)
        };
        let initial_bank = initial_bank.clone_without_scheduler();
        if config.unlimited_cost_limits {
            initial_bank.write_cost_tracker().unwrap().set_limits_max();
        }
        setup(&initial_bank);

        let ledger = TempDir::new().map_err(HarnessError::TemporaryLedger)?;
        let blockstore =
            Arc::new(Blockstore::open(ledger.path()).map_err(HarnessError::Blockstore)?);
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&root_bank));
        let (clear_bank_sender, clear_bank_receiver) = bounded(1);
        let (poh_recorder, entry_receiver) = PohRecorder::new_with_clear_signal(
            root_bank.tick_height(),
            root_bank.last_blockhash(),
            root_bank,
            Some((1, 1)),
            ticks_per_slot,
            false,
            blockstore,
            Some(clear_bank_sender),
            &leader_schedule_cache,
            &genesis_config_info.genesis_config.poh_config,
            exit.clone(),
        );
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let (record_sender, record_receiver) = record_channels(false);
        let transaction_recorder = TransactionRecorder::new(record_sender);
        let (mut poh_controller, poh_service_receiver) = PohController::new();
        let (record_receiver_sender, _record_receiver_receiver) = bounded(1);
        let poh_service = PohService::new(
            poh_recorder.clone(),
            &genesis_config_info.genesis_config.poh_config,
            exit.clone(),
            ticks_per_slot,
            DEFAULT_PINNED_CPU_CORE,
            DEFAULT_HASHES_PER_BATCH,
            record_receiver,
            poh_service_receiver,
            Arc::new(MigrationStatus::default()),
            record_receiver_sender,
        );
        poh_controller
            .set_bank_sync(
                bank_forks
                    .read()
                    .unwrap()
                    .get_with_scheduler(initial_bank.slot())
                    .expect("initial bank was inserted")
                    .clone_with_scheduler(),
            )
            .map_err(|_| HarnessError::PohServiceDisconnected)?;

        let entry_drainer = spawn_entry_drainer(exit.clone(), entry_receiver);
        Ok((
            Self {
                exit,
                bank_forks,
                rotation_thread: None,
                entry_drainer: Some(entry_drainer),
                poh_service: Some(poh_service),
                _ledger: ledger,
            },
            SchedulerSetup {
                transaction_recorder,
                poh_recorder,
                clear_bank_receiver,
                poh_controller,
                leader,
                ticks_per_slot,
                unlimited_cost_limits: config.unlimited_cost_limits,
            },
        ))
    }

    fn start_rotation(
        &mut self,
        clear_bank_receiver: Receiver<bool>,
        poh_controller: PohController,
        leader: SlotLeader,
        unlimited_cost_limits: bool,
    ) {
        self.rotation_thread = Some(spawn_bank_rotation(
            self.exit.clone(),
            clear_bank_receiver,
            self.bank_forks.clone(),
            poh_controller,
            leader,
            unlimited_cost_limits,
        ));
    }

    fn stop_rotation(&mut self) {
        if let Some(thread) = self.rotation_thread.take() {
            thread.join().expect("bank rotation thread must not panic");
        }
    }

    fn stop_poh(&mut self) {
        if let Some(poh_service) = self.poh_service.take() {
            poh_service
                .join()
                .expect("PoH service thread must not panic");
        }
        if let Some(thread) = self.entry_drainer.take() {
            thread
                .join()
                .expect("PoH entry drainer thread must not panic");
        }
    }
}

/// A production-worker, PoH-backed environment for an external scheduler.
pub struct Harness {
    services: PohServices,
    injector: TpuInjector,
    worker_threads: Vec<JoinHandle<()>>,
    progress_thread: Option<JoinHandle<()>>,
    _replay_vote_receiver: ReplayVoteReceiver,
}

impl Harness {
    /// Start the real check/execution workers and PoH services for `session`.
    ///
    /// `setup` runs against the first leader bank before the workers or PoH service begin
    /// processing, so it can install arbitrary account state without affecting measurements.
    pub fn start(
        session: AgaveSession,
        config: HarnessConfig,
        exit: Arc<AtomicBool>,
        setup: impl FnOnce(&Arc<Bank>),
    ) -> Result<Self, HarnessError> {
        let (mut services, scheduler_setup) = PohServices::start(config, exit, setup)?;
        let SchedulerSetup {
            transaction_recorder,
            poh_recorder,
            ticks_per_slot,
            ..
        } = &scheduler_setup;
        let (shared_leader_state, sharable_banks) = {
            let poh_recorder = poh_recorder.read().unwrap();
            (
                poh_recorder.shared_leader_state(),
                services.bank_forks.read().unwrap().sharable_banks(),
            )
        };
        let AgaveSession {
            flags: _,
            tpu_to_pack,
            progress_tracker: progress_producer,
            check_workers,
            workers,
        } = session;
        let (worker_threads, worker_metrics, replay_vote_receiver) = spawn_workers(
            services.exit.clone(),
            check_workers,
            workers,
            transaction_recorder.clone(),
            shared_leader_state.clone(),
            sharable_banks.clone(),
        );
        let progress_thread = progress_tracker::spawn(
            services.exit.clone(),
            progress_producer,
            shared_leader_state,
            sharable_banks,
            worker_metrics,
            *ticks_per_slot,
        );
        let SchedulerSetup {
            clear_bank_receiver,
            poh_controller,
            leader,
            unlimited_cost_limits,
            ..
        } = scheduler_setup;
        services.start_rotation(
            clear_bank_receiver,
            poh_controller,
            leader,
            unlimited_cost_limits,
        );

        Ok(Self {
            services,
            injector: TpuInjector::from(tpu_to_pack),
            worker_threads,
            progress_thread: Some(progress_thread),
            _replay_vote_receiver: replay_vote_receiver,
        })
    }

    /// Return a producer for direct, post-sigverify TPU injection.
    pub fn injector(&mut self) -> &mut TpuInjector {
        &mut self.injector
    }

    /// Return the current working bank.
    pub fn working_bank(&self) -> Arc<Bank> {
        self.services.bank_forks.read().unwrap().working_bank()
    }

    /// Return the shared exit signal used by every service in this harness.
    pub fn exit_signal(&self) -> Arc<AtomicBool> {
        self.services.exit.clone()
    }

    /// Shut down all worker, progress, and PoH threads.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.services.exit.store(true, Ordering::Relaxed);
        self.services.stop_rotation();
        for thread in self.worker_threads.drain(..) {
            thread.join().expect("worker thread must not panic");
        }
        if let Some(thread) = self.progress_thread.take() {
            thread
                .join()
                .expect("progress tracker thread must not panic");
        }
        self.services.stop_poh();
    }
}

impl LoadTestHarness for Harness {
    type Injector = TpuInjector;

    fn injector(&mut self) -> &mut Self::Injector {
        Self::injector(self)
    }

    fn working_bank(&self) -> Arc<Bank> {
        Self::working_bank(self)
    }

    fn exit_signal(&self) -> Arc<AtomicBool> {
        Self::exit_signal(self)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Errors returned by [`GreedyTpuInjector`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum GreedyTpuInjectorError {
    #[error("transaction exceeds the maximum packet size")]
    TransactionTooLarge,
    #[error("greedy scheduler ingress is disconnected")]
    Disconnected,
}

/// Direct producer for the in-process greedy scheduler's post-sigverify ingress channel.
pub struct GreedyTpuInjector {
    sender: BankingPacketSender,
}

impl GreedyTpuInjector {
    fn new(sender: BankingPacketSender) -> Self {
        Self { sender }
    }
}

impl TransactionInjector for GreedyTpuInjector {
    type Error = GreedyTpuInjectorError;

    fn sync(&mut self) {}

    fn try_push_transaction(&mut self, transaction: &[u8]) -> Result<bool, Self::Error> {
        if transaction.len() > PACKET_DATA_SIZE {
            return Err(GreedyTpuInjectorError::TransactionTooLarge);
        }

        let mut meta = Meta::default();
        meta.size = transaction.len();
        self.sender
            .send(Arc::new(vec![PacketBatch::Single(BytesPacket::new(
                Bytes::copy_from_slice(transaction),
                meta,
            ))]))
            .map(|_| ())
            .map_err(|_| GreedyTpuInjectorError::Disconnected)?;
        Ok(true)
    }

    fn commit(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A PoH-backed environment using BankingStage's in-process greedy scheduler.
pub struct GreedyHarness {
    services: PohServices,
    injector: GreedyTpuInjector,
    banking_stage: Option<BankingStageHandle>,
    _replay_vote_receiver: ReplayVoteReceiver,
}

impl GreedyHarness {
    /// Start BankingStage's greedy scheduler with real consume workers and post-sigverify ingress.
    pub fn start(
        config: HarnessConfig,
        execution_worker_count: NonZeroUsize,
        exit: Arc<AtomicBool>,
        setup: impl FnOnce(&Arc<Bank>),
    ) -> Result<Self, HarnessError> {
        let (mut services, scheduler_setup) = PohServices::start(config, exit, setup)?;
        let SchedulerSetup {
            transaction_recorder,
            poh_recorder,
            clear_bank_receiver,
            poh_controller,
            leader,
            unlimited_cost_limits,
            ..
        } = scheduler_setup;
        let (non_vote_sender, non_vote_receiver) =
            BankingTracer::new_disabled().create_channel_non_vote();
        let (replay_vote_sender, replay_vote_receiver) = bounded(1024);
        let banking_stage = BankingStage::new_num_threads(
            BlockProductionMethod::CentralSchedulerGreedy,
            poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            never(),
            never(),
            tokio::sync::mpsc::channel(1).1,
            execution_worker_count,
            GreedySchedulerConfig::default(),
            None,
            replay_vote_sender,
            None,
            services.bank_forks.clone(),
            None,
            Arc::default(),
            Arc::new(SchedulerPriorityFloor::new()),
        );
        services.start_rotation(
            clear_bank_receiver,
            poh_controller,
            leader,
            unlimited_cost_limits,
        );

        Ok(Self {
            services,
            injector: GreedyTpuInjector::new(non_vote_sender),
            banking_stage: Some(banking_stage),
            _replay_vote_receiver: replay_vote_receiver,
        })
    }

    /// Shut down BankingStage and the PoH services.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.services.exit.store(true, Ordering::Relaxed);
        self.services.stop_rotation();
        if let Some(banking_stage) = self.banking_stage.take() {
            banking_stage
                .join()
                .expect("banking stage thread must not panic");
        }
        self.services.stop_poh();
    }
}

impl LoadTestHarness for GreedyHarness {
    type Injector = GreedyTpuInjector;

    fn injector(&mut self) -> &mut Self::Injector {
        &mut self.injector
    }

    fn working_bank(&self) -> Arc<Bank> {
        self.services.bank_forks.read().unwrap().working_bank()
    }

    fn exit_signal(&self) -> Arc<AtomicBool> {
        self.services.exit.clone()
    }
}

impl Drop for GreedyHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

impl From<AgaveTpuToPackSession> for TpuInjector {
    fn from(
        AgaveTpuToPackSession {
            allocator,
            producer,
        }: AgaveTpuToPackSession,
    ) -> Self {
        Self {
            allocator,
            producer,
        }
    }
}

fn target_tick_duration(
    slot_duration: Duration,
    ticks_per_slot: u64,
) -> Result<Duration, HarnessError> {
    let ticks_per_slot = u128::from(ticks_per_slot);
    #[allow(clippy::arithmetic_side_effects)]
    let tick_nanos = slot_duration.as_nanos().wrapping_div(ticks_per_slot);
    let tick_nanos = u64::try_from(tick_nanos).map_err(|_| HarnessError::SlotDurationOutOfRange)?;
    (tick_nanos != 0)
        .then(|| Duration::from_nanos(tick_nanos))
        .ok_or(HarnessError::SlotDurationTooShort)
}

fn spawn_entry_drainer(
    exit: Arc<AtomicBool>,
    entry_receiver: Receiver<WorkingBankEntryOrMarker>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("solLoadPohDrain".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                let _ = entry_receiver.recv_timeout(ROTATION_RECEIVE_TIMEOUT);
            }
        })
        .expect("PoH entry drainer thread must start")
}

fn spawn_workers(
    exit: Arc<AtomicBool>,
    check_workers: Vec<agave_scheduling_utils::handshake::AgaveCheckWorkerSession>,
    workers: Vec<agave_scheduling_utils::handshake::AgaveWorkerSession>,
    transaction_recorder: TransactionRecorder,
    shared_leader_state: solana_poh::poh_recorder::SharedLeaderState,
    sharable_banks: solana_runtime::bank_forks::SharableBanks,
) -> (
    Vec<JoinHandle<()>>,
    Vec<Arc<ConsumeWorkerMetrics>>,
    ReplayVoteReceiver,
) {
    let (replay_vote_sender, replay_vote_receiver) = bounded(1024);
    let mut threads = Vec::with_capacity(check_workers.len().saturating_add(workers.len()));
    let mut metrics = Vec::with_capacity(workers.len());

    for (index, worker_session) in workers.into_iter().enumerate() {
        let worker = ExternalWorker::new(
            index as u32,
            exit.clone(),
            Consumer::new(
                Committer::new(None, replay_vote_sender.clone(), None),
                transaction_recorder.clone(),
                None,
            ),
            worker_session.worker_to_pack,
            worker_session.allocator,
            shared_leader_state.clone(),
        );
        metrics.push(worker.metrics_handle());
        threads.push(
            thread::Builder::new()
                .name(format!("solLoadExec{index:02}"))
                .spawn(move || {
                    let _ = worker.run(worker_session.pack_to_worker);
                })
                .expect("external execution worker thread must start"),
        );
    }

    for (index, check_worker_session) in check_workers.into_iter().enumerate() {
        let worker = ExternalCheckWorker::new(
            exit.clone(),
            check_worker_session.pack_to_check_worker,
            check_worker_session.check_worker_to_pack,
            check_worker_session.allocator,
            shared_leader_state.clone(),
            sharable_banks.clone(),
        );
        threads.push(
            thread::Builder::new()
                .name(format!("solLoadCheck{index:02}"))
                .spawn(move || {
                    let _ = worker.run();
                })
                .expect("external check worker thread must start"),
        );
    }

    (threads, metrics, replay_vote_receiver)
}

fn spawn_bank_rotation(
    exit: Arc<AtomicBool>,
    clear_bank_receiver: Receiver<bool>,
    bank_forks: Arc<RwLock<BankForks>>,
    mut poh_controller: PohController,
    leader: SlotLeader,
    unlimited_cost_limits: bool,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("solLoadBankRot".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                if clear_bank_receiver
                    .recv_timeout(ROTATION_RECEIVE_TIMEOUT)
                    .is_err()
                {
                    continue;
                }
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                let parent = bank_forks.read().unwrap().working_bank();
                parent.freeze();
                report_cost_tracker_stats(&parent);
                let child =
                    Bank::new_from_parent(parent.clone(), leader, parent.slot().saturating_add(1));
                if unlimited_cost_limits {
                    child.write_cost_tracker().unwrap().set_limits_max();
                }
                let child = {
                    let mut bank_forks = bank_forks.write().unwrap();
                    let child = bank_forks.insert(child);
                    let _ = bank_forks.set_root(parent.slot(), None, None);
                    child
                };
                if poh_controller.set_bank_sync(child).is_err() {
                    break;
                }
            }
        })
        .expect("bank rotation thread must start")
}

fn report_cost_tracker_stats(bank: &Bank) {
    let (total_transaction_fee, total_priority_fee) = {
        let collector_fee_details = bank.get_collector_fee_details();
        (
            collector_fee_details.total_transaction_fee(),
            collector_fee_details.total_priority_fee(),
        )
    };
    bank.read_cost_tracker().unwrap().report_stats(
        bank.slot(),
        true,
        total_transaction_fee,
        total_priority_fee,
    );
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_scheduler_bindings::tpu_message_flags,
        agave_scheduling_utils::handshake::{ClientLogon, client, server::Server},
    };

    fn test_logon() -> ClientLogon {
        ClientLogon {
            worker_count: 1,
            check_worker_count: 1,
            allocator_size: 64 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_capacity: 16,
            progress_tracker_capacity: 16,
            pack_to_worker_capacity: 16,
            worker_to_pack_capacity: 16,
            flags: 0,
            pack_to_check_worker_capacity: 16,
            check_worker_to_pack_capacity: 16,
        }
    }

    #[test]
    fn injector_transfers_transaction_ownership() {
        let logon = test_logon();
        let (agave_session, files) = Server::setup_session(logon).unwrap();
        let mut scheduler_session = client::setup_session(&logon, files).unwrap();
        let mut harness = Harness::start(
            agave_session,
            HarnessConfig::default(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap();

        let transaction = [1, 2, 3, 4];
        let injector = harness.injector();
        injector.sync();
        injector
            .try_push(&transaction, tpu_message_flags::NONE, [0; 16])
            .unwrap();
        injector.commit();

        scheduler_session.tpu_to_pack.sync();
        let message = *scheduler_session.tpu_to_pack.try_read().unwrap();
        let bytes = unsafe {
            // SAFETY: the harness allocated this region in the shared allocator and queue
            // ownership transferred it to this test's scheduler session.
            std::slice::from_raw_parts(
                scheduler_session.allocators[0]
                    .ptr_from_offset(message.transaction.offset)
                    .as_ptr(),
                message.transaction.length as usize,
            )
        };
        assert_eq!(bytes, transaction);
        // SAFETY: this test consumed the queued transaction and now owns the allocation.
        unsafe { scheduler_session.allocators[0].free_offset(message.transaction.offset) };
        scheduler_session.tpu_to_pack.finalize();
    }

    #[test]
    fn greedy_injector_enqueues_post_sigverify_packets() {
        let (sender, receiver) = BankingTracer::new_disabled().create_channel_non_vote();
        let mut injector = GreedyTpuInjector::new(sender);
        let transaction = [1, 2, 3, 4];

        injector.sync();
        assert!(injector.try_push_transaction(&transaction).unwrap());
        injector.commit().unwrap();

        let batch = receiver.try_recv().unwrap();
        assert_eq!(batch.len(), 1);
        assert!(matches!(batch[0], PacketBatch::Single(_)));
        let packet = batch[0].get(0).unwrap();
        assert_eq!(packet.data(..).unwrap(), transaction);
        assert_eq!(packet.meta().size, transaction.len());
    }

    #[test]
    fn greedy_harness_starts_and_stops() {
        let harness = GreedyHarness::start(
            HarnessConfig::default(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap();

        harness.shutdown();
    }

    #[test]
    fn poh_rotates_to_the_next_bank() {
        let logon = test_logon();
        let (agave_session, _files) = Server::setup_session(logon).unwrap();
        let harness = Harness::start(
            agave_session,
            HarnessConfig {
                slot_duration: Duration::from_millis(20),
                hashes_per_tick: 2,
                unlimited_cost_limits: false,
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while harness.working_bank().slot() == 1 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(harness.working_bank().slot() > 1);
    }
}
