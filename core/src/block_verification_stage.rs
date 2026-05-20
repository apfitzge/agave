pub use agave_block_verification_stage::session::{
    BlockVerificationSession, BlockVerificationSlotStatus, ReplayBlockVerification,
};
use {
    crate::{
        banking_stage::spawn_replay_block_verification_workers,
        block_verification_sigverify::spawn_replay_signature_verification_workers,
    },
    agave_block_verification_stage::{
        scheduler::BlockVerificationScheduler,
        setup::{
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig,
            ReplayEventBroadcast, SIGNATURE_VERIFICATION_WORKER_COUNT,
        },
    },
    log::warn,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime::{
        bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    std::{
        num::NonZeroUsize,
        path::Path,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
    },
};

const ALLOCATOR_SIZE: usize = 4 * 1024 * 1024 * 1024;
const REPLAY_TO_PACK_CAPACITY: usize = 16 * 1024;
const REPLAY_BLOCK_STATUS_CAPACITY: usize = 1024;
const PACK_TO_WORKER_CAPACITY: usize = 1024;
const WORKER_TO_PACK_CAPACITY: usize = 1024;

pub struct BlockVerificationStage {
    threads: Vec<JoinHandle<()>>,
}

pub struct BlockVerificationRuntime {
    stage: Option<BlockVerificationStage>,
    block_verification: ReplayBlockVerification,
}

pub struct BlockVerificationStageConfig<'a> {
    pub worker_count: NonZeroUsize,
    pub entry_verification_threads: NonZeroUsize,
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub replay_vote_sender: ReplayVoteSender,
    pub prioritization_fee_cache: Option<Arc<PrioritizationFeeCache>>,
    pub log_messages_bytes_limit: Option<usize>,
    pub shared_leader_state: SharedLeaderState,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub event_ledger_path: Option<&'a Path>,
}

impl BlockVerificationRuntime {
    pub fn new(
        exit: Arc<AtomicBool>,
        config: BlockVerificationStageConfig<'_>,
    ) -> Result<Self, String> {
        let (stage, session) = BlockVerificationStage::new(exit.clone(), config)?;
        let block_verification = ReplayBlockVerification::new(session, exit);

        Ok(Self {
            stage: Some(stage),
            block_verification,
        })
    }

    pub fn block_verification(&mut self) -> &mut ReplayBlockVerification {
        &mut self.block_verification
    }

    pub fn exit(&self) -> &AtomicBool {
        self.block_verification.exit()
    }

    pub fn shutdown(&mut self) {
        self.exit().store(true, Ordering::Relaxed);
        if let Some(stage) = self.stage.take() {
            if let Err(err) = stage.join() {
                warn!("block verification stage join failed: {err:?}");
            }
        }
    }
}

impl Drop for BlockVerificationRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl BlockVerificationStage {
    pub fn new(
        exit: Arc<AtomicBool>,
        config: BlockVerificationStageConfig<'_>,
    ) -> Result<(Self, BlockVerificationSession), String> {
        let BlockVerificationStageConfig {
            worker_count,
            entry_verification_threads,
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
            log_messages_bytes_limit,
            shared_leader_state,
            bank_forks,
            event_ledger_path,
        } = config;
        let event_broadcast = event_ledger_path
            .map(ReplayEventBroadcast::new)
            .transpose()
            .map_err(|err| format!("failed to set up replay event broadcast: {err}"))?
            .map(Arc::new);
        let sessions = BlockVerificationStageSessions::setup(BlockVerificationStageSetupConfig {
            allocator_size: ALLOCATOR_SIZE,
            replay_to_pack_capacity: REPLAY_TO_PACK_CAPACITY,
            replay_block_status_capacity: REPLAY_BLOCK_STATUS_CAPACITY,
            worker_count: worker_count.get(),
            pack_to_worker_capacity: PACK_TO_WORKER_CAPACITY,
            worker_to_pack_capacity: WORKER_TO_PACK_CAPACITY,
        })
        .map_err(|err| format!("failed to set up block verification stage: {err}"))?;

        let BlockVerificationStageSessions {
            block_verification_stage,
            replay_stage,
            workers,
            signature_verification_workers,
        } = sessions;

        let mut threads =
            Vec::with_capacity(worker_count.get() + SIGNATURE_VERIFICATION_WORKER_COUNT + 1);
        let scheduler_exit = exit.clone();
        let scheduler_event_broadcast = event_broadcast.clone();
        threads.push(
            Builder::new()
                .name("solBlkVerif".to_string())
                .spawn(move || {
                    BlockVerificationScheduler::new(
                        scheduler_exit,
                        block_verification_stage,
                        entry_verification_threads,
                        scheduler_event_broadcast,
                    )
                    .run();
                })
                .unwrap(),
        );
        threads.extend(spawn_replay_block_verification_workers(
            exit.clone(),
            workers,
            transaction_status_sender,
            replay_vote_sender.clone(),
            prioritization_fee_cache,
            log_messages_bytes_limit,
            shared_leader_state,
            bank_forks,
            event_broadcast.clone(),
        ));
        threads.extend(spawn_replay_signature_verification_workers(
            exit,
            signature_verification_workers,
            replay_vote_sender,
            event_broadcast,
        ));

        Ok((
            Self { threads },
            BlockVerificationSession::new(replay_stage),
        ))
    }

    #[cfg(test)]
    fn thread_count(&self) -> usize {
        self.threads.len()
    }

    pub fn join(self) -> thread::Result<()> {
        for thread in self.threads {
            thread.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::create_genesis_config,
            get_tmp_ledger_path_auto_delete,
        },
        solana_poh::poh_recorder::create_test_recorder,
        solana_runtime::bank::Bank,
        std::sync::atomic::Ordering,
    };

    #[test]
    fn test_block_verification_stage_runtime_exits() {
        let genesis_config = create_genesis_config(10_000).genesis_config;
        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let (exit, poh_recorder, _poh_controller, _transaction_recorder, poh_service, _) =
            create_test_recorder(bank, blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let worker_count = NonZeroUsize::new(1).unwrap();
        let (block_verification_stage, _replay_stage) = BlockVerificationStage::new(
            exit.clone(),
            BlockVerificationStageConfig {
                worker_count,
                entry_verification_threads: worker_count,
                transaction_status_sender: None,
                replay_vote_sender,
                prioritization_fee_cache: None,
                log_messages_bytes_limit: None,
                shared_leader_state: poh_recorder.read().unwrap().shared_leader_state(),
                bank_forks,
                event_ledger_path: Some(ledger_path.path()),
            },
        )
        .unwrap();

        assert!(ledger_path.path().join("agave_events.ipc").exists());
        assert_eq!(
            block_verification_stage.thread_count(),
            2 + SIGNATURE_VERIFICATION_WORKER_COUNT
        );
        exit.store(true, Ordering::Relaxed);
        block_verification_stage.join().unwrap();
        poh_service.join().unwrap();
    }
}
