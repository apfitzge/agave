use {
    crate::banking_stage::spawn_replay_block_verification_workers,
    agave_block_verification_stage::{
        scheduler::BlockVerificationScheduler,
        setup::{
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig, ReplayStageSession,
        },
    },
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime::{
        bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    std::{
        num::NonZeroUsize,
        sync::{Arc, RwLock, atomic::AtomicBool},
        thread::{self, Builder, JoinHandle},
    },
};

const ALLOCATOR_SIZE: usize = 4 * 1024 * 1024 * 1024;
const REPLAY_TO_PACK_CAPACITY: usize = 16 * 1024;
const REPLAY_BLOCK_STATUS_CAPACITY: usize = 1024;
const PACK_TO_WORKER_CAPACITY: usize = 1024;
const WORKER_TO_PACK_CAPACITY: usize = 1024;

pub(crate) struct BlockVerificationStage {
    threads: Vec<JoinHandle<()>>,
}

impl BlockVerificationStage {
    pub(crate) fn new(
        exit: Arc<AtomicBool>,
        worker_count: NonZeroUsize,
        entry_verification_threads: NonZeroUsize,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        prioritization_fee_cache: Option<Arc<PrioritizationFeeCache>>,
        log_messages_bytes_limit: Option<usize>,
        shared_leader_state: SharedLeaderState,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Result<(Self, ReplayStageSession), String> {
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
        } = sessions;

        let mut threads = Vec::with_capacity(worker_count.get() + 1);
        let scheduler_exit = exit.clone();
        threads.push(
            Builder::new()
                .name("solBlkVerif".to_string())
                .spawn(move || {
                    BlockVerificationScheduler::new(
                        scheduler_exit,
                        block_verification_stage,
                        entry_verification_threads,
                    )
                    .run();
                })
                .unwrap(),
        );
        threads.extend(spawn_replay_block_verification_workers(
            exit,
            workers,
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
            log_messages_bytes_limit,
            shared_leader_state,
            bank_forks,
        ));

        Ok((Self { threads }, replay_stage))
    }

    #[cfg(test)]
    fn thread_count(&self) -> usize {
        self.threads.len()
    }

    pub(crate) fn join(self) -> thread::Result<()> {
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
            worker_count,
            worker_count,
            None,
            replay_vote_sender,
            None,
            None,
            poh_recorder.read().unwrap().shared_leader_state(),
            bank_forks,
        )
        .unwrap();

        assert_eq!(block_verification_stage.thread_count(), 2);
        exit.store(true, Ordering::Relaxed);
        block_verification_stage.join().unwrap();
        poh_service.join().unwrap();
    }
}
