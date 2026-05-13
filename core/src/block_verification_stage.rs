use {
    crate::banking_stage::spawn_replay_block_verification_workers,
    agave_block_verification_stage::{
        scheduler::BlockVerificationScheduler,
        setup::{
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig, ReplayStageSession,
        },
    },
    agave_scheduler_bindings::{
        EntryHeader, ReplayBankMessage, ReplayBlockStatusMessage, ReplayToPackMessage,
        ReplayToPackMessagePayload, SharableTransactionRegion, replay_bank_message_kinds,
        replay_block_status_codes, replay_to_pack_message_types,
    },
    solana_clock::Slot,
    solana_entry::entry::Entry,
    solana_hash::Hash,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime::{
        bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    std::{
        collections::HashMap,
        num::NonZeroUsize,
        sync::{Arc, RwLock, atomic::AtomicBool},
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

const ALLOCATOR_SIZE: usize = 4 * 1024 * 1024 * 1024;
const REPLAY_TO_PACK_CAPACITY: usize = 16 * 1024;
const REPLAY_BLOCK_STATUS_CAPACITY: usize = 1024;
const PACK_TO_WORKER_CAPACITY: usize = 1024;
const WORKER_TO_PACK_CAPACITY: usize = 1024;
const QUEUE_RETRY_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockVerificationSlotStatus {
    InProgress,
    Success,
    Failed(u16),
    Aborted,
    Unknown,
}

impl BlockVerificationSlotStatus {
    fn from_message(message: ReplayBlockStatusMessage) -> Self {
        match message.status {
            replay_block_status_codes::SUCCESS => Self::Success,
            replay_block_status_codes::FAILED => Self::Failed(message.reason),
            replay_block_status_codes::ABORTED => Self::Aborted,
            _ => Self::Unknown,
        }
    }

    fn is_in_progress(&self) -> bool {
        matches!(self, Self::InProgress)
    }
}

pub(crate) struct BlockVerificationSession {
    session: ReplayStageSession,
    slot_statuses: HashMap<Slot, BlockVerificationSlotStatus>,
}

impl BlockVerificationSession {
    fn new(session: ReplayStageSession) -> Self {
        Self {
            session,
            slot_statuses: HashMap::new(),
        }
    }

    pub(crate) fn clean_remote_free_lists(&self) {
        self.session.allocator.clean_remote_free_lists();
    }

    pub(crate) fn begin_slot(
        &mut self,
        slot: Slot,
        last_entry_hash: Hash,
        exit: &AtomicBool,
    ) -> bool {
        if self
            .slot_statuses
            .get(&slot)
            .is_some_and(BlockVerificationSlotStatus::is_in_progress)
        {
            return true;
        }

        let message = ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::BEGIN,
                    slot,
                    last_entry_hash: last_entry_hash.to_bytes(),
                },
            },
        };
        if self.send_message(message, exit) {
            self.slot_statuses
                .insert(slot, BlockVerificationSlotStatus::InProgress);
            true
        } else {
            false
        }
    }

    pub(crate) fn send_entry(&mut self, slot: Slot, entry: &Entry, exit: &AtomicBool) -> bool {
        let mut transactions = Vec::with_capacity(entry.transactions.len());
        for transaction in &entry.transactions {
            let Some(transaction) =
                self.allocate_transaction(&wincode::serialize(transaction).unwrap())
            else {
                self.free_transactions(transactions);
                return false;
            };
            transactions.push(transaction);
        }

        let entry_header = ReplayToPackMessage {
            tag: replay_to_pack_message_types::ENTRY_HEADER,
            payload: ReplayToPackMessagePayload {
                entry_header: EntryHeader {
                    slot,
                    num_hashes: entry.num_hashes,
                    hash: entry.hash.to_bytes(),
                    num_transactions: entry.transactions.len().try_into().unwrap(),
                },
            },
        };

        if !self.send_message(entry_header, exit) {
            self.free_transactions(transactions);
            return false;
        }

        let mut next_transaction = 0;
        while next_transaction < transactions.len() {
            self.session.replay_to_pack.sync();
            while next_transaction < transactions.len() {
                let transaction = transactions[next_transaction];
                let message = ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload { transaction },
                };
                if self.session.replay_to_pack.try_write(message).is_err() {
                    self.session.replay_to_pack.commit();
                    if exit.load(std::sync::atomic::Ordering::Relaxed) {
                        self.free_transactions(transactions[next_transaction..].iter().copied());
                        return false;
                    }
                    thread::sleep(QUEUE_RETRY_SLEEP);
                    break;
                }
                next_transaction += 1;
            }
            self.session.replay_to_pack.commit();
        }

        true
    }

    pub(crate) fn complete_slot(&mut self, slot: Slot, exit: &AtomicBool) -> bool {
        let message = ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::COMPLETE,
                    slot,
                    last_entry_hash: [0; 32],
                },
            },
        };
        self.send_message(message, exit)
    }

    pub(crate) fn abort_slot(&mut self, slot: Slot, exit: &AtomicBool) -> bool {
        let message = ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::ABORT,
                    slot,
                    last_entry_hash: [0; 32],
                },
            },
        };
        self.send_message(message, exit)
    }

    pub(crate) fn finish_slot_before_bank_forks_removal(&mut self, slot: Slot, exit: &AtomicBool) {
        if self.poll_status(slot).is_some() {
            return;
        }
        if !self
            .slot_statuses
            .get(&slot)
            .is_some_and(BlockVerificationSlotStatus::is_in_progress)
        {
            return;
        }

        if !self.abort_slot(slot, exit) {
            return;
        }
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if self.poll_status(slot).is_some() {
                return;
            }
            thread::sleep(QUEUE_RETRY_SLEEP);
        }
    }

    pub(crate) fn poll_status(&mut self, slot: Slot) -> Option<BlockVerificationSlotStatus> {
        if self
            .slot_statuses
            .get(&slot)
            .is_some_and(|status| !status.is_in_progress())
        {
            return self.slot_statuses.remove(&slot);
        }

        let mut matched_status = None;
        self.session.replay_block_status.sync();
        while let Some(message) = self.session.replay_block_status.try_read().copied() {
            let status = BlockVerificationSlotStatus::from_message(message);
            if message.slot == slot {
                matched_status = Some(status);
            } else {
                self.slot_statuses.insert(message.slot, status);
            }
        }
        self.session.replay_block_status.finalize();

        if matched_status.is_some() {
            self.slot_statuses.remove(&slot);
        }
        matched_status
    }

    fn send_message(&mut self, message: ReplayToPackMessage, exit: &AtomicBool) -> bool {
        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            self.session.replay_to_pack.sync();
            match self.session.replay_to_pack.try_write(message) {
                Ok(()) => {
                    self.session.replay_to_pack.commit();
                    return true;
                }
                Err(_) => thread::sleep(QUEUE_RETRY_SLEEP),
            }
        }

        false
    }

    fn allocate_transaction(&self, data: &[u8]) -> Option<SharableTransactionRegion> {
        let length = data.len().try_into().ok()?;
        let ptr = self.session.allocator.allocate(length).or_else(|| {
            self.session.allocator.clean_remote_free_lists();
            self.session.allocator.allocate(length)
        })?;
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        }

        Some(SharableTransactionRegion {
            offset: unsafe { self.session.allocator.offset(ptr) },
            length,
        })
    }

    fn free_transactions(&self, transactions: impl IntoIterator<Item = SharableTransactionRegion>) {
        for transaction in transactions {
            unsafe {
                self.session.allocator.free_offset(transaction.offset);
            }
        }
    }
}

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
    ) -> Result<(Self, BlockVerificationSession), String> {
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

        Ok((
            Self { threads },
            BlockVerificationSession::new(replay_stage),
        ))
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
