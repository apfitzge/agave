use {
    crate::setup::ReplayStageSession,
    agave_scheduler_bindings::{
        EntryHeader, ReplayBankMessage, ReplayBlockStatusMessage, ReplayToPackMessage,
        ReplayToPackMessagePayload, SharableTransactionRegion, replay_bank_message_kinds,
        replay_block_status_codes, replay_block_status_reasons, replay_to_pack_message_types,
    },
    solana_clock::{BankId, Slot},
    solana_entry::entry::Entry,
    solana_hash::Hash,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

const QUEUE_RETRY_SLEEP: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockVerificationSlotStatus {
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

    pub fn is_invalid_entry_hash_failure(&self) -> bool {
        matches!(
            self,
            Self::Failed(replay_block_status_reasons::INVALID_ENTRY_HASH)
        )
    }

    pub fn is_invalid_transaction_failure(&self) -> bool {
        matches!(
            self,
            Self::Failed(replay_block_status_reasons::INVALID_TRANSACTION)
        )
    }
}

pub struct BlockVerificationSession {
    session: ReplayStageSession,
    slot_statuses: HashMap<Slot, BlockVerificationSlotStatus>,
}

pub struct ReplayBlockVerification {
    session: BlockVerificationSession,
    exit: Arc<AtomicBool>,
}

impl ReplayBlockVerification {
    pub fn new(session: BlockVerificationSession, exit: Arc<AtomicBool>) -> Self {
        Self { session, exit }
    }

    pub fn exit(&self) -> &AtomicBool {
        self.exit.as_ref()
    }

    pub fn clean_remote_free_lists(&self) {
        self.session.clean_remote_free_lists();
    }

    pub fn submit_entries_and_wait(
        &mut self,
        bank_id: BankId,
        slot: Slot,
        last_entry_hash: Hash,
        entries: &[Entry],
    ) -> Option<BlockVerificationSlotStatus> {
        self.session
            .submit_entries_and_wait(bank_id, slot, last_entry_hash, entries, &self.exit)
    }

    pub fn begin_slot(&mut self, bank_id: BankId, slot: Slot, last_entry_hash: Hash) -> bool {
        self.session
            .begin_slot(bank_id, slot, last_entry_hash, &self.exit)
    }

    pub fn send_entry(&mut self, slot: Slot, entry: &Entry) -> bool {
        self.session.send_entry(slot, entry, &self.exit)
    }

    pub fn complete_slot(&mut self, slot: Slot) -> bool {
        self.session.complete_slot(slot, &self.exit)
    }

    pub fn abort_slot(&mut self, slot: Slot) -> bool {
        self.session.abort_slot(slot, &self.exit)
    }

    pub fn finish_slot_before_bank_forks_removal(&mut self, slot: Slot) {
        self.session
            .finish_slot_before_bank_forks_removal(slot, &self.exit);
    }

    pub fn poll_status(&mut self, slot: Slot) -> Option<BlockVerificationSlotStatus> {
        self.session.poll_status(slot)
    }

    pub fn wait_for_status(&mut self, slot: Slot) -> Option<BlockVerificationSlotStatus> {
        self.session.wait_for_status(slot, &self.exit)
    }
}

impl BlockVerificationSession {
    pub fn new(session: ReplayStageSession) -> Self {
        Self {
            session,
            slot_statuses: HashMap::new(),
        }
    }

    pub fn clean_remote_free_lists(&self) {
        self.session.allocator.clean_remote_free_lists();
    }

    pub fn submit_entries_and_wait(
        &mut self,
        bank_id: BankId,
        slot: Slot,
        last_entry_hash: Hash,
        entries: &[Entry],
        exit: &AtomicBool,
    ) -> Option<BlockVerificationSlotStatus> {
        self.clean_remote_free_lists();
        if !self.begin_slot(bank_id, slot, last_entry_hash, exit) {
            return None;
        }
        for entry in entries {
            if !self.send_entry(slot, entry, exit) {
                let _ = self.abort_slot(slot, exit);
                return None;
            }
        }
        if !self.complete_slot(slot, exit) {
            return None;
        }

        self.wait_for_status(slot, exit)
    }

    pub fn wait_for_status(
        &mut self,
        slot: Slot,
        exit: &AtomicBool,
    ) -> Option<BlockVerificationSlotStatus> {
        while !exit.load(Ordering::Relaxed) {
            self.clean_remote_free_lists();
            if let Some(status) = self.poll_status(slot) {
                return Some(status);
            }
            thread::sleep(QUEUE_RETRY_SLEEP);
        }

        None
    }

    pub fn begin_slot(
        &mut self,
        bank_id: BankId,
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
                    bank_id,
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

    pub fn send_entry(&mut self, slot: Slot, entry: &Entry, exit: &AtomicBool) -> bool {
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
                    replay_send_time: Instant::now(),
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
                    if exit.load(Ordering::Relaxed) {
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

    pub fn complete_slot(&mut self, slot: Slot, exit: &AtomicBool) -> bool {
        let message = ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::COMPLETE,
                    slot,
                    bank_id: 0,
                    last_entry_hash: [0; 32],
                },
            },
        };
        self.send_message(message, exit)
    }

    pub fn abort_slot(&mut self, slot: Slot, exit: &AtomicBool) -> bool {
        let message = ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::ABORT,
                    slot,
                    bank_id: 0,
                    last_entry_hash: [0; 32],
                },
            },
        };
        self.send_message(message, exit)
    }

    pub fn finish_slot_before_bank_forks_removal(&mut self, slot: Slot, exit: &AtomicBool) {
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
        while !exit.load(Ordering::Relaxed) {
            if self.poll_status(slot).is_some() {
                return;
            }
            thread::sleep(QUEUE_RETRY_SLEEP);
        }
    }

    pub fn poll_status(&mut self, slot: Slot) -> Option<BlockVerificationSlotStatus> {
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
        while !exit.load(Ordering::Relaxed) {
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
