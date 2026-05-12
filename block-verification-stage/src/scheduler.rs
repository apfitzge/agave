use {
    crate::{
        entry_hash_verifier::{
            EntryHashVerificationResult, EntryHashVerificationTask, EntryHashVerifier,
        },
        setup::BlockVerificationStageSession,
    },
    agave_scheduler_bindings::{
        EntryHeader, ReplayBankMessage, ReplayBlockStatusMessage, SharableTransactionRegion,
        replay_bank_message_kinds, replay_block_status_codes, replay_block_status_reasons,
        replay_to_pack_message_types,
    },
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    solana_entry::entry::EntryVerificationData,
    solana_hash::Hash,
    std::{
        collections::{HashMap, VecDeque},
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    },
};

const IDLE_SLEEP: Duration = Duration::from_millis(1);
const INGRESS_MESSAGE_LIMIT: usize = 1024;
const ENTRY_VERIFICATION_RESULT_LIMIT: usize = 1024;
const ABORTED_SLOT_CLEANUP_LIMIT: usize = 1024;
const SCHEDULING_STATE_POOL_LIMIT: usize = 5;
const POOLED_ENTRY_HEADERS_CAPACITY: usize = 0;
const POOLED_TRANSACTIONS_CAPACITY: usize = 0;

/// Main block verification scheduler.
pub struct BlockVerificationScheduler {
    exit: Arc<AtomicBool>,
    session: BlockVerificationStageSession,
    scheduling_states: HashMap<u64, SchedulingState>,
    scheduling_state_pool: Vec<SchedulingState>,
    aborted_slot_queue: VecDeque<u64>,
    entry_hash_verifier: EntryHashVerifier,
}

struct SchedulingState {
    slot: u64,
    last_entry_hash: Hash,
    entry_headers: Vec<EntryHeader>,
    transactions: Vec<SharableTransactionRegion>,
    entry_verification: EntryVerificationProgress,
    in_flight_worker_messages: usize,
    aborted: bool,
}

impl SchedulingState {
    fn new(slot: u64, last_entry_hash: Hash) -> Self {
        Self {
            slot,
            last_entry_hash,
            entry_headers: Vec::new(),
            transactions: Vec::new(),
            entry_verification: EntryVerificationProgress::default(),
            in_flight_worker_messages: 0,
            aborted: false,
        }
    }

    fn reset_for_slot(&mut self, slot: u64, last_entry_hash: Hash) {
        self.slot = slot;
        self.last_entry_hash = last_entry_hash;
        self.entry_headers.clear();
        self.transactions.clear();
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }

    fn clear_for_pool(&mut self) {
        self.slot = 0;
        self.last_entry_hash = Hash::default();
        self.entry_headers = Vec::with_capacity(POOLED_ENTRY_HEADERS_CAPACITY);
        self.transactions = Vec::with_capacity(POOLED_TRANSACTIONS_CAPACITY);
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }
}

#[derive(Default)]
struct EntryVerificationProgress {
    pending_jobs: usize,
    first_failure: Option<u16>,
}

impl BlockVerificationScheduler {
    pub fn new(
        exit: Arc<AtomicBool>,
        session: BlockVerificationStageSession,
        entry_verification_threads: NonZeroUsize,
    ) -> Self {
        Self {
            exit,
            session,
            scheduling_states: HashMap::new(),
            scheduling_state_pool: Vec::new(),
            aborted_slot_queue: VecDeque::new(),
            entry_hash_verifier: EntryHashVerifier::new(entry_verification_threads),
        }
    }

    pub fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            let ingress_count = self.service_ingress_queue(INGRESS_MESSAGE_LIMIT);
            let entry_verification_count =
                self.service_entry_verification_results(ENTRY_VERIFICATION_RESULT_LIMIT);
            let aborted_cleanup_count = self.service_aborted_slots(ABORTED_SLOT_CLEANUP_LIMIT);
            if ingress_count == 0 && entry_verification_count == 0 && aborted_cleanup_count == 0 {
                thread::sleep(IDLE_SLEEP);
            }
        }
    }

    pub fn service_ingress_queue(&mut self, max_messages: usize) -> usize {
        if max_messages == 0 {
            return 0;
        }

        self.session.replay_to_pack.sync();

        let mut consumed = 0;
        while consumed < max_messages {
            let Some(message) = self.session.replay_to_pack.try_read().copied() else {
                break;
            };

            consumed += 1;
            match message.tag {
                replay_to_pack_message_types::BANK => {
                    // SAFETY: The replay ingress protocol is owned by Agave, and
                    // we trust Agave to set the BANK tag only when the bank
                    // payload field is active.
                    let bank_message = unsafe { message.payload.bank };
                    self.handle_bank_message(bank_message);
                }
                replay_to_pack_message_types::ENTRY_HEADER => {
                    // SAFETY: The replay ingress protocol is owned by Agave, and
                    // we trust Agave to set the ENTRY_HEADER tag only when the
                    // entry_header payload field is active.
                    let entry_header = unsafe { message.payload.entry_header };
                    consumed += self.handle_entry(entry_header);
                }
                replay_to_pack_message_types::TRANSACTION => {
                    // Entry transactions are consumed by `handle_entry` in the
                    // inner loop immediately following their entry header, so a
                    // transaction here is a malformed naked transaction.
                    panic!("transaction message without entry header");
                }
                tag => panic!("unknown replay ingress message tag: {tag}"),
            }
        }

        self.session.replay_to_pack.finalize();

        consumed
    }

    /// Drain completed async entry-hash verification jobs.
    ///
    /// This method is intentionally bounded and nonblocking so the scheduler
    /// can interleave replay ingress with verification result handling. Each
    /// drained result decrements the slot's pending entry-hash job count; the
    /// first invalid result records `FAILED / INVALID_ENTRY_HASH` for that
    /// slot.
    ///
    /// Returns the number of verification results consumed.
    pub fn service_entry_verification_results(&mut self, max_results: usize) -> usize {
        if max_results == 0 {
            return 0;
        }

        let mut consumed = 0;
        while consumed < max_results {
            let Some(result) = self.entry_hash_verifier.try_recv_result() else {
                break;
            };
            consumed += 1;
            self.handle_entry_verification_result(result);
        }

        consumed
    }

    fn handle_entry_verification_result(&mut self, result: EntryHashVerificationResult) {
        let slot = result.slot;
        let should_record_failure = if let Some(state) = self.scheduling_states.get_mut(&slot) {
            state.entry_verification.pending_jobs = state
                .entry_verification
                .pending_jobs
                .checked_sub(1)
                .expect("entry verification result without pending job");
            !result.is_valid
        } else {
            false
        };

        if should_record_failure {
            self.record_slot_failure(slot, replay_block_status_reasons::INVALID_ENTRY_HASH);
        }
    }

    /// Attempt cleanup for queued aborted slots.
    ///
    /// `handle_bank_abort()` only marks a slot aborted and appends it to the
    /// cleanup queue. This method is the only path that drops aborted slot
    /// state and sends `ReplayBlockStatusMessage::ABORTED`. A slot is cleaned
    /// up only after all retained scheduler-owned work has returned, including
    /// entry hash verification jobs and worker messages. Slots that are still
    /// waiting on work are requeued for a later scheduler loop iteration.
    ///
    /// Returns the number of aborted slots fully cleaned up.
    fn service_aborted_slots(&mut self, max_slots: usize) -> usize {
        if max_slots == 0 {
            return 0;
        }

        let mut cleaned = 0;
        let slots_to_check = max_slots.min(self.aborted_slot_queue.len());
        for _ in 0..slots_to_check {
            let slot = self.aborted_slot_queue.pop_front().unwrap();
            if self.try_cleanup_aborted_slot(slot) {
                cleaned += 1;
            } else if self
                .scheduling_states
                .get(&slot)
                .is_some_and(|state| state.aborted)
            {
                self.aborted_slot_queue.push_back(slot);
            }
        }

        cleaned
    }

    fn handle_bank_message(&mut self, message: ReplayBankMessage) {
        match message.kind {
            replay_bank_message_kinds::BEGIN => {
                self.handle_bank_begin(message.slot, Hash::new_from_array(message.last_entry_hash))
            }
            replay_bank_message_kinds::ABORT => self.handle_bank_abort(message.slot),
            kind => panic!("unknown replay bank message kind: {kind}"),
        }
    }

    fn handle_bank_begin(&mut self, slot: u64, last_entry_hash: Hash) {
        assert!(
            !self.scheduling_states.contains_key(&slot),
            "slot already has scheduling state: {slot}",
        );

        let mut state = self
            .scheduling_state_pool
            .pop()
            .unwrap_or_else(|| SchedulingState::new(slot, last_entry_hash));
        state.reset_for_slot(slot, last_entry_hash);

        let previous = self.scheduling_states.insert(slot, state);
        assert!(
            previous.is_none(),
            "slot already has scheduling state: {slot}"
        );
    }

    fn handle_bank_abort(&mut self, slot: u64) {
        let state = self
            .scheduling_states
            .get_mut(&slot)
            .expect("abort received for unknown slot");
        if !state.aborted {
            state.aborted = true;
            self.aborted_slot_queue.push_back(slot);
        }
    }

    fn handle_entry(&mut self, entry_header: EntryHeader) -> usize {
        let slot = entry_header.slot;
        let retain_entry = !self.is_slot_aborted(slot);
        if retain_entry {
            self.scheduling_state_mut(slot)
                .entry_headers
                .push(entry_header);
        }

        let mut consumed = 0;
        let mut entry_transactions =
            Vec::with_capacity(usize::try_from(entry_header.num_transactions).unwrap());
        for _ in 0..entry_header.num_transactions {
            let message = self
                .session
                .replay_to_pack
                .try_read()
                .copied()
                .expect("entry header missing transaction message");
            assert_eq!(
                message.tag,
                replay_to_pack_message_types::TRANSACTION,
                "entry header followed by non-transaction message",
            );

            // SAFETY: We asserted that this message is tagged as TRANSACTION,
            // and we trust Agave to make the transaction payload active for
            // that tag.
            let transaction = unsafe { message.payload.transaction };
            if self.handle_transaction(slot, transaction) {
                entry_transactions.push(transaction);
            }
            consumed += 1;
        }

        if retain_entry {
            self.spawn_entry_hash_verification(entry_header, &entry_transactions);
        }

        consumed
    }

    fn handle_transaction(&mut self, slot: u64, transaction: SharableTransactionRegion) -> bool {
        if self.is_slot_aborted(slot) {
            self.free_transaction_allocation(transaction);
            return false;
        }

        self.scheduling_state_mut(slot)
            .transactions
            .push(transaction);
        true
    }

    fn spawn_entry_hash_verification(
        &mut self,
        entry_header: EntryHeader,
        entry_transactions: &[SharableTransactionRegion],
    ) {
        let slot = entry_header.slot;
        let entry_hash = Hash::new_from_array(entry_header.hash);
        let verification_data =
            self.build_entry_verification_data(entry_header, entry_transactions);

        let state = self.scheduling_state_mut(slot);
        let start_hash = state.last_entry_hash;
        state.last_entry_hash = entry_hash;
        state.entry_verification.pending_jobs += 1;

        let task = EntryHashVerificationTask::new(slot, start_hash, verification_data);
        if let Err(err) = self.entry_hash_verifier.try_submit(task) {
            self.handle_entry_verification_result(err.verify_inline());
        }
    }

    fn build_entry_verification_data(
        &self,
        entry_header: EntryHeader,
        entry_transactions: &[SharableTransactionRegion],
    ) -> EntryVerificationData {
        let num_transactions = usize::try_from(entry_header.num_transactions).unwrap();
        assert_eq!(
            num_transactions,
            entry_transactions.len(),
            "entry transaction count mismatch",
        );

        let mut signatures = Vec::new();
        for transaction in entry_transactions {
            let transaction_bytes = self.transaction_bytes(*transaction);
            if let Ok(transaction_view) =
                UnsanitizedTransactionView::try_new_unsanitized(transaction_bytes)
            {
                signatures.extend_from_slice(transaction_view.signatures());
            }
        }

        EntryVerificationData {
            num_hashes: entry_header.num_hashes,
            hash: Hash::new_from_array(entry_header.hash),
            num_transactions,
            signatures,
        }
    }

    fn transaction_bytes(&self, transaction: SharableTransactionRegion) -> &[u8] {
        // SAFETY: Replay transaction messages transfer ownership of valid
        // shared-memory regions to the scheduler. This borrows the retained
        // transaction bytes only long enough to copy signatures out of them.
        unsafe {
            let ptr = self.session.allocator.ptr_from_offset(transaction.offset);
            core::slice::from_raw_parts(ptr.as_ptr(), transaction.length as usize)
        }
    }

    fn is_slot_aborted(&self, slot: u64) -> bool {
        self.scheduling_states
            .get(&slot)
            .expect("replay ingress received for unknown slot")
            .aborted
    }

    fn scheduling_state_mut(&mut self, slot: u64) -> &mut SchedulingState {
        self.scheduling_states
            .get_mut(&slot)
            .expect("replay ingress received for unknown slot")
    }

    fn try_cleanup_aborted_slot(&mut self, slot: u64) -> bool {
        let Some(state) = self.scheduling_states.get(&slot) else {
            return false;
        };
        if !state.aborted
            || state.in_flight_worker_messages != 0
            || state.entry_verification.pending_jobs != 0
        {
            return false;
        }

        let mut state = self.scheduling_states.remove(&slot).unwrap();
        self.free_scheduling_state_allocations(&mut state);
        if self.scheduling_state_pool.len() < SCHEDULING_STATE_POOL_LIMIT {
            state.clear_for_pool();
            self.scheduling_state_pool.push(state);
        }
        self.send_replay_block_status(ReplayBlockStatusMessage {
            slot,
            status: replay_block_status_codes::ABORTED,
            reason: replay_block_status_reasons::NONE,
        });

        true
    }

    fn free_scheduling_state_allocations(&mut self, state: &mut SchedulingState) {
        for transaction in state.transactions.drain(..) {
            self.free_transaction_allocation(transaction);
        }
    }

    fn free_transaction_allocation(&mut self, transaction: SharableTransactionRegion) {
        // SAFETY: Replay transaction messages transfer ownership to the
        // scheduler. We only call this for transactions still owned by this
        // scheduler state, or for aborted-slot transactions we intentionally
        // drop instead of retaining.
        unsafe {
            self.session.allocator.free_offset(transaction.offset);
        }
    }

    fn record_slot_failure(&mut self, slot: u64, reason: u16) {
        let should_send = {
            let Some(state) = self.scheduling_states.get_mut(&slot) else {
                return;
            };
            if state.entry_verification.first_failure.is_some() {
                false
            } else {
                state.entry_verification.first_failure = Some(reason);
                !state.aborted
            }
        };

        if should_send {
            self.send_replay_block_status(ReplayBlockStatusMessage {
                slot,
                status: replay_block_status_codes::FAILED,
                reason,
            });
        }
    }

    fn send_replay_block_status(&mut self, message: ReplayBlockStatusMessage) {
        self.session
            .replay_block_status
            .try_write(message)
            .expect("replay block status queue full");
        self.session.replay_block_status.commit();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::setup::{
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig, ReplayStageSession,
        },
        agave_scheduler_bindings::{ReplayToPackMessage, ReplayToPackMessagePayload},
        solana_entry::entry as solana_entry,
        solana_message::{Message, MessageHeader, VersionedMessage},
        solana_pubkey::Pubkey,
        solana_signature::{SIGNATURE_BYTES, Signature},
        solana_transaction::versioned::VersionedTransaction,
    };

    fn setup_sessions() -> BlockVerificationStageSessions {
        BlockVerificationStageSessions::setup(BlockVerificationStageSetupConfig {
            allocator_size: 64 * 1024 * 1024,
            replay_to_pack_capacity: 8,
            replay_block_status_capacity: 8,
            worker_count: 1,
            pack_to_worker_capacity: 8,
            worker_to_pack_capacity: 8,
        })
        .unwrap()
    }

    fn setup_scheduler_and_replay_stage() -> (BlockVerificationScheduler, ReplayStageSession) {
        let sessions = setup_sessions();
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler = BlockVerificationScheduler::new(
            exit,
            sessions.block_verification_stage,
            NonZeroUsize::new(1).unwrap(),
        );

        (scheduler, sessions.replay_stage)
    }

    fn write_replay_messages(
        replay_stage: &mut ReplayStageSession,
        messages: impl IntoIterator<Item = ReplayToPackMessage>,
    ) {
        for message in messages {
            assert!(replay_stage.replay_to_pack.try_write(message).is_ok());
        }
        replay_stage.replay_to_pack.commit();
    }

    fn bank_message(kind: u8, slot: u64) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind,
                    slot,
                    last_entry_hash: [0; 32],
                },
            },
        }
    }

    fn begin(slot: u64) -> ReplayToPackMessage {
        bank_message(replay_bank_message_kinds::BEGIN, slot)
    }

    fn begin_with_last_entry_hash(slot: u64, last_entry_hash: Hash) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage {
                    kind: replay_bank_message_kinds::BEGIN,
                    slot,
                    last_entry_hash: last_entry_hash.to_bytes(),
                },
            },
        }
    }

    fn abort(slot: u64) -> ReplayToPackMessage {
        bank_message(replay_bank_message_kinds::ABORT, slot)
    }

    fn entry(slot: u64, num_transactions: u32) -> ReplayToPackMessage {
        entry_with_hash(
            slot,
            1,
            Hash::new_from_array([slot as u8; 32]),
            num_transactions,
        )
    }

    fn entry_with_hash(
        slot: u64,
        num_hashes: u64,
        hash: Hash,
        num_transactions: u32,
    ) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::ENTRY_HEADER,
            payload: ReplayToPackMessagePayload {
                entry_header: EntryHeader {
                    slot,
                    num_hashes,
                    hash: hash.to_bytes(),
                    num_transactions,
                },
            },
        }
    }

    fn transaction(offset: usize) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::TRANSACTION,
            payload: ReplayToPackMessagePayload {
                transaction: SharableTransactionRegion { offset, length: 0 },
            },
        }
    }

    fn allocate_transaction(
        allocator: &rts_alloc::Allocator,
        data: &[u8],
    ) -> SharableTransactionRegion {
        let ptr = allocator.allocate(data.len().try_into().unwrap()).unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        }

        SharableTransactionRegion {
            offset: unsafe { allocator.offset(ptr) },
            length: data.len().try_into().unwrap(),
        }
    }

    fn read_replay_block_status(
        replay_stage: &mut ReplayStageSession,
    ) -> Option<ReplayBlockStatusMessage> {
        replay_stage.replay_block_status.sync();
        let message = replay_stage.replay_block_status.try_read().copied();
        replay_stage.replay_block_status.finalize();

        message
    }

    fn wait_for_entry_verification(scheduler: &mut BlockVerificationScheduler, slot: u64) {
        for _ in 0..1000 {
            scheduler.service_entry_verification_results(1024);
            if scheduler
                .scheduling_states
                .get(&slot)
                .map_or(true, |state| state.entry_verification.pending_jobs == 0)
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }

        panic!("timed out waiting for entry verification results");
    }

    fn next_tick_hash(start_hash: &Hash, num_hashes: u64) -> Hash {
        solana_entry::next_hash(start_hash, num_hashes, &[])
    }

    fn serialized_minimal_transaction(signature: Signature) -> Vec<u8> {
        let tx = VersionedTransaction {
            signatures: vec![signature],
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![Pubkey::default()],
                recent_blockhash: Hash::default(),
                instructions: vec![],
            }),
        };
        wincode::serialize(&tx).unwrap()
    }

    #[test]
    fn run_exits_when_exit_flag_is_set() {
        let sessions = setup_sessions();
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler = BlockVerificationScheduler::new(
            exit.clone(),
            sessions.block_verification_stage,
            NonZeroUsize::new(1).unwrap(),
        );

        let thread_hdl = thread::spawn(move || scheduler.run());
        exit.store(true, Ordering::Relaxed);

        thread_hdl.join().unwrap();
    }

    #[test]
    fn service_ingress_queue_respects_message_limit_between_items() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(1), begin(2), begin(3)]);

        assert_eq!(scheduler.service_ingress_queue(2), 2);
        assert!(scheduler.scheduling_states.contains_key(&1));
        assert!(scheduler.scheduling_states.contains_key(&2));
        assert!(!scheduler.scheduling_states.contains_key(&3));

        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert!(scheduler.scheduling_states.contains_key(&3));
    }

    #[test]
    fn service_ingress_queue_finishes_entry_over_message_limit() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 2),
                transaction(11),
                transaction(12),
                begin(43),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(2), 4);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_headers.len(), 1);
        assert_eq!(state.transactions.len(), 2);
        assert_eq!(state.transactions[0].offset, 11);
        assert_eq!(state.transactions[1].offset, 12);
        assert!(!scheduler.scheduling_states.contains_key(&43));

        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert!(scheduler.scheduling_states.contains_key(&43));
    }

    #[test]
    fn service_ingress_queue_routes_entries_by_slot() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(
            &mut replay_stage,
            [
                begin(1),
                begin(2),
                entry(2, 1),
                transaction(21),
                entry(1, 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(5), 5);
        let state_1 = scheduler.scheduling_states.get(&1).unwrap();
        let state_2 = scheduler.scheduling_states.get(&2).unwrap();
        assert_eq!(state_1.entry_headers.len(), 1);
        assert!(state_1.transactions.is_empty());
        assert_eq!(state_2.entry_headers.len(), 1);
        assert_eq!(state_2.transactions.len(), 1);
        assert_eq!(state_2.transactions[0].offset, 21);
    }

    #[test]
    #[should_panic(expected = "entry header followed by non-transaction message")]
    fn service_ingress_queue_panics_if_entry_is_not_followed_by_transactions() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), entry(42, 1), begin(43)]);

        scheduler.service_ingress_queue(3);
    }

    #[test]
    fn entry_verification_data_extracts_transaction_signatures() {
        let (mut scheduler, replay_stage) = setup_scheduler_and_replay_stage();
        let signature = Signature::from([9; SIGNATURE_BYTES]);
        let transaction = allocate_transaction(
            &replay_stage.allocator,
            &serialized_minimal_transaction(signature),
        );
        let entry_header = EntryHeader {
            slot: 42,
            num_hashes: 1,
            hash: [1; 32],
            num_transactions: 1,
        };

        let verification_data =
            scheduler.build_entry_verification_data(entry_header, &[transaction]);

        assert_eq!(verification_data.num_transactions, 1);
        assert_eq!(verification_data.signatures, vec![signature]);
        scheduler.free_transaction_allocation(transaction);
    }

    #[test]
    fn valid_entry_hash_job_completes_without_failure() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let start_hash = Hash::new_from_array([9; 32]);
        let entry_hash = next_tick_hash(&start_hash, 1);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, entry_hash, 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(2), 2);
        wait_for_entry_verification(&mut scheduler, 42);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_verification.pending_jobs, 0);
        assert_eq!(state.entry_verification.first_failure, None);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn invalid_entry_hash_records_failure() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let start_hash = Hash::new_from_array([9; 32]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, Hash::new_from_array([7; 32]), 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(2), 2);
        wait_for_entry_verification(&mut scheduler, 42);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_verification.pending_jobs, 0);
        assert_eq!(
            state.entry_verification.first_failure,
            Some(replay_block_status_reasons::INVALID_ENTRY_HASH),
        );
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::FAILED,
                reason: replay_block_status_reasons::INVALID_ENTRY_HASH,
            }),
        );
    }

    #[test]
    fn multiple_entry_hash_jobs_chain_through_last_entry_hash() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let start_hash = Hash::new_from_array([9; 32]);
        let first_entry_hash = next_tick_hash(&start_hash, 1);
        let second_entry_hash = next_tick_hash(&first_entry_hash, 1);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, first_entry_hash, 0),
                entry_with_hash(42, 1, second_entry_hash, 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(3), 3);
        wait_for_entry_verification(&mut scheduler, 42);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.last_entry_hash, second_entry_hash);
        assert_eq!(state.entry_verification.pending_jobs, 0);
        assert_eq!(state.entry_verification.first_failure, None);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn abort_without_in_flight_work_cleans_up_and_reports_status() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3, 4]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 1),
                ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload { transaction },
                },
                abort(42),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(4), 4);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.aborted);
        assert_eq!(state.entry_verification.pending_jobs, 1);
        assert!(scheduler.scheduling_state_pool.is_empty());
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_aborted_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert!(scheduler.scheduling_state_pool[0].entry_headers.is_empty());
        assert!(scheduler.scheduling_state_pool[0].transactions.is_empty());
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::ABORTED,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    fn abort_with_in_flight_work_waits_for_cleanup() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        scheduler
            .scheduling_states
            .get_mut(&42)
            .unwrap()
            .in_flight_worker_messages = 1;

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.aborted);
        assert_eq!(state.in_flight_worker_messages, 1);
        assert!(scheduler.scheduling_state_pool.is_empty());
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
        assert_eq!(scheduler.service_aborted_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));

        let dropped_transaction = allocate_transaction(&replay_stage.allocator, &[9, 10, 11]);
        write_replay_messages(
            &mut replay_stage,
            [
                entry(42, 1),
                ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload {
                        transaction: dropped_transaction,
                    },
                },
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(2), 2);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.entry_headers.is_empty());
        assert!(state.transactions.is_empty());

        scheduler
            .scheduling_states
            .get_mut(&42)
            .unwrap()
            .in_flight_worker_messages = 0;
        assert_eq!(scheduler.service_aborted_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::ABORTED,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    fn cleanup_returns_state_to_pool_for_reuse() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), entry(42, 0), abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_aborted_slots(1), 1);

        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert_eq!(
            scheduler.scheduling_state_pool[0].entry_headers.capacity(),
            POOLED_ENTRY_HEADERS_CAPACITY,
        );
        assert_eq!(
            scheduler.scheduling_state_pool[0].transactions.capacity(),
            POOLED_TRANSACTIONS_CAPACITY,
        );

        write_replay_messages(&mut replay_stage, [begin(43)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);

        assert!(scheduler.scheduling_state_pool.is_empty());
        let state = scheduler.scheduling_states.get(&43).unwrap();
        assert_eq!(state.slot, 43);
        assert!(state.entry_headers.is_empty());
        assert!(state.transactions.is_empty());
    }

    #[test]
    fn cleanup_caps_scheduling_state_pool() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let slots: Vec<_> = (0..u64::try_from(SCHEDULING_STATE_POOL_LIMIT + 2).unwrap()).collect();

        write_replay_messages(&mut replay_stage, slots.iter().copied().map(begin));
        assert_eq!(scheduler.service_ingress_queue(slots.len()), slots.len());
        assert_eq!(scheduler.scheduling_states.len(), slots.len());

        write_replay_messages(&mut replay_stage, slots.iter().copied().map(abort));
        assert_eq!(scheduler.service_ingress_queue(slots.len()), slots.len());
        assert_eq!(
            scheduler.service_aborted_slots(slots.len()),
            SCHEDULING_STATE_POOL_LIMIT + 2,
        );

        assert_eq!(
            scheduler.scheduling_state_pool.len(),
            SCHEDULING_STATE_POOL_LIMIT
        );
    }

    #[test]
    fn cleanup_shrinks_pooled_scheduling_state_allocations() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();

        write_replay_messages(&mut replay_stage, [begin(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        {
            let state = scheduler.scheduling_states.get_mut(&42).unwrap();
            state.entry_headers.reserve(128);
            state.transactions.reserve(128);
            assert!(state.entry_headers.capacity() >= 128);
            assert!(state.transactions.capacity() >= 128);
        }

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert_eq!(scheduler.service_aborted_slots(1), 1);

        let pooled_state = &scheduler.scheduling_state_pool[0];
        assert_eq!(
            pooled_state.entry_headers.capacity(),
            POOLED_ENTRY_HEADERS_CAPACITY,
        );
        assert_eq!(
            pooled_state.transactions.capacity(),
            POOLED_TRANSACTIONS_CAPACITY,
        );
    }
}
