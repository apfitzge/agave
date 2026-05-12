use {
    crate::{
        entry_hash_verifier::{
            EntryHashVerificationResult, EntryHashVerificationTask, EntryHashVerifier,
        },
        setup::BlockVerificationStageSession,
    },
    agave_scheduler_bindings::{
        EntryHeader, PackToWorkerMessage, ReplayBankMessage, ReplayBlockStatusMessage,
        SharablePubkeys, SharableTransactionBatchRegion, SharableTransactionRegion,
        TransactionResponseRegion, WorkerToPackMessage,
        pack_message_flags::{self, check_flags},
        processed_codes, replay_bank_message_kinds, replay_block_status_codes,
        replay_block_status_reasons, replay_to_pack_message_types,
        worker_message_types::{
            CHECK_RESPONSE, CheckResponse, parsing_and_sanitization_flags, resolve_flags,
            signature_verification_flags,
        },
    },
    agave_scheduling_utils::{
        responses_region::CheckResponsesPtr, transaction_ptr::TransactionPtrBatch,
    },
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    solana_entry::entry::EntryVerificationData,
    solana_hash::Hash,
    std::{
        collections::{BTreeMap, HashMap, VecDeque},
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
const SIGNATURE_CHECK_DISPATCH_LIMIT: usize = 1024;
const WORKER_RESPONSE_LIMIT: usize = 1024;
const ABORTED_SLOT_CLEANUP_LIMIT: usize = 1024;
const SCHEDULING_STATE_POOL_LIMIT: usize = 5;
const POOLED_ENTRY_HEADERS_CAPACITY: usize = 0;
const POOLED_OWNED_TRANSACTIONS_CAPACITY: usize = 0;
const POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY: usize = 0;
const TRANSACTION_BATCH_ALLOCATION_SIZE: u32 =
    TransactionPtrBatch::<PendingWorkerCheck>::TRANSACTION_META_END as u32;
const REPLAY_TRANSACTION_CHECK_FLAGS: u16 = pack_message_flags::CHECK
    | check_flags::VERIFY_SIGNATURES
    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES
    | check_flags::REPLAY;

fn is_check_response_region(
    batch: SharableTransactionBatchRegion,
    responses: TransactionResponseRegion,
) -> bool {
    batch.num_transactions == 1
        && responses.tag == CHECK_RESPONSE
        && responses.num_transaction_responses == batch.num_transactions
}

fn check_response_is_invalid(response: &CheckResponse) -> bool {
    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0
        || response.signature_verification_flags & signature_verification_flags::FAILED != 0
        || response.signature_verification_flags & signature_verification_flags::PERFORMED == 0
        || response.resolve_flags & resolve_flags::FAILED != 0
        || response.resolve_flags & resolve_flags::PERFORMED == 0
}

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
    // Scheduler-owned transaction allocations retained until the slot is
    // cleaned up. Dispatch queues and later checked-result buffers store
    // indices into this vector.
    owned_transactions: Vec<SharableTransactionRegion>,
    pending_transaction_checks: VecDeque<PendingTransactionCheck>,
    checked_transactions: BTreeMap<usize, CheckedTransaction>,
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
            owned_transactions: Vec::new(),
            pending_transaction_checks: VecDeque::new(),
            checked_transactions: BTreeMap::new(),
            entry_verification: EntryVerificationProgress::default(),
            in_flight_worker_messages: 0,
            aborted: false,
        }
    }

    fn reset_for_slot(&mut self, slot: u64, last_entry_hash: Hash) {
        self.slot = slot;
        self.last_entry_hash = last_entry_hash;
        self.entry_headers.clear();
        self.owned_transactions.clear();
        self.pending_transaction_checks.clear();
        self.checked_transactions.clear();
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }

    fn clear_for_pool(&mut self) {
        self.slot = 0;
        self.last_entry_hash = Hash::default();
        self.entry_headers = Vec::with_capacity(POOLED_ENTRY_HEADERS_CAPACITY);
        self.owned_transactions = Vec::with_capacity(POOLED_OWNED_TRANSACTIONS_CAPACITY);
        self.pending_transaction_checks =
            VecDeque::with_capacity(POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY);
        self.checked_transactions = BTreeMap::new();
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }

    fn accepts_work(&self) -> bool {
        !self.aborted && !self.is_failed()
    }

    fn is_failed(&self) -> bool {
        self.entry_verification.first_failure.is_some()
    }
}

#[derive(Clone, Copy)]
struct PendingTransactionCheck {
    transaction_index: usize,
    transaction: SharableTransactionRegion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct PendingWorkerCheck {
    slot: u64,
    transaction_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckedTransaction {
    response: CheckResponse,
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
            let signature_check_dispatch_count =
                self.service_transaction_check_dispatches(SIGNATURE_CHECK_DISPATCH_LIMIT);
            let worker_response_count = self.service_worker_responses(WORKER_RESPONSE_LIMIT);
            let aborted_cleanup_count = self.service_aborted_slots(ABORTED_SLOT_CLEANUP_LIMIT);
            if ingress_count == 0
                && entry_verification_count == 0
                && signature_check_dispatch_count == 0
                && worker_response_count == 0
                && aborted_cleanup_count == 0
            {
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
        let retain_entry = self.is_slot_accepting_work(slot);
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
        if !self.is_slot_accepting_work(slot) {
            self.free_transaction_allocation(transaction);
            return false;
        }

        let state = self.scheduling_state_mut(slot);
        let transaction_index = state.owned_transactions.len();
        state.owned_transactions.push(transaction);
        state
            .pending_transaction_checks
            .push_back(PendingTransactionCheck {
                transaction_index,
                transaction,
            });
        true
    }

    fn service_transaction_check_dispatches(&mut self, max_checks: usize) -> usize {
        if max_checks == 0 || self.session.workers.is_empty() {
            return 0;
        }

        let slots: Vec<_> = self.scheduling_states.keys().copied().collect();
        let mut dispatched = 0;
        for slot in slots {
            while dispatched < max_checks && self.has_pending_transaction_checks(slot) {
                let mut made_progress = false;
                for worker_index in 0..self.session.workers.len() {
                    if dispatched == max_checks {
                        return dispatched;
                    }
                    let Some(pending_check) = self.pending_transaction_check(slot) else {
                        break;
                    };
                    if !self.worker_queue_has_capacity(worker_index) {
                        continue;
                    }

                    let Some(batch) = self.allocate_transaction_check_batch(slot, pending_check)
                    else {
                        return dispatched;
                    };

                    let message = PackToWorkerMessage {
                        flags: REPLAY_TRANSACTION_CHECK_FLAGS,
                        max_working_slot: slot,
                        batch,
                    };
                    if let Err(returned_message) = self.session.workers[worker_index]
                        .pack_to_worker
                        .try_write(message)
                    {
                        self.free_transaction_batch_allocation(returned_message.batch);
                        return dispatched;
                    }
                    self.session.workers[worker_index].pack_to_worker.commit();

                    let state = self.scheduling_state_mut(slot);
                    state.pending_transaction_checks.pop_front();
                    state.in_flight_worker_messages += 1;
                    dispatched += 1;
                    made_progress = true;
                }

                if !made_progress {
                    return dispatched;
                }
            }
        }

        dispatched
    }

    fn has_pending_transaction_checks(&self, slot: u64) -> bool {
        self.scheduling_states
            .get(&slot)
            .filter(|state| state.accepts_work())
            .is_some_and(|state| !state.pending_transaction_checks.is_empty())
    }

    fn pending_transaction_check(&self, slot: u64) -> Option<PendingTransactionCheck> {
        let state = self.scheduling_states.get(&slot)?;
        if !state.accepts_work() {
            return None;
        }

        state.pending_transaction_checks.front().copied()
    }

    fn worker_queue_has_capacity(&mut self, worker_index: usize) -> bool {
        let queue = &mut self.session.workers[worker_index].pack_to_worker;
        queue.sync();
        queue.len() < queue.capacity()
    }

    fn allocate_transaction_check_batch(
        &self,
        slot: u64,
        pending_check: PendingTransactionCheck,
    ) -> Option<SharableTransactionBatchRegion> {
        let ptr = self
            .session
            .allocator
            .allocate(TRANSACTION_BATCH_ALLOCATION_SIZE)?;
        // SAFETY: `ptr` was allocated by this scheduler's allocator above.
        let transactions_offset = unsafe { self.session.allocator.offset(ptr) };
        let batch_ptr = ptr.cast::<SharableTransactionRegion>();

        // SAFETY: The allocation size is
        // `TransactionPtrBatch::<PendingWorkerCheck>::TRANSACTION_META_END`,
        // and `TRANSACTION_META_START` is the aligned offset for the metadata
        // region within that allocation.
        let meta_ptr = unsafe {
            ptr.byte_add(TransactionPtrBatch::<PendingWorkerCheck>::TRANSACTION_META_START)
                .cast::<PendingWorkerCheck>()
        };

        // SAFETY: `batch_ptr` points at the transaction-region portion of the
        // batch allocation. CHECK dispatches intentionally send one transaction
        // per worker message, so writing the first transaction region is
        // in-bounds.
        unsafe {
            batch_ptr.as_ptr().write(pending_check.transaction);
        }
        // SAFETY: `meta_ptr` points at the first `PendingWorkerCheck`
        // metadata slot computed from `TransactionPtrBatch`'s layout.
        unsafe {
            meta_ptr.as_ptr().write(PendingWorkerCheck {
                slot,
                transaction_index: pending_check.transaction_index,
            });
        }

        Some(SharableTransactionBatchRegion {
            num_transactions: 1,
            transactions_offset,
        })
    }

    /// Drain completed worker CHECK responses.
    ///
    /// All worker response queues are synchronized before any response is
    /// handled, then responses are consumed in a bounded round-robin pass
    /// across workers. Empty worker queues do not stop the pass; servicing
    /// stops after `max_responses` responses or after a full worker cycle
    /// produces no response.
    ///
    /// Returns the number of worker responses consumed.
    fn service_worker_responses(&mut self, max_responses: usize) -> usize {
        if max_responses == 0 || self.session.workers.is_empty() {
            return 0;
        }

        for worker in &mut self.session.workers {
            worker.worker_to_pack.sync();
        }

        let worker_count = self.session.workers.len();
        let mut consumed = 0;
        let mut empty_reads = 0;
        let mut worker_indices = (0..worker_count).cycle();
        while consumed < max_responses && empty_reads < worker_count {
            let worker_index = worker_indices
                .next()
                .expect("cycled worker index iterator should not end");

            let Some(message) = self.session.workers[worker_index]
                .worker_to_pack
                .try_read()
                .copied()
            else {
                empty_reads += 1;
                continue;
            };

            empty_reads = 0;
            consumed += 1;
            self.handle_worker_response(message);
        }

        for worker in &mut self.session.workers {
            worker.worker_to_pack.finalize();
        }

        consumed
    }

    fn handle_worker_response(&mut self, message: WorkerToPackMessage) {
        assert_eq!(
            message.processed_code,
            processed_codes::PROCESSED,
            "replay CHECK worker response was not processed",
        );

        match message.responses.tag {
            CHECK_RESPONSE => self.handle_worker_check_response(message),
            tag => panic!("unsupported replay worker response tag: {tag}"),
        }
    }

    fn handle_worker_check_response(&mut self, message: WorkerToPackMessage) {
        assert!(
            is_check_response_region(message.batch, message.responses),
            "malformed replay CHECK worker response",
        );

        let worker_check = self.worker_check_metadata(message.batch);
        let slot = worker_check.slot;

        let check_response = self.read_check_response(message.responses);
        self.free_transaction_batch_allocation(message.batch);

        if check_response_is_invalid(&check_response) {
            self.free_check_response_allocations(check_response);
            self.record_slot_failure(slot, replay_block_status_reasons::INVALID_TRANSACTION);
        } else {
            self.record_successful_check(worker_check, check_response);
        }

        self.decrement_in_flight_worker_messages(slot);
    }

    fn worker_check_metadata(&self, batch: SharableTransactionBatchRegion) -> PendingWorkerCheck {
        let ptr_batch = unsafe {
            // SAFETY: CHECK dispatch allocates every worker batch with
            // `TransactionPtrBatch<PendingWorkerCheck>` layout.
            TransactionPtrBatch::<PendingWorkerCheck>::from_sharable_transaction_batch_region(
                &batch,
                &self.session.allocator,
            )
        };
        assert_eq!(
            ptr_batch.len(),
            1,
            "replay CHECK batches are one transaction"
        );
        let metadata = ptr_batch.iter().next().unwrap().1;

        metadata
    }

    fn read_check_response(&self, responses: TransactionResponseRegion) -> CheckResponse {
        let responses_ptr = unsafe {
            // SAFETY: Caller validated that `responses` is a CHECK_RESPONSE
            // region with one response allocated by the shared allocator.
            CheckResponsesPtr::from_transaction_response_region(&responses, &self.session.allocator)
        };
        let response = *responses_ptr.iter().next().unwrap();
        unsafe {
            // SAFETY: `responses_ptr` is exclusively owned by this scheduler
            // after the worker returned the response message.
            responses_ptr.free(&self.session.allocator);
        }

        response
    }

    fn record_successful_check(
        &mut self,
        worker_check: PendingWorkerCheck,
        check_response: CheckResponse,
    ) {
        let should_retain = self
            .scheduling_states
            .get(&worker_check.slot)
            .is_some_and(|state| state.accepts_work());

        if !should_retain {
            self.free_check_response_allocations(check_response);
            return;
        }

        let state = self.scheduling_state_mut(worker_check.slot);
        let previous = state.checked_transactions.insert(
            worker_check.transaction_index,
            CheckedTransaction {
                response: check_response,
            },
        );
        assert!(
            previous.is_none(),
            "duplicate checked transaction index: {}",
            worker_check.transaction_index,
        );
    }

    fn decrement_in_flight_worker_messages(&mut self, slot: u64) {
        if let Some(state) = self.scheduling_states.get_mut(&slot) {
            state.in_flight_worker_messages = state
                .in_flight_worker_messages
                .checked_sub(1)
                .expect("worker response without in-flight worker message");
        }
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

    fn is_slot_accepting_work(&self, slot: u64) -> bool {
        self.scheduling_states
            .get(&slot)
            .expect("replay ingress received for unknown slot")
            .accepts_work()
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
        for transaction in state.owned_transactions.drain(..) {
            self.free_transaction_allocation(transaction);
        }
        while let Some((_, checked_transaction)) = state.checked_transactions.pop_first() {
            self.free_check_response_allocations(checked_transaction.response);
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

    fn free_transaction_batch_allocation(&mut self, batch: SharableTransactionBatchRegion) {
        // SAFETY: Transaction batch regions are allocated by this scheduler
        // and remain scheduler-owned until a worker response returns them or
        // a dispatch attempt fails before handing the batch to a worker.
        unsafe {
            self.session
                .allocator
                .free_offset(batch.transactions_offset);
        }
    }

    fn free_check_response_allocations(&mut self, response: CheckResponse) {
        self.free_resolved_pubkeys_allocation(response.resolved_pubkeys);
    }

    fn free_resolved_pubkeys_allocation(&mut self, pubkeys: SharablePubkeys) {
        if pubkeys.num_pubkeys == 0 {
            return;
        }

        // SAFETY: Non-empty resolved pubkey regions are worker-returned
        // allocations now owned by this scheduler.
        unsafe {
            self.session.allocator.free_offset(pubkeys.offset);
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
                state.pending_transaction_checks.clear();
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
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig,
            BlockVerificationWorkerSession, ReplayStageSession,
        },
        agave_scheduler_bindings::{ReplayToPackMessage, ReplayToPackMessagePayload},
        agave_scheduling_utils::responses_region::resolve_responses_from_iter,
        solana_entry::entry as solana_entry,
        solana_message::{Message, MessageHeader, VersionedMessage},
        solana_pubkey::Pubkey,
        solana_signature::{SIGNATURE_BYTES, Signature},
        solana_transaction::versioned::VersionedTransaction,
    };

    fn setup_sessions() -> BlockVerificationStageSessions {
        setup_sessions_with_config(BlockVerificationStageSetupConfig {
            allocator_size: 64 * 1024 * 1024,
            replay_to_pack_capacity: 8,
            replay_block_status_capacity: 8,
            worker_count: 1,
            pack_to_worker_capacity: 8,
            worker_to_pack_capacity: 8,
        })
    }

    fn setup_sessions_with_config(
        config: BlockVerificationStageSetupConfig,
    ) -> BlockVerificationStageSessions {
        BlockVerificationStageSessions::setup(config).unwrap()
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

    fn setup_scheduler_replay_stage_and_workers(
        config: BlockVerificationStageSetupConfig,
    ) -> (
        BlockVerificationScheduler,
        ReplayStageSession,
        Vec<BlockVerificationWorkerSession>,
    ) {
        let sessions = setup_sessions_with_config(config);
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler = BlockVerificationScheduler::new(
            exit,
            sessions.block_verification_stage,
            NonZeroUsize::new(1).unwrap(),
        );

        (scheduler, sessions.replay_stage, sessions.workers)
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

    fn transaction_message(transaction: SharableTransactionRegion) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::TRANSACTION,
            payload: ReplayToPackMessagePayload { transaction },
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

    fn read_pack_to_worker_message(
        worker: &mut BlockVerificationWorkerSession,
    ) -> Option<PackToWorkerMessage> {
        worker.pack_to_worker.sync();
        let message = worker.pack_to_worker.try_read().copied();
        worker.pack_to_worker.finalize();

        message
    }

    fn transaction_batch_regions(
        allocator: &rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> Vec<SharableTransactionRegion> {
        let ptr = unsafe {
            allocator
                .ptr_from_offset(batch.transactions_offset)
                .cast::<SharableTransactionRegion>()
        };
        let transactions = unsafe {
            core::slice::from_raw_parts(ptr.as_ptr(), usize::from(batch.num_transactions))
        };

        transactions.to_vec()
    }

    fn transaction_check_metadata(
        allocator: &rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> Vec<PendingWorkerCheck> {
        let ptr_batch = unsafe {
            TransactionPtrBatch::<PendingWorkerCheck>::from_sharable_transaction_batch_region(
                &batch, allocator,
            )
        };

        ptr_batch.iter().map(|(_, meta)| meta).collect()
    }

    fn successful_check_response() -> CheckResponse {
        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: 0,
            fee_payer_balance_flags: 0,
            resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            signature_verification_flags: signature_verification_flags::REQUESTED
                | signature_verification_flags::PERFORMED,
            included_slot: 0,
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

    fn parsing_failed_check_response() -> CheckResponse {
        CheckResponse {
            parsing_and_sanitization_flags: parsing_and_sanitization_flags::FAILED,
            resolve_flags: resolve_flags::REQUESTED,
            signature_verification_flags: signature_verification_flags::REQUESTED,
            ..successful_check_response()
        }
    }

    fn signature_failed_check_response() -> CheckResponse {
        CheckResponse {
            signature_verification_flags: signature_verification_flags::REQUESTED
                | signature_verification_flags::PERFORMED
                | signature_verification_flags::FAILED,
            ..successful_check_response()
        }
    }

    fn resolve_failed_check_response() -> CheckResponse {
        CheckResponse {
            resolve_flags: resolve_flags::REQUESTED
                | resolve_flags::PERFORMED
                | resolve_flags::FAILED,
            ..successful_check_response()
        }
    }

    fn allocate_pubkeys(allocator: &rts_alloc::Allocator, pubkeys: &[Pubkey]) -> SharablePubkeys {
        let byte_len = pubkeys
            .len()
            .checked_mul(core::mem::size_of::<Pubkey>())
            .unwrap();
        let ptr = allocator.allocate(byte_len.try_into().unwrap()).unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(pubkeys.as_ptr().cast(), ptr.as_ptr(), byte_len);
        }

        SharablePubkeys {
            offset: unsafe { allocator.offset(ptr) },
            num_pubkeys: pubkeys.len().try_into().unwrap(),
        }
    }

    fn queue_worker_check_response(
        worker: &mut BlockVerificationWorkerSession,
        batch: SharableTransactionBatchRegion,
        response: CheckResponse,
    ) {
        let responses =
            resolve_responses_from_iter(&worker.allocator, [response].into_iter()).unwrap();
        worker
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
        worker.worker_to_pack.commit();
    }

    fn queue_worker_unprocessed_response(
        worker: &mut BlockVerificationWorkerSession,
        batch: SharableTransactionBatchRegion,
    ) {
        worker
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch,
                processed_code: processed_codes::BANK_NOT_AVAILABLE,
                responses: TransactionResponseRegion {
                    tag: 0,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .unwrap();
        worker.worker_to_pack.commit();
    }

    fn queue_unsupported_worker_response(
        worker: &mut BlockVerificationWorkerSession,
        batch: SharableTransactionBatchRegion,
    ) {
        worker
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses: TransactionResponseRegion {
                    tag: 99,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .unwrap();
        worker.worker_to_pack.commit();
    }

    fn queue_malformed_worker_check_response(
        worker: &mut BlockVerificationWorkerSession,
        batch: SharableTransactionBatchRegion,
    ) {
        worker
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses: TransactionResponseRegion {
                    tag: CHECK_RESPONSE,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .unwrap();
        worker.worker_to_pack.commit();
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
        assert_eq!(state.owned_transactions.len(), 2);
        assert_eq!(state.owned_transactions[0].offset, 11);
        assert_eq!(state.owned_transactions[1].offset, 12);
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
        assert!(state_1.owned_transactions.is_empty());
        assert_eq!(state_2.entry_headers.len(), 1);
        assert_eq!(state_2.owned_transactions.len(), 1);
        assert_eq!(state_2.owned_transactions[0].offset, 21);
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
    fn entry_transactions_are_sent_to_workers_one_by_one() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let first_transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        let second_transaction = allocate_transaction(&replay_stage.allocator, &[4, 5, 6]);

        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 2),
                transaction_message(first_transaction),
                transaction_message(second_transaction),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(4), 4);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 2);

        for (transaction_index, transaction) in [first_transaction, second_transaction]
            .into_iter()
            .enumerate()
        {
            let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
            assert_eq!(
                worker_message.flags,
                pack_message_flags::CHECK
                    | check_flags::VERIFY_SIGNATURES
                    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES
                    | check_flags::REPLAY,
            );
            assert_eq!(worker_message.max_working_slot, 42);
            assert_eq!(worker_message.batch.num_transactions, 1);
            assert_eq!(
                transaction_check_metadata(&replay_stage.allocator, worker_message.batch),
                vec![PendingWorkerCheck {
                    slot: 42,
                    transaction_index,
                }],
            );
            assert_eq!(
                transaction_batch_regions(&replay_stage.allocator, worker_message.batch),
                vec![transaction],
            );
        }

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.pending_transaction_checks.is_empty());
        assert_eq!(state.in_flight_worker_messages, 2);
    }

    #[test]
    fn transaction_checks_round_robin_across_available_workers() {
        let transaction_count = 8;
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 16,
                replay_block_status_capacity: 8,
                worker_count: 4,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transactions: Vec<_> = (0..transaction_count)
            .map(|index| allocate_transaction(&replay_stage.allocator, &[index as u8]))
            .collect();
        let mut messages = Vec::with_capacity(transaction_count + 2);
        messages.push(begin(42));
        messages.push(entry(42, transaction_count.try_into().unwrap()));
        messages.extend(transactions.iter().copied().map(transaction_message));

        write_replay_messages(&mut replay_stage, messages);

        assert_eq!(
            scheduler.service_ingress_queue(transaction_count + 2),
            transaction_count + 2
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            transaction_count,
        );

        for worker_index in 0..4 {
            for transaction_index in [worker_index, worker_index + 4] {
                let worker_message =
                    read_pack_to_worker_message(&mut workers[worker_index]).unwrap();
                assert_eq!(worker_message.batch.num_transactions, 1);
                assert_eq!(
                    transaction_batch_regions(&replay_stage.allocator, worker_message.batch),
                    vec![transactions[transaction_index]],
                );
                assert_eq!(
                    transaction_check_metadata(&replay_stage.allocator, worker_message.batch),
                    vec![PendingWorkerCheck {
                        slot: 42,
                        transaction_index,
                    }],
                );
            }
        }

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.pending_transaction_checks.is_empty());
        assert_eq!(state.in_flight_worker_messages, transaction_count);
    }

    #[test]
    fn successful_worker_check_records_ordered_result_and_keeps_transaction() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let mut response = successful_check_response();
        response.resolved_pubkeys =
            allocate_pubkeys(&workers[0].allocator, &[Pubkey::new_unique()]);
        queue_worker_check_response(&mut workers[0], worker_message.batch, response);

        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.in_flight_worker_messages, 0);
        assert_eq!(state.owned_transactions, vec![transaction]);
        assert!(state.checked_transactions.contains_key(&0));
        assert_eq!(
            state.checked_transactions[&0]
                .response
                .resolved_pubkeys
                .num_pubkeys,
            1,
        );
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn worker_responses_are_serviced_round_robin() {
        let transaction_count = 4;
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 3,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transactions: Vec<_> = (0..transaction_count)
            .map(|index| allocate_transaction(&replay_stage.allocator, &[index as u8]))
            .collect();
        let mut messages = Vec::with_capacity(transaction_count + 2);
        messages.push(begin(42));
        messages.push(entry(42, transaction_count.try_into().unwrap()));
        messages.extend(transactions.iter().copied().map(transaction_message));

        write_replay_messages(&mut replay_stage, messages);

        assert_eq!(
            scheduler.service_ingress_queue(transaction_count + 2),
            transaction_count + 2
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            transaction_count,
        );

        let worker_0_first = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let worker_1 = read_pack_to_worker_message(&mut workers[1]).unwrap();
        let worker_2 = read_pack_to_worker_message(&mut workers[2]).unwrap();
        let worker_0_second = read_pack_to_worker_message(&mut workers[0]).unwrap();

        queue_worker_check_response(
            &mut workers[0],
            worker_0_first.batch,
            successful_check_response(),
        );
        queue_worker_check_response(
            &mut workers[0],
            worker_0_second.batch,
            successful_check_response(),
        );
        queue_worker_check_response(&mut workers[2], worker_2.batch, successful_check_response());

        assert_eq!(scheduler.service_worker_responses(2), 2);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(
            state
                .checked_transactions
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![0, 2],
        );
        assert_eq!(state.in_flight_worker_messages, 2);

        queue_worker_check_response(&mut workers[1], worker_1.batch, successful_check_response());
        assert_eq!(scheduler.service_worker_responses(2), 2);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(
            state
                .checked_transactions
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3],
        );
        assert_eq!(state.in_flight_worker_messages, 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn signature_failure_sends_invalid_transaction() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            signature_failed_check_response(),
        );

        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.in_flight_worker_messages, 0);
        assert!(state.checked_transactions.is_empty());
        assert_eq!(
            state.entry_verification.first_failure,
            Some(replay_block_status_reasons::INVALID_TRANSACTION),
        );
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::FAILED,
                reason: replay_block_status_reasons::INVALID_TRANSACTION,
            }),
        );
    }

    #[test]
    fn parsing_failure_sends_invalid_transaction() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            parsing_failed_check_response(),
        );

        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.in_flight_worker_messages, 0);
        assert!(state.checked_transactions.is_empty());
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::FAILED,
                reason: replay_block_status_reasons::INVALID_TRANSACTION,
            }),
        );
    }

    #[test]
    fn resolve_failure_sends_invalid_transaction() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            resolve_failed_check_response(),
        );

        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.in_flight_worker_messages, 0);
        assert!(state.checked_transactions.is_empty());
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::FAILED,
                reason: replay_block_status_reasons::INVALID_TRANSACTION,
            }),
        );
    }

    #[test]
    #[should_panic(expected = "replay CHECK worker response was not processed")]
    fn unprocessed_worker_check_panics() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_unprocessed_response(&mut workers[0], worker_message.batch);

        scheduler.service_worker_responses(1);
    }

    #[test]
    #[should_panic(expected = "unsupported replay worker response tag")]
    fn unsupported_worker_response_panics_before_check_metadata() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_unsupported_worker_response(&mut workers[0], worker_message.batch);

        scheduler.service_worker_responses(1);
    }

    #[test]
    #[should_panic(expected = "malformed replay CHECK worker response")]
    fn malformed_worker_check_response_panics_before_check_metadata() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_malformed_worker_check_response(&mut workers[0], worker_message.batch);

        scheduler.service_worker_responses(1);
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
        assert!(
            scheduler.scheduling_state_pool[0]
                .owned_transactions
                .is_empty()
        );
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
        assert!(state.owned_transactions.is_empty());

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
    fn aborted_slot_waits_for_worker_check_response_before_cleanup() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let start_hash = Hash::new_from_array([9; 32]);
        let entry_hash = next_tick_hash(&start_hash, 1);
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, entry_hash, 1),
                transaction_message(transaction),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_aborted_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_aborted_slots(1), 1);

        assert!(!scheduler.scheduling_states.contains_key(&42));
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
            scheduler.scheduling_state_pool[0]
                .owned_transactions
                .capacity(),
            POOLED_OWNED_TRANSACTIONS_CAPACITY,
        );
        assert_eq!(
            scheduler.scheduling_state_pool[0]
                .pending_transaction_checks
                .capacity(),
            POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY,
        );

        write_replay_messages(&mut replay_stage, [begin(43)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);

        assert!(scheduler.scheduling_state_pool.is_empty());
        let state = scheduler.scheduling_states.get(&43).unwrap();
        assert_eq!(state.slot, 43);
        assert!(state.entry_headers.is_empty());
        assert!(state.owned_transactions.is_empty());
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
            state.owned_transactions.reserve(128);
            state.pending_transaction_checks.reserve(128);
            assert!(state.entry_headers.capacity() >= 128);
            assert!(state.owned_transactions.capacity() >= 128);
            assert!(state.pending_transaction_checks.capacity() >= 128);
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
            pooled_state.owned_transactions.capacity(),
            POOLED_OWNED_TRANSACTIONS_CAPACITY,
        );
        assert_eq!(
            pooled_state.pending_transaction_checks.capacity(),
            POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY,
        );
    }
}
