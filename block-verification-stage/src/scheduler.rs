use {
    crate::{
        entry_hash_verifier::{
            EntryHashVerificationResult, EntryHashVerificationTask, EntryHashVerifier,
        },
        setup::{BlockVerificationStageSession, BlockVerificationStageWorkerSession},
    },
    agave_scheduler_bindings::{
        EntryHeader, PackToWorkerMessage, ReplayBankMessage, ReplayBlockStatusMessage,
        SharablePubkeys, SharableTransactionBatchRegion, SharableTransactionRegion,
        TransactionResponseRegion, WorkerToPackMessage,
        pack_message_flags::{self, check_flags, execution_flags},
        processed_codes, replay_bank_message_kinds, replay_block_status_codes,
        replay_block_status_reasons, replay_to_pack_message_types,
        worker_message_types::{
            CHECK_RESPONSE, CheckResponse, EXECUTION_RESPONSE, ExecutionResponse,
            not_included_reasons, parsing_and_sanitization_flags, resolve_flags,
            signature_verification_flags,
        },
    },
    agave_scheduling_utils::{
        pubkeys_ptr::PubkeysPtr,
        responses_region::{CheckResponsesPtr, ExecutionResponsesPtr},
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet},
        transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    },
    agave_transaction_view::transaction_view::{
        SanitizedTransactionView, UnsanitizedTransactionView,
    },
    slab::Slab,
    solana_entry::entry::EntryVerificationData,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet, VecDeque},
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
const TRANSACTION_EXECUTION_DISPATCH_LIMIT: usize = 1024;
const TRANSACTION_EXECUTION_SCAN_LIMIT: usize = 1024;
const MAX_OUTSTANDING_EXECUTIONS_PER_WORKER: usize = 16;
const WORKER_RESPONSE_LIMIT: usize = 1024;
const TERMINAL_SLOT_CLEANUP_LIMIT: usize = 1024;
const SCHEDULING_STATE_POOL_LIMIT: usize = 5;
const POOLED_ENTRY_HEADERS_CAPACITY: usize = 0;
const POOLED_TRANSACTIONS_CAPACITY: usize = 0;
const POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY: usize = 0;
const POOLED_UNSCHEDULABLE_LOCKS_CAPACITY: usize = 0;
const CHECK_TRANSACTION_BATCH_ALLOCATION_SIZE: u32 =
    TransactionPtrBatch::<PendingWorkerCheck>::TRANSACTION_META_END as u32;
const EXECUTION_TRANSACTION_BATCH_ALLOCATION_SIZE: u32 =
    TransactionPtrBatch::<PendingWorkerExecution>::TRANSACTION_META_END as u32;
const REPLAY_TRANSACTION_CHECK_FLAGS: u16 = pack_message_flags::CHECK
    | check_flags::VERIFY_SIGNATURES
    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES
    | check_flags::ESTIMATE_COST
    | check_flags::REPLAY;
const REPLAY_TRANSACTION_EXECUTION_FLAGS: u16 =
    pack_message_flags::EXECUTE | execution_flags::REPLAY;

fn is_check_response_region(
    batch: SharableTransactionBatchRegion,
    responses: TransactionResponseRegion,
) -> bool {
    batch.num_transactions == 1
        && responses.tag == CHECK_RESPONSE
        && responses.num_transaction_responses == batch.num_transactions
}

fn is_execution_response_region(
    batch: SharableTransactionBatchRegion,
    responses: TransactionResponseRegion,
) -> bool {
    batch.num_transactions == 1
        && responses.tag == EXECUTION_RESPONSE
        && responses.num_transaction_responses == batch.num_transactions
}

fn check_response_is_invalid(response: &CheckResponse) -> bool {
    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0
        || response.signature_verification_flags & signature_verification_flags::FAILED != 0
        || response.signature_verification_flags & signature_verification_flags::PERFORMED == 0
        || response.resolve_flags & resolve_flags::FAILED != 0
        || response.resolve_flags & resolve_flags::PERFORMED == 0
}

fn execution_response_is_invalid(response: &ExecutionResponse) -> bool {
    response.not_included_reason != not_included_reasons::NONE
}

/// Main block verification scheduler.
pub struct BlockVerificationScheduler {
    exit: Arc<AtomicBool>,
    session: BlockVerificationStageSession,
    scheduling_states: HashMap<u64, SchedulingState>,
    scheduling_state_pool: Vec<SchedulingState>,
    terminal_slot_queue: VecDeque<u64>,
    entry_hash_verifier: EntryHashVerifier,
    in_flight_execution_messages: usize,
    in_flight_executions_per_thread: Vec<usize>,
}

struct SchedulingState {
    slot: u64,
    last_entry_hash: Hash,
    entry_headers: Vec<EntryHeader>,
    transactions: Slab<TransactionState>,
    account_locks: ThreadAwareAccountLocks,
    pending_transaction_checks: VecDeque<PendingTransactionCheck>,
    next_ready_transaction_index: usize,
    ready_transactions: VecDeque<usize>,
    unschedulable_read_locks: HashSet<Pubkey>,
    unschedulable_write_locks: HashSet<Pubkey>,
    ingress_complete: bool,
    entry_verification: EntryVerificationProgress,
    in_flight_worker_messages: usize,
    in_flight_execution_messages: usize,
    terminal_status: Option<SlotTerminalStatus>,
}

impl SchedulingState {
    fn new(slot: u64, last_entry_hash: Hash, worker_count: usize) -> Self {
        Self {
            slot,
            last_entry_hash,
            entry_headers: Vec::new(),
            transactions: Slab::new(),
            account_locks: ThreadAwareAccountLocks::new(worker_count),
            pending_transaction_checks: VecDeque::new(),
            next_ready_transaction_index: 0,
            ready_transactions: VecDeque::new(),
            unschedulable_read_locks: HashSet::new(),
            unschedulable_write_locks: HashSet::new(),
            ingress_complete: false,
            entry_verification: EntryVerificationProgress::default(),
            in_flight_worker_messages: 0,
            in_flight_execution_messages: 0,
            terminal_status: None,
        }
    }

    fn reset_for_slot(&mut self, slot: u64, last_entry_hash: Hash, worker_count: usize) {
        self.slot = slot;
        self.last_entry_hash = last_entry_hash;
        self.entry_headers.clear();
        self.transactions.clear();
        self.account_locks = ThreadAwareAccountLocks::new(worker_count);
        self.pending_transaction_checks.clear();
        self.next_ready_transaction_index = 0;
        self.ready_transactions.clear();
        self.unschedulable_read_locks.clear();
        self.unschedulable_write_locks.clear();
        self.ingress_complete = false;
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.in_flight_execution_messages = 0;
        self.terminal_status = None;
    }

    fn clear_for_pool(&mut self) {
        self.slot = 0;
        self.last_entry_hash = Hash::default();
        self.entry_headers = Vec::with_capacity(POOLED_ENTRY_HEADERS_CAPACITY);
        self.transactions = Slab::with_capacity(POOLED_TRANSACTIONS_CAPACITY);
        self.account_locks = ThreadAwareAccountLocks::new(1);
        self.pending_transaction_checks =
            VecDeque::with_capacity(POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY);
        self.next_ready_transaction_index = 0;
        self.ready_transactions = VecDeque::new();
        self.unschedulable_read_locks = HashSet::with_capacity(POOLED_UNSCHEDULABLE_LOCKS_CAPACITY);
        self.unschedulable_write_locks =
            HashSet::with_capacity(POOLED_UNSCHEDULABLE_LOCKS_CAPACITY);
        self.ingress_complete = false;
        self.entry_verification = EntryVerificationProgress::default();
        self.in_flight_worker_messages = 0;
        self.in_flight_execution_messages = 0;
        self.terminal_status = None;
    }

    fn accepts_ingress(&self) -> bool {
        !self.ingress_complete && self.terminal_status.is_none()
    }

    fn allows_transaction_processing(&self) -> bool {
        matches!(
            self.terminal_status,
            None | Some(SlotTerminalStatus::Success)
        )
    }

    fn promote_ready_transactions(&mut self) {
        while self
            .transactions
            .get(self.next_ready_transaction_index)
            .is_some_and(TransactionState::is_checked)
        {
            self.ready_transactions
                .push_back(self.next_ready_transaction_index);
            self.next_ready_transaction_index += 1;
        }
    }

    fn service_transaction_execution_dispatches(
        &mut self,
        dispatch_context: &mut ExecutionDispatchContext<'_>,
        max_executions: usize,
        max_scanned_transactions: usize,
    ) -> ExecutionDispatchCounts {
        if max_executions == 0
            || max_scanned_transactions == 0
            || !self.allows_transaction_processing()
        {
            return ExecutionDispatchCounts::default();
        }

        self.prune_scheduled_ready_prefix();
        if self.ready_transactions.is_empty() {
            return ExecutionDispatchCounts::default();
        }

        self.unschedulable_read_locks.clear();
        self.unschedulable_write_locks.clear();
        let mut ready_transaction_index = 0;
        let mut counts = ExecutionDispatchCounts::default();
        while ready_transaction_index < self.ready_transactions.len()
            && counts.scanned < max_scanned_transactions
            && counts.scheduled < max_executions
            && dispatch_context.has_capacity()
        {
            let transaction_index = *self
                .ready_transactions
                .get(ready_transaction_index)
                .expect("ready transaction index must be in-bounds");
            match self.try_dispatch_ready_transaction(transaction_index, dispatch_context) {
                ReadyTransactionDispatchResult::AlreadyScheduled => {}
                ReadyTransactionDispatchResult::Deferred => {
                    counts.scanned += 1;
                }
                ReadyTransactionDispatchResult::Scheduled => {
                    counts.scanned += 1;
                    counts.scheduled += 1;
                }
            }
            ready_transaction_index += 1;
        }

        self.prune_scheduled_ready_prefix();
        self.unschedulable_read_locks.clear();
        self.unschedulable_write_locks.clear();

        counts
    }

    fn try_dispatch_ready_transaction(
        &mut self,
        transaction_index: usize,
        dispatch_context: &mut ExecutionDispatchContext<'_>,
    ) -> ReadyTransactionDispatchResult {
        let slot = self.slot;
        let thread_id = {
            let transaction = self
                .transactions
                .get(transaction_index)
                .expect("ready transaction must exist");

            if transaction.is_in_flight_or_executed() {
                return ReadyTransactionDispatchResult::AlreadyScheduled;
            }
            assert!(
                transaction.is_checked(),
                "ready transaction must be checked or already scheduled",
            );

            if transaction.conflicts_with_unschedulable_locks(
                &self.unschedulable_read_locks,
                &self.unschedulable_write_locks,
            ) {
                transaction.record_unschedulable_locks(
                    &mut self.unschedulable_read_locks,
                    &mut self.unschedulable_write_locks,
                );
                return ReadyTransactionDispatchResult::Deferred;
            }

            let Some(thread_id) = dispatch_context.try_dispatch_transaction_execution(
                slot,
                transaction_index,
                transaction,
                &mut self.account_locks,
            ) else {
                transaction.record_unschedulable_locks(
                    &mut self.unschedulable_read_locks,
                    &mut self.unschedulable_write_locks,
                );
                return ReadyTransactionDispatchResult::Deferred;
            };
            thread_id
        };

        self.move_checked_transaction_to_in_flight(transaction_index, thread_id);
        ReadyTransactionDispatchResult::Scheduled
    }

    fn prune_scheduled_ready_prefix(&mut self) {
        while self
            .ready_transactions
            .front()
            .is_some_and(|transaction_index| {
                self.transactions
                    .get(*transaction_index)
                    .expect("ready transaction must exist")
                    .is_in_flight_or_executed()
            })
        {
            self.ready_transactions.pop_front();
        }
    }

    #[cfg(test)]
    fn try_lock_ready_transaction(
        &mut self,
        transaction_index: usize,
        available_threads: ThreadSet,
        thread_selector: impl FnOnce(ThreadSet) -> ThreadId,
    ) -> Option<ThreadId> {
        let Self {
            transactions,
            account_locks,
            ..
        } = self;
        let transaction = transactions
            .get(transaction_index)
            .expect("ready transaction must exist");
        account_locks
            .try_lock_accounts(
                transaction.write_locks(),
                transaction.read_locks(),
                available_threads,
                thread_selector,
            )
            .ok()
    }

    fn unlock_transaction_accounts(&mut self, transaction_index: usize, thread_id: ThreadId) {
        let Self {
            transactions,
            account_locks,
            ..
        } = self;
        let transaction = transactions
            .get(transaction_index)
            .expect("locked transaction must exist");
        account_locks.unlock_accounts(
            transaction.write_locks(),
            transaction.read_locks(),
            thread_id,
        );
    }

    fn move_checked_transaction_to_in_flight(
        &mut self,
        transaction_index: usize,
        thread_id: ThreadId,
    ) {
        let transaction_state = self
            .transactions
            .get_mut(transaction_index)
            .expect("scheduled transaction must exist");
        let previous_transaction_state =
            core::mem::replace(transaction_state, TransactionState::Transitioning);
        let TransactionState::Checked {
            transaction,
            resolved_pubkeys,
            scheduling_metadata,
            check_response,
        } = previous_transaction_state
        else {
            panic!("scheduled transaction was not checked: {transaction_index}");
        };
        *transaction_state = TransactionState::InFlight {
            transaction,
            resolved_pubkeys,
            scheduling_metadata,
            check_response,
            thread_id,
        };
        self.in_flight_execution_messages += 1;
    }

    fn finish_in_flight_transaction(
        &mut self,
        transaction_index: usize,
        thread_id: ThreadId,
    ) -> TransactionState {
        self.unlock_transaction_accounts(transaction_index, thread_id);

        let previous_transaction_state = {
            let transaction_state = self
                .transactions
                .get_mut(transaction_index)
                .expect("executed transaction must exist");
            let previous_transaction_state =
                core::mem::replace(transaction_state, TransactionState::Executed);
            let state_thread_id = match &previous_transaction_state {
                TransactionState::InFlight { thread_id, .. } => *thread_id,
                _ => panic!("execution response for transaction that was not in-flight"),
            };
            assert_eq!(
                state_thread_id, thread_id,
                "execution response thread id mismatch",
            );
            previous_transaction_state
        };

        self.in_flight_execution_messages = self
            .in_flight_execution_messages
            .checked_sub(1)
            .expect("execution response without in-flight execution");
        self.prune_scheduled_ready_prefix();

        previous_transaction_state
    }
}

fn select_execution_thread(
    thread_set: ThreadSet,
    in_flight_executions_per_thread: &[usize],
) -> ThreadId {
    thread_set
        .contained_threads_iter()
        .min_by_key(|thread_id| (in_flight_executions_per_thread[*thread_id], *thread_id))
        .expect("schedulable thread set must not be empty")
}

struct ExecutionDispatchContext<'a> {
    allocator: &'a rts_alloc::Allocator,
    workers: &'a mut [BlockVerificationStageWorkerSession],
    in_flight_execution_messages: &'a mut usize,
    in_flight_executions_per_thread: &'a mut [usize],
}

#[derive(Default)]
struct ExecutionDispatchCounts {
    scheduled: usize,
    scanned: usize,
}

enum ReadyTransactionDispatchResult {
    AlreadyScheduled,
    Deferred,
    Scheduled,
}

impl ExecutionDispatchContext<'_> {
    fn has_capacity(&self) -> bool {
        self.dispatch_capacity() != 0
    }

    fn dispatch_capacity(&self) -> usize {
        self.max_outstanding_execution_messages()
            .saturating_sub(*self.in_flight_execution_messages)
    }

    fn max_outstanding_execution_messages(&self) -> usize {
        self.workers.len() * MAX_OUTSTANDING_EXECUTIONS_PER_WORKER
    }

    fn try_dispatch_transaction_execution(
        &mut self,
        slot: u64,
        transaction_index: usize,
        transaction: &TransactionState,
        account_locks: &mut ThreadAwareAccountLocks,
    ) -> Option<ThreadId> {
        if !self.has_capacity() {
            return None;
        }

        let available_threads = self.available_worker_threads();
        if available_threads.is_empty() {
            return None;
        }

        let Ok(thread_id) = account_locks.try_lock_accounts(
            transaction.write_locks(),
            transaction.read_locks(),
            available_threads,
            |thread_set| select_execution_thread(thread_set, self.in_flight_executions_per_thread),
        ) else {
            return None;
        };

        let Some(batch) = self.allocate_transaction_execution_batch(
            slot,
            transaction_index,
            thread_id,
            transaction,
        ) else {
            account_locks.unlock_accounts(
                transaction.write_locks(),
                transaction.read_locks(),
                thread_id,
            );
            return None;
        };

        let message = PackToWorkerMessage {
            flags: REPLAY_TRANSACTION_EXECUTION_FLAGS,
            max_working_slot: slot,
            batch,
        };
        if let Err(returned_message) = self.workers[thread_id].pack_to_worker.try_write(message) {
            self.free_transaction_batch_allocation(returned_message.batch);
            account_locks.unlock_accounts(
                transaction.write_locks(),
                transaction.read_locks(),
                thread_id,
            );
            return None;
        }
        self.workers[thread_id].pack_to_worker.commit();

        self.increment_in_flight_execution_messages(thread_id);
        Some(thread_id)
    }

    fn available_worker_threads(&mut self) -> ThreadSet {
        let mut available_threads = ThreadSet::none();
        for (worker_index, worker) in self.workers.iter_mut().enumerate() {
            let queue = &mut worker.pack_to_worker;
            queue.sync();
            if queue.len() < queue.capacity()
                && self.in_flight_executions_per_thread[worker_index]
                    < MAX_OUTSTANDING_EXECUTIONS_PER_WORKER
            {
                available_threads.insert(worker_index);
            }
        }

        available_threads
    }

    fn increment_in_flight_execution_messages(&mut self, thread_id: ThreadId) {
        *self.in_flight_execution_messages += 1;
        self.in_flight_executions_per_thread[thread_id] += 1;
    }

    fn allocate_transaction_execution_batch(
        &self,
        slot: u64,
        transaction_index: usize,
        thread_id: ThreadId,
        transaction: &TransactionState,
    ) -> Option<SharableTransactionBatchRegion> {
        let ptr = self
            .allocator
            .allocate(EXECUTION_TRANSACTION_BATCH_ALLOCATION_SIZE)?;
        // SAFETY: `ptr` was allocated by this scheduler's allocator above.
        let transactions_offset = unsafe { self.allocator.offset(ptr) };
        let batch_ptr = ptr.cast::<SharableTransactionRegion>();

        // SAFETY: The allocation size is
        // `TransactionPtrBatch::<PendingWorkerExecution>::TRANSACTION_META_END`,
        // and `TRANSACTION_META_START` is the aligned offset for the metadata
        // region within that allocation.
        let meta_ptr = unsafe {
            ptr.byte_add(TransactionPtrBatch::<PendingWorkerExecution>::TRANSACTION_META_START)
                .cast::<PendingWorkerExecution>()
        };

        let transaction = self.transaction_region_for_execution(transaction);
        unsafe {
            // SAFETY: EXECUTION dispatches intentionally send one transaction
            // per worker message, so writing the first transaction region is
            // in-bounds.
            batch_ptr.as_ptr().write(transaction);
            // SAFETY: `meta_ptr` points at the first metadata slot computed
            // from `TransactionPtrBatch`'s layout.
            meta_ptr.as_ptr().write(PendingWorkerExecution {
                slot,
                transaction_index,
                thread_id,
            });
        }

        Some(SharableTransactionBatchRegion {
            num_transactions: 1,
            transactions_offset,
        })
    }

    fn transaction_region_for_execution(
        &self,
        transaction: &TransactionState,
    ) -> SharableTransactionRegion {
        // SAFETY: `transactions` contains pointers constructed from regions
        // allocated by this scheduler's shared allocator.
        unsafe {
            transaction
                .transaction_ptr()
                .to_sharable_transaction_region(self.allocator)
        }
    }

    fn free_transaction_batch_allocation(&self, batch: SharableTransactionBatchRegion) {
        // SAFETY: Transaction batch regions are allocated by this scheduler
        // and remain scheduler-owned if dispatch fails before handing the
        // batch to a worker.
        unsafe {
            self.allocator.free_offset(batch.transactions_offset);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotTerminalStatus {
    Success,
    Failed(u16),
    Aborted,
}

impl SlotTerminalStatus {
    fn into_replay_block_status(self, slot: u64) -> ReplayBlockStatusMessage {
        match self {
            Self::Success => ReplayBlockStatusMessage {
                slot,
                status: replay_block_status_codes::SUCCESS,
                reason: replay_block_status_reasons::NONE,
            },
            Self::Failed(reason) => ReplayBlockStatusMessage {
                slot,
                status: replay_block_status_codes::FAILED,
                reason,
            },
            Self::Aborted => ReplayBlockStatusMessage {
                slot,
                status: replay_block_status_codes::ABORTED,
                reason: replay_block_status_reasons::NONE,
            },
        }
    }
}

struct FinishedSlotStatus {
    message: ReplayBlockStatusMessage,
}

#[derive(Clone, Copy)]
struct PendingTransactionCheck {
    transaction_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct PendingWorkerCheck {
    slot: u64,
    transaction_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct PendingWorkerExecution {
    slot: u64,
    transaction_index: usize,
    thread_id: ThreadId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionCostMetadata {
    cost_model_flags: u8,
    estimated_cost_units: u64,
    allocated_accounts_data_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionSchedulingMetadata {
    cost: TransactionCostMetadata,
    writable_account_bitfields: [u64; 4],
}

impl TransactionSchedulingMetadata {
    fn from_check_response(response: &CheckResponse) -> Self {
        Self {
            cost: TransactionCostMetadata {
                cost_model_flags: response.cost_model_flags,
                estimated_cost_units: response.estimated_cost_units,
                allocated_accounts_data_size: response.allocated_accounts_data_size,
            },
            writable_account_bitfields: response.writable_account_bitfields,
        }
    }

    fn is_writable(&self, index: usize) -> bool {
        let bitfield = self
            .writable_account_bitfields
            .get(index / 64)
            .expect("account index must fit in CHECK writable account bitfields");
        bitfield & (1u64 << (index % 64)) != 0
    }
}

enum TransactionState {
    Pending {
        transaction: TransactionPtr,
    },
    Checked {
        transaction: SanitizedTransactionView<TransactionPtr>,
        resolved_pubkeys: Option<PubkeysPtr>,
        scheduling_metadata: TransactionSchedulingMetadata,
        check_response: CheckResponse,
    },
    InFlight {
        transaction: SanitizedTransactionView<TransactionPtr>,
        resolved_pubkeys: Option<PubkeysPtr>,
        scheduling_metadata: TransactionSchedulingMetadata,
        check_response: CheckResponse,
        thread_id: ThreadId,
    },
    Executed,
    Transitioning,
}

impl TransactionState {
    fn is_checked(&self) -> bool {
        matches!(self, Self::Checked { .. })
    }

    fn is_in_flight_or_executed(&self) -> bool {
        matches!(self, Self::InFlight { .. } | Self::Executed)
    }

    fn transaction_ptr(&self) -> &TransactionPtr {
        match self {
            Self::Pending { transaction } => transaction,
            Self::Checked { transaction, .. } => transaction.inner_data(),
            Self::InFlight { transaction, .. } => transaction.inner_data(),
            Self::Executed => panic!("transaction state is executed"),
            Self::Transitioning => panic!("transaction state is transitioning"),
        }
    }

    fn transaction_view(&self) -> &SanitizedTransactionView<TransactionPtr> {
        match self {
            Self::Checked { transaction, .. } | Self::InFlight { transaction, .. } => transaction,
            Self::Pending { .. } => panic!("transaction state is pending"),
            Self::Executed => panic!("transaction state is executed"),
            Self::Transitioning => panic!("transaction state is transitioning"),
        }
    }

    fn resolved_pubkeys_slice(&self) -> &[Pubkey] {
        match self {
            Self::Checked {
                resolved_pubkeys, ..
            }
            | Self::InFlight {
                resolved_pubkeys, ..
            } => resolved_pubkeys
                .as_ref()
                .map(PubkeysPtr::as_slice)
                .unwrap_or_default(),
            Self::Pending { .. } => panic!("transaction state is pending"),
            Self::Executed => panic!("transaction state is executed"),
            Self::Transitioning => panic!("transaction state is transitioning"),
        }
    }

    fn scheduling_metadata(&self) -> &TransactionSchedulingMetadata {
        match self {
            Self::Checked {
                scheduling_metadata,
                ..
            }
            | Self::InFlight {
                scheduling_metadata,
                ..
            } => scheduling_metadata,
            Self::Pending { .. } => panic!("transaction state is pending"),
            Self::Executed => panic!("transaction state is executed"),
            Self::Transitioning => panic!("transaction state is transitioning"),
        }
    }

    fn account_keys(&self) -> impl Iterator<Item = &Pubkey> + Clone {
        self.transaction_view()
            .static_account_keys()
            .iter()
            .chain(self.resolved_pubkeys_slice().iter())
    }

    fn write_locks(&self) -> impl Iterator<Item = &Pubkey> + Clone {
        self.account_keys()
            .enumerate()
            .filter(|(index, _)| self.is_writable(*index))
            .map(|(_, key)| key)
    }

    fn read_locks(&self) -> impl Iterator<Item = &Pubkey> + Clone {
        self.account_keys()
            .enumerate()
            .filter(|(index, _)| !self.is_writable(*index))
            .map(|(_, key)| key)
    }

    fn conflicts_with_unschedulable_locks(
        &self,
        unschedulable_read_locks: &HashSet<Pubkey>,
        unschedulable_write_locks: &HashSet<Pubkey>,
    ) -> bool {
        self.write_locks().any(|write_lock| {
            unschedulable_write_locks.contains(write_lock)
                || unschedulable_read_locks.contains(write_lock)
        }) || self
            .read_locks()
            .any(|read_lock| unschedulable_write_locks.contains(read_lock))
    }

    fn record_unschedulable_locks(
        &self,
        unschedulable_read_locks: &mut HashSet<Pubkey>,
        unschedulable_write_locks: &mut HashSet<Pubkey>,
    ) {
        unschedulable_write_locks.extend(self.write_locks().copied());
        unschedulable_read_locks.extend(self.read_locks().copied());
    }

    fn is_writable(&self, index: usize) -> bool {
        self.scheduling_metadata().is_writable(index)
    }
}

#[derive(Default)]
struct EntryVerificationProgress {
    pending_jobs: usize,
}

impl BlockVerificationScheduler {
    pub fn new(
        exit: Arc<AtomicBool>,
        session: BlockVerificationStageSession,
        entry_verification_threads: NonZeroUsize,
    ) -> Self {
        assert!(
            !session.workers.is_empty(),
            "block verification scheduler requires at least one worker",
        );
        let worker_count = session.workers.len();
        Self {
            exit,
            session,
            scheduling_states: HashMap::new(),
            scheduling_state_pool: Vec::new(),
            terminal_slot_queue: VecDeque::new(),
            entry_hash_verifier: EntryHashVerifier::new(entry_verification_threads),
            in_flight_execution_messages: 0,
            in_flight_executions_per_thread: vec![0; worker_count],
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
            let transaction_execution_dispatch_count = self
                .service_transaction_execution_dispatches(
                    TRANSACTION_EXECUTION_DISPATCH_LIMIT,
                    TRANSACTION_EXECUTION_SCAN_LIMIT,
                );
            let terminal_cleanup_count = self.service_terminal_slots(TERMINAL_SLOT_CLEANUP_LIMIT);
            if ingress_count == 0
                && entry_verification_count == 0
                && signature_check_dispatch_count == 0
                && worker_response_count == 0
                && transaction_execution_dispatch_count == 0
                && terminal_cleanup_count == 0
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
    /// first invalid result marks the slot terminal with
    /// `FAILED / INVALID_ENTRY_HASH`.
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
            self.mark_slot_failed(slot, replay_block_status_reasons::INVALID_ENTRY_HASH);
        }
    }

    /// Attempt cleanup for queued terminal slots.
    ///
    /// Terminal markers such as failures and aborts only append slots to the
    /// cleanup queue. This method is the only path that drops terminal slot
    /// state and sends a `ReplayBlockStatusMessage`. A slot is cleaned up only
    /// after all retained scheduler-owned work has returned, including entry
    /// hash verification jobs and worker messages. Slots that are still
    /// waiting on work are requeued for a later scheduler loop iteration.
    ///
    /// Returns the number of terminal slots fully cleaned up and reported.
    fn service_terminal_slots(&mut self, max_slots: usize) -> usize {
        if max_slots == 0 {
            return 0;
        }

        let mut cleaned = 0;
        let slots_to_check = max_slots.min(self.terminal_slot_queue.len());
        for _ in 0..slots_to_check {
            let slot = self.terminal_slot_queue.pop_front().unwrap();
            if let Some(status) = self.try_finish_terminal_slot(slot) {
                self.send_replay_block_status(status);
                cleaned += 1;
            } else if self
                .scheduling_states
                .get(&slot)
                .is_some_and(|state| state.terminal_status.is_some())
            {
                self.terminal_slot_queue.push_back(slot);
            }
        }

        cleaned
    }

    fn handle_bank_message(&mut self, message: ReplayBankMessage) {
        match message.kind {
            replay_bank_message_kinds::BEGIN => {
                self.handle_bank_begin(message.slot, Hash::new_from_array(message.last_entry_hash))
            }
            replay_bank_message_kinds::COMPLETE => self.handle_bank_complete(message.slot),
            replay_bank_message_kinds::ABORT => self.handle_bank_abort(message.slot),
            kind => panic!("unknown replay bank message kind: {kind}"),
        }
    }

    fn handle_bank_begin(&mut self, slot: u64, last_entry_hash: Hash) {
        assert!(
            !self.scheduling_states.contains_key(&slot),
            "slot already has scheduling state: {slot}",
        );

        let worker_count = self.worker_count();
        let mut state = self
            .scheduling_state_pool
            .pop()
            .unwrap_or_else(|| SchedulingState::new(slot, last_entry_hash, worker_count));
        state.reset_for_slot(slot, last_entry_hash, worker_count);

        let previous = self.scheduling_states.insert(slot, state);
        assert!(
            previous.is_none(),
            "slot already has scheduling state: {slot}"
        );
    }

    fn handle_bank_complete(&mut self, slot: u64) {
        let state = self
            .scheduling_states
            .get_mut(&slot)
            .expect("complete received for unknown slot");
        assert!(
            !state.ingress_complete,
            "duplicate complete received for slot: {slot}",
        );
        state.ingress_complete = true;
        if state.terminal_status.is_none() {
            state.terminal_status = Some(SlotTerminalStatus::Success);
            self.terminal_slot_queue.push_back(slot);
        }
    }

    fn handle_bank_abort(&mut self, slot: u64) {
        self.mark_slot_terminal(slot, SlotTerminalStatus::Aborted);
    }

    fn handle_entry(&mut self, entry_header: EntryHeader) -> usize {
        let slot = entry_header.slot;
        assert!(
            !self.is_slot_ingress_complete(slot),
            "entry received after complete for slot: {slot}",
        );
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
            self.free_transaction_region_allocation(transaction);
            return false;
        }

        // SAFETY: Replay transaction messages transfer ownership of valid
        // shared-memory regions to the scheduler. The resulting pointer is
        // retained in scheduling state until the slot is cleaned up.
        let transaction = unsafe {
            TransactionPtr::from_sharable_transaction_region(&transaction, &self.session.allocator)
        };

        let state = self.scheduling_state_mut(slot);
        let transaction_index = state.transactions.len();
        let transaction_key = state
            .transactions
            .insert(TransactionState::Pending { transaction });
        assert_eq!(
            transaction_key, transaction_index,
            "slab key must match ingress transaction index",
        );
        state
            .pending_transaction_checks
            .push_back(PendingTransactionCheck { transaction_index });
        true
    }

    fn service_transaction_check_dispatches(&mut self, max_checks: usize) -> usize {
        if max_checks == 0 {
            return 0;
        }

        let mut dispatched = 0;
        let mut last_slot = None;
        while dispatched < max_checks {
            let Some(slot) = self.next_pending_check_slot_after(last_slot) else {
                break;
            };
            last_slot = Some(slot);
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

    fn next_pending_check_slot_after(&self, last_slot: Option<u64>) -> Option<u64> {
        self.scheduling_states
            .iter()
            .filter(|(slot, state)| {
                last_slot.is_none_or(|last_slot| **slot > last_slot)
                    && state.allows_transaction_processing()
                    && !state.pending_transaction_checks.is_empty()
            })
            .map(|(slot, _)| *slot)
            .min()
    }

    fn has_pending_transaction_checks(&self, slot: u64) -> bool {
        self.scheduling_states
            .get(&slot)
            .filter(|state| state.allows_transaction_processing())
            .is_some_and(|state| !state.pending_transaction_checks.is_empty())
    }

    fn pending_transaction_check(&self, slot: u64) -> Option<PendingTransactionCheck> {
        let state = self.scheduling_states.get(&slot)?;
        if !state.allows_transaction_processing() {
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
            .allocate(CHECK_TRANSACTION_BATCH_ALLOCATION_SIZE)?;
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
        let transaction = self.transaction_region_for_check(slot, pending_check);
        unsafe {
            batch_ptr.as_ptr().write(transaction);
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

    fn transaction_region_for_check(
        &self,
        slot: u64,
        pending_check: PendingTransactionCheck,
    ) -> SharableTransactionRegion {
        let state = self
            .scheduling_states
            .get(&slot)
            .expect("transaction check dispatch for unknown slot");
        let transaction = state
            .transactions
            .get(pending_check.transaction_index)
            .expect("transaction check dispatch for unknown transaction");

        // SAFETY: `transactions` contains pointers constructed from regions
        // allocated by this scheduler's shared allocator.
        unsafe {
            transaction
                .transaction_ptr()
                .to_sharable_transaction_region(&self.session.allocator)
        }
    }

    pub fn service_transaction_execution_dispatches(
        &mut self,
        max_executions: usize,
        max_scanned_transactions: usize,
    ) -> usize {
        if max_executions == 0 || max_scanned_transactions == 0 {
            return 0;
        }

        let mut dispatch_context = ExecutionDispatchContext {
            allocator: &self.session.allocator,
            workers: &mut self.session.workers,
            in_flight_execution_messages: &mut self.in_flight_execution_messages,
            in_flight_executions_per_thread: &mut self.in_flight_executions_per_thread,
        };
        if !dispatch_context.has_capacity() {
            return 0;
        }

        let mut counts = ExecutionDispatchCounts::default();
        for state in self.scheduling_states.values_mut() {
            if counts.scheduled == max_executions
                || counts.scanned == max_scanned_transactions
                || !dispatch_context.has_capacity()
            {
                break;
            }
            let state_counts = state.service_transaction_execution_dispatches(
                &mut dispatch_context,
                max_executions - counts.scheduled,
                max_scanned_transactions - counts.scanned,
            );
            counts.scheduled += state_counts.scheduled;
            counts.scanned += state_counts.scanned;
        }

        counts.scheduled
    }

    /// Drain completed worker responses.
    ///
    /// All worker response queues are synchronized before any response is
    /// handled, then responses are consumed in a bounded round-robin pass
    /// across workers. Empty worker queues do not stop the pass; servicing
    /// stops after `max_responses` responses or after a full worker cycle
    /// produces no response.
    ///
    /// Returns the number of worker responses consumed.
    fn service_worker_responses(&mut self, max_responses: usize) -> usize {
        if max_responses == 0 {
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
            "replay worker response was not processed",
        );

        match message.responses.tag {
            CHECK_RESPONSE => self.handle_worker_check_response(message),
            EXECUTION_RESPONSE => self.handle_worker_execution_response(message),
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

        let check_responses = self.check_response_ptr(message.responses);
        let check_response = Self::read_check_response(&check_responses);
        self.free_check_response_region(check_responses);
        self.free_transaction_batch_allocation(message.batch);

        if check_response_is_invalid(&check_response) {
            self.free_check_response_allocations(check_response);
            self.mark_slot_failed(slot, replay_block_status_reasons::INVALID_TRANSACTION);
        } else {
            self.record_successful_check(worker_check, check_response);
        }

        self.decrement_in_flight_worker_messages(slot);
    }

    fn handle_worker_execution_response(&mut self, message: WorkerToPackMessage) {
        assert!(
            is_execution_response_region(message.batch, message.responses),
            "malformed replay EXECUTION worker response",
        );

        let worker_execution = self.worker_execution_metadata(message.batch);
        let slot = worker_execution.slot;

        let execution_responses = self.execution_response_ptr(message.responses);
        let execution_response = Self::read_execution_response(&execution_responses);
        self.free_execution_response_region(execution_responses);
        self.free_transaction_batch_allocation(message.batch);

        let previous_transaction_state = self.finish_worker_execution(worker_execution);
        self.free_transaction_state_allocations(previous_transaction_state);
        self.decrement_in_flight_execution_messages(worker_execution.thread_id);

        if execution_response_is_invalid(&execution_response) {
            self.mark_slot_failed(slot, replay_block_status_reasons::INVALID_TRANSACTION);
        }
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

    fn worker_execution_metadata(
        &self,
        batch: SharableTransactionBatchRegion,
    ) -> PendingWorkerExecution {
        let ptr_batch = unsafe {
            // SAFETY: EXECUTION dispatch allocates every worker batch with
            // `TransactionPtrBatch<PendingWorkerExecution>` layout.
            TransactionPtrBatch::<PendingWorkerExecution>::from_sharable_transaction_batch_region(
                &batch,
                &self.session.allocator,
            )
        };
        assert_eq!(
            ptr_batch.len(),
            1,
            "replay EXECUTION batches are one transaction",
        );
        let metadata = ptr_batch.iter().next().unwrap().1;

        metadata
    }

    fn check_response_ptr(&self, responses: TransactionResponseRegion) -> CheckResponsesPtr {
        unsafe {
            // SAFETY: Caller validated that `responses` is a CHECK_RESPONSE
            // region with one response allocated by the shared allocator.
            CheckResponsesPtr::from_transaction_response_region(&responses, &self.session.allocator)
        }
    }

    fn read_check_response(responses: &CheckResponsesPtr) -> CheckResponse {
        let response = *responses.iter().next().unwrap();

        response
    }

    fn free_check_response_region(&self, responses: CheckResponsesPtr) {
        unsafe {
            // SAFETY: The response region is exclusively owned by this
            // scheduler after the worker returned the response message.
            responses.free(&self.session.allocator);
        }
    }

    fn execution_response_ptr(
        &self,
        responses: TransactionResponseRegion,
    ) -> ExecutionResponsesPtr {
        unsafe {
            // SAFETY: Caller validated that `responses` is an
            // EXECUTION_RESPONSE region with one response allocated by the
            // shared allocator.
            ExecutionResponsesPtr::from_transaction_response_region(
                &responses,
                &self.session.allocator,
            )
        }
    }

    fn read_execution_response(responses: &ExecutionResponsesPtr) -> ExecutionResponse {
        *responses.iter().next().unwrap()
    }

    fn free_execution_response_region(&self, responses: ExecutionResponsesPtr) {
        unsafe {
            // SAFETY: The response region is exclusively owned by this
            // scheduler after the worker returned the response message.
            responses.free(&self.session.allocator);
        }
    }

    fn finish_worker_execution(
        &mut self,
        worker_execution: PendingWorkerExecution,
    ) -> TransactionState {
        self.scheduling_state_mut(worker_execution.slot)
            .finish_in_flight_transaction(
                worker_execution.transaction_index,
                worker_execution.thread_id,
            )
    }

    fn record_successful_check(
        &mut self,
        worker_check: PendingWorkerCheck,
        mut check_response: CheckResponse,
    ) {
        let should_retain = self
            .scheduling_states
            .get(&worker_check.slot)
            .is_some_and(|state| state.allows_transaction_processing());

        if !should_retain {
            self.free_check_response_allocations(check_response);
            return;
        }

        let scheduling_metadata =
            TransactionSchedulingMetadata::from_check_response(&check_response);
        let resolved_pubkeys = self.take_resolved_pubkeys(&mut check_response);
        let state = self.scheduling_state_mut(worker_check.slot);
        let transaction_state = state
            .transactions
            .get_mut(worker_check.transaction_index)
            .expect("successful check for unknown transaction");
        let previous_transaction_state =
            core::mem::replace(transaction_state, TransactionState::Transitioning);
        let TransactionState::Pending { transaction } = previous_transaction_state else {
            panic!(
                "successful check for transaction that was not pending: {}",
                worker_check.transaction_index,
            );
        };
        let transaction = SanitizedTransactionView::try_new_sanitized(transaction, true)
            .expect("successful CHECK transaction failed scheduler parse");
        *transaction_state = TransactionState::Checked {
            transaction,
            resolved_pubkeys,
            scheduling_metadata,
            check_response,
        };

        state.promote_ready_transactions();
    }

    fn take_resolved_pubkeys(&self, check_response: &mut CheckResponse) -> Option<PubkeysPtr> {
        if check_response.resolved_pubkeys.num_pubkeys == 0 {
            return None;
        }

        let resolved_pubkeys = check_response.resolved_pubkeys;
        check_response.resolved_pubkeys = SharablePubkeys {
            offset: 0,
            num_pubkeys: 0,
        };
        Some(unsafe {
            // SAFETY: Non-empty resolved pubkey regions are worker-returned
            // allocations now owned by this scheduler state.
            PubkeysPtr::from_sharable_pubkeys(&resolved_pubkeys, &self.session.allocator)
        })
    }

    fn decrement_in_flight_worker_messages(&mut self, slot: u64) {
        if let Some(state) = self.scheduling_states.get_mut(&slot) {
            state.in_flight_worker_messages = state
                .in_flight_worker_messages
                .checked_sub(1)
                .expect("worker response without in-flight worker message");
        }
    }

    fn decrement_in_flight_execution_messages(&mut self, thread_id: ThreadId) {
        self.in_flight_execution_messages = self
            .in_flight_execution_messages
            .checked_sub(1)
            .expect("execution response without in-flight execution");
        self.in_flight_executions_per_thread[thread_id] = self.in_flight_executions_per_thread
            [thread_id]
            .checked_sub(1)
            .expect("execution response without in-flight execution on thread");
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
            .accepts_ingress()
    }

    fn is_slot_ingress_complete(&self, slot: u64) -> bool {
        self.scheduling_states
            .get(&slot)
            .expect("replay ingress received for unknown slot")
            .ingress_complete
    }

    fn scheduling_state_mut(&mut self, slot: u64) -> &mut SchedulingState {
        self.scheduling_states
            .get_mut(&slot)
            .expect("replay ingress received for unknown slot")
    }

    fn worker_count(&self) -> usize {
        self.session.workers.len()
    }

    fn try_finish_terminal_slot(&mut self, slot: u64) -> Option<FinishedSlotStatus> {
        let Some(state) = self.scheduling_states.get(&slot) else {
            return None;
        };
        let terminal_status = state.terminal_status?;
        if !matches!(terminal_status, SlotTerminalStatus::Aborted) && !state.ingress_complete {
            return None;
        }
        if state.in_flight_worker_messages != 0
            || state.in_flight_execution_messages != 0
            || state.entry_verification.pending_jobs != 0
            || !state.pending_transaction_checks.is_empty()
            || !state.ready_transactions.is_empty()
        {
            return None;
        }

        let mut state = self.scheduling_states.remove(&slot).unwrap();
        self.free_scheduling_state_allocations(&mut state);
        if self.scheduling_state_pool.len() < SCHEDULING_STATE_POOL_LIMIT {
            state.clear_for_pool();
            self.scheduling_state_pool.push(state);
        }

        Some(FinishedSlotStatus {
            message: terminal_status.into_replay_block_status(slot),
        })
    }

    fn free_scheduling_state_allocations(&mut self, state: &mut SchedulingState) {
        for transaction_state in state.transactions.drain() {
            self.free_transaction_state_allocations(transaction_state);
        }
        state.ready_transactions.clear();
        state.unschedulable_read_locks.clear();
        state.unschedulable_write_locks.clear();
    }

    fn free_transaction_allocation(&mut self, transaction: TransactionPtr) {
        // SAFETY: Replay transaction messages transfer ownership to the
        // scheduler. We only call this for transactions still owned by this
        // scheduler state.
        unsafe {
            transaction.free(&self.session.allocator);
        }
    }

    fn free_transaction_state_allocations(&mut self, transaction_state: TransactionState) {
        match transaction_state {
            TransactionState::Pending { transaction } => {
                self.free_transaction_allocation(transaction);
            }
            TransactionState::Checked {
                transaction,
                resolved_pubkeys,
                check_response,
                ..
            }
            | TransactionState::InFlight {
                transaction,
                resolved_pubkeys,
                check_response,
                ..
            } => {
                self.free_transaction_allocation(transaction.into_inner_data());
                if let Some(resolved_pubkeys) = resolved_pubkeys {
                    self.free_pubkeys_ptr_allocation(resolved_pubkeys);
                }
                self.free_check_response_allocations(check_response);
            }
            TransactionState::Executed => {}
            TransactionState::Transitioning => {}
        }
    }

    fn free_transaction_region_allocation(&mut self, transaction: SharableTransactionRegion) {
        // SAFETY: Replay transaction messages transfer ownership to the
        // scheduler. We only call this for terminal-slot transactions we
        // intentionally drop instead of retaining.
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

    fn free_pubkeys_ptr_allocation(&mut self, pubkeys: PubkeysPtr) {
        unsafe {
            // SAFETY: `PubkeysPtr` values stored by transaction state are
            // exclusively owned by that state until cleanup.
            pubkeys.free(&self.session.allocator);
        }
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

    fn mark_slot_failed(&mut self, slot: u64, reason: u16) {
        let Some(state) = self.scheduling_states.get_mut(&slot) else {
            return;
        };
        match state.terminal_status {
            Some(SlotTerminalStatus::Failed(_) | SlotTerminalStatus::Aborted) => {}
            previous_status => {
                state.terminal_status = Some(SlotTerminalStatus::Failed(reason));
                state.pending_transaction_checks.clear();
                state.ready_transactions.clear();
                state.unschedulable_read_locks.clear();
                state.unschedulable_write_locks.clear();
                if previous_status.is_none() {
                    self.terminal_slot_queue.push_back(slot);
                }
            }
        }
    }

    fn mark_slot_terminal(&mut self, slot: u64, terminal_status: SlotTerminalStatus) {
        let Some(state) = self.scheduling_states.get_mut(&slot) else {
            return;
        };
        match state.terminal_status {
            Some(SlotTerminalStatus::Failed(_) | SlotTerminalStatus::Aborted) => {}
            previous_status => {
                state.terminal_status = Some(terminal_status);
                state.pending_transaction_checks.clear();
                state.ready_transactions.clear();
                state.unschedulable_read_locks.clear();
                state.unschedulable_write_locks.clear();
                if previous_status.is_none() {
                    self.terminal_slot_queue.push_back(slot);
                }
            }
        }
    }

    fn send_replay_block_status(&mut self, status: FinishedSlotStatus) {
        self.session
            .replay_block_status
            .try_write(status.message)
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
        agave_scheduler_bindings::{
            ReplayToPackMessage, ReplayToPackMessagePayload, worker_message_types::cost_model_flags,
        },
        agave_scheduling_utils::responses_region::{
            execution_responses_from_iter, resolve_responses_from_iter,
        },
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

    fn complete(slot: u64) -> ReplayToPackMessage {
        bank_message(replay_bank_message_kinds::COMPLETE, slot)
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

    fn transaction_batch_regions<'a>(
        allocator: &'a rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> &'a [SharableTransactionRegion] {
        let ptr = unsafe {
            allocator
                .ptr_from_offset(batch.transactions_offset)
                .cast::<SharableTransactionRegion>()
        };
        unsafe { core::slice::from_raw_parts(ptr.as_ptr(), usize::from(batch.num_transactions)) }
    }

    fn transaction_state_regions<'a>(
        allocator: &'a rts_alloc::Allocator,
        transactions: &'a Slab<TransactionState>,
    ) -> impl Iterator<Item = SharableTransactionRegion> + 'a {
        transactions.iter().map(|(_, transaction)| {
            // SAFETY: Test transaction pointers are constructed from
            // regions allocated by this shared allocator.
            unsafe {
                transaction
                    .transaction_ptr()
                    .to_sharable_transaction_region(allocator)
            }
        })
    }

    fn checked_transaction_state(
        state: &SchedulingState,
        transaction_index: usize,
    ) -> (
        &SanitizedTransactionView<TransactionPtr>,
        Option<&PubkeysPtr>,
        &CheckResponse,
    ) {
        let TransactionState::Checked {
            transaction,
            resolved_pubkeys,
            check_response,
            ..
        } = state
            .transactions
            .get(transaction_index)
            .expect("transaction state should exist")
        else {
            panic!("transaction state should be checked");
        };

        (transaction, resolved_pubkeys.as_ref(), check_response)
    }

    fn checked_transaction_scheduling_metadata(
        state: &SchedulingState,
        transaction_index: usize,
    ) -> &TransactionSchedulingMetadata {
        let TransactionState::Checked {
            scheduling_metadata,
            ..
        } = state
            .transactions
            .get(transaction_index)
            .expect("transaction state should exist")
        else {
            panic!("transaction state should be checked");
        };

        scheduling_metadata
    }

    fn in_flight_transaction_thread_id(
        state: &SchedulingState,
        transaction_index: usize,
    ) -> ThreadId {
        let TransactionState::InFlight { thread_id, .. } = state
            .transactions
            .get(transaction_index)
            .expect("transaction state should exist")
        else {
            panic!("transaction state should be in-flight");
        };

        *thread_id
    }

    fn first_thread(thread_set: ThreadSet) -> ThreadId {
        thread_set
            .contained_threads_iter()
            .next()
            .expect("thread set should not be empty")
    }

    fn transaction_check_metadata(
        allocator: &rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> PendingWorkerCheck {
        let ptr_batch = unsafe {
            TransactionPtrBatch::<PendingWorkerCheck>::from_sharable_transaction_batch_region(
                &batch, allocator,
            )
        };

        let mut metadata_iter = ptr_batch.iter().map(|(_, meta)| meta);
        let metadata = metadata_iter
            .next()
            .expect("test worker check batch should contain metadata");
        assert!(metadata_iter.next().is_none());
        metadata
    }

    fn transaction_execution_metadata(
        allocator: &rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> PendingWorkerExecution {
        let ptr_batch = unsafe {
            TransactionPtrBatch::<PendingWorkerExecution>::from_sharable_transaction_batch_region(
                &batch, allocator,
            )
        };

        let mut metadata_iter = ptr_batch.iter().map(|(_, meta)| meta);
        let metadata = metadata_iter
            .next()
            .expect("test worker execution batch should contain metadata");
        assert!(metadata_iter.next().is_none());
        metadata
    }

    fn transaction_execution_slot_and_index(
        allocator: &rts_alloc::Allocator,
        batch: SharableTransactionBatchRegion,
    ) -> (u64, usize) {
        let metadata = transaction_execution_metadata(allocator, batch);
        (metadata.slot, metadata.transaction_index)
    }

    fn successful_check_response() -> CheckResponse {
        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: 0,
            fee_payer_balance_flags: 0,
            resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            signature_verification_flags: signature_verification_flags::REQUESTED
                | signature_verification_flags::PERFORMED,
            cost_model_flags: 0,
            included_slot: 0,
            balance_slot: 0,
            fee_payer_balance: 0,
            resolution_slot: 0,
            min_alt_deactivation_slot: u64::MAX,
            resolved_pubkeys: SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            },
            estimated_cost_units: 0,
            allocated_accounts_data_size: 0,
            writable_account_bitfields: [1, 0, 0, 0],
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

    fn successful_execution_response() -> ExecutionResponse {
        ExecutionResponse {
            execution_slot: 42,
            not_included_reason: not_included_reasons::NONE,
            cost_units: 0,
            fee_payer_balance: 0,
        }
    }

    fn queue_worker_execution_response(
        worker: &mut BlockVerificationWorkerSession,
        batch: SharableTransactionBatchRegion,
        response: ExecutionResponse,
    ) {
        let responses =
            execution_responses_from_iter(&worker.allocator, [response].into_iter()).unwrap();
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

    fn minimal_transaction(signature: Signature) -> VersionedTransaction {
        minimal_transaction_with_account(signature, Pubkey::default())
    }

    fn minimal_transaction_with_account(
        signature: Signature,
        account_key: Pubkey,
    ) -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![signature],
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![account_key],
                recent_blockhash: Hash::default(),
                instructions: vec![],
            }),
        }
    }

    fn serialized_minimal_transaction(signature: Signature) -> Vec<u8> {
        wincode::serialize(&minimal_transaction(signature)).unwrap()
    }

    fn allocate_minimal_transaction_region(
        allocator: &rts_alloc::Allocator,
        signature_byte: u8,
    ) -> SharableTransactionRegion {
        allocate_minimal_transaction_region_with_account(
            allocator,
            signature_byte,
            Pubkey::default(),
        )
    }

    fn allocate_minimal_transaction_region_with_account(
        allocator: &rts_alloc::Allocator,
        signature_byte: u8,
        account_key: Pubkey,
    ) -> SharableTransactionRegion {
        allocate_transaction(
            allocator,
            &wincode::serialize(&minimal_transaction_with_account(
                Signature::from([signature_byte; SIGNATURE_BYTES]),
                account_key,
            ))
            .unwrap(),
        )
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

        exit.store(true, Ordering::Relaxed);
        scheduler.run();
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
        let first_transaction = allocate_transaction(&replay_stage.allocator, &[1]);
        let second_transaction = allocate_transaction(&replay_stage.allocator, &[2]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 2),
                transaction_message(first_transaction),
                transaction_message(second_transaction),
                begin(43),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(2), 4);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_headers.len(), 1);
        assert!(
            transaction_state_regions(&scheduler.session.allocator, &state.transactions)
                .eq([first_transaction, second_transaction])
        );
        assert!(!scheduler.scheduling_states.contains_key(&43));

        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert!(scheduler.scheduling_states.contains_key(&43));
    }

    #[test]
    fn service_ingress_queue_routes_entries_by_slot() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let transaction = allocate_transaction(&replay_stage.allocator, &[21]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin(1),
                begin(2),
                entry(2, 1),
                transaction_message(transaction),
                entry(1, 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(5), 5);
        let state_1 = scheduler.scheduling_states.get(&1).unwrap();
        let state_2 = scheduler.scheduling_states.get(&2).unwrap();
        assert_eq!(state_1.entry_headers.len(), 1);
        assert!(state_1.transactions.is_empty());
        assert_eq!(state_2.entry_headers.len(), 1);
        assert!(
            transaction_state_regions(&scheduler.session.allocator, &state_2.transactions)
                .eq([transaction])
        );
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
        scheduler.free_transaction_region_allocation(transaction);
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
                    | check_flags::ESTIMATE_COST
                    | check_flags::REPLAY,
            );
            assert_eq!(worker_message.max_working_slot, 42);
            assert_eq!(worker_message.batch.num_transactions, 1);
            assert_eq!(
                transaction_check_metadata(&replay_stage.allocator, worker_message.batch),
                PendingWorkerCheck {
                    slot: 42,
                    transaction_index,
                },
            );
            assert_eq!(
                transaction_batch_regions(&replay_stage.allocator, worker_message.batch),
                &[transaction],
            );
        }

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.pending_transaction_checks.is_empty());
        assert_eq!(state.in_flight_worker_messages, 2);
    }

    #[test]
    fn transaction_checks_round_robin_across_available_workers() {
        const TRANSACTION_COUNT: usize = 8;
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 16,
                replay_block_status_capacity: 8,
                worker_count: 4,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transactions: [SharableTransactionRegion; TRANSACTION_COUNT] =
            core::array::from_fn(|index| {
                allocate_transaction(&replay_stage.allocator, &[index as u8])
            });
        assert!(replay_stage.replay_to_pack.try_write(begin(42)).is_ok());
        assert!(
            replay_stage
                .replay_to_pack
                .try_write(entry(42, TRANSACTION_COUNT.try_into().unwrap()))
                .is_ok()
        );
        for transaction in transactions.iter().copied() {
            assert!(
                replay_stage
                    .replay_to_pack
                    .try_write(transaction_message(transaction))
                    .is_ok()
            );
        }
        replay_stage.replay_to_pack.commit();

        assert_eq!(
            scheduler.service_ingress_queue(TRANSACTION_COUNT + 2),
            TRANSACTION_COUNT + 2
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            TRANSACTION_COUNT,
        );

        for worker_index in 0..4 {
            for transaction_index in [worker_index, worker_index + 4] {
                let worker_message =
                    read_pack_to_worker_message(&mut workers[worker_index]).unwrap();
                assert_eq!(worker_message.batch.num_transactions, 1);
                assert_eq!(
                    transaction_batch_regions(&replay_stage.allocator, worker_message.batch),
                    &[transactions[transaction_index]],
                );
                assert_eq!(
                    transaction_check_metadata(&replay_stage.allocator, worker_message.batch),
                    PendingWorkerCheck {
                        slot: 42,
                        transaction_index,
                    },
                );
            }
        }

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.pending_transaction_checks.is_empty());
        assert_eq!(state.in_flight_worker_messages, TRANSACTION_COUNT);
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
        let transaction = allocate_minimal_transaction_region(&replay_stage.allocator, 1);
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
        assert!(
            transaction_state_regions(&scheduler.session.allocator, &state.transactions)
                .eq([transaction])
        );
        assert!(state.ready_transactions.iter().copied().eq([0]));
        assert_eq!(state.next_ready_transaction_index, 1);
        let (transaction, resolved_pubkeys, check_response) = checked_transaction_state(state, 0);
        assert_eq!(transaction.num_required_signatures(), 1);
        assert_eq!(resolved_pubkeys.unwrap().as_slice().len(), 1);
        assert_eq!(check_response.resolved_pubkeys.num_pubkeys, 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn replay_check_requests_estimated_cost_metadata_and_retains_response() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_minimal_transaction_region(&replay_stage.allocator, 1);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);

        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        assert_eq!(worker_message.flags, REPLAY_TRANSACTION_CHECK_FLAGS);

        let mut response = successful_check_response();
        response.cost_model_flags = cost_model_flags::REQUESTED
            | cost_model_flags::PERFORMED
            | cost_model_flags::TRACK_AS_SIMPLE_VOTE;
        response.estimated_cost_units = 12_345;
        response.allocated_accounts_data_size = 67_890;
        response.writable_account_bitfields = [0b11, 1 << 5, 0, u64::MAX];
        response.resolution_slot = 42;
        let expected_response = response;
        queue_worker_check_response(&mut workers[0], worker_message.batch, response);

        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        let (_, resolved_pubkeys, check_response) = checked_transaction_state(state, 0);
        let scheduling_metadata = checked_transaction_scheduling_metadata(state, 0);
        assert!(resolved_pubkeys.is_none());
        assert_eq!(*check_response, expected_response);
        assert_eq!(
            scheduling_metadata.cost,
            TransactionCostMetadata {
                cost_model_flags: expected_response.cost_model_flags,
                estimated_cost_units: expected_response.estimated_cost_units,
                allocated_accounts_data_size: expected_response.allocated_accounts_data_size,
            },
        );
        assert_eq!(
            scheduling_metadata.writable_account_bitfields,
            expected_response.writable_account_bitfields,
        );
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn checked_transaction_moves_to_in_flight_and_executed() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_minimal_transaction_region(&replay_stage.allocator, 1);
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
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);

        let previous_transaction_state = {
            let state = scheduler.scheduling_state_mut(42);
            let thread_id = state
                .try_lock_ready_transaction(0, ThreadSet::any(1), first_thread)
                .unwrap();
            state.move_checked_transaction_to_in_flight(0, thread_id);
            assert_eq!(in_flight_transaction_thread_id(state, 0), thread_id);
            state.finish_in_flight_transaction(0, thread_id)
        };
        assert!(matches!(
            previous_transaction_state,
            TransactionState::InFlight { .. },
        ));
        scheduler.free_transaction_state_allocations(previous_transaction_state);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(matches!(
            state.transactions.get(0).unwrap(),
            TransactionState::Executed,
        ));
    }

    #[test]
    fn account_locks_force_conflicting_ready_transactions_to_same_thread() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let account = Pubkey::new_unique();
        let first_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 1, account);
        let second_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 2, account);
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
        let first_check = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_check = read_pack_to_worker_message(&mut workers[1]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            first_check.batch,
            successful_check_response(),
        );
        queue_worker_check_response(
            &mut workers[1],
            second_check.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(2), 2);

        let state = scheduler.scheduling_state_mut(42);
        let first_thread_id = state
            .try_lock_ready_transaction(0, ThreadSet::any(2), first_thread)
            .unwrap();
        assert_eq!(first_thread_id, 0);
        assert!(
            state
                .try_lock_ready_transaction(1, ThreadSet::only(1), first_thread)
                .is_none()
        );

        let second_thread_id = state
            .try_lock_ready_transaction(1, ThreadSet::any(2), |thread_set| {
                assert_eq!(thread_set.only_one_contained(), Some(first_thread_id));
                first_thread_id
            })
            .unwrap();
        assert_eq!(second_thread_id, first_thread_id);

        state.unlock_transaction_accounts(1, second_thread_id);
        state.unlock_transaction_accounts(0, first_thread_id);
    }

    #[test]
    fn account_locks_use_check_demoted_writable_bitfields() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let account = Pubkey::new_unique();
        let first_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 1, account);
        let second_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 2, account);
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
        let first_check = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_check = read_pack_to_worker_message(&mut workers[1]).unwrap();
        let mut first_response = successful_check_response();
        let mut second_response = successful_check_response();
        first_response.writable_account_bitfields = [0; 4];
        second_response.writable_account_bitfields = [0; 4];
        queue_worker_check_response(&mut workers[0], first_check.batch, first_response);
        queue_worker_check_response(&mut workers[1], second_check.batch, second_response);
        assert_eq!(scheduler.service_worker_responses(2), 2);

        let state = scheduler.scheduling_state_mut(42);
        let first_thread_id = state
            .try_lock_ready_transaction(0, ThreadSet::any(2), first_thread)
            .unwrap();
        assert_eq!(first_thread_id, 0);
        let second_thread_id = state
            .try_lock_ready_transaction(1, ThreadSet::only(1), first_thread)
            .unwrap();
        assert_eq!(second_thread_id, 1);

        state.unlock_transaction_accounts(1, second_thread_id);
        state.unlock_transaction_accounts(0, first_thread_id);
    }

    #[test]
    fn ready_transaction_dispatches_execution_to_worker() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_minimal_transaction_region(&replay_stage.allocator, 1);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let check_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            check_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);

        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);

        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        assert_eq!(worker_message.flags, REPLAY_TRANSACTION_EXECUTION_FLAGS);
        assert_eq!(worker_message.max_working_slot, 42);
        assert_eq!(worker_message.batch.num_transactions, 1);
        assert_eq!(
            transaction_batch_regions(&replay_stage.allocator, worker_message.batch),
            &[transaction],
        );
        assert_eq!(
            transaction_execution_metadata(&replay_stage.allocator, worker_message.batch),
            PendingWorkerExecution {
                slot: 42,
                transaction_index: 0,
                thread_id: 0,
            },
        );

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.ready_transactions.is_empty());
        assert_eq!(state.in_flight_execution_messages, 1);
        assert_eq!(scheduler.in_flight_execution_messages, 1);
        assert_eq!(scheduler.in_flight_executions_per_thread, vec![1]);
        assert_eq!(in_flight_transaction_thread_id(state, 0), 0);
    }

    #[test]
    fn successful_execution_response_finishes_transaction() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 1,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transaction = allocate_minimal_transaction_region(&replay_stage.allocator, 1);
        write_replay_messages(
            &mut replay_stage,
            [begin(42), entry(42, 1), transaction_message(transaction)],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let check_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            check_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let execution_message = read_pack_to_worker_message(&mut workers[0]).unwrap();

        queue_worker_execution_response(
            &mut workers[0],
            execution_message.batch,
            successful_execution_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.in_flight_execution_messages, 0);
        assert_eq!(scheduler.in_flight_execution_messages, 0);
        assert_eq!(scheduler.in_flight_executions_per_thread, vec![0]);
        assert!(state.ready_transactions.is_empty());
        assert!(matches!(
            state.transactions.get(0).unwrap(),
            TransactionState::Executed,
        ));
    }

    #[test]
    fn execution_failure_sends_invalid_transaction() {
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
        let signature = Signature::from([9; SIGNATURE_BYTES]);
        let transaction = minimal_transaction(signature);
        let entry = solana_entry::next_versioned_entry(&start_hash, 1, vec![transaction.clone()]);
        let transaction_region = allocate_transaction(
            &replay_stage.allocator,
            &wincode::serialize(&transaction).unwrap(),
        );
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, entry.num_hashes, entry.hash, 1),
                transaction_message(transaction_region),
                complete(42),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(4), 4);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let check_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        wait_for_entry_verification(&mut scheduler, 42);
        queue_worker_check_response(
            &mut workers[0],
            check_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let execution_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        queue_worker_execution_response(
            &mut workers[0],
            execution_message.batch,
            ExecutionResponse {
                not_included_reason: not_included_reasons::ACCOUNT_IN_USE,
                ..successful_execution_response()
            },
        );

        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
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
    fn execution_failure_waits_for_other_in_flight_execution_before_status() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let start_hash = Hash::new_from_array([9; 32]);
        let first_transaction = minimal_transaction_with_account(
            Signature::from([1; SIGNATURE_BYTES]),
            Pubkey::new_unique(),
        );
        let second_transaction = minimal_transaction_with_account(
            Signature::from([2; SIGNATURE_BYTES]),
            Pubkey::new_unique(),
        );
        let entry = solana_entry::next_versioned_entry(
            &start_hash,
            1,
            vec![first_transaction.clone(), second_transaction.clone()],
        );
        let first_transaction_region = allocate_transaction(
            &replay_stage.allocator,
            &wincode::serialize(&first_transaction).unwrap(),
        );
        let second_transaction_region = allocate_transaction(
            &replay_stage.allocator,
            &wincode::serialize(&second_transaction).unwrap(),
        );
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, entry.num_hashes, entry.hash, 2),
                transaction_message(first_transaction_region),
                transaction_message(second_transaction_region),
                complete(42),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(5), 5);
        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 2);
        let first_check = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_check = read_pack_to_worker_message(&mut workers[1]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            first_check.batch,
            successful_check_response(),
        );
        queue_worker_check_response(
            &mut workers[1],
            second_check.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(2), 2);

        assert_eq!(scheduler.service_transaction_execution_dispatches(2, 2), 2);
        let first_execution = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_execution = read_pack_to_worker_message(&mut workers[1]).unwrap();
        queue_worker_execution_response(
            &mut workers[0],
            first_execution.batch,
            ExecutionResponse {
                not_included_reason: not_included_reasons::ACCOUNT_IN_USE,
                ..successful_execution_response()
            },
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_TRANSACTION,
            )),
        );
        assert_eq!(state.in_flight_execution_messages, 1);
        assert_eq!(scheduler.in_flight_execution_messages, 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_execution_response(
            &mut workers[1],
            second_execution.batch,
            successful_execution_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
    fn execution_dispatch_load_balances_non_conflicting_transactions() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let first_transaction = allocate_minimal_transaction_region_with_account(
            &replay_stage.allocator,
            1,
            Pubkey::new_unique(),
        );
        let second_transaction = allocate_minimal_transaction_region_with_account(
            &replay_stage.allocator,
            2,
            Pubkey::new_unique(),
        );
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
        let first_check = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_check = read_pack_to_worker_message(&mut workers[1]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            first_check.batch,
            successful_check_response(),
        );
        queue_worker_check_response(
            &mut workers[1],
            second_check.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(2), 2);

        assert_eq!(scheduler.service_transaction_execution_dispatches(2, 2), 2);

        let first_execution = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_execution = read_pack_to_worker_message(&mut workers[1]).unwrap();
        assert_eq!(
            transaction_execution_metadata(&replay_stage.allocator, first_execution.batch),
            PendingWorkerExecution {
                slot: 42,
                transaction_index: 0,
                thread_id: 0,
            },
        );
        assert_eq!(
            transaction_execution_metadata(&replay_stage.allocator, second_execution.batch),
            PendingWorkerExecution {
                slot: 42,
                transaction_index: 1,
                thread_id: 1,
            },
        );
    }

    #[test]
    fn execution_dispatch_load_balances_across_slots() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let first_transaction = allocate_minimal_transaction_region_with_account(
            &replay_stage.allocator,
            1,
            Pubkey::new_unique(),
        );
        let second_transaction = allocate_minimal_transaction_region_with_account(
            &replay_stage.allocator,
            2,
            Pubkey::new_unique(),
        );
        write_replay_messages(
            &mut replay_stage,
            [
                begin(41),
                entry(41, 1),
                transaction_message(first_transaction),
                begin(42),
                entry(42, 1),
                transaction_message(second_transaction),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(6), 6);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 2);
        let mut check_message_count = 0;
        for worker in &mut workers {
            while let Some(message) = read_pack_to_worker_message(worker) {
                queue_worker_check_response(worker, message.batch, successful_check_response());
                check_message_count += 1;
            }
        }
        assert_eq!(check_message_count, 2);
        assert_eq!(scheduler.service_worker_responses(2), 2);

        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let first_execution = read_pack_to_worker_message(&mut workers[0]).unwrap();
        assert!(read_pack_to_worker_message(&mut workers[1]).is_none());

        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let second_execution = read_pack_to_worker_message(&mut workers[1]).unwrap();
        assert!(read_pack_to_worker_message(&mut workers[0]).is_none());

        let mut execution_slots = [
            transaction_execution_metadata(&replay_stage.allocator, first_execution.batch).slot,
            transaction_execution_metadata(&replay_stage.allocator, second_execution.batch).slot,
        ];
        execution_slots.sort_unstable();
        assert_eq!(execution_slots, [41, 42]);
        assert_eq!(scheduler.in_flight_executions_per_thread, vec![1, 1]);
    }

    #[test]
    fn execution_dispatch_is_bounded_by_worker_backlog() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 64,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 64,
                worker_to_pack_capacity: 64,
            });
        let transaction_count = MAX_OUTSTANDING_EXECUTIONS_PER_WORKER * 2 + 8;
        assert!(replay_stage.replay_to_pack.try_write(begin(42)).is_ok());
        assert!(
            replay_stage
                .replay_to_pack
                .try_write(entry(42, transaction_count as u32))
                .is_ok()
        );
        for index in 0..transaction_count {
            let transaction = allocate_minimal_transaction_region_with_account(
                &replay_stage.allocator,
                index as u8,
                Pubkey::new_unique(),
            );
            assert!(
                replay_stage
                    .replay_to_pack
                    .try_write(transaction_message(transaction))
                    .is_ok()
            );
        }
        replay_stage.replay_to_pack.commit();
        assert_eq!(
            scheduler.service_ingress_queue(transaction_count + 2),
            transaction_count + 2,
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            transaction_count,
        );
        let mut check_message_count = 0;
        for worker in &mut workers {
            while let Some(message) = read_pack_to_worker_message(worker) {
                queue_worker_check_response(worker, message.batch, successful_check_response());
                check_message_count += 1;
            }
        }
        assert_eq!(check_message_count, transaction_count);
        assert_eq!(scheduler.service_worker_responses(1024), transaction_count);

        assert_eq!(
            scheduler.service_transaction_execution_dispatches(1024, 1024),
            MAX_OUTSTANDING_EXECUTIONS_PER_WORKER * 2,
        );

        assert_eq!(
            scheduler.in_flight_execution_messages,
            MAX_OUTSTANDING_EXECUTIONS_PER_WORKER * 2,
        );
        assert_eq!(
            scheduler.in_flight_executions_per_thread,
            vec![
                MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
                MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
            ],
        );
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.ready_transactions.len(), 8);
    }

    #[test]
    fn execution_dispatch_scan_limit_bounds_conflict_walk() {
        const SCAN_LIMIT: usize = 3;
        const TRANSACTION_COUNT: usize = MAX_OUTSTANDING_EXECUTIONS_PER_WORKER + SCAN_LIMIT + 2;
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 64,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 64,
                worker_to_pack_capacity: 64,
            });
        let account = Pubkey::new_unique();
        assert!(replay_stage.replay_to_pack.try_write(begin(42)).is_ok());
        assert!(
            replay_stage
                .replay_to_pack
                .try_write(entry(42, TRANSACTION_COUNT.try_into().unwrap()))
                .is_ok()
        );
        for index in 0..TRANSACTION_COUNT {
            let transaction = allocate_minimal_transaction_region_with_account(
                &replay_stage.allocator,
                index as u8,
                account,
            );
            assert!(
                replay_stage
                    .replay_to_pack
                    .try_write(transaction_message(transaction))
                    .is_ok()
            );
        }
        replay_stage.replay_to_pack.commit();
        assert_eq!(
            scheduler.service_ingress_queue(TRANSACTION_COUNT + 2),
            TRANSACTION_COUNT + 2,
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            TRANSACTION_COUNT,
        );
        let mut check_message_count = 0;
        for worker in &mut workers {
            while let Some(message) = read_pack_to_worker_message(worker) {
                queue_worker_check_response(worker, message.batch, successful_check_response());
                check_message_count += 1;
            }
        }
        assert_eq!(check_message_count, TRANSACTION_COUNT);
        assert_eq!(scheduler.service_worker_responses(1024), TRANSACTION_COUNT);

        assert_eq!(
            scheduler.service_transaction_execution_dispatches(
                MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
                MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
            ),
            MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
        );
        let mut execution_message_count = 0;
        while read_pack_to_worker_message(&mut workers[0]).is_some() {
            execution_message_count += 1;
        }
        assert_eq!(
            execution_message_count,
            MAX_OUTSTANDING_EXECUTIONS_PER_WORKER,
        );
        assert!(read_pack_to_worker_message(&mut workers[1]).is_none());

        assert_eq!(
            scheduler.service_transaction_execution_dispatches(1024, SCAN_LIMIT),
            0,
        );
        assert!(read_pack_to_worker_message(&mut workers[0]).is_none());
        assert!(read_pack_to_worker_message(&mut workers[1]).is_none());

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(
            state
                .ready_transactions
                .iter()
                .copied()
                .eq(MAX_OUTSTANDING_EXECUTIONS_PER_WORKER..TRANSACTION_COUNT)
        );
    }

    #[test]
    fn conflicting_execution_dispatches_to_same_worker_in_order() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let account = Pubkey::new_unique();
        let first_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 1, account);
        let second_transaction =
            allocate_minimal_transaction_region_with_account(&replay_stage.allocator, 2, account);
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
        let first_check = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_check = read_pack_to_worker_message(&mut workers[1]).unwrap();
        queue_worker_check_response(
            &mut workers[0],
            first_check.batch,
            successful_check_response(),
        );
        queue_worker_check_response(
            &mut workers[1],
            second_check.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(2), 2);

        assert_eq!(scheduler.service_transaction_execution_dispatches(2, 2), 2);

        let first_execution = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_execution = read_pack_to_worker_message(&mut workers[0]).unwrap();
        assert!(read_pack_to_worker_message(&mut workers[1]).is_none());
        assert_eq!(
            transaction_execution_slot_and_index(&replay_stage.allocator, first_execution.batch),
            (42, 0),
        );
        assert_eq!(
            transaction_execution_slot_and_index(&replay_stage.allocator, second_execution.batch,),
            (42, 1),
        );
    }

    #[test]
    fn worker_responses_are_serviced_round_robin() {
        const TRANSACTION_COUNT: usize = 4;
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 3,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let transactions: [SharableTransactionRegion; TRANSACTION_COUNT] =
            core::array::from_fn(|index| {
                allocate_minimal_transaction_region(&replay_stage.allocator, index as u8)
            });
        assert!(replay_stage.replay_to_pack.try_write(begin(42)).is_ok());
        assert!(
            replay_stage
                .replay_to_pack
                .try_write(entry(42, TRANSACTION_COUNT.try_into().unwrap()))
                .is_ok()
        );
        for transaction in transactions.iter().copied() {
            assert!(
                replay_stage
                    .replay_to_pack
                    .try_write(transaction_message(transaction))
                    .is_ok()
            );
        }
        replay_stage.replay_to_pack.commit();

        assert_eq!(
            scheduler.service_ingress_queue(TRANSACTION_COUNT + 2),
            TRANSACTION_COUNT + 2
        );
        assert_eq!(
            scheduler.service_transaction_check_dispatches(1024),
            TRANSACTION_COUNT,
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
        assert!(state.ready_transactions.iter().copied().eq([0]));
        assert_eq!(state.next_ready_transaction_index, 1);
        assert_eq!(state.in_flight_worker_messages, 2);

        queue_worker_check_response(&mut workers[1], worker_1.batch, successful_check_response());
        assert_eq!(scheduler.service_worker_responses(2), 2);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.ready_transactions.iter().copied().eq([0, 1, 2, 3]));
        assert_eq!(state.next_ready_transaction_index, 4);
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
        assert_eq!(
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_TRANSACTION,
            )),
        );
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        write_replay_messages(&mut replay_stage, [complete(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
    fn failed_slot_waits_for_all_pending_work_before_status() {
        let (mut scheduler, mut replay_stage, mut workers) =
            setup_scheduler_replay_stage_and_workers(BlockVerificationStageSetupConfig {
                allocator_size: 64 * 1024 * 1024,
                replay_to_pack_capacity: 8,
                replay_block_status_capacity: 8,
                worker_count: 2,
                pack_to_worker_capacity: 8,
                worker_to_pack_capacity: 8,
            });
        let start_hash = Hash::new_from_array([9; 32]);
        let entry_hash = next_tick_hash(&start_hash, 1);
        let first_transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3]);
        let second_transaction = allocate_transaction(&replay_stage.allocator, &[4, 5, 6]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, entry_hash, 2),
                transaction_message(first_transaction),
                transaction_message(second_transaction),
                complete(42),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(5), 5);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 2);
        let first_worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        let second_worker_message = read_pack_to_worker_message(&mut workers[1]).unwrap();

        queue_worker_check_response(
            &mut workers[0],
            first_worker_message.batch,
            signature_failed_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);

        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_TRANSACTION,
            )),
        );
        assert_eq!(state.in_flight_worker_messages, 1);
        assert_eq!(state.entry_verification.pending_jobs, 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_verification.pending_jobs, 0);
        assert_eq!(state.in_flight_worker_messages, 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_check_response(
            &mut workers[1],
            second_worker_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
            [
                begin(42),
                entry(42, 1),
                transaction_message(transaction),
                complete(42),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(4), 4);
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
        assert_eq!(
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_TRANSACTION,
            )),
        );
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
            [
                begin(42),
                entry(42, 1),
                transaction_message(transaction),
                complete(42),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(4), 4);
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
        assert_eq!(
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_TRANSACTION,
            )),
        );
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
    #[should_panic(expected = "replay worker response was not processed")]
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
        assert_eq!(state.terminal_status, None);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn complete_empty_slot_sends_success() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), complete(42)]);

        assert_eq!(scheduler.service_ingress_queue(2), 2);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.ingress_complete);
        assert_eq!(state.terminal_status, Some(SlotTerminalStatus::Success));
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::SUCCESS,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    fn complete_slot_waits_for_entry_verification_before_success() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let start_hash = Hash::new_from_array([9; 32]);
        let entry_hash = next_tick_hash(&start_hash, 1);
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, 1, entry_hash, 0),
                complete(42),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(3), 3);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.terminal_status, Some(SlotTerminalStatus::Success));
        assert_eq!(state.entry_verification.pending_jobs, 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::SUCCESS,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    fn complete_slot_waits_for_transaction_execution_before_success() {
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
        let signature = Signature::from([9; SIGNATURE_BYTES]);
        let transaction = minimal_transaction(signature);
        let entry = solana_entry::next_versioned_entry(&start_hash, 1, vec![transaction.clone()]);
        let transaction_region = allocate_transaction(
            &replay_stage.allocator,
            &wincode::serialize(&transaction).unwrap(),
        );
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, entry.num_hashes, entry.hash, 1),
                transaction_message(transaction_region),
                complete(42),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(4), 4);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let worker_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let execution_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_execution_response(
            &mut workers[0],
            execution_message.batch,
            successful_execution_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::SUCCESS,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    #[should_panic(expected = "entry received after complete")]
    fn entry_after_complete_panics() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), complete(42), entry(42, 0)]);

        scheduler.service_ingress_queue(3);
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
            state.terminal_status,
            Some(SlotTerminalStatus::Failed(
                replay_block_status_reasons::INVALID_ENTRY_HASH,
            )),
        );
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));
        write_replay_messages(&mut replay_stage, [complete(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
        assert!(!scheduler.scheduling_states.contains_key(&42));
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
        assert_eq!(state.terminal_status, None);
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
        assert_eq!(state.terminal_status, Some(SlotTerminalStatus::Aborted));
        assert_eq!(state.entry_verification.pending_jobs, 1);
        assert!(scheduler.scheduling_state_pool.is_empty());
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        wait_for_entry_verification(&mut scheduler, 42);
        assert_eq!(scheduler.service_terminal_slots(1), 1);
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
        assert_eq!(state.terminal_status, Some(SlotTerminalStatus::Aborted));
        assert_eq!(state.in_flight_worker_messages, 1);
        assert!(scheduler.scheduling_state_pool.is_empty());
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
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
        assert_eq!(scheduler.service_terminal_slots(1), 1);
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
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_check_response(
            &mut workers[0],
            worker_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);

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
    fn aborted_slot_waits_for_worker_execution_response_before_cleanup() {
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
        let signature = Signature::from([9; SIGNATURE_BYTES]);
        let transaction = minimal_transaction(signature);
        let entry = solana_entry::next_versioned_entry(&start_hash, 1, vec![transaction.clone()]);
        let transaction_region = allocate_transaction(
            &replay_stage.allocator,
            &wincode::serialize(&transaction).unwrap(),
        );
        write_replay_messages(
            &mut replay_stage,
            [
                begin_with_last_entry_hash(42, start_hash),
                entry_with_hash(42, entry.num_hashes, entry.hash, 1),
                transaction_message(transaction_region),
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(3), 3);
        assert_eq!(scheduler.service_transaction_check_dispatches(1024), 1);
        let check_message = read_pack_to_worker_message(&mut workers[0]).unwrap();
        wait_for_entry_verification(&mut scheduler, 42);
        queue_worker_check_response(
            &mut workers[0],
            check_message.batch,
            successful_check_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_transaction_execution_dispatches(1, 1), 1);
        let execution_message = read_pack_to_worker_message(&mut workers[0]).unwrap();

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 0);
        assert!(scheduler.scheduling_states.contains_key(&42));
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        queue_worker_execution_response(
            &mut workers[0],
            execution_message.batch,
            successful_execution_response(),
        );
        assert_eq!(scheduler.service_worker_responses(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);

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
        assert_eq!(scheduler.service_terminal_slots(1), 1);

        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert_eq!(
            scheduler.scheduling_state_pool[0].entry_headers.capacity(),
            POOLED_ENTRY_HEADERS_CAPACITY,
        );
        assert_eq!(
            scheduler.scheduling_state_pool[0].transactions.capacity(),
            POOLED_TRANSACTIONS_CAPACITY,
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
        assert!(state.transactions.is_empty());
    }

    #[test]
    fn cleanup_caps_scheduling_state_pool() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let slot_count = SCHEDULING_STATE_POOL_LIMIT + 2;
        let slots = 0..u64::try_from(slot_count).unwrap();

        write_replay_messages(&mut replay_stage, slots.clone().map(begin));
        assert_eq!(scheduler.service_ingress_queue(slot_count), slot_count);
        assert_eq!(scheduler.scheduling_states.len(), slot_count);

        write_replay_messages(&mut replay_stage, slots.map(abort));
        assert_eq!(scheduler.service_ingress_queue(slot_count), slot_count);
        assert_eq!(
            scheduler.service_terminal_slots(slot_count),
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
            state.pending_transaction_checks.reserve(128);
            assert!(state.entry_headers.capacity() >= 128);
            assert!(state.transactions.capacity() >= 128);
            assert!(state.pending_transaction_checks.capacity() >= 128);
        }

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert_eq!(scheduler.service_terminal_slots(1), 1);

        let pooled_state = &scheduler.scheduling_state_pool[0];
        assert_eq!(
            pooled_state.entry_headers.capacity(),
            POOLED_ENTRY_HEADERS_CAPACITY,
        );
        assert_eq!(
            pooled_state.transactions.capacity(),
            POOLED_TRANSACTIONS_CAPACITY,
        );
        assert_eq!(
            pooled_state.pending_transaction_checks.capacity(),
            POOLED_PENDING_TRANSACTION_CHECKS_CAPACITY,
        );
    }
}
