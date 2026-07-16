use {
    crate::{
        in_flight::InFlightTracker,
        state_container::StateContainer,
        transaction::{ExecutionBatch, ExecutionTransactionMeta, MAX_PACKETS_PER_EXEC_BATCH},
    },
    agave_scheduler_bindings::{
        PackToExecutionWorkerMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
    },
    agave_scheduling_utils::{
        handshake::ClientWorkerSession,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet},
    },
    rts_alloc::Allocator,
};

// Matches greedy scheduler's 15% target without depending on the ledger crate.
const TARGET_ENTRY_BYTES_PER_BATCH: u64 = 4_622;
const ENTRY_OVERHEAD_BYTES: u64 = 48;

pub(crate) struct SchedulingScratch {
    removal_ids: Vec<usize>,
    batches: Batches,
}

impl SchedulingScratch {
    pub(crate) fn new(transaction_state_capacity: usize, execution_worker_count: usize) -> Self {
        Self {
            removal_ids: Vec::with_capacity(transaction_state_capacity),
            batches: Batches::new(execution_worker_count),
        }
    }
}

struct Batches {
    // `ExecutionBatch` borrows its allocator. Keep only its sharable descriptor here so this
    // scheduler-lifetime scratch space is not tied to a borrow of the session allocator.
    regions: Box<[Option<SharableTransactionBatchRegion>]>,
    cost_units: Box<[u64]>,
    entry_bytes: Box<[u64]>,
}

impl Batches {
    fn new(execution_worker_count: usize) -> Self {
        Self {
            regions: vec![None; execution_worker_count].into_boxed_slice(),
            cost_units: vec![0; execution_worker_count].into_boxed_slice(),
            entry_bytes: vec![ENTRY_OVERHEAD_BYTES; execution_worker_count].into_boxed_slice(),
        }
    }

    fn allocate(&mut self, worker_id: ThreadId, allocator: &Allocator) -> bool {
        debug_assert!(self.regions[worker_id].is_none());
        self.cost_units[worker_id] = 0;
        self.entry_bytes[worker_id] = ENTRY_OVERHEAD_BYTES;
        let Some(batch) = ExecutionBatch::allocate(allocator) else {
            return false;
        };
        self.regions[worker_id] = Some(batch.to_sharable_transaction_batch_region());
        true
    }

    fn has_batch(&self, worker_id: ThreadId) -> bool {
        self.regions[worker_id].is_some()
    }

    fn should_flush_before_add(&self, worker_id: ThreadId, transaction_bytes: u64) -> bool {
        self.len(worker_id) > 0
            && self.entry_bytes[worker_id].saturating_add(transaction_bytes)
                > TARGET_ENTRY_BYTES_PER_BATCH
    }

    fn add_transaction(
        &mut self,
        worker_id: ThreadId,
        transaction: SharableTransactionRegion,
        meta: ExecutionTransactionMeta,
        cost_units: u64,
        transaction_bytes: u64,
        allocator: &Allocator,
    ) {
        let region =
            self.regions[worker_id].expect("every allowed worker has an allocated execution batch");
        // SAFETY: `region` was allocated by this `Batches` using the matching allocator and
        // batch layout, and it has not been sent.
        let mut batch =
            unsafe { ExecutionBatch::from_sharable_transaction_batch_region(&region, allocator) };
        assert!(
            batch.push(transaction, meta).is_ok(),
            "an execution batch is sent before it reaches capacity"
        );
        self.regions[worker_id] = Some(batch.to_sharable_transaction_batch_region());
        self.cost_units[worker_id] = self.cost_units[worker_id].saturating_add(cost_units);
        self.entry_bytes[worker_id] = self.entry_bytes[worker_id].saturating_add(transaction_bytes);
    }

    fn should_flush(&self, worker_id: ThreadId) -> bool {
        self.len(worker_id) == MAX_PACKETS_PER_EXEC_BATCH
            || self.entry_bytes[worker_id] >= TARGET_ENTRY_BYTES_PER_BATCH
    }

    fn cost_units(&self, worker_id: ThreadId) -> u64 {
        self.cost_units[worker_id]
    }

    fn len(&self, worker_id: ThreadId) -> usize {
        self.regions[worker_id].map_or(0, |region| usize::from(region.num_transactions))
    }

    fn send(
        &mut self,
        worker_id: ThreadId,
        queue: &mut shaq::spsc::Producer<PackToExecutionWorkerMessage>,
        removal_ids: &mut Vec<usize>,
        in_flight: &mut InFlightTracker,
        current_slot: u64,
        allocator: &Allocator,
    ) {
        let Some(region) = self.regions[worker_id].take() else {
            return;
        };
        // SAFETY: `region` was allocated by this `Batches` using the matching allocator and
        // batch layout, and it has not been sent.
        let batch =
            unsafe { ExecutionBatch::from_sharable_transaction_batch_region(&region, allocator) };
        if batch.is_empty() {
            // SAFETY: this batch was allocated locally and has not been sent.
            unsafe { batch.free() };
            return;
        }

        queue
            .try_write(PackToExecutionWorkerMessage {
                flags: 0,
                max_working_slot: current_slot,
                batch: region,
            })
            .expect("in-flight batch limit leaves space in every execution-worker queue");
        queue.commit();

        for (_, meta) in batch.iter() {
            removal_ids.push(meta.transaction_id);
        }
        in_flight.track_batch(
            worker_id,
            batch.len(),
            core::mem::take(&mut self.cost_units[worker_id]),
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn schedule(
    workers: &mut [ClientWorkerSession],
    allocator: &Allocator,
    transactions: &mut StateContainer,
    account_locks: &mut ThreadAwareAccountLocks,
    in_flight: &mut InFlightTracker,
    scratch: &mut SchedulingScratch,
    current_slot: u64,
    remaining_cost_units: u64,
    target_scheduled_cus: u64,
    max_scheduled_transactions: usize,
) -> usize {
    let is_new_slot = in_flight.scheduling_slot() != Some(current_slot);
    if !in_flight.enter_slot(current_slot) {
        return 0;
    }
    if is_new_slot {
        transactions.flush_held(|transaction| {
            // SAFETY: state-container eviction returns ownership of both scheduler allocations.
            unsafe { transaction.transaction.free(allocator) };
        });
    }
    if transactions.is_empty() {
        return 0;
    }

    let in_flight_cost_units = in_flight
        .cost_units_in_flight_per_worker()
        .iter()
        .fold(0_u64, |total, &cost_units| total.saturating_add(cost_units));
    let mut budget = remaining_cost_units
        .min(target_scheduled_cus)
        .saturating_sub(in_flight_cost_units);
    if budget == 0 {
        return 0;
    }

    let SchedulingScratch {
        removal_ids,
        batches,
    } = scratch;
    removal_ids.clear();
    #[allow(clippy::arithmetic_side_effects)]
    let target_cus_per_worker = target_scheduled_cus / workers.len() as u64;
    let mut allowed_workers = prepare_workers(
        workers,
        allocator,
        batches,
        in_flight,
        target_cus_per_worker,
    );
    if allowed_workers.is_empty() {
        return 0;
    }

    let mut num_scheduled_transactions = 0;
    {
        let mut priority_ids = transactions.descending_from(None);
        while budget > 0
            && num_scheduled_transactions < max_scheduled_transactions
            && !allowed_workers.is_empty()
        {
            let Some(priority_id) = priority_ids.next() else {
                break;
            };
            let priority_id = *priority_id;

            let transaction = transactions.get(priority_id.id);
            let transaction_cost = transaction.meta.cost;
            let transaction_bytes = transaction.transaction.serialized_size() as u64;
            let account_locks_for_transaction = transaction.transaction.account_locks();
            let worker_id = account_locks.try_lock_accounts(
                account_locks_for_transaction.write_locks(),
                account_locks_for_transaction.read_locks(),
                allowed_workers,
                |eligible_workers| select_worker(eligible_workers, batches, in_flight),
            );
            let Ok(worker_id) = worker_id else {
                continue;
            };

            if batches.should_flush_before_add(worker_id, transaction_bytes) {
                batches.send(
                    worker_id,
                    &mut workers[worker_id].pack_to_worker,
                    removal_ids,
                    in_flight,
                    current_slot,
                    allocator,
                );
                refresh_worker(
                    allocator,
                    batches,
                    in_flight,
                    &mut allowed_workers,
                    worker_id,
                    target_cus_per_worker,
                );
                if !allowed_workers.contains(worker_id) {
                    account_locks.unlock_accounts(
                        account_locks_for_transaction.write_locks(),
                        account_locks_for_transaction.read_locks(),
                        worker_id,
                    );
                    continue;
                }
            }

            // SAFETY: the resolved transaction remains owned by `transactions` until its
            // execution response is handled.
            let transaction_region = unsafe {
                transaction
                    .transaction
                    .to_sharable_transaction_region(allocator)
            };
            batches.add_transaction(
                worker_id,
                transaction_region,
                ExecutionTransactionMeta {
                    transaction_id: priority_id.id,
                },
                transaction_cost,
                transaction_bytes,
                allocator,
            );
            num_scheduled_transactions = num_scheduled_transactions.wrapping_add(1);
            budget = budget.saturating_sub(transaction_cost);

            if batches.should_flush(worker_id)
                || worker_at_target(worker_id, batches, in_flight, target_cus_per_worker)
            {
                batches.send(
                    worker_id,
                    &mut workers[worker_id].pack_to_worker,
                    removal_ids,
                    in_flight,
                    current_slot,
                    allocator,
                );
                refresh_worker(
                    allocator,
                    batches,
                    in_flight,
                    &mut allowed_workers,
                    worker_id,
                    target_cus_per_worker,
                );
            }
        }
    }

    for (worker_id, worker) in workers.iter_mut().enumerate() {
        if batches.has_batch(worker_id) {
            batches.send(
                worker_id,
                &mut worker.pack_to_worker,
                removal_ids,
                in_flight,
                current_slot,
                allocator,
            );
        }
    }

    debug_assert_eq!(num_scheduled_transactions, removal_ids.len());
    let scheduled_transactions = removal_ids.len();
    for transaction_id in removal_ids.drain(..) {
        transactions.dequeue(transaction_id);
    }
    scheduled_transactions
}

fn prepare_workers(
    workers: &mut [ClientWorkerSession],
    allocator: &Allocator,
    batches: &mut Batches,
    in_flight: &InFlightTracker,
    target_cus_per_worker: u64,
) -> ThreadSet {
    let mut allowed_workers = ThreadSet::any(workers.len());
    for (worker_id, worker) in workers.iter_mut().enumerate() {
        // Reclaim queue slots consumed by the worker since the prior scheduling pass.
        worker.pack_to_worker.sync();
        if !in_flight.can_schedule_batch(worker_id)
            || in_flight.cost_units_in_flight_per_worker()[worker_id] >= target_cus_per_worker
        {
            allowed_workers.remove(worker_id);
            continue;
        }
        if !batches.allocate(worker_id, allocator) {
            allowed_workers.remove(worker_id);
        }
    }
    allowed_workers
}

fn refresh_worker(
    allocator: &Allocator,
    batches: &mut Batches,
    in_flight: &InFlightTracker,
    allowed_workers: &mut ThreadSet,
    worker_id: ThreadId,
    target_cus_per_worker: u64,
) {
    if !in_flight.can_schedule_batch(worker_id)
        || in_flight.cost_units_in_flight_per_worker()[worker_id] >= target_cus_per_worker
    {
        allowed_workers.remove(worker_id);
        return;
    }

    if !batches.allocate(worker_id, allocator) {
        allowed_workers.remove(worker_id);
    }
}

fn worker_at_target(
    worker_id: ThreadId,
    batches: &Batches,
    in_flight: &InFlightTracker,
    target_cus_per_worker: u64,
) -> bool {
    in_flight.cost_units_in_flight_per_worker()[worker_id]
        .saturating_add(batches.cost_units(worker_id))
        >= target_cus_per_worker
}

fn select_worker(
    eligible_workers: ThreadSet,
    batches: &Batches,
    in_flight: &InFlightTracker,
) -> ThreadId {
    eligible_workers
        .contained_threads_iter()
        .min_by_key(|&worker_id| {
            (
                in_flight.cost_units_in_flight_per_worker()[worker_id]
                    .saturating_add(batches.cost_units(worker_id)),
                in_flight.num_in_flight_per_worker()[worker_id]
                    .saturating_add(batches.len(worker_id)),
            )
        })
        .expect("account locking only invokes the worker selector with an eligible worker")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            SchedulerConfig,
            resolved_transaction::{ResolvedTransaction, sanitize_config},
            state_container::CheckedTransaction,
            transaction::TpuTransactionMeta,
        },
        agave_scheduler_bindings::SharablePubkeys,
        agave_scheduling_utils::{
            handshake::{client, server::Server},
            transaction_ptr::TransactionPtr,
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
        std::collections::HashSet,
    };

    fn checked_transaction(allocator: &Allocator, priority: u64, cost: u64) -> CheckedTransaction {
        let payer = Keypair::new();
        let message = Message::new(
            &[system_instruction::transfer(
                &payer.pubkey(),
                &Pubkey::new_from_array([1; 32]),
                1,
            )],
            Some(&payer.pubkey()),
        );
        let bytes = wincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&payer],
            message,
            Hash::default(),
        )))
        .unwrap();
        let allocation = allocator.allocate(bytes.len() as u32).unwrap();
        // SAFETY: both pointers are valid for `bytes.len()` bytes and do not overlap.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), allocation.as_ptr(), bytes.len());
        }
        // SAFETY: `allocation` was created by this allocator immediately above.
        let transaction = unsafe { TransactionPtr::from_raw_parts(allocation, bytes.len()) };
        // SAFETY: this test owns the transaction allocation and has no resolved pubkeys.
        let transaction = unsafe {
            ResolvedTransaction::try_new(
                transaction,
                SharablePubkeys {
                    offset: 0,
                    num_pubkeys: 0,
                },
                allocator,
                &sanitize_config(false),
                &HashSet::new(),
            )
        }
        .unwrap();
        CheckedTransaction::new(
            transaction,
            TpuTransactionMeta {
                priority,
                cost,
                flags: 0,
                src_addr: [0; 16],
            },
        )
    }

    #[test]
    fn selects_least_loaded_eligible_worker() {
        let mut in_flight = InFlightTracker::new(3, 2);
        in_flight.track_batch(0, 1, 10);
        in_flight.track_batch(1, 3, 5);
        in_flight.track_batch(2, 2, 5);
        let batches = Batches::new(3);

        assert_eq!(select_worker(ThreadSet::any(3), &batches, &in_flight), 2);
    }

    #[test]
    fn synchronizes_execution_queue_between_scheduling_passes() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.execution_worker_count = 1;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let num_transactions = MAX_PACKETS_PER_EXEC_BATCH + 1;
        let mut transactions = StateContainer::new(num_transactions);
        for priority in 0..num_transactions {
            assert!(
                transactions
                    .push(checked_transaction(allocator, priority as u64, 1))
                    .is_none()
            );
        }
        let mut account_locks = ThreadAwareAccountLocks::new(1);
        let mut in_flight = InFlightTracker::new(1, config.pack_to_worker_capacity);
        let mut scratch = SchedulingScratch::new(num_transactions, config.execution_worker_count);

        schedule(
            &mut client_session.workers,
            allocator,
            &mut transactions,
            &mut account_locks,
            &mut in_flight,
            &mut scratch,
            42,
            100,
            100,
            num_transactions,
        );

        assert_eq!(transactions.len(), 0);
        assert_eq!(transactions.buffer_len(), num_transactions);
        agave_session.workers[0].pack_to_worker.sync();
        let mut transaction_ids = Vec::new();
        for num_transactions in [MAX_PACKETS_PER_EXEC_BATCH, 1] {
            let message = *agave_session.workers[0].pack_to_worker.try_read().unwrap();
            assert_eq!(message.max_working_slot, 42);
            assert_eq!(message.batch.num_transactions as usize, num_transactions);
            // SAFETY: the scheduler wrote this execution batch with the matching metadata
            // layout.
            let batch = unsafe {
                ExecutionBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
            };
            transaction_ids.extend(batch.iter().map(|(_, meta)| meta.transaction_id));
            // SAFETY: the test received this batch and no worker holds references to it.
            unsafe { batch.free() };
        }
        assert_eq!(in_flight.num_in_flight_per_worker(), &[num_transactions]);
        assert_eq!(
            in_flight.cost_units_in_flight_per_worker(),
            &[num_transactions as u64]
        );
        agave_session.workers[0].pack_to_worker.finalize();
        in_flight.complete_batch(
            0,
            MAX_PACKETS_PER_EXEC_BATCH,
            MAX_PACKETS_PER_EXEC_BATCH as u64,
        );
        in_flight.complete_batch(0, 1, 1);
        assert!(in_flight.is_empty());

        for transaction_id in transaction_ids {
            let transaction = transactions.remove(transaction_id);
            // SAFETY: the test now exclusively owns this transaction's shared allocations.
            unsafe { transaction.transaction.free(allocator) };
        }

        assert!(
            transactions
                .push(checked_transaction(allocator, u64::MAX, 1))
                .is_none()
        );
        schedule(
            &mut client_session.workers,
            allocator,
            &mut transactions,
            &mut account_locks,
            &mut in_flight,
            &mut scratch,
            42,
            100,
            100,
            1,
        );

        agave_session.workers[0].pack_to_worker.sync();
        let message = *agave_session.workers[0].pack_to_worker.try_read().unwrap();
        assert_eq!(message.batch.num_transactions, 1);
        // SAFETY: the scheduler wrote this execution batch with the matching metadata layout.
        let batch = unsafe {
            ExecutionBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
        };
        let transaction_id = batch.iter().next().unwrap().1.transaction_id;
        // SAFETY: this test received the batch and no worker holds references to it.
        unsafe { batch.free() };
        agave_session.workers[0].pack_to_worker.finalize();

        let transaction = transactions.remove(transaction_id);
        // SAFETY: the test has removed the transaction from scheduler state.
        unsafe { transaction.transaction.free(allocator) };
    }

    #[test]
    fn limits_scheduled_transactions_per_pass() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.execution_worker_count = 1;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let mut transactions = StateContainer::new(2);
        assert!(
            transactions
                .push(checked_transaction(allocator, 2, 1))
                .is_none()
        );
        assert!(
            transactions
                .push(checked_transaction(allocator, 1, 1))
                .is_none()
        );
        let mut account_locks = ThreadAwareAccountLocks::new(1);
        let mut in_flight = InFlightTracker::new(1, config.pack_to_worker_capacity);
        let mut scratch = SchedulingScratch::new(2, config.execution_worker_count);

        assert_eq!(
            schedule(
                &mut client_session.workers,
                allocator,
                &mut transactions,
                &mut account_locks,
                &mut in_flight,
                &mut scratch,
                42,
                100,
                100,
                1,
            ),
            1
        );
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions.buffer_len(), 2);

        agave_session.workers[0].pack_to_worker.sync();
        let message = *agave_session.workers[0].pack_to_worker.try_read().unwrap();
        let batch = unsafe {
            ExecutionBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
        };
        assert_eq!(batch.len(), 1);
        let transaction_id = batch.iter().next().unwrap().1.transaction_id;
        // SAFETY: this test received the batch and no worker holds references to it.
        unsafe { batch.free() };
        agave_session.workers[0].pack_to_worker.finalize();

        let transaction = transactions.remove(transaction_id);
        // SAFETY: the test now exclusively owns this transaction's shared allocations.
        unsafe { transaction.transaction.free(allocator) };
        let transaction = transactions.pop().unwrap();
        // SAFETY: the test now exclusively owns this transaction's shared allocations.
        unsafe { transaction.transaction.free(allocator) };
    }
}
