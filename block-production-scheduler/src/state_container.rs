use {
    crate::{resolved_transaction::ResolvedTransaction, transaction::TpuTransactionMeta},
    agave_scheduling_utils::transaction_priority_queue::{
        TransactionPriorityId, TransactionPriorityQueue,
    },
    slab::Slab,
};

/// A transaction that has passed external check-worker validation.
pub(crate) struct CheckedTransaction {
    pub(crate) transaction: ResolvedTransaction,
    pub(crate) meta: TpuTransactionMeta,
    // References held by execution or recheck work outside this container.
    reference_count: u8,
    should_drop: bool,
}

impl CheckedTransaction {
    pub(crate) fn new(transaction: ResolvedTransaction, meta: TpuTransactionMeta) -> Self {
        Self {
            transaction,
            meta,
            reference_count: 0,
            should_drop: false,
        }
    }
}

/// Owns checked transactions for their entire scheduler lifetime and orders queued ones by
/// priority.
pub(crate) struct StateContainer {
    transactions: Slab<CheckedTransaction>,
    queue: TransactionPriorityQueue,
}

impl StateContainer {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "transaction state capacity must be non-zero");
        Self {
            transactions: Slab::with_capacity(capacity.saturating_add(1)),
            queue: TransactionPriorityQueue::with_capacity(capacity),
        }
    }

    /// Inserts a checked transaction and returns any transaction evicted for capacity.
    ///
    /// In-flight transactions remain in the slab but not the priority queue, so they are never
    /// evicted to make room for a new transaction.
    pub(crate) fn push(&mut self, transaction: CheckedTransaction) -> Option<CheckedTransaction> {
        let entry = self.transactions.vacant_entry();
        let id = entry.key();
        let priority_id = TransactionPriorityId::new(transaction.meta.priority, id);
        entry.insert(transaction);

        let (queue, transactions) = (&mut self.queue, &mut self.transactions);
        let mut dropped = None;
        queue.push(std::iter::once(priority_id), transactions.len(), |id| {
            dropped = Some(transactions.remove(id.id));
        });
        dropped
    }

    /// Iterates in descending priority order, resuming strictly below `cursor` when present.
    ///
    /// The caller must collect selected IDs and dequeue them only after this iterator is dropped.
    /// When the iterator reaches the bottom of the queue, the caller resets its cursor to `None`
    /// before the next scan.
    pub(crate) fn descending_from(
        &self,
        cursor: Option<&TransactionPriorityId>,
    ) -> std::iter::Rev<std::collections::btree_set::Range<'_, TransactionPriorityId>> {
        self.queue.descending_from(cursor)
    }

    pub(crate) fn get(&self, transaction_id: usize) -> &CheckedTransaction {
        &self.transactions[transaction_id]
    }

    /// Removes a selected transaction from the priority queue while it is in flight.
    pub(crate) fn dequeue(&mut self, transaction_id: usize) {
        let priority_id = TransactionPriorityId::new(
            self.transactions[transaction_id].meta.priority,
            transaction_id,
        );
        assert!(
            self.queue.remove(&priority_id),
            "selected transaction must remain queued until removed"
        );
        self.acquire_reference(transaction_id);
    }

    /// Removes a terminal transaction from scheduler state.
    pub(crate) fn remove(&mut self, transaction_id: usize) -> CheckedTransaction {
        let priority_id = TransactionPriorityId::new(
            self.transactions[transaction_id].meta.priority,
            transaction_id,
        );
        self.queue.remove(&priority_id);
        self.transactions.remove(transaction_id)
    }

    /// Marks a transaction as owned by an outstanding recheck request.
    pub(crate) fn start_recheck(&mut self, transaction_id: usize) {
        self.acquire_reference(transaction_id);
    }

    pub(crate) fn has_references(&self, transaction_id: usize) -> bool {
        self.transactions[transaction_id].reference_count != 0
    }

    /// Completes a recheck and returns a transaction that can now be freed.
    pub(crate) fn complete_recheck(
        &mut self,
        transaction_id: usize,
        valid: bool,
    ) -> Option<CheckedTransaction> {
        if !valid {
            self.transactions[transaction_id].should_drop = true;
        }
        self.release_reference(transaction_id)
    }

    /// Completes execution, returning a terminal transaction that can now be freed.
    pub(crate) fn complete_execution(
        &mut self,
        transaction_id: usize,
        retryability: Option<bool>,
        mut on_evict: impl FnMut(CheckedTransaction),
    ) -> Option<CheckedTransaction> {
        let should_retry = retryability.is_some() && !self.transactions[transaction_id].should_drop;
        if !should_retry {
            self.transactions[transaction_id].should_drop = true;
        }
        let dropped = self.release_reference(transaction_id);
        if dropped.is_some() {
            return dropped;
        }

        if should_retry {
            let immediately_retryable =
                retryability.expect("retryable response must have a reason");
            self.retry(transaction_id, immediately_retryable, &mut on_evict);
        }
        None
    }

    fn acquire_reference(&mut self, transaction_id: usize) {
        self.transactions[transaction_id].reference_count = self.transactions[transaction_id]
            .reference_count
            .wrapping_add(1);
    }

    fn release_reference(&mut self, transaction_id: usize) -> Option<CheckedTransaction> {
        let transaction = &mut self.transactions[transaction_id];
        transaction.reference_count = transaction.reference_count.wrapping_sub(1);
        if transaction.should_drop && transaction.reference_count == 0 {
            Some(self.remove(transaction_id))
        } else {
            None
        }
    }

    /// Requeue a completed transaction, either immediately or after the next slot transition.
    pub(crate) fn retry(
        &mut self,
        transaction_id: usize,
        immediately_retryable: bool,
        mut on_evict: impl FnMut(CheckedTransaction),
    ) {
        let priority_id = TransactionPriorityId::new(
            self.transactions[transaction_id].meta.priority,
            transaction_id,
        );
        if !immediately_retryable {
            self.queue.hold(priority_id);
            return;
        }

        let (queue, transactions) = (&mut self.queue, &mut self.transactions);
        queue.push(std::iter::once(priority_id), transactions.len(), |id| {
            on_evict(transactions.remove(id.id));
        });
    }

    /// Returns delayed retries to the priority queue at a slot boundary.
    pub(crate) fn flush_held(&mut self, mut on_evict: impl FnMut(CheckedTransaction)) {
        let (queue, transactions) = (&mut self.queue, &mut self.transactions);
        queue.flush_held(transactions.len(), |id| {
            on_evict(transactions.remove(id.id));
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn pop(&mut self) -> Option<CheckedTransaction> {
        let priority_id = self.queue.pop_highest()?;
        Some(self.transactions.remove(priority_id.id))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub(crate) fn buffer_len(&self) -> usize {
        self.transactions.len()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{SchedulerConfig, resolved_transaction::sanitize_config},
        agave_scheduler_bindings::SharablePubkeys,
        agave_scheduling_utils::{handshake::server::Server, transaction_ptr::TransactionPtr},
        rts_alloc::Allocator,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
        std::collections::HashSet,
    };

    fn checked_transaction(allocator: &Allocator, priority: u64) -> CheckedTransaction {
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
                cost: 0,
                flags: 0,
                src_addr: [0; 16],
            },
        )
    }

    fn session() -> agave_scheduling_utils::handshake::AgaveSession {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let (session, _) = Server::setup_session(config.client_logon()).unwrap();
        session
    }

    #[test]
    fn descending_scan_resumes_after_dequeued_cursor() {
        let session = session();
        let allocator = &session.tpu_to_pack.allocator;
        let mut container = StateContainer::new(4);
        for priority in [5, 10, 5, 1] {
            assert!(
                container
                    .push(checked_transaction(&allocator, priority))
                    .is_none()
            );
        }

        let mut cursor = None;
        let mut removal_ids = Vec::new();
        {
            let mut priority_ids = container.descending_from(cursor.as_ref());
            for _ in 0..2 {
                let priority_id = *priority_ids.next().unwrap();
                // Advance the cursor for every visited ID, including the IDs selected for
                // removal below.
                cursor = Some(priority_id);
                assert_eq!(
                    container.get(priority_id.id).meta.priority,
                    priority_id.priority
                );
                removal_ids.push(priority_id);
            }
        }

        // The iterator's immutable borrow has ended, so dequeue selected IDs in a separate pass.
        for priority_id in removal_ids {
            container.dequeue(priority_id.id);
        }
        assert_eq!(container.len(), 2);
        assert_eq!(container.buffer_len(), 4);

        let remaining_priorities = container
            .descending_from(cursor.as_ref())
            .map(|priority_id| priority_id.priority)
            .collect::<Vec<_>>();
        assert_eq!(remaining_priorities, [5, 1]);

        let bottom = *container
            .descending_from(cursor.as_ref())
            .next_back()
            .unwrap();
        assert!(container.descending_from(Some(&bottom)).next().is_none());
        assert_eq!(container.descending_from(None).next().unwrap().priority, 5);
    }

    #[test]
    fn does_not_evict_in_flight_transactions() {
        let session = session();
        let allocator = &session.tpu_to_pack.allocator;
        let mut container = StateContainer::new(2);
        container.push(checked_transaction(&allocator, 10));
        container.push(checked_transaction(&allocator, 5));

        let in_flight = *container.descending_from(None).next().unwrap();
        container.dequeue(in_flight.id);
        let dropped = container.push(checked_transaction(&allocator, 7)).unwrap();
        assert_eq!(dropped.meta.priority, 5);
        assert_eq!(container.get(in_flight.id).meta.priority, 10);
        assert_eq!(container.buffer_len(), 2);

        let queued = *container.descending_from(None).next().unwrap();
        container.dequeue(queued.id);
        let dropped = container.push(checked_transaction(&allocator, 1)).unwrap();
        assert_eq!(dropped.meta.priority, 1);
        assert_eq!(container.buffer_len(), 2);
    }

    #[test]
    fn returns_delayed_retries_to_the_queue_on_flush() {
        let session = session();
        let allocator = &session.tpu_to_pack.allocator;
        let mut container = StateContainer::new(1);
        assert!(container.push(checked_transaction(allocator, 10)).is_none());
        let transaction_id = container.descending_from(None).next().unwrap().id;
        container.dequeue(transaction_id);

        container.retry(transaction_id, false, |_| unreachable!());
        assert!(container.is_empty());
        assert_eq!(container.buffer_len(), 1);

        container.flush_held(|_| unreachable!());
        let transaction = container.pop().unwrap();
        assert_eq!(transaction.meta.priority, 10);
        // SAFETY: the test has removed the transaction from scheduler state.
        unsafe { transaction.transaction.free(allocator) };
    }

    #[test]
    fn invalid_recheck_waits_for_execution_to_complete() {
        let session = session();
        let allocator = &session.tpu_to_pack.allocator;
        let mut container = StateContainer::new(1);
        assert!(container.push(checked_transaction(allocator, 10)).is_none());
        let transaction_id = container.descending_from(None).next().unwrap().id;

        container.start_recheck(transaction_id);
        container.dequeue(transaction_id);
        assert!(container.complete_recheck(transaction_id, false).is_none());
        assert_eq!(container.buffer_len(), 1);

        let transaction = container
            .complete_execution(transaction_id, Some(true), |_| unreachable!())
            .expect("invalid recheck drops after execution completes");
        // SAFETY: the test has removed the transaction from scheduler state.
        unsafe { transaction.transaction.free(allocator) };
    }

    #[test]
    fn terminal_execution_waits_for_recheck_to_complete() {
        let session = session();
        let allocator = &session.tpu_to_pack.allocator;
        let mut container = StateContainer::new(1);
        assert!(container.push(checked_transaction(allocator, 10)).is_none());
        let transaction_id = container.descending_from(None).next().unwrap().id;

        container.start_recheck(transaction_id);
        container.dequeue(transaction_id);
        assert!(
            container
                .complete_execution(transaction_id, None, |_| unreachable!())
                .is_none()
        );
        assert_eq!(container.buffer_len(), 1);

        let transaction = container
            .complete_recheck(transaction_id, true)
            .expect("terminal execution drops after recheck completes");
        // SAFETY: the test has removed the transaction from scheduler state.
        unsafe { transaction.transaction.free(allocator) };
    }
}
