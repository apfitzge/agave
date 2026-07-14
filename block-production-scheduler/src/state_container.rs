use {
    crate::transaction::{TpuTransactionMeta, TransactionId},
    agave_scheduler_bindings::SharablePubkeys,
    agave_scheduling_utils::transaction_ptr::TransactionPtr,
    slab::Slab,
    std::{
        collections::{BTreeSet, btree_set::Range},
        iter::Rev,
        ops::Bound,
    },
};

/// A transaction that has passed external check-worker validation.
#[allow(dead_code)]
pub(crate) struct CheckedTransaction {
    pub(crate) transaction: TransactionPtr,
    pub(crate) meta: TpuTransactionMeta,
    pub(crate) resolved_pubkeys: SharablePubkeys,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransactionPriorityId {
    priority: u64,
    id: TransactionId,
}

impl TransactionPriorityId {
    #[allow(dead_code)]
    pub(crate) const fn transaction_id(self) -> TransactionId {
        self.id
    }
}

/// Owns checked transactions for their entire scheduler lifetime and orders queued ones by
/// priority.
pub(crate) struct StateContainer {
    capacity: usize,
    transactions: Slab<CheckedTransaction>,
    queue: BTreeSet<TransactionPriorityId>,
}

impl StateContainer {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "transaction state capacity must be non-zero");
        Self {
            capacity,
            transactions: Slab::with_capacity(capacity),
            queue: BTreeSet::new(),
        }
    }

    /// Inserts a checked transaction and returns any transaction evicted for capacity.
    ///
    /// In-flight transactions remain in the slab but not the priority queue, so they are never
    /// evicted to make room for a new transaction.
    pub(crate) fn push(&mut self, transaction: CheckedTransaction) -> Option<CheckedTransaction> {
        if self.transactions.len() == self.capacity {
            let Some(lowest_priority) = self.queue.first().copied() else {
                return Some(transaction);
            };
            if transaction.meta.priority < lowest_priority.priority {
                return Some(transaction);
            }

            self.queue.remove(&lowest_priority);
            let dropped = self.transactions.remove(lowest_priority.id);
            self.insert(transaction);
            return Some(dropped);
        }

        self.insert(transaction);
        None
    }

    fn insert(&mut self, transaction: CheckedTransaction) {
        let entry = self.transactions.vacant_entry();
        let id = entry.key();
        self.queue.insert(TransactionPriorityId {
            priority: transaction.meta.priority,
            id,
        });
        entry.insert(transaction);
    }

    /// Iterates in descending priority order, resuming strictly below `cursor` when present.
    ///
    /// The caller must collect selected IDs and dequeue them only after this iterator is dropped.
    /// When the iterator reaches the bottom of the queue, the caller resets its cursor to `None`
    /// before the next scan.
    #[allow(dead_code)]
    pub(crate) fn descending_from(
        &self,
        cursor: Option<&TransactionPriorityId>,
    ) -> Rev<Range<'_, TransactionPriorityId>> {
        match cursor {
            None => self.queue.range(..).rev(),
            Some(cursor) => self
                .queue
                .range((Bound::Unbounded, Bound::Excluded(cursor)))
                .rev(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, priority_id: TransactionPriorityId) -> &CheckedTransaction {
        &self.transactions[priority_id.id]
    }

    /// Removes a selected transaction from the priority queue while it is in flight.
    #[allow(dead_code)]
    pub(crate) fn dequeue(&mut self, priority_id: TransactionPriorityId) {
        assert!(
            self.queue.remove(&priority_id),
            "selected transaction must remain queued until removed"
        );
    }

    /// Removes a terminal transaction from scheduler state.
    #[allow(dead_code)]
    pub(crate) fn remove(&mut self, priority_id: TransactionPriorityId) -> CheckedTransaction {
        self.queue.remove(&priority_id);
        self.transactions.remove(priority_id.id)
    }

    #[cfg(test)]
    pub(crate) fn pop(&mut self) -> Option<CheckedTransaction> {
        let priority_id = *self.queue.last()?;
        self.dequeue(priority_id);
        Some(self.remove(priority_id))
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
    use {super::*, core::ptr::NonNull};

    fn checked_transaction(priority: u64) -> CheckedTransaction {
        // SAFETY: the test only uses the pointer as scheduler state; it never dereferences or
        // frees it.
        let transaction = unsafe { TransactionPtr::from_raw_parts(NonNull::dangling(), 0) };
        CheckedTransaction {
            transaction,
            meta: TpuTransactionMeta {
                priority,
                cost: 0,
                flags: 0,
                src_addr: [0; 16],
            },
            resolved_pubkeys: SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            },
        }
    }

    #[test]
    fn descending_scan_resumes_after_dequeued_cursor() {
        let mut container = StateContainer::new(4);
        for priority in [5, 10, 5, 1] {
            assert!(container.push(checked_transaction(priority)).is_none());
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
                    container.get(priority_id).meta.priority,
                    priority_id.priority
                );
                removal_ids.push(priority_id);
            }
        }

        // The iterator's immutable borrow has ended, so dequeue selected IDs in a separate pass.
        for priority_id in removal_ids {
            container.dequeue(priority_id);
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
        let mut container = StateContainer::new(2);
        container.push(checked_transaction(10));
        container.push(checked_transaction(5));

        let in_flight = *container.descending_from(None).next().unwrap();
        container.dequeue(in_flight);
        let dropped = container.push(checked_transaction(7)).unwrap();
        assert_eq!(dropped.meta.priority, 5);
        assert_eq!(container.get(in_flight).meta.priority, 10);
        assert_eq!(container.buffer_len(), 2);

        let queued = *container.descending_from(None).next().unwrap();
        container.dequeue(queued);
        let dropped = container.push(checked_transaction(1)).unwrap();
        assert_eq!(dropped.meta.priority, 1);
        assert_eq!(container.buffer_len(), 2);
    }
}
