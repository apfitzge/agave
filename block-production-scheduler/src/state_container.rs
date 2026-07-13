use {
    crate::transaction::TpuTransactionMeta,
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
#[allow(
    dead_code,
    reason = "the following scheduling stage consumes checked transactions from this container"
)]
pub(crate) struct CheckedTransaction {
    pub(crate) transaction: TransactionPtr,
    pub(crate) meta: TpuTransactionMeta,
    pub(crate) resolved_pubkeys: SharablePubkeys,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransactionPriorityId {
    priority: u64,
    id: usize,
}

/// Owns checked transactions and orders them by priority for scheduling.
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
    pub(crate) fn push(&mut self, transaction: CheckedTransaction) -> Option<CheckedTransaction> {
        if self.transactions.len() == self.capacity {
            let lowest_priority = *self.queue.first().expect("full state has a queued entry");
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
    /// The caller must collect selected IDs and remove them only after this iterator is dropped.
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

    /// Removes a transaction selected during a completed priority scan.
    #[allow(dead_code)]
    pub(crate) fn remove(&mut self, priority_id: TransactionPriorityId) -> CheckedTransaction {
        assert!(
            self.queue.remove(&priority_id),
            "selected transaction must remain queued until removed"
        );
        self.transactions.remove(priority_id.id)
    }

    #[cfg(test)]
    pub(crate) fn pop(&mut self) -> Option<CheckedTransaction> {
        let priority_id = *self.queue.last()?;
        Some(self.remove(priority_id))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
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
    fn descending_scan_resumes_after_removed_cursor() {
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

        // The iterator's immutable borrow has ended, so remove from both the priority queue and
        // the slab in a separate pass.
        for priority_id in removal_ids {
            container.remove(priority_id);
        }
        assert_eq!(container.len(), 2);

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
}
