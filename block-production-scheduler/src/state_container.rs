use {
    crate::transaction::TpuTransactionMeta, agave_scheduler_bindings::SharablePubkeys,
    agave_scheduling_utils::transaction_ptr::TransactionPtr, slab::Slab,
    std::collections::BTreeSet,
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
struct TransactionPriorityId {
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

    #[allow(
        dead_code,
        reason = "the following scheduling stage drains this priority queue"
    )]
    pub(crate) fn pop(&mut self) -> Option<CheckedTransaction> {
        let TransactionPriorityId { id, .. } = self.queue.pop_last()?;
        Some(self.transactions.remove(id))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }
}
