use std::{
    collections::{BTreeSet, btree_set::Range},
    iter::Rev,
    ops::Bound,
};

/// A unique transaction identifier paired with its scheduling priority.
///
/// IDs are `usize` so both Slab indices and future shared-memory allocation offsets can use the
/// same priority queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionPriorityId {
    pub priority: u64,
    pub id: usize,
}

impl TransactionPriorityId {
    pub const fn new(priority: u64, id: usize) -> Self {
        Self { priority, id }
    }
}

/// Priority ordering, held retries, and resumable scans for transaction IDs.
pub struct TransactionPriorityQueue {
    capacity: usize,
    queued: BTreeSet<TransactionPriorityId>,
    held: Vec<TransactionPriorityId>,
}

impl TransactionPriorityQueue {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            queued: BTreeSet::new(),
            held: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.queued.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub fn pop_highest(&mut self) -> Option<TransactionPriorityId> {
        self.queued.pop_last()
    }

    pub fn remove(&mut self, id: &TransactionPriorityId) -> bool {
        self.queued.remove(id)
    }

    pub fn min_priority(&self) -> Option<u64> {
        self.queued.first().map(|id| id.priority)
    }

    pub fn min_max_priority(&self) -> Option<(u64, u64)> {
        Some((self.queued.first()?.priority, self.queued.last()?.priority))
    }

    /// Iterates in descending priority order, resuming strictly below `cursor` when present.
    pub fn descending_from(
        &self,
        cursor: Option<&TransactionPriorityId>,
    ) -> Rev<Range<'_, TransactionPriorityId>> {
        match cursor {
            None => self.queued.range(..).rev(),
            Some(cursor) => self
                .queued
                .range((Bound::Unbounded, Bound::Excluded(cursor)))
                .rev(),
        }
    }

    pub fn hold(&mut self, id: TransactionPriorityId) {
        self.held.push(id);
    }

    /// Queues IDs and evicts the lowest-priority queued IDs until `num_transactions` fits.
    ///
    /// `num_transactions` includes queued, held, and in-flight state. The queue has no knowledge
    /// of the state store, so the callback removes evicted state from its owner. This ensures that
    /// in-flight state consumes capacity but cannot be evicted.
    pub fn push(
        &mut self,
        ids: impl Iterator<Item = TransactionPriorityId>,
        num_transactions: usize,
        mut on_evict: impl FnMut(TransactionPriorityId),
    ) -> usize {
        self.queued.extend(ids);

        let num_evicted = num_transactions.saturating_sub(self.capacity);
        for _ in 0..num_evicted {
            let id = self
                .queued
                .pop_first()
                .expect("state over capacity must have a queued transaction");
            on_evict(id);
        }
        num_evicted
    }

    /// Returns held transactions to the queue and evicts as needed without reallocating the held
    /// buffer.
    pub fn flush_held(
        &mut self,
        num_transactions: usize,
        on_evict: impl FnMut(TransactionPriorityId),
    ) -> usize {
        let mut held = core::mem::take(&mut self.held);
        let num_evicted = self.push(held.drain(..), num_transactions, on_evict);
        self.held = held;
        num_evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_id_orders_priority_then_id() {
        assert!(TransactionPriorityId::new(1, 2) < TransactionPriorityId::new(2, 1));
        assert!(TransactionPriorityId::new(1, 1) < TransactionPriorityId::new(1, 2));
    }

    #[test]
    fn held_transactions_resume_on_flush() {
        let mut queue = TransactionPriorityQueue::with_capacity(1);
        let id = TransactionPriorityId::new(10, 0);

        queue.hold(id);
        assert!(queue.is_empty());
        assert_eq!(queue.flush_held(1, |_| unreachable!()), 0);
        assert_eq!(queue.pop_highest(), Some(id));
    }

    #[test]
    fn descending_scan_resumes_below_cursor() {
        let mut queue = TransactionPriorityQueue::with_capacity(4);
        for id in [
            TransactionPriorityId::new(5, 0),
            TransactionPriorityId::new(10, 1),
            TransactionPriorityId::new(5, 2),
            TransactionPriorityId::new(1, 3),
        ] {
            assert_eq!(queue.push(std::iter::once(id), 4, |_| unreachable!()), 0);
        }

        let mut scan = queue.descending_from(None);
        let first = *scan.next().unwrap();
        let second = *scan.next().unwrap();
        drop(scan);

        assert_eq!(first.priority, 10);
        assert_eq!(second.priority, 5);
        assert_eq!(
            queue
                .descending_from(Some(&second))
                .map(|id| id.priority)
                .collect::<Vec<_>>(),
            [5, 1]
        );
    }

    #[test]
    fn evicts_only_queued_transactions() {
        let mut queue = TransactionPriorityQueue::with_capacity(2);
        let high = TransactionPriorityId::new(10, 0);
        let low = TransactionPriorityId::new(5, 1);
        assert_eq!(
            queue.push([high, low].into_iter(), 2, |_| unreachable!()),
            0
        );

        assert_eq!(queue.pop_highest(), Some(high));
        let mut evicted = Vec::new();
        assert_eq!(
            queue.push(std::iter::once(TransactionPriorityId::new(7, 2)), 3, |id| {
                evicted.push(id)
            },),
            1
        );
        assert_eq!(evicted, [low]);
    }
}
