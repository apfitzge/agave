use std::{cmp::Ordering, collections::BTreeSet};

/// Orders transactions by priority, favoring smaller transaction IDs when priorities tie.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransactionPriorityId {
    pub(crate) priority: u64,
    pub(crate) id: usize,
}

impl TransactionPriorityId {
    pub(crate) const fn new(priority: u64, id: usize) -> Self {
        Self { priority, id }
    }
}

impl Ord for TransactionPriorityId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for TransactionPriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Orders queued transaction IDs. The transaction container enforces capacity.
#[derive(Default)]
pub(crate) struct TransactionPriorityQueue {
    queue: BTreeSet<TransactionPriorityId>,
}

impl TransactionPriorityQueue {
    pub(crate) fn insert(&mut self, id: TransactionPriorityId) -> bool {
        self.queue.insert(id)
    }

    pub(crate) fn remove(&mut self, id: &TransactionPriorityId) -> bool {
        self.queue.remove(id)
    }

    pub(crate) fn pop_highest(&mut self) -> Option<TransactionPriorityId> {
        self.queue.pop_last()
    }

    pub(crate) fn pop_lowest(&mut self) -> Option<TransactionPriorityId> {
        self.queue.pop_first()
    }

    pub(crate) fn min_priority(&self) -> Option<u64> {
        self.queue.first().map(|id| id.priority)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_priority_then_id() {
        let mut queue = TransactionPriorityQueue::default();
        let low = TransactionPriorityId::new(1, 3);
        let high = TransactionPriorityId::new(2, 0);
        let tied = TransactionPriorityId::new(2, 1);
        queue.insert(high);
        queue.insert(low);
        queue.insert(tied);

        assert_eq!(queue.min_priority(), Some(low.priority));
        assert_eq!(queue.pop_highest(), Some(high));
        assert_eq!(queue.pop_highest(), Some(tied));
        assert_eq!(queue.pop_highest(), Some(low));
        assert_eq!(queue.pop_highest(), None);
        assert_eq!(queue.min_priority(), None);

        queue.insert(high);
        queue.insert(low);
        queue.insert(tied);
        assert_eq!(queue.pop_lowest(), Some(low));
        assert_eq!(queue.pop_lowest(), Some(tied));
        assert_eq!(queue.pop_lowest(), Some(high));
        assert_eq!(queue.pop_lowest(), None);
    }

    #[test]
    fn removes_selected_and_lowest_priority_transactions() {
        let mut queue = TransactionPriorityQueue::default();
        let low = TransactionPriorityId::new(1, 0);
        let middle = TransactionPriorityId::new(2, 1);
        let high = TransactionPriorityId::new(3, 2);
        queue.insert(low);
        queue.insert(middle);
        queue.insert(high);

        assert!(queue.remove(&middle));
        assert_eq!(queue.pop_lowest(), Some(low));
        assert_eq!(queue.min_priority(), Some(high.priority));
        assert_eq!(queue.pop_highest(), Some(high));
        assert_eq!(queue.pop_lowest(), None);
    }
}
