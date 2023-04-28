use {
    super::transaction_priority_id::TransactionPriorityId,
    crate::{
        banking_stage::scheduler_messages::TransactionId,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        unprocessed_packet_batches::DeserializedPacket,
    },
    min_max_heap::MinMaxHeap,
    solana_poh::poh_recorder::Slot,
    solana_sdk::transaction::SanitizedTransaction,
    std::collections::{
        hash_map::{Entry, OccupiedEntry},
        HashMap,
    },
};

pub(crate) struct SanitizedTransactionTTL {
    pub(crate) transaction: SanitizedTransaction,
    pub(crate) max_age_slot: Slot,
}

pub(crate) struct TransactionPacketContainer {
    priority_queue: MinMaxHeap<TransactionPriorityId>,
    id_to_transaction_ttl: HashMap<TransactionId, SanitizedTransactionTTL>,
    id_to_packet: HashMap<TransactionId, DeserializedPacket>,
}

impl TransactionPacketContainer {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            priority_queue: MinMaxHeap::with_capacity(capacity),
            id_to_transaction_ttl: HashMap::with_capacity(capacity),
            id_to_packet: HashMap::with_capacity(capacity),
        }
    }

    /// Returns true if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.priority_queue.is_empty()
    }

    /// Returns the remaining capacity of the queue
    pub(crate) fn remaining_queue_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    /// Draining iterator (leaves the queue empty).
    pub(crate) fn drain_queue(&mut self) -> impl Iterator<Item = TransactionPriorityId> + '_ {
        self.priority_queue.drain_desc()
    }

    /// Get a non-consuming iterator over the top `n` transactions in the queue.
    pub(crate) fn take_top_n(
        &mut self,
        n: usize,
    ) -> impl Iterator<Item = TransactionPriorityId> + '_ {
        (0..n).map_while(|_| self.priority_queue.pop_max())
    }

    /// Get packet by id.
    pub(crate) fn get_packet_entry(
        &mut self,
        id: TransactionId,
    ) -> Option<OccupiedEntry<TransactionId, DeserializedPacket>> {
        match self.id_to_packet.entry(id) {
            Entry::Occupied(entry) => Some(entry),
            Entry::Vacant(_) => None,
        }
    }

    /// Get transaction by id.
    /// Panics if the transaction does not exist.
    pub(crate) fn get_transaction_entry(
        &mut self,
        id: TransactionId,
    ) -> OccupiedEntry<TransactionId, SanitizedTransactionTTL> {
        match self.id_to_transaction_ttl.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => panic!("transaction must exist"),
        }
    }

    /// Get transaction and packet entries by id.
    /// Panics if either does not exist.
    pub(crate) fn get_transaction_and_packet_entries(
        &mut self,
        id: TransactionId,
    ) -> (
        OccupiedEntry<TransactionId, SanitizedTransactionTTL>,
        OccupiedEntry<TransactionId, DeserializedPacket>,
    ) {
        let Entry::Occupied(transaction_entry) = self.id_to_transaction_ttl.entry(id) else {
            panic!("transaction must exist");
        };

        let Entry::Occupied(packet_entry) = self.id_to_packet.entry(id) else {
            panic!("packet must exist");
        };

        (transaction_entry, packet_entry)
    }

    /// Insert a new transaction into the container's queues and maps.
    pub(crate) fn insert_new_transaction(
        &mut self,
        transaction_id: TransactionId,
        packet: ImmutableDeserializedPacket,
        transaction_ttl: SanitizedTransactionTTL,
    ) {
        let priority_id = TransactionPriorityId::new(packet.priority(), transaction_id);
        if self.push_id_into_queue(priority_id) {
            self.id_to_packet.insert(
                transaction_id,
                DeserializedPacket::from_immutable_section(packet),
            );
            self.id_to_transaction_ttl
                .insert(transaction_id, transaction_ttl);
        }
    }

    /// Retries a transaction - inserts transaction back into map (but not packet).
    pub(crate) fn retry_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction: SanitizedTransaction,
        max_age_slot: Slot,
    ) {
        let priority = self
            .id_to_packet
            .get(&transaction_id)
            .unwrap()
            .immutable_section()
            .priority();
        let priority_id = TransactionPriorityId::new(priority, transaction_id);
        if self.push_id_into_queue(priority_id) {
            self.id_to_transaction_ttl.insert(
                transaction_id,
                SanitizedTransactionTTL {
                    transaction,
                    max_age_slot,
                },
            );
        }
    }

    /// Pushes a transaction id into the priority queue, without inserting the packet or transaction.
    /// Returns true if the id was successfully pushed into the priority queue
    pub(crate) fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
        if self.priority_queue.len() == self.priority_queue.capacity() {
            let popped_id = self.priority_queue.push_pop_min(priority_id);
            if popped_id == priority_id {
                return false;
            } else {
                self.remove_by_id(&popped_id.id);
            }
        } else {
            self.priority_queue.push(priority_id);
        }

        true
    }

    /// Remove packet and transaction by id.
    pub(crate) fn remove_by_id(&mut self, id: &TransactionId) {
        self.id_to_packet.remove(id);
        self.id_to_transaction_ttl.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_perf::packet::Packet,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction, hash::Hash, message::Message,
            signature::Keypair, signer::Signer, system_instruction, transaction::Transaction,
        },
    };

    fn test_packet_and_transaction(
        priority: u64,
    ) -> (ImmutableDeserializedPacket, SanitizedTransactionTTL) {
        let from_keypair = Keypair::new();
        let ixs = vec![
            system_instruction::transfer(
                &from_keypair.pubkey(),
                &solana_sdk::pubkey::new_rand(),
                1,
            ),
            ComputeBudgetInstruction::set_compute_unit_price(priority),
        ];
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = Transaction::new(&[&from_keypair], message, Hash::default());

        let packet = Packet::from_data(None, tx.clone()).unwrap();
        let packet = ImmutableDeserializedPacket::new(packet, None).unwrap();

        let transaction_ttl = SanitizedTransactionTTL {
            transaction: SanitizedTransaction::from_transaction_for_tests(tx),
            max_age_slot: Slot::MAX,
        };
        (packet, transaction_ttl)
    }

    fn push_to_container(container: &mut TransactionPacketContainer, num: usize) {
        for id in 0..num as u64 {
            let priority = id;
            let (packet, transaction_ttl) = test_packet_and_transaction(priority);
            container.insert_new_transaction(TransactionId::new(id), packet, transaction_ttl);
        }
    }

    #[test]
    fn test_is_empty() {
        let mut container = TransactionPacketContainer::with_capacity(1);
        assert!(container.is_empty());

        push_to_container(&mut container, 1);
        assert!(!container.is_empty());
    }

    #[test]
    fn test_priority_queue_capacity() {
        let mut container = TransactionPacketContainer::with_capacity(1);
        push_to_container(&mut container, 5);

        assert_eq!(container.priority_queue.len(), 1);
        assert_eq!(container.id_to_packet.len(), 1);
        assert_eq!(container.id_to_transaction_ttl.len(), 1);
        assert_eq!(
            container
                .id_to_packet
                .iter()
                .map(|p| p.1.immutable_section().priority())
                .next()
                .unwrap(),
            4
        );
    }

    #[test]
    fn test_drain() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let drained = container.drain_queue().collect::<Vec<_>>();
        assert_eq!(
            drained,
            vec![
                TransactionPriorityId::new(4, TransactionId::new(4)),
                TransactionPriorityId::new(3, TransactionId::new(3)),
                TransactionPriorityId::new(2, TransactionId::new(2)),
                TransactionPriorityId::new(1, TransactionId::new(1)),
                TransactionPriorityId::new(0, TransactionId::new(0)),
            ]
        );
        assert!(container.is_empty());
    }

    #[test]
    fn test_take_top_n() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let taken = container.take_top_n(3).collect::<Vec<_>>();
        assert_eq!(
            taken,
            vec![
                TransactionPriorityId::new(4, TransactionId::new(4)),
                TransactionPriorityId::new(3, TransactionId::new(3)),
                TransactionPriorityId::new(2, TransactionId::new(2)),
            ]
        );
        assert_eq!(container.priority_queue.len(), 2);
    }

    #[test]
    fn test_remove_by_id() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        container.remove_by_id(&TransactionId::new(3));
        assert_eq!(container.priority_queue.len(), 5); // remove_by_id does not remove from priority queue
        assert_eq!(container.id_to_packet.len(), 4);
        assert_eq!(container.id_to_transaction_ttl.len(), 4);

        container.remove_by_id(&TransactionId::new(7));
        assert_eq!(container.id_to_packet.len(), 4);
        assert_eq!(container.id_to_transaction_ttl.len(), 4);
    }

    #[test]
    fn test_push_id_into_queue() {
        let mut container = TransactionPacketContainer::with_capacity(1);
        assert!(container.push_id_into_queue(TransactionPriorityId::new(1, TransactionId::new(0))));
        assert_eq!(container.priority_queue.len(), 1);
        assert_eq!(container.id_to_packet.len(), 0);
        assert_eq!(container.id_to_transaction_ttl.len(), 0);

        assert!(container.push_id_into_queue(TransactionPriorityId::new(1, TransactionId::new(1))));
        assert_eq!(container.priority_queue.len(), 1);
        // should be dropped due to capacity
        assert!(!container.push_id_into_queue(TransactionPriorityId::new(0, TransactionId::new(2))));
        assert_eq!(container.priority_queue.len(), 1);
    }

    #[test]
    fn test_get_packet_entry_missing() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        assert!(container.get_packet_entry(TransactionId::new(7)).is_none());
    }

    #[test]
    fn test_get_packet_entry() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let transaction_id = TransactionId::new(3);
        let packet_entry = container.get_packet_entry(transaction_id).unwrap();
        assert_eq!(*packet_entry.key(), transaction_id);
    }

    #[test]
    #[should_panic(expected = "transaction must exist")]
    fn test_get_transaction_entry_panic() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let _ = container.get_transaction_entry(TransactionId::new(7));
    }

    #[test]
    fn test_get_transaction_entry() {
        let mut container = TransactionPacketContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let transaction_id = TransactionId::new(3);
        let transaction_ttl_entry = container.get_transaction_entry(transaction_id);
        assert_eq!(*transaction_ttl_entry.key(), transaction_id);
    }
}