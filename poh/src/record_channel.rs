use {
    crate::poh_recorder::Record,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError},
    solana_clock::Slot,
    std::{
        ops::Deref,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
};

/// Create a channel pair for communicating `Record`s.
pub fn record_channels(track_transaction_indexes: bool) -> (RecordSender, RecordReceiver) {
    const CAPACITY: u64 = 1024;
    let (sender, receiver) = bounded(CAPACITY as usize);
    let slot_allowed_insertions = SlotAllowedInsertions::new(0, CAPACITY);
    let transaction_indexes = if track_transaction_indexes {
        Some(Arc::new(RwLock::new(0)))
    } else {
        None
    };
    (
        RecordSender {
            slot_allowed_insertions: slot_allowed_insertions.clone(),
            sender,
            transaction_indexes: transaction_indexes.clone(),
        },
        RecordReceiver {
            is_shutdown: false,
            capacity: CAPACITY,
            slot_allowed_insertions,
            receiver,
            transaction_indexes,
        },
    )
}

#[derive(Clone, Debug)]
pub struct RecordSender {
    slot_allowed_insertions: SlotAllowedInsertions,
    sender: Sender<Record>,
    transaction_indexes: Option<Arc<RwLock<usize>>>,
}

pub enum RecordSenderError {
    Full(Record),
    InactiveSlot,
    Disconnected,
}

impl RecordSender {
    pub fn try_send(&self, record: Record) -> Result<Option<usize>, RecordSenderError> {
        let num_transactions = record
            .transaction_batches
            .iter()
            .map(|batch| batch.len())
            .sum::<usize>();
        loop {
            // Grab lock on transaction_indexes here to ensure we are sequential sending,
            // ONLY if this exists.
            let transaction_indexes = self
                .transaction_indexes
                .as_ref()
                .map(|transaction_indexes| transaction_indexes.write().unwrap());

            // Get the current slot and allowed insertions.
            // If the number of allowed insertions is 0, the channel is full - just return immediately.
            // If the `record`'s slot is different from the current slot, return immediately.
            let current_slot_allowed_insertions =
                self.slot_allowed_insertions.load(Ordering::Acquire);
            let slot = SlotAllowedInsertions::slot(current_slot_allowed_insertions);
            let allowed_insertions =
                SlotAllowedInsertions::allowed_insertions(current_slot_allowed_insertions);
            if slot != record.slot {
                return Err(RecordSenderError::InactiveSlot);
            }
            if allowed_insertions == 0 {
                return Err(RecordSenderError::Full(record));
            }

            let slot_allowed_insertions =
                SlotAllowedInsertions::encoded_value(slot, allowed_insertions - 1);

            // Decrement the remaining capacity.
            match self.slot_allowed_insertions.compare_exchange(
                current_slot_allowed_insertions,
                slot_allowed_insertions,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Send the value over the channel, space has been reserved successfully.
                    if let Err(err) = self.sender.try_send(record) {
                        // The channel is disconnected, return the error.
                        assert!(err.is_disconnected());
                        return Err(RecordSenderError::Disconnected);
                    }
                    return Ok(transaction_indexes.map(|mut transaction_indexes| {
                        let transaction_starting_index = *transaction_indexes;
                        *transaction_indexes = transaction_starting_index + num_transactions;
                        transaction_starting_index
                    }));
                }
                Err(_) => {
                    // Value was changed by another thread (producer or consumer).
                    continue;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RecordReceiver {
    is_shutdown: bool,
    capacity: u64,
    slot_allowed_insertions: SlotAllowedInsertions,
    receiver: Receiver<Record>,
    transaction_indexes: Option<Arc<RwLock<usize>>>,
}

impl RecordReceiver {
    pub fn should_shutdown(&self, remaining_hashes: u64, ticks_per_slot: u64) -> bool {
        // Make sure we **always** have enough space to record `capacity`
        // entries, where each entry results in 1 hash.
        // This is very conservative and does not assume we have already
        // included ANY ticks.
        remaining_hashes.saturating_sub(ticks_per_slot) <= 2 * self.capacity
    }

    /// Shut the channel down immediately.
    pub fn shutdown(&mut self) {
        self.is_shutdown = true;
        // The slot value doesn't matter here because we are done with whatever
        // slot we were on.
        self.slot_allowed_insertions
            .store(Slot::MAX, Ordering::Release);
    }

    /// Re-enable the channel after a shutdown.
    pub fn restart(&mut self, slot: Slot) {
        assert!(self.is_shutdown);
        assert!(self.receiver.is_empty());
        self.is_shutdown = false;
        self.slot_allowed_insertions.store(
            SlotAllowedInsertions::encoded_value(slot, self.capacity),
            Ordering::Release,
        );
        if let Some(transaction_indexes) = self.transaction_indexes.as_ref() {
            *transaction_indexes.write().unwrap() = 0;
        }
    }

    /// Drain the channel - this should only be called if the channel is shutdown.
    pub fn drain(&self) -> impl Iterator<Item = Record> + '_ {
        assert!(self.is_shutdown);
        std::iter::from_fn(|| self.try_recv().ok())
    }

    /// Check if the channel is empty.
    /// This is only accurate if the channel is shutdown.
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Try to receive a record from the channel.
    pub fn try_recv(&self) -> Result<Record, TryRecvError> {
        let record = self.receiver.try_recv()?;
        self.on_received_record();
        Ok(record)
    }

    pub fn recv_timeout(&self, duration: Duration) -> Result<Record, RecvTimeoutError> {
        let record = self.receiver.recv_timeout(duration)?;
        self.on_received_record();
        Ok(record)
    }

    fn on_received_record(&self) {
        // If we received a record AND are not shutdown, increment the allowed insertions.
        if !self.is_shutdown {
            self.slot_allowed_insertions.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// AtomicU64 that represents a combination of the current `slot` and the
/// number of allowed insertions into the record channel.
/// This is done because we need to ensure that the record insertion is
/// done atomically wrt channel shutdown/capacity, but we also need to know
/// the slot.
///
/// The `slot` is stored in the upper 48 bits, and the `allowed_insertions` in
/// the lower 16 bits.
#[derive(Clone, Debug)]
struct SlotAllowedInsertions(Arc<AtomicU64>);

impl SlotAllowedInsertions {
    const MAX_SLOT: Slot = (1 << 48) - 1;
    const MAX_ALLOWED_INSERTIONS: u64 = (1 << 16) - 1;

    fn new(slot: Slot, allowed_insertions: u64) -> Self {
        let value = Self::encoded_value(slot, allowed_insertions);
        Self(Arc::new(AtomicU64::new(value)))
    }

    fn encoded_value(slot: Slot, allowed_insertions: u64) -> u64 {
        assert!(slot <= Self::MAX_SLOT);
        assert!(allowed_insertions <= Self::MAX_ALLOWED_INSERTIONS);
        slot << 16 | allowed_insertions
    }

    fn slot(value: u64) -> Slot {
        (value >> 16) as Slot
    }

    fn allowed_insertions(value: u64) -> u64 {
        value & 0xffff
    }
}

impl Deref for SlotAllowedInsertions {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
