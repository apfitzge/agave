use {
    crate::poh_recorder::Record,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError, TrySendError},
    std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
};

/// Create a channel pair for communicating `Record`s.
pub fn record_channels() -> (RecordSender, RecordReceiver) {
    const CAPACITY: u64 = 1024;
    let (sender, receiver) = bounded(CAPACITY as usize);
    let allowed_insertions = Arc::new(AtomicU64::new(CAPACITY));
    let transaction_indexes = None;
    (
        RecordSender {
            allowed_insertions: allowed_insertions.clone(),
            sender,
            transaction_indexes: transaction_indexes.clone(),
        },
        RecordReceiver {
            is_shutdown: false,
            capacity: CAPACITY,
            allowed_insertions,
            receiver,
            transaction_indexes,
        },
    )
}

#[derive(Clone, Debug)]
pub struct RecordSender {
    allowed_insertions: Arc<AtomicU64>,
    sender: Sender<Record>,
    transaction_indexes: Option<Arc<RwLock<usize>>>,
}

impl RecordSender {
    pub fn try_send(&self, record: Record) -> Result<Option<usize>, TrySendError<Record>> {
        let num_transactions = record.transactions.len();
        loop {
            // Grab lock on transaction_indexes here to ensure we are sequential sending,
            // ONLY if this exists.
            let transaction_indexes = self
                .transaction_indexes
                .as_ref()
                .map(|transaction_indexes| transaction_indexes.write().unwrap());
            // Get the current remaining capacity.
            // If it's 0, the channel is either full or closed - just return immediately.
            let remaining_capacity = self.allowed_insertions.load(Ordering::Acquire);
            if remaining_capacity == 0 {
                return Err(TrySendError::Full(record));
            }

            // Decrement the remaining capacity.
            match self.allowed_insertions.compare_exchange(
                remaining_capacity,
                remaining_capacity - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Send the value over the channel, space has been reserved successfully.
                    self.sender.try_send(record)?;
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
    allowed_insertions: Arc<AtomicU64>,
    receiver: Receiver<Record>,
    transaction_indexes: Option<Arc<RwLock<usize>>>,
}

impl RecordReceiver {
    pub fn should_shutdown(&self, remaining_hashes: u64, ticks_per_slot: u64) -> bool {
        // Make sure we **always** have enough space to record `capacity`
        // entries, where each entry results in 1 hash.
        // This is very conservative and does not assume we have already
        // included ANY ticks.
        remaining_hashes.saturating_sub(ticks_per_slot) < self.capacity
    }

    /// Shut the channel down immediately.
    pub fn shutdown(&mut self) {
        self.is_shutdown = true;
        self.allowed_insertions.store(0, Ordering::Release);
    }

    /// Re-enable the channel after a shutdown.
    pub fn restart(&mut self) {
        assert!(self.is_shutdown);
        assert!(self.receiver.is_empty());
        self.is_shutdown = false;
        self.allowed_insertions
            .store(self.capacity, Ordering::Release);
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
            self.allowed_insertions.fetch_add(1, Ordering::AcqRel);
        }
    }
}
