use {
    crate::poh_recorder::Record,
    crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError},
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Create a channel pair for communicating `Record`s.
pub fn record_channels(capacity: u64) -> (RecordSender, RecordReceiver) {
    let (sender, receiver) = bounded(capacity as usize);
    let allowed_insertions = Arc::new(AtomicU64::new(capacity));
    (
        RecordSender {
            allowed_insertions: allowed_insertions.clone(),
            sender,
        },
        RecordReceiver {
            is_shutdown: false,
            capacity,
            allowed_insertions,
            receiver,
        },
    )
}

#[derive(Clone)]
pub struct RecordSender {
    allowed_insertions: Arc<AtomicU64>,
    sender: Sender<Record>,
}

impl RecordSender {
    pub fn try_send(&self, record: Record) -> Result<(), TrySendError<Record>> {
        loop {
            // Get the current remaining capacity.
            // If it's 0, the channel is either full or closed - just return immediately.
            let remaining_capacity = self.allowed_insertions.load(Ordering::Acquire);
            if remaining_capacity == 0 {
                return Ok(());
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
                    return Ok(());
                }
                Err(_) => {
                    // Value was changed by another thread (producer or consumer).
                    continue;
                }
            }
        }
    }
}

pub struct RecordReceiver {
    is_shutdown: bool,
    capacity: u64,
    allowed_insertions: Arc<AtomicU64>,
    receiver: Receiver<Record>,
}

impl RecordReceiver {
    /// Shut the channel down immediately.
    pub fn shutdown(&mut self) {
        self.is_shutdown = true;
        self.allowed_insertions.store(0, Ordering::Release);
    }

    /// Re-enable the channel after a shutdown.
    pub fn restart(&mut self) {
        self.is_shutdown = false;
        self.allowed_insertions
            .store(self.capacity, Ordering::Release);
    }

    /// Drain the channel - this should only be called if the channel is shutdown.
    pub fn drain(&self) -> impl Iterator<Item = Record> + '_ {
        assert!(self.is_shutdown);
        std::iter::from_fn(|| self.try_recv().ok())
    }

    /// Try to receive a record from the channel.
    pub fn try_recv(&self) -> Result<Record, TryRecvError> {
        let record = self.receiver.try_recv()?;

        // If we received a record AND are not shutdown, increment the allowed insertions.
        if !self.is_shutdown {
            self.allowed_insertions.fetch_add(1, Ordering::AcqRel);
        }

        Ok(record)
    }
}
