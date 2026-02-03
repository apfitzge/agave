#[cfg(feature = "shuttle-test")]
use shuttle::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
#[cfg(not(feature = "shuttle-test"))]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use {
    crate::poh_recorder::Record,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError},
    log::error,
    solana_clock::BankId,
    std::time::Duration,
};

/// Create a channel pair for communicating [`Record`]s.
/// Transaction processing threads (workers/vote thread) send records, and
/// PohService receives them.
///
/// The receiver can shutdown the channel, preventing any further sends,
/// and can restart the channel for a new bank id, re-enabling sends.
/// The sender does not wait for the receiver to pick up records, and will return
/// immediately if the channel is full, shutdown, or if the bank id has changed.
///
/// The channel has a bounded capacity based on the maximum number of allowed
/// insertions at a given time. This is for guaranteeing that once shutdown the
/// service can always process all sent records correctly without dropping any
/// i.e. once sent records can be guaranteed to be recorded.
pub fn record_channels(track_transaction_indexes: bool) -> (RecordSender, RecordReceiver) {
    const CAPACITY: usize = BankIdAllowedInsertions::MAX_ALLOWED_INSERTIONS as usize;
    let (sender, receiver) = bounded(CAPACITY);

    // Begin in a shutdown state.
    let bank_id_allowed_insertions = BankIdAllowedInsertions::new_shutdown();
    let transaction_indexes = if track_transaction_indexes {
        Some(Arc::new(Mutex::new(0)))
    } else {
        None
    };

    let active_senders = Arc::new(AtomicU64::new(0));
    (
        RecordSender {
            active_senders: active_senders.clone(),
            bank_id_allowed_insertions: bank_id_allowed_insertions.clone(),
            sender,
            transaction_indexes: transaction_indexes.clone(),
        },
        RecordReceiver {
            active_senders,
            bank_id_allowed_insertions,
            receiver,
            capacity: CAPACITY as u64,
            transaction_indexes,
        },
    )
}

#[derive(Debug)]
pub enum RecordSenderError {
    /// The channel is full, the record was not sent.
    Full,
    /// The channel is in a shutdown state, it is not valid to
    /// send records for this bank anymore.
    Shutdown,
    /// The record's bank id does not match the current bank id of the channel.
    InactiveBankId,
    /// The receiver has been dropped, the channel is disconnected.
    Disconnected,
}

/// A sender for sending [`Record`]s to PohService.
/// The sender does not wait for service to pick up the records, and will return
/// immediately if the channel is full, shutdown, or if the bank id has changed.
#[derive(Clone, Debug)]
pub struct RecordSender {
    /// Used to track active senders for the current bank id. Used so that the receiver
    /// side can determine that no more sends are in-flight while shutting down.
    active_senders: Arc<AtomicU64>,
    bank_id_allowed_insertions: BankIdAllowedInsertions,
    sender: Sender<Record>,
    transaction_indexes: Option<Arc<Mutex<usize>>>,
}

impl RecordSender {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    pub fn try_send(&self, record: Record) -> Result<Option<usize>, RecordSenderError> {
        let num_transactions: usize = record
            .transaction_batches
            .iter()
            .map(|batch| batch.len())
            .sum();
        assert!(num_transactions > 0);
        loop {
            // Grab lock on `transaction_indexes` here to ensure we are sending
            // sequentially, ONLY if this exists.
            let transaction_indexes = self
                .transaction_indexes
                .as_ref()
                .map(|transaction_indexes| transaction_indexes.lock().unwrap());

            // Get the current bank_id and allowed insertions.
            // If the number of allowed insertions is less than the number of
            // batches, the channel is full - just return immediately.
            // If the `record`'s bank_id is different from the current bank_id,
            // return immediately.
            // Use SeqCst to ensure we see the shutdown store if it happened.
            let current_bank_id_allowed_insertions =
                self.bank_id_allowed_insertions.0.load(Ordering::SeqCst);
            let (bank_id, allowed_insertions) = (
                BankIdAllowedInsertions::bank_id(current_bank_id_allowed_insertions),
                BankIdAllowedInsertions::allowed_insertions(current_bank_id_allowed_insertions),
            );

            if bank_id == BankIdAllowedInsertions::DISABLED_BANK_ID {
                return Err(RecordSenderError::Shutdown);
            }
            if bank_id != record.bank_id {
                return Err(RecordSenderError::InactiveBankId);
            }
            if allowed_insertions < record.transaction_batches.len() as u64 {
                return Err(RecordSenderError::Full);
            }

            // Increment active_senders before CAS so the receiver can see this send is in-flight.
            self.active_senders.fetch_add(1, Ordering::SeqCst);

            let new_bank_id_allowed_insertions = BankIdAllowedInsertions::encoded_value(
                bank_id,
                allowed_insertions.wrapping_sub(record.transaction_batches.len() as u64),
            );

            if self
                .bank_id_allowed_insertions
                .0
                .compare_exchange(
                    current_bank_id_allowed_insertions,
                    new_bank_id_allowed_insertions,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                // Failed to reserve space, decrement active senders and try again.
                self.active_senders.fetch_sub(1, Ordering::SeqCst);
                continue;
            }

            match self.sender.try_send(record) {
                Ok(_) => {
                    self.active_senders.fetch_sub(1, Ordering::SeqCst);
                    return Ok(transaction_indexes.map(|mut transaction_indexes| {
                        let transaction_starting_index = *transaction_indexes;
                        *transaction_indexes += num_transactions;
                        transaction_starting_index
                    }));
                }
                Err(err) => {
                    assert!(err.is_disconnected());
                    self.active_senders.fetch_sub(1, Ordering::SeqCst);
                    return Err(RecordSenderError::Disconnected);
                }
            }
        }
    }
}

/// A receiver for receiving [`Record`]s in PohService.
/// The receiver can shutdown the channel, preventing any further sends,
/// and can restart the channel for a new bank id, re-enabling sends.
pub struct RecordReceiver {
    capacity: u64,
    active_senders: Arc<AtomicU64>,
    bank_id_allowed_insertions: BankIdAllowedInsertions,
    receiver: Receiver<Record>,
    transaction_indexes: Option<Arc<Mutex<usize>>>,
}

impl RecordReceiver {
    /// Returns true if the channel should be shutdown.
    pub fn should_shutdown(&self, remaining_hashes_in_slot: u64, ticks_per_slot: u64) -> bool {
        // This channel must guarantee that all sent records are recorded.
        // Each batch in a record consumes one hash in the PoH stream,
        // each tick also consumes at least one hash in the PoH stream.
        // As a conservative estimate, we assume no ticks have been recorded.
        remaining_hashes_in_slot.saturating_sub(ticks_per_slot) <= self.capacity
    }

    /// Shutdown the channel immediately.
    pub fn shutdown(&mut self) {
        let current_state = self.bank_id_allowed_insertions.0.load(Ordering::Acquire);
        let current_bank_id = BankIdAllowedInsertions::bank_id(current_state);
        let active_senders = self.active_senders.load(Ordering::Acquire);
        let receiver_len = self.receiver.len();
        error!(
            "#ASH: RecordReceiver::shutdown called: current_bank_id={}, active_senders={}, \
             receiver_len={}, is_empty={}",
            current_bank_id,
            active_senders,
            receiver_len,
            self.receiver.is_empty()
        );
        self.bank_id_allowed_insertions.shutdown();
    }

    /// Check if the channel is shutdown.
    pub fn is_shutdown(&self) -> bool {
        BankIdAllowedInsertions::bank_id(self.bank_id_allowed_insertions.0.load(Ordering::Acquire))
            == BankIdAllowedInsertions::DISABLED_BANK_ID
    }

    /// Re-enable the channel after a shutdown.
    pub fn restart(&mut self, bank_id: BankId) {
        assert!(bank_id <= BankIdAllowedInsertions::MAX_BANK_ID);
        let is_empty = self.receiver.is_empty();
        let active_senders = self.active_senders.load(Ordering::Acquire);
        let current_state = self.bank_id_allowed_insertions.0.load(Ordering::Acquire);
        let current_bank_id = BankIdAllowedInsertions::bank_id(current_state);
        let allowed_insertions = BankIdAllowedInsertions::allowed_insertions(current_state);
        let is_shutdown = current_bank_id == BankIdAllowedInsertions::DISABLED_BANK_ID;
        error!(
            "#ASH: RecordReceiver::restart called: new_bank_id={}, is_empty={}, \
             active_senders={}, current_bank_id={}, allowed_insertions={}, is_shutdown={}, \
             capacity={}",
            bank_id,
            is_empty,
            active_senders,
            current_bank_id,
            allowed_insertions,
            is_shutdown,
            self.capacity
        );
        if !is_empty {
            error!(
                "#ASH: RecordReceiver::restart ASSERTION WILL FAIL - channel not empty! Dumping \
                 channel contents:"
            );
            // Log what's in the channel without consuming
            error!(
                "#ASH: RecordReceiver::restart - receiver.len()={}, is_safe_to_restart={}",
                self.receiver.len(),
                active_senders == 0 && is_empty
            );
        }
        assert!(is_empty); // Should be empty before restarting.

        // Reset transaction indexes if tracking them - BEFORE allowing new insertions.
        let transaction_indexes_lock =
            self.transaction_indexes
                .as_ref()
                .map(|transaction_indexes| {
                    let mut lock = transaction_indexes.lock().unwrap();
                    *lock = 0;
                    lock
                });

        self.bank_id_allowed_insertions.0.store(
            BankIdAllowedInsertions::encoded_value(bank_id, self.capacity),
            Ordering::Release,
        );

        // Drop lock AFTER allowing new insertions. This makes any sends grabbing locks
        // wait until after the bank id has been changed. Meaning the CAS in try_send
        // will always succeed, if passing previous checks.
        drop(transaction_indexes_lock);
    }

    /// Drain all available records from the channel with `try_recv` loop.
    pub fn drain(&self) -> impl Iterator<Item = Record> + '_ {
        core::iter::from_fn(|| self.try_recv().ok())
    }

    /// Channel is empty and there are no active threads attempting to send.
    pub fn is_safe_to_restart(&self) -> bool {
        // We need to check active_senders TWICE with is_empty in between.
        // This prevents a TOCTOU race where:
        // 1) receiver loads active_senders = 0
        // 2) sender increments active_senders = 1 and loads state (could be valid!)
        // 3) receiver checks is_empty = true
        // 4) receiver returns true, but sender might complete a send!
        //
        // By checking active_senders again after is_empty, we ensure no sender
        // started between the two checks.
        //
        // Additionally, checking active_senders first (before is_empty) prevents:
        // 1) sender has not sent yet, active_senders = 1. is_empty = true.
        // 2) sender sends, decrements active_senders = 0.
        // 3) receiver checks active_senders == 0 && is_empty == true,
        //    thinks the channel is empty with no active senders, but there is
        //    actually a record in the channel now!
        // Use SeqCst to ensure we see the most recent active_senders from any sender
        let active_senders_before = self.active_senders.load(Ordering::SeqCst);
        if active_senders_before != 0 {
            error!(
                "#ASH: RecordReceiver::is_safe_to_restart: \
                 active_senders_before={active_senders_before}, result=false",
            );
            return false;
        }
        let is_empty = self.receiver.is_empty();
        if !is_empty {
            error!("#ASH: RecordReceiver::is_safe_to_restart: is_empty=false, result=false");
            return false;
        }
        // Double-check: ensure no sender started between the two checks
        let active_senders_after = self.active_senders.load(Ordering::SeqCst);
        let result = active_senders_after == 0;
        error!(
            "#ASH: RecordReceiver::is_safe_to_restart: \
             active_senders_before={active_senders_before}, is_empty={is_empty}, \
             active_senders_after={active_senders_after}, result={result}",
        );
        result
    }

    /// Try to receive a record from the channel.
    pub fn try_recv(&self) -> Result<Record, TryRecvError> {
        // In order to avoid returning None when there was an active sender
        // we load `active_senders` prior to try_recv.
        let mut sender_active = self.active_senders.load(Ordering::Acquire) > 0;

        loop {
            match self.receiver.try_recv() {
                Ok(record) => {
                    let num_batches = record.transaction_batches.len();
                    error!(
                        "#ASH: RecordReceiver::try_recv received record: bank_id={}, \
                         num_batches={}, receiver_len_after={}",
                        record.bank_id,
                        num_batches,
                        self.receiver.len()
                    );
                    self.on_received_record(num_batches as u64);
                    return Ok(record);
                }
                Err(TryRecvError::Empty) => {
                    if sender_active {
                        // If the sender is STILL active then we must continue to wait.
                        // If there is no longer an active sender then we can break,
                        //   **after** checking the channel again.
                        // Both cases here are handled if we update `sender_active` and
                        // go to the next iteration of the loop.
                        sender_active = self.active_senders.load(Ordering::Acquire) > 0;
                        continue;
                    }
                    return Err(TryRecvError::Empty);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Receive a record from the channel, waiting up to `duration`.
    pub fn recv_timeout(&self, duration: Duration) -> Result<Record, RecvTimeoutError> {
        let record = self.receiver.recv_timeout(duration)?;
        let num_batches = record.transaction_batches.len();
        error!(
            "#ASH: RecordReceiver::recv_timeout received record: bank_id={}, num_batches={}, \
             receiver_len_after={}",
            record.bank_id,
            num_batches,
            self.receiver.len()
        );
        self.on_received_record(num_batches as u64);
        Ok(record)
    }

    fn on_received_record(&self, num_batches: u64) {
        // The record has been received and processed, so increment the number
        // of allowed insertions, so that new records can be sent.
        self.bank_id_allowed_insertions
            .0
            .fetch_add(num_batches, Ordering::AcqRel);
    }
}

/// Encoded u64 where the upper 54 bits are the bank_id and the lower 10 bits are
/// the number of allowed insertions at the current time.
/// The number of allowed insertions is based on the number of **batches** sent,
/// not the number of [`Record`]. This is because each batch is a separate hash
/// in the PoH stream, and we must guarantee enough space for each hash, if we
/// allow a [`Record`] to be sent.
/// The allowed insertions uses 10 bits allowing up to 1023 insertions at a
/// given time. This is for messages that have been sent but not yet processed
/// by the receiver.
/// The `allowed_insertions` is a budget and is decremented when something is
/// sent/inserted into the channel, and incremented when something is received
/// from the channel.
#[derive(Clone, Debug)]
struct BankIdAllowedInsertions(Arc<AtomicU64>);

impl BankIdAllowedInsertions {
    const NUM_BITS: u64 = 64;
    /// Number of bits used to track allowed insertions.
    const ALLOWED_INSERTIONS_BITS: u64 = 10;
    const BANK_ID_BITS: u64 = Self::NUM_BITS - Self::ALLOWED_INSERTIONS_BITS;

    const DISABLED_BANK_ID: BankId = (1 << Self::BANK_ID_BITS) - 1;
    const MAX_BANK_ID: BankId = Self::DISABLED_BANK_ID - 1;
    const MAX_ALLOWED_INSERTIONS: u64 = (1 << Self::ALLOWED_INSERTIONS_BITS) - 1;

    const SHUTDOWN: u64 = Self::encoded_value(Self::DISABLED_BANK_ID, 0);

    /// Create a new `BankIdAllowedInsertions` with state consistent with a
    /// shutdown state:
    /// - bank_id = `DISABLED_BANK_ID`
    /// - allowed_insertions = 0
    fn new_shutdown() -> Self {
        Self(Arc::new(AtomicU64::new(Self::SHUTDOWN)))
    }

    /// Shutdown the channel immediately.
    fn shutdown(&self) {
        self.0.store(Self::SHUTDOWN, Ordering::SeqCst);
    }

    const fn encoded_value(bank_id: BankId, allowed_insertions: u64) -> u64 {
        assert!(bank_id <= Self::DISABLED_BANK_ID);
        assert!(allowed_insertions <= Self::MAX_ALLOWED_INSERTIONS);
        (bank_id << Self::ALLOWED_INSERTIONS_BITS) | allowed_insertions
    }

    /// The current bank_id, or [`Self::DISABLED_BANK_ID`] if shutdown.
    fn bank_id(value: u64) -> BankId {
        (value >> Self::ALLOWED_INSERTIONS_BITS) & Self::DISABLED_BANK_ID
    }

    /// How many insertions/sends are allowed at this time.
    fn allowed_insertions(value: u64) -> u64 {
        value & Self::MAX_ALLOWED_INSERTIONS
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_hash::Hash, solana_transaction::versioned::VersionedTransaction};

    pub(super) fn test_record(bank_id: BankId, num_batches: usize) -> Record {
        Record {
            bank_id,
            transaction_batches: (0..num_batches)
                .map(|_| vec![VersionedTransaction::default()])
                .collect(),
            mixins: (0..num_batches).map(|_| Hash::default()).collect(),
        }
    }

    #[test]
    fn test_record_channels() {
        let (sender, mut receiver) = record_channels(false);

        // Initially shutdown.
        assert!(matches!(
            sender.try_send(test_record(0, 1)),
            Err(RecordSenderError::Shutdown)
        ));

        // Restart for bank_id 1.
        receiver.restart(1);

        // Record for bank_id 0 fails.
        assert!(matches!(
            sender.try_send(test_record(0, 1)),
            Err(RecordSenderError::InactiveBankId)
        ));

        // Record for bank_id 1 with 1 batch succeeds.
        assert!(matches!(sender.try_send(test_record(1, 1)), Ok(None)));

        // Record for bank_id 1 with 1023 batches fails (channel full).
        assert!(matches!(
            sender.try_send(test_record(1, 1023)),
            Err(RecordSenderError::Full)
        ));

        // Record for bank_id 1 with 1022 batches succeeds (channel now full).
        assert!(matches!(sender.try_send(test_record(1, 1022)), Ok(None)));

        // Record for bank_id 1 with 1 batch fails (channel full).
        assert!(matches!(
            sender.try_send(test_record(1, 1)),
            Err(RecordSenderError::Full)
        ));

        // Receive 1 record.
        assert!(receiver.try_recv().is_ok());
        assert!(!receiver.is_safe_to_restart());
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.is_safe_to_restart());
    }

    #[test]
    fn test_record_channels_track_indexes() {
        let (sender, mut receiver) = record_channels(true);

        // Initially shutdown.
        assert!(matches!(
            sender.try_send(test_record(0, 1)),
            Err(RecordSenderError::Shutdown)
        ));

        // Restart for bank_id 1.
        receiver.restart(1);

        // Record for bank_id 0 fails.
        assert!(matches!(
            sender.try_send(test_record(0, 1)),
            Err(RecordSenderError::InactiveBankId)
        ));

        // Record for bank_id 1 with 1 batch succeeds.
        assert!(matches!(sender.try_send(test_record(1, 1)), Ok(Some(0))));

        // Record for bank_id 1 with 2 batches (3 transactions) succeeds.
        let mut record = test_record(1, 2);
        record
            .transaction_batches
            .last_mut()
            .unwrap()
            .push(VersionedTransaction::default());
        assert!(matches!(sender.try_send(record), Ok(Some(1))));

        assert!(*sender.transaction_indexes.as_ref().unwrap().lock().unwrap() == 4);
    }

    /// Stress test to try to reproduce the shutdown/drain race condition.
    /// The bug: is_safe_to_restart() returns true, but a sender completes a send afterward.
    #[test]
    fn test_shutdown_drain_race_stress() {
        use std::{
            sync::{atomic::AtomicBool, Arc, Barrier},
            thread,
        };

        const NUM_ITERATIONS: usize = 10_000;
        const NUM_SENDERS: usize = 4;

        for iteration in 0..NUM_ITERATIONS {
            let (sender, mut receiver) = record_channels(false);
            receiver.restart(0);

            let stop = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(NUM_SENDERS + 1));

            // Spawn sender threads that continuously try to send
            let handles: Vec<_> = (0..NUM_SENDERS)
                .map(|_| {
                    let sender = sender.clone();
                    let stop = stop.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        barrier.wait();
                        while !stop.load(Ordering::Relaxed) {
                            let _ = sender.try_send(test_record(0, 1));
                            // Small yield to increase interleaving
                            thread::yield_now();
                        }
                    })
                })
                .collect();

            // Wait for all senders to start
            barrier.wait();

            // Let senders run for a bit
            thread::yield_now();

            // Now do the shutdown + drain sequence
            receiver.shutdown();

            // Drain loop (same as record_and_complete_block)
            while !receiver.is_safe_to_restart() {
                let _ = receiver.try_recv();
            }

            // CRITICAL: After is_safe_to_restart() returned true, channel must be empty
            // and must STAY empty (no in-flight sends can complete)
            let empty_immediately = receiver.receiver.is_empty();
            let active_senders = sender.active_senders.load(Ordering::Acquire);

            // Give any "in-flight" operations a chance to complete
            thread::yield_now();

            let empty_after_yield = receiver.receiver.is_empty();

            // Stop senders
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }

            // Final check
            let empty_final = receiver.receiver.is_empty();

            if !empty_immediately || !empty_after_yield || !empty_final {
                panic!(
                    "Race condition detected at iteration \
                     {iteration}!\nempty_immediately={empty_immediately}, \
                     active_senders={active_senders}, empty_after_yield={empty_after_yield}, \
                     empty_final={empty_final}\nA sender completed a send after \
                     is_safe_to_restart() returned true."
                );
            }
        }
    }

    /// Test that specifically checks the invariant: once shutdown is called and
    /// is_safe_to_restart() returns true, no sender can successfully send.
    #[test]
    fn test_no_send_after_safe_to_restart() {
        use std::{
            sync::{atomic::AtomicBool, Arc, Barrier},
            thread,
        };

        const NUM_ITERATIONS: usize = 5_000;

        for _ in 0..NUM_ITERATIONS {
            let (sender, mut receiver) = record_channels(false);
            receiver.restart(0);

            let can_proceed = Arc::new(AtomicBool::new(false));
            let sender_done = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(2));

            // Sender thread: waits at barrier, then tries to send
            let sender_clone = sender.clone();
            let can_proceed_clone = can_proceed.clone();
            let sender_done_clone = sender_done.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait();
                // Spin until receiver signals we can proceed
                while !can_proceed_clone.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                // Now try to send - this simulates a sender that was "in-flight"
                let result = sender_clone.try_send(test_record(0, 1));
                sender_done_clone.store(true, Ordering::Release);
                result
            });

            // Receiver: sync with sender, then shutdown
            barrier.wait();

            // Shutdown and drain
            receiver.shutdown();
            while !receiver.is_safe_to_restart() {
                let _ = receiver.try_recv();
            }

            // Now is_safe_to_restart() has returned true.
            // Signal sender to proceed with its send attempt.
            can_proceed.store(true, Ordering::Release);

            // Wait for sender to complete
            while !sender_done.load(Ordering::Acquire) {
                thread::yield_now();
            }

            let send_result = handle.join().unwrap();

            // The sender's send MUST have failed (Shutdown error)
            // If it succeeded, we have a bug!
            assert!(
                matches!(send_result, Err(RecordSenderError::Shutdown)),
                "Send succeeded after is_safe_to_restart() returned true! Result: {send_result:?}"
            );

            // Double-check channel is empty
            assert!(
                receiver.receiver.is_empty(),
                "Channel not empty after is_safe_to_restart() returned true!"
            );
        }
    }
}

#[cfg(all(test, feature = "shuttle-test"))]
mod shuttle_tests {
    use super::{tests::test_record, *};

    /// Test that reproduces the race condition where:
    /// 1. Receiver calls shutdown(), is_safe_to_restart() returns true
    /// 2. Sender that loaded state before shutdown completes its send
    /// 3. Channel is non-empty when restart() is called
    ///
    /// The invariant we're testing: after shutdown() + drain loop completes with
    /// is_safe_to_restart() == true, no more records can arrive in the channel.
    #[test]
    fn test_shutdown_drain_restart_race() {
        const NUM_TEST_RUNS: usize = 100_000;
        shuttle::check_random(
            || {
                let (sender, mut receiver) = record_channels(false);
                receiver.restart(0);

                let sender_clone = sender.clone();
                shuttle::thread::spawn(move || {
                    // Sender tries to send - may or may not succeed depending on timing
                    let _ = sender_clone.try_send(test_record(0, 1));
                });

                // Simulate the drain loop from record_and_complete_block
                receiver.shutdown();
                while !receiver.is_safe_to_restart() {
                    // Drain any records
                    if receiver.try_recv().is_ok() {
                        // Record received and processed
                    }
                }

                // At this point, is_safe_to_restart() returned true.
                // The invariant is: no more records should arrive after this.
                // If the channel is not empty now, we have a bug!
                assert!(
                    receiver.receiver.is_empty(),
                    "Channel not empty after is_safe_to_restart() returned true! This means a \
                     sender completed a send after the drain loop exited."
                );
            },
            NUM_TEST_RUNS,
        )
    }

    /// Test the specific scenario from the bug report:
    /// Sender loads state, gets preempted, receiver shuts down and exits drain loop,
    /// then sender resumes and completes the send.
    #[test]
    fn test_sender_preempted_during_send() {
        const NUM_TEST_RUNS: usize = 100_000;
        shuttle::check_random(
            || {
                let (sender, mut receiver) = record_channels(false);
                receiver.restart(0);

                // Spawn multiple senders to increase chance of race
                for _ in 0..3 {
                    let sender_clone = sender.clone();
                    shuttle::thread::spawn(move || {
                        for _ in 0..10 {
                            let _ = sender_clone.try_send(test_record(0, 1));
                        }
                    });
                }

                // Receiver does shutdown + drain multiple times
                for _ in 0..5 {
                    receiver.shutdown();

                    // Drain loop
                    while !receiver.is_safe_to_restart() {
                        let _ = receiver.try_recv();
                    }

                    // CRITICAL CHECK: After drain loop exits, channel MUST be empty
                    // and no more records should be able to arrive
                    let empty_after_drain = receiver.receiver.is_empty();
                    let active = sender.active_senders.load(Ordering::Acquire);

                    // Give a tiny window for any in-flight operations
                    shuttle::thread::yield_now();

                    let empty_after_yield = receiver.receiver.is_empty();

                    assert!(
                        empty_after_drain && empty_after_yield,
                        "Race detected! empty_after_drain={}, active_senders={}, \
                         empty_after_yield={}",
                        empty_after_drain,
                        active,
                        empty_after_yield
                    );

                    receiver.restart(0);
                }
            },
            NUM_TEST_RUNS,
        )
    }

    #[test]
    fn test_sender_shutdown_safety_race() {
        const NUM_TEST_RUNS: usize = 100;
        shuttle::check_random(
            || {
                let (sender, mut receiver) = record_channels(false);

                const ITERATIONS_PER_RUN: usize = 1024;

                shuttle::thread::spawn(move || {
                    let mut successful_sends = 0;
                    let mut bank_id = 0;
                    let mut had_successful_send = false;
                    while successful_sends < ITERATIONS_PER_RUN {
                        if sender.try_send(test_record(bank_id, 1)).is_ok() {
                            had_successful_send = true;
                            successful_sends += 1;
                        } else if had_successful_send {
                            bank_id += 1;
                            had_successful_send = false;
                        }
                    }
                });

                // If receiver/sender interaction is buggy there is a race where
                // the receiver can receive a record after shutdown is called.
                // This can cause PoH to panic because it may receive a record
                // for a bank_id that has already been completed.
                let mut current_bank_id = 0;
                receiver.restart(current_bank_id);
                let mut receives = 0;
                while receives < ITERATIONS_PER_RUN {
                    if receiver.is_shutdown() && receiver.is_safe_to_restart() {
                        current_bank_id += 1;
                        receiver.restart(current_bank_id);
                    }

                    if let Ok(record) = receiver.try_recv() {
                        assert!(record.bank_id == current_bank_id, "bank_id mismatch!");
                        receives += 1;
                        receiver.shutdown();
                    }
                }
            },
            NUM_TEST_RUNS,
        )
    }

    #[test]
    fn test_try_recv_not_sent_on_inner_channel_yet() {
        const NUM_TEST_RUNS: usize = 100_000;
        shuttle::check_random(
            || {
                let (sender, mut receiver) = record_channels(false);
                receiver.restart(0);

                {
                    let sender = sender.clone();
                    shuttle::thread::spawn(move || {
                        let _ = sender.try_send(test_record(0, 1));
                    });
                }

                // Snapshot active_senders *before* try_recv
                let active_at_start = sender.active_senders.load(Ordering::Acquire);

                // Perform try_recv
                let result = receiver.try_recv();

                // Only fail if it returned None *and* we know there was an active sender at start
                if result.is_err() && active_at_start > 0 {
                    panic!(
                        "try_recv returned None while a sender was active at start of call \
                         (active_senders={})",
                        active_at_start
                    );
                }
            },
            NUM_TEST_RUNS,
        )
    }
}
