/// File name, under the validator ledger directory, for replay scheduler event
/// broadcasts.
pub const REPLAY_EVENTS_IPC_FILE: &str = "agave_events.ipc";

/// Number of payload bytes available after the common event header.
///
/// The current largest payload is transaction ingest:
/// `slot: u64`, `transaction_index: u64`, `signature: [u8; 64]`.
/// Check dispatch transaction events reuse the signature bytes as:
/// `check_queue_len: u64`.
/// Worker-associated transaction events reuse the signature bytes as:
/// `worker_id: u64`.
/// Worker dispatch transaction events also include `worker_queue_len: u64`.
/// Check-passed transaction events also include `estimated_cost_units: u64`.
/// Execution result transaction events also include `cost_units: u64`.
/// Scheduling-skipped transaction events reuse the signature bytes as:
/// `unscheduled_ready_transactions_ahead: u64`.
/// Worker dispatch transaction events also include
/// `unscheduled_ready_transactions_ahead: u64`.
/// Signature verification submission events also include
/// `signature_verification_queue_len: u64`.
/// Signature verification worker transaction events reuse the signature bytes as:
/// `signature_verification_worker_id: u64`.
/// Ready-for-scheduling transaction events reuse the signature bytes as:
/// `ready_released_by_transaction_index: u64`.
pub const REPLAY_EVENT_PAYLOAD_BYTES: usize = 80;

use crate::thread_aware_account_locks::ThreadId;

/// Replay scheduler event written to the optional ledger-local broadcast.
///
/// This is intentionally stored as a raw tag plus initialized byte payload. The
/// `shaq` broadcast by-value API requires no padding or uninitialized bytes in
/// `T`, so avoid Rust data-carrying enum layout here and expose constructors
/// for the supported tagged variants instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ReplayEvent {
    /// Approximate Unix timestamp in nanoseconds.
    pub timestamp_ns: u64,
    /// See [`replay_event_tags`] for the payload shape.
    pub tag: u64,
    /// Variant payload bytes. Multi-byte integers are little-endian.
    pub payload: [u8; REPLAY_EVENT_PAYLOAD_BYTES],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ReplayTransactionCheckMetadata {
    pub slot: u64,
    pub transaction_index: usize,
    pub thread_id: ThreadId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ReplayTransactionExecutionMetadata {
    pub slot: u64,
    pub transaction_index: usize,
    pub thread_id: ThreadId,
}

pub mod replay_event_tags {
    /// Scheduler began processing a slot.
    pub const SLOT_BEGIN: u64 = 0;
    /// Scheduler aborted a slot.
    pub const SLOT_ABORT: u64 = 1;
    /// Transaction was accepted into replay scheduler state.
    ///
    /// Payload: `slot: u64`, `transaction_index: u64`, `signature: [u8; 64]`.
    pub const TRANSACTION_INGESTED: u64 = 2;
    /// Transaction was sent to the replay check pool.
    ///
    /// Payload includes `check_queue_len: u64`.
    pub const TRANSACTION_SENT_FOR_CHECK: u64 = 3;
    /// Worker check response rejected the transaction.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_CHECK_FAILED: u64 = 4;
    /// Worker check response accepted the transaction.
    ///
    /// Payload includes `worker_id: u64` and `estimated_cost_units: u64`.
    pub const TRANSACTION_CHECK_PASSED: u64 = 5;
    /// Transaction was moved into the ready queue for scheduling.
    ///
    /// Payload includes `ready_released_by_transaction_index: u64`.
    pub const TRANSACTION_READY_FOR_SCHEDULING: u64 = 12;
    /// Replay worker began processing a transaction message.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_PICKED_UP: u64 = 13;
    /// Replay worker finished check processing for a transaction.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_COMPLETED: u64 = 14;
    /// Replay worker finished execution processing for a transaction.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_EXECUTION_COMPLETED: u64 = 15;
    /// Transaction signatures were submitted for Agave-side verification.
    ///
    /// Payload includes `signature_verification_queue_len: u64`.
    pub const TRANSACTION_SIGNATURES_SUBMITTED: u64 = 16;
    /// Agave-side signature verification returned for a transaction.
    ///
    /// Payload includes `verified: u64`.
    pub const TRANSACTION_SIGNATURES_RETURNED: u64 = 17;
    /// Replay worker acquired the check bank.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_BANK_ACQUIRED: u64 = 18;
    /// Replay worker completed transaction parsing for check.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_PARSED: u64 = 19;
    /// Replay worker completed signature checks.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE: u64 = 20;
    /// Replay worker completed fee-payer balance checks.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE: u64 = 21;
    /// Replay worker completed transaction resolution for check.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_RESOLVED: u64 = 22;
    /// Replay worker completed address lookup table checks.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE: u64 = 23;
    /// Replay worker completed status checks.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_CHECK_STATUS_COMPLETE: u64 = 24;
    /// Replay worker acquired the execution bank.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED: u64 = 25;
    /// Replay worker completed transaction translation for execution.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_EXECUTION_TRANSLATED: u64 = 26;
    /// Replay worker completed transaction processing/recording.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_EXECUTION_PROCESSED: u64 = 27;
    /// Replay worker received execution commit results.
    ///
    /// Payload includes `worker_id: u64`.
    pub const TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY: u64 = 28;
    /// Signature verification worker began processing a transaction.
    ///
    /// Payload includes `signature_verification_worker_id: u64`.
    pub const TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP: u64 = 29;
    /// Signature verification worker parsed the transaction.
    ///
    /// Payload includes `signature_verification_worker_id: u64`.
    pub const TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED: u64 = 30;
    /// Signature verification worker completed signature checks.
    ///
    /// Payload includes `signature_verification_worker_id: u64` and `verified: u64`.
    pub const TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE: u64 = 31;
    /// Signature verification worker sent the verification result.
    ///
    /// Payload includes `signature_verification_worker_id: u64` and `verified: u64`.
    pub const TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT: u64 = 32;
    /// Transaction could not currently be scheduled.
    ///
    /// Payload includes `unscheduled_ready_transactions_ahead: u64`.
    pub const TRANSACTION_SCHEDULING_SKIPPED: u64 = 6;
    /// Transaction was sent to a worker for execution.
    ///
    /// Payload includes `worker_id: u64`, `worker_queue_len: u64`, and
    /// `unscheduled_ready_transactions_ahead: u64`.
    pub const TRANSACTION_SCHEDULED_FOR_EXEC: u64 = 7;
    /// Worker execution response completed.
    ///
    /// Payload includes `worker_id: u64` and `cost_units: u64`.
    pub const TRANSACTION_FINISHED_EXEC: u64 = 8;
    /// Worker execution response reported the transaction was not included.
    ///
    /// Payload includes `worker_id: u64` and `cost_units: u64`.
    pub const TRANSACTION_EXEC_FAILED: u64 = 9;
    /// Replay block verification sent a successful final status for the slot.
    pub const SLOT_COMPLETE: u64 = 10;
    /// Replay block verification sent a failed final status for the slot.
    ///
    /// Payload: `slot: u64`, `reason: u64`.
    pub const SLOT_FAILED: u64 = 11;
}

const SLOT_OFFSET: usize = 0;
const SLOT_REASON_OFFSET: usize = SLOT_OFFSET + core::mem::size_of::<u64>();
const TRANSACTION_INDEX_OFFSET: usize = SLOT_REASON_OFFSET;
const SIGNATURE_OFFSET: usize = TRANSACTION_INDEX_OFFSET + core::mem::size_of::<u64>();
const CHECK_QUEUE_LENGTH_OFFSET: usize = SIGNATURE_OFFSET;
const WORKER_ID_OFFSET: usize = SIGNATURE_OFFSET;
const WORKER_QUEUE_LENGTH_OFFSET: usize = WORKER_ID_OFFSET + core::mem::size_of::<u64>();
const SCHEDULING_SKIPPED_UNSCHEDULED_READY_AHEAD_OFFSET: usize = SIGNATURE_OFFSET;
const WORKER_DISPATCH_UNSCHEDULED_READY_AHEAD_OFFSET: usize =
    WORKER_QUEUE_LENGTH_OFFSET + core::mem::size_of::<u64>();
const ESTIMATED_COST_UNITS_OFFSET: usize = WORKER_QUEUE_LENGTH_OFFSET;
const COST_UNITS_OFFSET: usize = WORKER_QUEUE_LENGTH_OFFSET;
const SIGNATURE_VERIFICATION_QUEUE_LENGTH_OFFSET: usize = SIGNATURE_OFFSET;
const SIGNATURE_VERIFICATION_RESULT_OFFSET: usize = SIGNATURE_OFFSET;
const SIGNATURE_VERIFICATION_WORKER_ID_OFFSET: usize = SIGNATURE_OFFSET;
const SIGNATURE_VERIFICATION_WORKER_RESULT_OFFSET: usize =
    SIGNATURE_VERIFICATION_WORKER_ID_OFFSET + core::mem::size_of::<u64>();
const READY_RELEASED_BY_TRANSACTION_INDEX_OFFSET: usize = SIGNATURE_OFFSET;

impl ReplayEvent {
    pub fn slot_begin(timestamp_ns: u64, slot: u64) -> Self {
        Self::slot_event(timestamp_ns, replay_event_tags::SLOT_BEGIN, slot)
    }

    pub fn slot_abort(timestamp_ns: u64, slot: u64) -> Self {
        Self::slot_event(timestamp_ns, replay_event_tags::SLOT_ABORT, slot)
    }

    pub fn slot_complete(timestamp_ns: u64, slot: u64) -> Self {
        Self::slot_event(timestamp_ns, replay_event_tags::SLOT_COMPLETE, slot)
    }

    pub fn slot_failed(timestamp_ns: u64, slot: u64, reason: u16) -> Self {
        let mut event = Self::slot_event(timestamp_ns, replay_event_tags::SLOT_FAILED, slot);
        event.write_u64(SLOT_REASON_OFFSET, u64::from(reason));
        event
    }

    pub fn transaction_ingested(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        signature: [u8; 64],
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_INGESTED,
            slot,
            transaction_index,
        );
        event.payload[SIGNATURE_OFFSET..].copy_from_slice(&signature);
        event
    }

    pub fn transaction_event(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
    ) -> Self {
        debug_assert!(is_transaction_event_tag(tag));
        let mut event = Self::new(timestamp_ns, tag);
        event.write_u64(SLOT_OFFSET, slot);
        event.write_u64(TRANSACTION_INDEX_OFFSET, transaction_index);
        event
    }

    pub fn transaction_sent_for_check(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        check_queue_len: u64,
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_SENT_FOR_CHECK,
            slot,
            transaction_index,
        );
        event.write_u64(CHECK_QUEUE_LENGTH_OFFSET, check_queue_len);
        event
    }

    pub fn transaction_ready_for_scheduling(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        ready_released_by_transaction_index: u64,
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING,
            slot,
            transaction_index,
        );
        event.write_u64(
            READY_RELEASED_BY_TRANSACTION_INDEX_OFFSET,
            ready_released_by_transaction_index,
        );
        event
    }

    pub fn transaction_scheduling_skipped(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        unscheduled_ready_transactions_ahead: u64,
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED,
            slot,
            transaction_index,
        );
        event.write_u64(
            SCHEDULING_SKIPPED_UNSCHEDULED_READY_AHEAD_OFFSET,
            unscheduled_ready_transactions_ahead,
        );
        event
    }

    pub fn transaction_signatures_returned(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        verified: bool,
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_SIGNATURES_RETURNED,
            slot,
            transaction_index,
        );
        event.write_u64(SIGNATURE_VERIFICATION_RESULT_OFFSET, u64::from(verified));
        event
    }

    pub fn transaction_signatures_submitted(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        signature_verification_queue_len: u64,
    ) -> Self {
        let mut event = Self::transaction_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED,
            slot,
            transaction_index,
        );
        event.write_u64(
            SIGNATURE_VERIFICATION_QUEUE_LENGTH_OFFSET,
            signature_verification_queue_len,
        );
        event
    }

    pub fn transaction_worker_event(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
        worker_id: u64,
    ) -> Self {
        debug_assert!(is_transaction_worker_event_tag(tag));
        let mut event = Self::transaction_event(timestamp_ns, tag, slot, transaction_index);
        event.write_u64(WORKER_ID_OFFSET, worker_id);
        event
    }

    pub fn transaction_check_passed(
        timestamp_ns: u64,
        slot: u64,
        transaction_index: u64,
        worker_id: u64,
        estimated_cost_units: u64,
    ) -> Self {
        let mut event = Self::transaction_worker_event(
            timestamp_ns,
            replay_event_tags::TRANSACTION_CHECK_PASSED,
            slot,
            transaction_index,
            worker_id,
        );
        event.write_u64(ESTIMATED_COST_UNITS_OFFSET, estimated_cost_units);
        event
    }

    pub fn transaction_execution_result(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
        worker_id: u64,
        cost_units: u64,
    ) -> Self {
        debug_assert!(is_transaction_execution_result_event_tag(tag));
        let mut event =
            Self::transaction_worker_event(timestamp_ns, tag, slot, transaction_index, worker_id);
        event.write_u64(COST_UNITS_OFFSET, cost_units);
        event
    }

    pub fn transaction_worker_dispatch_event(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
        worker_id: u64,
        worker_queue_len: u64,
        unscheduled_ready_transactions_ahead: u64,
    ) -> Self {
        debug_assert!(is_transaction_worker_dispatch_event_tag(tag));
        let mut event =
            Self::transaction_worker_event(timestamp_ns, tag, slot, transaction_index, worker_id);
        event.write_u64(WORKER_QUEUE_LENGTH_OFFSET, worker_queue_len);
        event.write_u64(
            WORKER_DISPATCH_UNSCHEDULED_READY_AHEAD_OFFSET,
            unscheduled_ready_transactions_ahead,
        );
        event
    }

    pub fn transaction_signature_verification_worker_event(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
        signature_verification_worker_id: u64,
    ) -> Self {
        debug_assert!(is_transaction_signature_verification_worker_event_tag(tag));
        let mut event = Self::transaction_event(timestamp_ns, tag, slot, transaction_index);
        event.write_u64(
            SIGNATURE_VERIFICATION_WORKER_ID_OFFSET,
            signature_verification_worker_id,
        );
        event
    }

    pub fn transaction_signature_verification_worker_result_event(
        timestamp_ns: u64,
        tag: u64,
        slot: u64,
        transaction_index: u64,
        signature_verification_worker_id: u64,
        verified: bool,
    ) -> Self {
        debug_assert!(is_transaction_signature_verification_worker_result_event_tag(tag));
        let mut event = Self::transaction_signature_verification_worker_event(
            timestamp_ns,
            tag,
            slot,
            transaction_index,
            signature_verification_worker_id,
        );
        event.write_u64(
            SIGNATURE_VERIFICATION_WORKER_RESULT_OFFSET,
            u64::from(verified),
        );
        event
    }

    pub fn slot(&self) -> u64 {
        self.read_u64(SLOT_OFFSET)
    }

    pub fn transaction_index(&self) -> Option<u64> {
        is_transaction_event_tag(self.tag).then(|| self.read_u64(TRANSACTION_INDEX_OFFSET))
    }

    pub fn slot_failure_reason(&self) -> Option<u16> {
        if self.tag != replay_event_tags::SLOT_FAILED {
            return None;
        }

        self.read_u64(SLOT_REASON_OFFSET).try_into().ok()
    }

    pub fn worker_id(&self) -> Option<u64> {
        if is_transaction_worker_event_tag(self.tag) {
            Some(self.read_u64(WORKER_ID_OFFSET))
        } else {
            None
        }
    }

    pub fn worker_queue_len(&self) -> Option<u64> {
        is_transaction_worker_dispatch_event_tag(self.tag)
            .then(|| self.read_u64(WORKER_QUEUE_LENGTH_OFFSET))
    }

    pub fn check_queue_len(&self) -> Option<u64> {
        (self.tag == replay_event_tags::TRANSACTION_SENT_FOR_CHECK)
            .then(|| self.read_u64(CHECK_QUEUE_LENGTH_OFFSET))
    }

    pub fn estimated_cost_units(&self) -> Option<u64> {
        (self.tag == replay_event_tags::TRANSACTION_CHECK_PASSED)
            .then(|| self.read_u64(ESTIMATED_COST_UNITS_OFFSET))
    }

    pub fn cost_units(&self) -> Option<u64> {
        is_transaction_execution_result_event_tag(self.tag)
            .then(|| self.read_u64(COST_UNITS_OFFSET))
    }

    pub fn signature_verification_worker_id(&self) -> Option<u64> {
        is_transaction_signature_verification_worker_event_tag(self.tag)
            .then(|| self.read_u64(SIGNATURE_VERIFICATION_WORKER_ID_OFFSET))
    }

    pub fn signature(&self) -> Option<[u8; 64]> {
        if self.tag != replay_event_tags::TRANSACTION_INGESTED {
            return None;
        }

        let mut signature = [0; 64];
        signature.copy_from_slice(&self.payload[SIGNATURE_OFFSET..]);
        Some(signature)
    }

    pub fn signature_verification_result(&self) -> Option<bool> {
        if self.tag != replay_event_tags::TRANSACTION_SIGNATURES_RETURNED {
            return None;
        }

        Some(self.read_u64(SIGNATURE_VERIFICATION_RESULT_OFFSET) != 0)
    }

    pub fn signature_verification_queue_len(&self) -> Option<u64> {
        if self.tag != replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED {
            return None;
        }

        Some(self.read_u64(SIGNATURE_VERIFICATION_QUEUE_LENGTH_OFFSET))
    }

    pub fn signature_verification_worker_result(&self) -> Option<bool> {
        if !is_transaction_signature_verification_worker_result_event_tag(self.tag) {
            return None;
        }

        Some(self.read_u64(SIGNATURE_VERIFICATION_WORKER_RESULT_OFFSET) != 0)
    }

    pub fn ready_released_by_transaction_index(&self) -> Option<u64> {
        if self.tag != replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING {
            return None;
        }

        Some(self.read_u64(READY_RELEASED_BY_TRANSACTION_INDEX_OFFSET))
    }

    pub fn unscheduled_ready_transactions_ahead(&self) -> Option<u64> {
        match self.tag {
            replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED => {
                Some(self.read_u64(SCHEDULING_SKIPPED_UNSCHEDULED_READY_AHEAD_OFFSET))
            }
            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC => {
                Some(self.read_u64(WORKER_DISPATCH_UNSCHEDULED_READY_AHEAD_OFFSET))
            }
            _ => None,
        }
    }

    fn slot_event(timestamp_ns: u64, tag: u64, slot: u64) -> Self {
        let mut event = Self::new(timestamp_ns, tag);
        event.write_u64(SLOT_OFFSET, slot);
        event
    }

    fn new(timestamp_ns: u64, tag: u64) -> Self {
        Self {
            timestamp_ns,
            tag,
            payload: [0; REPLAY_EVENT_PAYLOAD_BYTES],
        }
    }

    fn read_u64(&self, offset: usize) -> u64 {
        let end = offset.checked_add(core::mem::size_of::<u64>()).unwrap();
        let bytes = self.payload.get(offset..end).unwrap().try_into().unwrap();
        u64::from_le_bytes(bytes)
    }

    fn write_u64(&mut self, offset: usize, value: u64) {
        let end = offset.checked_add(core::mem::size_of::<u64>()).unwrap();
        self.payload
            .get_mut(offset..end)
            .unwrap()
            .copy_from_slice(&value.to_le_bytes());
    }
}

pub const fn is_transaction_worker_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::TRANSACTION_CHECK_FAILED
            | replay_event_tags::TRANSACTION_CHECK_PASSED
            | replay_event_tags::TRANSACTION_WORKER_PICKED_UP
            | replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY
            | replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC
            | replay_event_tags::TRANSACTION_FINISHED_EXEC
            | replay_event_tags::TRANSACTION_EXEC_FAILED
    )
}

pub const fn is_transaction_worker_dispatch_event_tag(tag: u64) -> bool {
    matches!(tag, replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC)
}

pub const fn is_transaction_execution_result_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::TRANSACTION_FINISHED_EXEC | replay_event_tags::TRANSACTION_EXEC_FAILED
    )
}

pub const fn is_transaction_signature_verification_worker_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT
    )
}

pub const fn is_transaction_signature_verification_worker_result_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT
    )
}

pub const fn is_slot_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::SLOT_BEGIN
            | replay_event_tags::SLOT_ABORT
            | replay_event_tags::SLOT_COMPLETE
            | replay_event_tags::SLOT_FAILED
    )
}

pub const fn is_transaction_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::TRANSACTION_INGESTED
            | replay_event_tags::TRANSACTION_SENT_FOR_CHECK
            | replay_event_tags::TRANSACTION_CHECK_FAILED
            | replay_event_tags::TRANSACTION_CHECK_PASSED
            | replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED
            | replay_event_tags::TRANSACTION_SIGNATURES_RETURNED
            | replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING
            | replay_event_tags::TRANSACTION_WORKER_PICKED_UP
            | replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED
            | replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED
            | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE
            | replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT
            | replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED
            | replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC
            | replay_event_tags::TRANSACTION_FINISHED_EXEC
            | replay_event_tags::TRANSACTION_EXEC_FAILED
    )
}

#[cfg(test)]
mod tests {
    use {
        super::{
            REPLAY_EVENT_PAYLOAD_BYTES, ReplayEvent, ReplayTransactionCheckMetadata,
            ReplayTransactionExecutionMetadata, SIGNATURE_OFFSET,
            is_transaction_worker_dispatch_event_tag, replay_event_tags,
        },
        core::mem,
    };

    #[test]
    fn replay_event_has_expected_wire_layout() {
        assert_eq!(
            mem::size_of::<ReplayEvent>(),
            mem::size_of::<u64>() * 2 + REPLAY_EVENT_PAYLOAD_BYTES,
        );
        assert_eq!(mem::align_of::<ReplayEvent>(), mem::align_of::<u64>());
    }

    #[test]
    fn replay_worker_metadata_has_expected_layout() {
        assert_eq!(
            mem::size_of::<ReplayTransactionCheckMetadata>(),
            mem::size_of::<u64>() + mem::size_of::<usize>() * 2,
        );
        assert_eq!(
            mem::size_of::<ReplayTransactionExecutionMetadata>(),
            mem::size_of::<u64>() + mem::size_of::<usize>() * 2,
        );
    }

    #[test]
    fn transaction_ingested_carries_index_and_signature() {
        let signature = [7; 64];
        let event = ReplayEvent::transaction_ingested(1, 2, 3, signature);

        assert_eq!(event.timestamp_ns, 1);
        assert_eq!(event.tag, replay_event_tags::TRANSACTION_INGESTED);
        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.signature(), Some(signature));
    }

    #[test]
    fn non_ingest_transaction_events_reference_index_without_signature() {
        let event = ReplayEvent::transaction_event(
            1,
            replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED,
            2,
            3,
        );

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.signature(), None);
        assert_eq!(event.payload[SIGNATURE_OFFSET..], [0; 64]);
    }

    #[test]
    fn transaction_ready_for_scheduling_carries_releasing_transaction_index() {
        let event = ReplayEvent::transaction_ready_for_scheduling(1, 2, 3, 4);

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.ready_released_by_transaction_index(), Some(4));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn transaction_scheduling_skipped_carries_unscheduled_ready_transactions_ahead() {
        let event = ReplayEvent::transaction_scheduling_skipped(1, 2, 3, 4);

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.unscheduled_ready_transactions_ahead(), Some(4));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn transaction_sent_for_check_carries_check_queue_len() {
        let event = ReplayEvent::transaction_sent_for_check(1, 2, 3, 4);

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.check_queue_len(), Some(4));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.worker_queue_len(), None);
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn transaction_worker_events_carry_worker_id_without_signature() {
        for tag in [
            replay_event_tags::TRANSACTION_CHECK_FAILED,
            replay_event_tags::TRANSACTION_CHECK_PASSED,
            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
            replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED,
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
            replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED,
            replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED,
            replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE,
            replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE,
            replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED,
            replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE,
            replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE,
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED,
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED,
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED,
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY,
            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            replay_event_tags::TRANSACTION_EXEC_FAILED,
        ] {
            let event = ReplayEvent::transaction_worker_event(1, tag, 2, 3, 4);

            assert_eq!(event.slot(), 2);
            assert_eq!(event.transaction_index(), Some(3));
            assert_eq!(event.worker_id(), Some(4));
            assert_eq!(event.signature_verification_worker_id(), None);
            let expected_worker_queue_len =
                is_transaction_worker_dispatch_event_tag(tag).then_some(0);
            assert_eq!(event.worker_queue_len(), expected_worker_queue_len);
            let expected_unscheduled_ready_transactions_ahead =
                is_transaction_worker_dispatch_event_tag(tag).then_some(0);
            assert_eq!(
                event.unscheduled_ready_transactions_ahead(),
                expected_unscheduled_ready_transactions_ahead
            );
            assert_eq!(event.signature(), None);
        }
    }

    #[test]
    fn transaction_worker_dispatch_events_carry_queue_len_and_unscheduled_ready_transactions_ahead()
    {
        let event = ReplayEvent::transaction_worker_dispatch_event(
            1,
            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
            2,
            3,
            4,
            5,
            6,
        );

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.worker_id(), Some(4));
        assert_eq!(event.worker_queue_len(), Some(5));
        assert_eq!(event.unscheduled_ready_transactions_ahead(), Some(6));
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn transaction_check_passed_carries_estimated_cost_units() {
        let event = ReplayEvent::transaction_check_passed(1, 2, 3, 4, 5);

        assert_eq!(event.tag, replay_event_tags::TRANSACTION_CHECK_PASSED);
        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.worker_id(), Some(4));
        assert_eq!(event.estimated_cost_units(), Some(5));
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn transaction_execution_results_carry_cost_units() {
        for tag in [
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            replay_event_tags::TRANSACTION_EXEC_FAILED,
        ] {
            let event = ReplayEvent::transaction_execution_result(1, tag, 2, 3, 4, 5);

            assert_eq!(event.tag, tag);
            assert_eq!(event.slot(), 2);
            assert_eq!(event.transaction_index(), Some(3));
            assert_eq!(event.worker_id(), Some(4));
            assert_eq!(event.cost_units(), Some(5));
            assert_eq!(event.signature(), None);
        }
    }

    #[test]
    fn transaction_signature_verification_worker_events_carry_worker_id() {
        for tag in [
            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP,
            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED,
        ] {
            let event =
                ReplayEvent::transaction_signature_verification_worker_event(1, tag, 2, 3, 4);

            assert_eq!(event.slot(), 2);
            assert_eq!(event.transaction_index(), Some(3));
            assert_eq!(event.worker_id(), None);
            assert_eq!(event.signature_verification_worker_id(), Some(4));
            assert_eq!(event.signature_verification_worker_result(), None);
            assert_eq!(event.signature(), None);
        }
    }

    #[test]
    fn transaction_signature_verification_worker_result_events_carry_result() {
        for tag in [
            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE,
            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT,
        ] {
            let event = ReplayEvent::transaction_signature_verification_worker_result_event(
                1, tag, 2, 3, 4, true,
            );

            assert_eq!(event.slot(), 2);
            assert_eq!(event.transaction_index(), Some(3));
            assert_eq!(event.worker_id(), None);
            assert_eq!(event.signature_verification_worker_id(), Some(4));
            assert_eq!(event.signature_verification_worker_result(), Some(true));
            assert_eq!(event.signature(), None);
        }
    }

    #[test]
    fn transaction_signature_submission_events_carry_queue_len() {
        let event = ReplayEvent::transaction_signatures_submitted(1, 2, 3, 4);

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), Some(3));
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.worker_queue_len(), None);
        assert_eq!(event.signature_verification_queue_len(), Some(4));
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn slot_events_do_not_have_transaction_index_or_signature() {
        let event = ReplayEvent::slot_abort(1, 2);

        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), None);
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.slot_failure_reason(), None);
        assert_eq!(event.signature(), None);
    }

    #[test]
    fn slot_failed_carries_reason() {
        let event = ReplayEvent::slot_failed(1, 2, 3);

        assert_eq!(event.tag, replay_event_tags::SLOT_FAILED);
        assert_eq!(event.slot(), 2);
        assert_eq!(event.transaction_index(), None);
        assert_eq!(event.worker_id(), None);
        assert_eq!(event.slot_failure_reason(), Some(3));
        assert_eq!(event.signature(), None);
    }
}
