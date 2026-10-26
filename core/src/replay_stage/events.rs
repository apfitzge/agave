use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::{BankId, Slot},
};

pub const SLOT_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.slot_event");

/// Slot lifecycle events observed by replay.
/// All timestamps are monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum SlotEvent {
    /// Replay inserted a bank for this slot and parent into BankForks.
    /// The slot may begin again if its bank is discarded and recreated.
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        parent: Slot,
        bank_id: BankId,
    },
    /// Replay execution and verification succeeded, and the bank is frozen.
    ExecutionComplete {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: BankId,
    },
}
