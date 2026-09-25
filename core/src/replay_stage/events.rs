use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::Slot,
};

pub const SLOT_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.slot_event");

/// Slot lifecycle events observed by replay.
/// All timestamps are monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum SlotEvent {
    /// Replay is inserting a new bank for this slot and parent.
    /// A slot may begin again after its previous bank is discarded and recreated.
    Begin {
        timestamp_ns: u64,
        slot: Slot,
        parent: Slot,
    },
    /// Execution and verification succeeded and the bank has been frozen.
    ExecutionComplete {
        timestamp_ns: u64,
        slot: Slot,
    },
    /// The bank was removed from BankForks, not necessarily destroyed.
    Removed {
        timestamp_ns: u64,
        slot: Slot,
    },
}
