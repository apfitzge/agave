use agave_event_system::{event, stream_name::StreamName};

pub const SLOT_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.slot_event");

/// Slot lifecycle events observed by replay.
/// Variants are added as their producers are connected to this stream.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum SlotEvent {}
