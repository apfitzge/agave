use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::Slot,
};

pub const SHRED_RECEIVED_STREAM: StreamName = agave_event_system::stream_name!("shred.received");

/// A parseable shred received before filtering, signature verification, and deduplication.
/// The timestamp is monotonic nanoseconds sampled at socket batch handoff.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct ShredReceived {
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub fec_set_index: u32,
    pub shred_index: u32,
    pub is_coding: bool,
    pub is_repair: bool,
}
