use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::Slot,
};

pub const FEC_SET_COMPLETED_STREAM: StreamName =
    agave_event_system::stream_name!("shred.fec_set_completed");

/// All data shreds of a FEC set have been committed to blockstore, including any
/// recovered shreds. The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct FecSetCompleted {
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub fec_set_index: u32,
}
