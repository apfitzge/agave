use agave_scheduling_utils::transaction_ptr::TransactionPtrBatch;

pub(crate) const MAX_PACKETS_PER_CHECK_BATCH: usize = 16;

/// Metadata retained by the scheduler for each transaction sent to a check worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct TpuTransactionMeta {
    pub(crate) priority: u64,
    pub(crate) cost: u64,
    pub(crate) flags: u8,
    pub(crate) src_addr: [u8; 16],
}

pub(crate) type CheckBatch<'a> =
    TransactionPtrBatch<'a, TpuTransactionMeta, MAX_PACKETS_PER_CHECK_BATCH>;
