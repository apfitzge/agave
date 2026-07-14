use agave_scheduling_utils::transaction_ptr::TransactionPtrBatch;

pub(crate) type TransactionId = usize;

pub(crate) const MAX_PACKETS_PER_CHECK_BATCH: usize = 16;
#[allow(dead_code)]
pub(crate) const MAX_PACKETS_PER_EXEC_BATCH: usize = MAX_PACKETS_PER_CHECK_BATCH;

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

/// Scheduler state associated with a transaction sent to an execution worker.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct ExecutionTransactionMeta {
    pub(crate) transaction_id: TransactionId,
}

#[allow(dead_code)]
pub(crate) type ExecutionBatch<'a> =
    TransactionPtrBatch<'a, ExecutionTransactionMeta, MAX_PACKETS_PER_EXEC_BATCH>;
