/// Message from [pack] to [agave].
#[repr(C)]
pub struct PackMessage {
    /// A unique identifier for the message - this will be used in response
    /// messages. See [SchedulerMessage] for more details.
    pub id: u64,

    /// Flags for how to execute the transactions.
    // NOTE: reserved but no meaningful values yet.
    pub flags: u64,

    /// The number of transactions in the message.
    pub num_transactions: u16,

    /// The transactions in the message.
    pub transactions: [PackMessageTransaction; 0],
}

/// Part of [PackMessage] - a single transaction with length.
#[repr(C)]
pub struct PackMessageTransaction {
    /// Number of bytes in the transaction.
    pub len: u16,
    /// The transaction data.
    pub transaction: [u8; 0],
}
