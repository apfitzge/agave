pub mod tags {
    /// Corresponds to [super::InvalidMessageFormat].
    pub const INVALID_MESSAGE_FORMAT: u8 = 0b0000_0000;
    /// Corresponds to [super::SlotProgress].
    pub const SLOT_PROGRESS: u8 = 0b0000_0001;
    /// Corresponds to [super::TransactionStatus].
    pub const TRANSACTION_STATUS: u8 = 0b0000_0010;
}

/// Message from [agave] to [pack].
#[repr(C)]
pub struct SchedulerMessage {
    /// The kind of scheduler message being passed.
    pub tag: u8,

    /// The actual message data - total length is implied by the length of the
    /// passed message: `message_len - core::mem::size_of::<SchedulerMessage>()`.
    pub data: [u8; 0],
}

/// Corresponds to `tags::INVALID_MESSAGE_FORMAT`.
/// Response indicate the message with `id` was not understood by the
/// scheduler and will be ignored.
#[repr(C)]
pub struct InvalidMessageFormat {
    id: u64,
}

/// Corresponds to `tags::SLOT_PROGRESS`.
#[repr(C)]
pub struct SlotProgress {
    /// The next or current working slot.
    pub slot: u64,
    /// Progress in slot in percent.
    /// - Negative values indicate the validator's working slot has not yet
    ///   started. This may be multiple slots.
    /// - Positive values indicate the slot has started.
    pub progress: i16,
}

/// Corresponds to `tags::TRANSACTION_STATUS`.
#[repr(C)]
pub struct TransactionStatus {
    /// The ID of the batch this transaction was sent in.
    /// See [PackMessage::id]
    id: u64,
    /// The index of the transaction within the batch.
    index: u16,
    /// The status of the transaction.
    status: u32, // TODO: figure out status codes here.

    /// Balance of the fee payer in the transaction.
    /// Only non-zero if the transaction got to point of processing.
    fee_payer_lamports: u64,

    /// Number of CUs used by the transaction.
    /// Only non-zero if the transaction got to point of processing.
    compute_units_used: u64,

    /// The number of write accounts in the transaction.
    /// Only non-zero if the transaction got to point of processing.
    num_write_accounts: u8,
    /// The write accounts in the transaction.
    /// Only populated if `num_write_accounts` is non-zero.
    write_accounts: [[u8; 32]; 0],
}
