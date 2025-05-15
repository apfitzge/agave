/// Message from [agave] to [pack].
#[repr(C)]
pub struct TPUPacketMessage {
    /// See [solana-packet::PacketFlags].
    pub flags: u8,

    /// Trailing data. The length of this field is implicit from the length
    /// of the passed message: `message_len - core::mem::size_of::<TPUPacketMessage>()`.
    ///
    // DEVELOPER NOTE:
    // The use of a zero-length array is a workaround until `ptr_metadata` feature is
    // stabilized - at that point we can use [u8] and fat-pointers.
    pub data: [u8; 0],
}
