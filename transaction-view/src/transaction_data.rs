/// Trait for accessing transaction data from an abstract byte container.
pub trait TransactionData {
    /// Returns a reference to the serialized transaction data.
    fn data(&self) -> &[u8];
}

impl TransactionData for &[u8] {
    #[inline]
    fn data(&self) -> &[u8] {
        self
    }
}

impl TransactionData for std::sync::Arc<Vec<u8>> {
    #[inline]
    fn data(&self) -> &[u8] {
        self.as_ref()
    }
}

impl TransactionData for core::ptr::NonNull<[u8]> {
    #[inline]
    fn data(&self) -> &[u8] {
        // SAFETY: NonNull guarantees that the pointer is not null.
        //         Construction of NonNull SHOULD ensure the slice
        //         is valid and properly aligned.
        unsafe { self.as_ref() }
    }
}
