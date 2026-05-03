use {
    crate::{
        bytes::{check_remaining, unchecked_copy_value},
        result::Result,
    },
    solana_hash::Hash,
};

pub const SERIALIZED_ENTRY_HEADER_LEN: usize =
    core::mem::size_of::<u64>() + core::mem::size_of::<Hash>() + core::mem::size_of::<u64>();

/// A parsed view of a serialized `Entry` header.
///
/// This parses only the fixed entry prefix and transaction vector length:
/// `num_hashes`, `hash`, and `num_transactions`. The entry hash is borrowed
/// from the input bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryHeaderFrame<'a> {
    num_hashes: u64,
    hash: &'a Hash,
    num_transactions: u64,
}

impl<'a> EntryHeaderFrame<'a> {
    /// Parse an entry header from the beginning of `bytes`.
    ///
    /// Returns the parsed header and the number of bytes consumed. The consumed
    /// length is the first offset after the entry header, which is the start of
    /// the first serialized transaction when `num_transactions` is nonzero.
    pub fn try_new_from_prefix(bytes: &'a [u8]) -> Result<(Self, usize)> {
        check_remaining(bytes, 0, SERIALIZED_ENTRY_HEADER_LEN)?;

        let hash_offset = core::mem::size_of::<u64>();
        let num_transactions_offset = hash_offset + core::mem::size_of::<Hash>();

        // SAFETY: `check_remaining` verified that the full fixed-size entry
        // header is in bounds. The two u64 offsets each start a full u64
        // within that range; any byte pattern is valid for u64, and
        // `unchecked_copy_value` uses an unaligned read.
        let num_hashes = u64::from_le(unsafe { unchecked_copy_value::<u64>(bytes, 0) });
        let num_transactions =
            u64::from_le(unsafe { unchecked_copy_value::<u64>(bytes, num_transactions_offset) });

        const _: () = assert!(core::mem::align_of::<Hash>() == 1, "Hash alignment");
        // SAFETY: `check_remaining` verified that the full 32-byte hash range
        // is in bounds. `Hash` is `repr(transparent)` over `[u8; 32]`, has
        // alignment 1 as asserted above, and any 32-byte value is valid.
        let hash = unsafe { &*(bytes.as_ptr().add(hash_offset) as *const Hash) };

        Ok((
            Self {
                num_hashes,
                hash,
                num_transactions,
            },
            SERIALIZED_ENTRY_HEADER_LEN,
        ))
    }

    /// Return the number of hashes since the previous entry ID.
    #[inline]
    pub fn num_hashes(&self) -> u64 {
        self.num_hashes
    }

    /// Return the entry hash without copying it.
    #[inline]
    pub fn hash(&self) -> &'a Hash {
        self.hash
    }

    /// Return the number of transactions serialized after this header.
    #[inline]
    pub fn num_transactions(&self) -> u64 {
        self.num_transactions
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_hash::HASH_BYTES, solana_transaction::versioned::VersionedTransaction};

    fn serialized_entry_header(num_hashes: u64, hash: &Hash, num_transactions: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&num_hashes.to_le_bytes());
        bytes.extend_from_slice(hash.as_ref());
        bytes.extend_from_slice(&num_transactions.to_le_bytes());
        bytes
    }

    #[test]
    fn test_try_new_from_prefix() {
        let num_hashes = 42;
        let hash = Hash::new_from_array([7; HASH_BYTES]);
        let num_transactions = 3;
        let mut bytes = serialized_entry_header(num_hashes, &hash, num_transactions);
        bytes.extend_from_slice(&[1, 2, 3, 4]);

        let (header, consumed_len) = EntryHeaderFrame::try_new_from_prefix(&bytes).unwrap();

        assert_eq!(consumed_len, SERIALIZED_ENTRY_HEADER_LEN);
        assert_eq!(header.num_hashes(), num_hashes);
        assert_eq!(header.hash(), &hash);
        assert_eq!(header.num_transactions(), num_transactions);
        assert_eq!(
            header.hash() as *const Hash,
            bytes[core::mem::size_of::<u64>()..].as_ptr() as *const Hash,
        );
    }

    #[test]
    fn test_try_new_from_prefix_rejects_truncated_bytes() {
        let hash = Hash::new_from_array([7; HASH_BYTES]);
        let bytes = serialized_entry_header(42, &hash, 3);

        for len in 0..SERIALIZED_ENTRY_HEADER_LEN {
            assert!(EntryHeaderFrame::try_new_from_prefix(&bytes[..len]).is_err());
        }
    }

    #[test]
    fn test_try_new_from_prefix_matches_wincode_entry_header() {
        let entry = solana_entry::entry::Entry {
            num_hashes: 42,
            hash: Hash::new_from_array([7; HASH_BYTES]),
            transactions: vec![
                VersionedTransaction::default(),
                VersionedTransaction::default(),
            ],
        };
        let bytes = wincode::serialize(&entry).unwrap();

        let (header, consumed_len) = EntryHeaderFrame::try_new_from_prefix(&bytes).unwrap();

        assert_eq!(consumed_len, SERIALIZED_ENTRY_HEADER_LEN);
        assert_eq!(header.num_hashes(), entry.num_hashes);
        assert_eq!(header.hash(), &entry.hash);
        assert_eq!(header.num_transactions(), entry.transactions.len() as u64);
    }
}
