use {
    crate::{
        bytes::{advance_offset_for_type, read_type},
        result::{Result, TransactionViewError},
        transaction_data::TransactionData,
        transaction_frame::TransactionFrame,
        transaction_view::{TransactionView, UnsanitizedTransactionView},
    },
    solana_sdk::hash::Hash,
};

pub struct EntryView<D: TransactionData> {
    /// Underlying data buffer for the entry.
    data: D,

    /// List of `TransactionFrame` and starting offsets for
    /// each transaction in the entry.
    frames_and_offsets: Vec<(usize, TransactionFrame)>,
}

impl<D: TransactionData> EntryView<D> {
    pub fn try_new(data: D) -> Result<Self> {
        // The alignment of the data buffer must be 8 for the deserialization here to work correctly.
        let bytes = data.data();
        if bytes.as_ptr() as usize % 8 != 0 {
            return Err(TransactionViewError::IncorrectAlignment);
        }

        let mut offset = 0;

        // `num_hashes` - u64
        advance_offset_for_type::<u64>(bytes, &mut offset)?;
        // `hash` - Hash
        advance_offset_for_type::<Hash>(bytes, &mut offset)?;

        let frames_and_offsets = Self::parse_entry_transactions(bytes, offset)?;
        Ok(Self {
            data,
            frames_and_offsets,
        })
    }

    /// Returns the number of hashes in the entry.
    #[inline]
    pub fn num_hashes(&self) -> u64 {
        let bytes = self.data.data();
        // SAFETY:
        // - The data buffer is guaranteed to be correctly aligned to 8.
        // - The `EntryView` was constructed with `data` so there are at least 8 bytes.
        unsafe { *bytes.as_ptr().cast::<u64>() }
    }

    /// Returns the hash of the entry.
    #[inline]
    pub fn hash(&self) -> Hash {
        let bytes = self.data.data();
        // SAFETY:
        // - The data buffer is guaranteed to be correctly aligned to 8.
        // - The `EntryView` was constructed with `data` so there are at least 8 bytes.
        unsafe {
            *bytes
                .as_ptr()
                .add(core::mem::size_of::<u64>())
                .cast::<Hash>()
        }
    }

    /// Get the number of transactions in the entry.
    #[inline]
    pub fn num_transactions(&self) -> usize {
        self.frames_and_offsets.len()
    }

    /// Get the transaction at the given index.
    /// If the index is out of bounds, `None` is returned.
    #[inline]
    pub fn get_transaction(&self, index: usize) -> Option<UnsanitizedTransactionView<&[u8]>> {
        self.frames_and_offsets.get(index).map(|(offset, frame)| {
            // SAFETY: The format was checked on construction in `try_new`.
            unsafe {
                TransactionView::from_bytes_unchecked(&self.data.data()[*offset..], frame.clone())
            }
        })
    }

    #[inline]
    fn parse_entry_transactions(
        data: &[u8],
        mut offset: usize,
    ) -> Result<Vec<(usize, TransactionFrame)>> {
        // Read the number of transactions
        let num_transactions = unsafe { *read_type::<u64>(data, &mut offset)? } as usize;

        let mut frames_and_offsets = Vec::with_capacity(num_transactions);
        while frames_and_offsets.len() < num_transactions {
            let (frame, bytes_read) = TransactionFrame::try_new(&data[offset..])?;
            frames_and_offsets.push((offset, frame));
            offset += bytes_read;
        }

        Ok(frames_and_offsets)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_entry::entry::Entry,
        solana_sdk::{
            message::{Message, VersionedMessage},
            pubkey::Pubkey,
            signature::Signature,
            system_instruction,
            transaction::VersionedTransaction,
        },
    };

    fn simple_transfer() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message::new(
                &[system_instruction::transfer(
                    &payer,
                    &Pubkey::new_unique(),
                    1,
                )],
                Some(&payer),
            )),
        }
    }

    fn create_entry_and_serialize(transactions: Vec<VersionedTransaction>) -> (Entry, Vec<u8>) {
        let entry = Entry {
            num_hashes: 42,
            hash: Hash::default(),
            transactions,
        };

        let serialized_entry = bincode::serialize(&entry).unwrap();
        (entry, serialized_entry)
    }

    #[test]
    fn test_tick_entry() {
        let (entry, bytes) = create_entry_and_serialize(vec![]);
        let entry_view = EntryView::try_new(&bytes[..]).unwrap();

        assert_eq!(entry_view.num_hashes(), entry.num_hashes);
        assert_eq!(entry_view.hash(), entry.hash);
        assert_eq!(entry_view.num_transactions(), entry.transactions.len());
    }

    #[test]
    fn test_single_transaction() {
        let (entry, bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        let entry_view = EntryView::try_new(&bytes[..]).unwrap();

        assert_eq!(entry_view.num_hashes(), entry.num_hashes);
        assert_eq!(entry_view.hash(), entry.hash);
        assert_eq!(entry_view.num_transactions(), entry.transactions.len());

        let transaction = entry_view.get_transaction(0).unwrap();
        assert_eq!(transaction.signatures().len(), 1);
        assert_eq!(
            transaction
                .instructions_iter()
                .next()
                .unwrap()
                .program_id_index,
            2
        );
    }

    #[test]
    fn test_multiple_transactions() {
        let (entry, bytes) = create_entry_and_serialize(vec![
            simple_transfer(),
            simple_transfer(),
            simple_transfer(),
        ]);
        let entry_view = EntryView::try_new(&bytes[..]).unwrap();

        assert_eq!(entry_view.num_hashes(), entry.num_hashes);
        assert_eq!(entry_view.hash(), entry.hash);
        assert_eq!(entry_view.num_transactions(), entry.transactions.len());

        for i in 0..entry.transactions.len() {
            let transaction = entry_view.get_transaction(i).unwrap();
            assert_eq!(transaction.signatures().len(), 1);
            assert_eq!(
                transaction
                    .instructions_iter()
                    .next()
                    .unwrap()
                    .program_id_index,
                2
            );
        }
    }

    #[test]
    fn test_trailing_bytes() {
        let (entry, mut bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        bytes.push(0); // trailing bytes are okay for entries.

        let entry_view = EntryView::try_new(&bytes[..]).unwrap();

        assert_eq!(entry_view.num_hashes(), entry.num_hashes);
        assert_eq!(entry_view.hash(), entry.hash);
        assert_eq!(entry_view.num_transactions(), entry.transactions.len());
    }
}
