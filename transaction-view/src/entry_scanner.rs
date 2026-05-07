use {
    crate::{
        bytes::{check_remaining, unchecked_copy_value},
        entry_header_frame::EntryHeaderFrame,
        result::TransactionViewError,
        transaction_view::{TransactionView, UnsanitizedTransactionView},
    },
    core::mem::size_of,
};

#[derive(Debug)]
pub enum EntryScanItem<'a> {
    EntryHeader {
        entry_index: u64,
        header: EntryHeaderFrame<'a>,
    },
    Transaction {
        entry_index: u64,
        transaction_index: u64,
        transaction: UnsanitizedTransactionView<&'a [u8]>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum EntryScanError<E> {
    Parse(TransactionViewError),
    Visit(E),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryScanResult {
    pub num_entries: u64,
    pub consumed_len: usize,
}

/// Scan a serialized `Vec<Entry>` from the beginning of `bytes`.
///
/// Trailing bytes are allowed. On success, [`EntryScanResult::consumed_len`]
/// is the first offset after the serialized `Vec<Entry>` prefix.
pub fn scan_serialized_entries_from_prefix<'a, E>(
    bytes: &'a [u8],
    mut visitor: impl FnMut(EntryScanItem<'a>) -> core::result::Result<(), E>,
) -> core::result::Result<EntryScanResult, EntryScanError<E>> {
    let num_entries = read_vec_len(bytes).map_err(EntryScanError::Parse)?;
    let mut offset = size_of::<u64>();

    for entry_index in 0..num_entries {
        let (header, consumed_len) = EntryHeaderFrame::try_new_from_prefix(&bytes[offset..])
            .map_err(EntryScanError::Parse)?;
        offset = offset
            .checked_add(consumed_len)
            .ok_or(EntryScanError::Parse(TransactionViewError::ParseError))?;

        visitor(EntryScanItem::EntryHeader {
            entry_index,
            header,
        })
        .map_err(EntryScanError::Visit)?;

        for transaction_index in 0..header.num_transactions() {
            let (transaction, consumed_len) =
                TransactionView::try_new_unsanitized_from_prefix(&bytes[offset..])
                    .map_err(EntryScanError::Parse)?;
            offset = offset
                .checked_add(consumed_len)
                .ok_or(EntryScanError::Parse(TransactionViewError::ParseError))?;

            visitor(EntryScanItem::Transaction {
                entry_index,
                transaction_index,
                transaction,
            })
            .map_err(EntryScanError::Visit)?;
        }
    }

    Ok(EntryScanResult {
        num_entries,
        consumed_len: offset,
    })
}

fn read_vec_len(bytes: &[u8]) -> core::result::Result<u64, TransactionViewError> {
    check_remaining(bytes, 0, size_of::<u64>())?;
    // SAFETY: `check_remaining` verified that `bytes` starts with a full u64.
    // Any byte pattern is valid for u64, and `unchecked_copy_value` uses an
    // unaligned read.
    Ok(u64::from_le(unsafe {
        unchecked_copy_value::<u64>(bytes, 0)
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_entry::entry::Entry,
        solana_hash::{HASH_BYTES, Hash},
        solana_message::{Message, VersionedMessage},
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::versioned::VersionedTransaction,
    };

    #[derive(Debug, PartialEq, Eq)]
    enum ScannedItem {
        Header {
            entry_index: u64,
            num_hashes: u64,
            hash: Hash,
            num_transactions: u64,
        },
        Transaction {
            entry_index: u64,
            transaction_index: u64,
            bytes: Vec<u8>,
        },
    }

    fn transaction() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()],
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

    fn collect_scan(bytes: &[u8]) -> core::result::Result<(EntryScanResult, Vec<ScannedItem>), ()> {
        let mut items = Vec::new();
        let result = scan_serialized_entries_from_prefix(bytes, |item| {
            match item {
                EntryScanItem::EntryHeader {
                    entry_index,
                    header,
                } => items.push(ScannedItem::Header {
                    entry_index,
                    num_hashes: header.num_hashes(),
                    hash: *header.hash(),
                    num_transactions: header.num_transactions(),
                }),
                EntryScanItem::Transaction {
                    entry_index,
                    transaction_index,
                    transaction,
                } => items.push(ScannedItem::Transaction {
                    entry_index,
                    transaction_index,
                    bytes: transaction.data().to_vec(),
                }),
            }

            Ok::<(), ()>(())
        })
        .map_err(|_| ())?;

        Ok((result, items))
    }

    #[test]
    fn test_scan_empty_entries() {
        let entries: Vec<Entry> = vec![];
        let bytes = wincode::serialize(&entries).unwrap();

        let (result, items) = collect_scan(&bytes).unwrap();

        assert_eq!(
            result,
            EntryScanResult {
                num_entries: 0,
                consumed_len: size_of::<u64>(),
            }
        );
        assert!(items.is_empty());
    }

    #[test]
    fn test_scan_mixed_entries_with_trailing_bytes() {
        let tx0 = transaction();
        let tx1 = transaction();
        let entries = vec![
            Entry {
                num_hashes: 1,
                hash: Hash::new_from_array([1; HASH_BYTES]),
                transactions: vec![],
            },
            Entry {
                num_hashes: 2,
                hash: Hash::new_from_array([2; HASH_BYTES]),
                transactions: vec![tx0.clone(), tx1.clone()],
            },
        ];
        let entries_bytes = wincode::serialize(&entries).unwrap();
        let mut bytes = entries_bytes.clone();
        bytes.extend_from_slice(&[9, 8, 7]);

        let (result, items) = collect_scan(&bytes).unwrap();

        assert_eq!(
            result,
            EntryScanResult {
                num_entries: entries.len() as u64,
                consumed_len: entries_bytes.len(),
            }
        );
        assert_eq!(
            items,
            vec![
                ScannedItem::Header {
                    entry_index: 0,
                    num_hashes: entries[0].num_hashes,
                    hash: entries[0].hash,
                    num_transactions: 0,
                },
                ScannedItem::Header {
                    entry_index: 1,
                    num_hashes: entries[1].num_hashes,
                    hash: entries[1].hash,
                    num_transactions: 2,
                },
                ScannedItem::Transaction {
                    entry_index: 1,
                    transaction_index: 0,
                    bytes: wincode::serialize(&tx0).unwrap(),
                },
                ScannedItem::Transaction {
                    entry_index: 1,
                    transaction_index: 1,
                    bytes: wincode::serialize(&tx1).unwrap(),
                },
            ]
        );
        assert_eq!(&bytes[result.consumed_len..], &[9, 8, 7]);
    }

    #[test]
    fn test_scan_header_hash_is_borrowed() {
        let entries = vec![Entry {
            num_hashes: 1,
            hash: Hash::new_from_array([1; HASH_BYTES]),
            transactions: vec![],
        }];
        let bytes = wincode::serialize(&entries).unwrap();
        let mut hash_ptr = core::ptr::null();

        scan_serialized_entries_from_prefix(&bytes, |item| {
            if let EntryScanItem::EntryHeader { header, .. } = item {
                hash_ptr = header.hash() as *const Hash;
            }
            Ok::<(), ()>(())
        })
        .unwrap();

        assert_eq!(
            hash_ptr,
            bytes[size_of::<u64>() + size_of::<u64>()..].as_ptr() as *const Hash,
        );
    }

    #[test]
    fn test_scan_rejects_truncated_vec_len() {
        let bytes = [0; size_of::<u64>() - 1];

        assert_eq!(
            scan_serialized_entries_from_prefix(&bytes, |_| Ok::<(), ()>(())),
            Err(EntryScanError::Parse(TransactionViewError::ParseError))
        );
    }

    #[test]
    fn test_scan_rejects_truncated_entry_header() {
        let mut bytes = 1u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0; 4]);

        assert_eq!(
            scan_serialized_entries_from_prefix(&bytes, |_| Ok::<(), ()>(())),
            Err(EntryScanError::Parse(TransactionViewError::ParseError))
        );
    }

    #[test]
    fn test_scan_rejects_truncated_transaction() {
        let tx = transaction();
        let entries = vec![Entry {
            num_hashes: 1,
            hash: Hash::new_from_array([1; HASH_BYTES]),
            transactions: vec![tx],
        }];
        let mut bytes = wincode::serialize(&entries).unwrap();
        bytes.pop();

        assert_eq!(
            scan_serialized_entries_from_prefix(&bytes, |_| Ok::<(), ()>(())),
            Err(EntryScanError::Parse(TransactionViewError::ParseError))
        );
    }

    #[test]
    fn test_scan_rejects_missing_declared_transaction() {
        let mut bytes = 1u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(Hash::new_from_array([1; HASH_BYTES]).as_ref());
        bytes.extend_from_slice(&1u64.to_le_bytes());

        assert_eq!(
            scan_serialized_entries_from_prefix(&bytes, |_| Ok::<(), ()>(())),
            Err(EntryScanError::Parse(TransactionViewError::ParseError))
        );
    }

    #[test]
    fn test_scan_returns_visitor_error() {
        let entries = vec![Entry {
            num_hashes: 1,
            hash: Hash::new_from_array([1; HASH_BYTES]),
            transactions: vec![],
        }];
        let bytes = wincode::serialize(&entries).unwrap();

        assert_eq!(
            scan_serialized_entries_from_prefix(&bytes, |_| Err("stop")),
            Err(EntryScanError::Visit("stop"))
        );
    }
}
