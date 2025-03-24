use {
    crate::{
        bytes::{read_byte, read_slice_data},
        result::{Result, TransactionViewError},
    },
    solana_packet::PACKET_DATA_SIZE,
    solana_pubkey::Pubkey,
};

// The packet has a maximum length of 1232 bytes.
// This means the maximum number of 32 byte keys is 38.
// 38 as an min-sized encoded u16 is 1 byte.
// We can simply read this byte, if it's >38 we can return None.
pub const MAX_STATIC_ACCOUNTS_PER_PACKET: u8 =
    (PACKET_DATA_SIZE / core::mem::size_of::<Pubkey>()) as u8;

/// Contains metadata about the static account keys in a transaction packet.
#[derive(Debug, Default)]
pub(crate) struct StaticAccountKeysFrame {
    // /// The number of static accounts in the transaction.
    // pub(crate) num_static_accounts: u8,
    // /// The offset to the first static account in the transaction.
    // pub(crate) offset: u16,
    pub(crate) keys: Vec<Pubkey>,
}

impl StaticAccountKeysFrame {
    #[inline(always)]
    pub(crate) fn try_new(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        // Max size must not have the MSB set so that it is size 1.
        const _: () = assert!(MAX_STATIC_ACCOUNTS_PER_PACKET & 0b1000_0000 == 0);

        let num_static_accounts = read_byte(bytes, offset)?;
        if num_static_accounts == 0 || num_static_accounts > MAX_STATIC_ACCOUNTS_PER_PACKET {
            return Err(TransactionViewError::ParseError);
        }

        // Update offset for array of static accounts.
        let pubkeys: &[Pubkey] =
            unsafe { read_slice_data(bytes, offset, u16::from(num_static_accounts)) }?;
        let keys = pubkeys.to_vec();

        Ok(Self { keys })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_short_vec::ShortVec};

    #[test]
    fn test_zero_accounts() {
        let bytes = bincode::serialize(&ShortVec(Vec::<Pubkey>::new())).unwrap();
        let mut offset = 0;
        assert!(StaticAccountKeysFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_one_account() {
        let bytes = bincode::serialize(&ShortVec(vec![Pubkey::default()])).unwrap();
        let mut offset = 0;
        let frame = StaticAccountKeysFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.keys.len(), 1);
        assert_eq!(offset, 1 + core::mem::size_of::<Pubkey>());
    }

    #[test]
    fn test_max_accounts() {
        let signatures = vec![Pubkey::default(); usize::from(MAX_STATIC_ACCOUNTS_PER_PACKET)];
        let bytes = bincode::serialize(&ShortVec(signatures)).unwrap();
        let mut offset = 0;
        let frame = StaticAccountKeysFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.keys.len(), 38);
        assert_eq!(offset, 1 + 38 * core::mem::size_of::<Pubkey>());
    }

    #[test]
    fn test_too_many_accounts() {
        let signatures = vec![Pubkey::default(); usize::from(MAX_STATIC_ACCOUNTS_PER_PACKET) + 1];
        let bytes = bincode::serialize(&ShortVec(signatures)).unwrap();
        let mut offset = 0;
        assert!(StaticAccountKeysFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_u16_max_accounts() {
        let signatures = vec![Pubkey::default(); u16::MAX as usize];
        let bytes = bincode::serialize(&ShortVec(signatures)).unwrap();
        let mut offset = 0;
        assert!(StaticAccountKeysFrame::try_new(&bytes, &mut offset).is_err());
    }
}
