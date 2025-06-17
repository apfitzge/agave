use core::num;

#[repr(C)]
pub struct TransactionHeader {
    pub version: u8,
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub payload_length: u16,
    pub num_instructions: u8,
    pub num_addresses: u8,
    pub resource_mask: u64,
}

#[repr(C)]
pub struct InstructionMeta {
    pub program_id_index: u8,
    pub num_accounts: u8,
    pub accounts_offset: u16,
    pub num_data_bytes: u16,
    pub data_offset: u16,
}

pub struct TransactionView<'a> {
    bytes: &'a [u8],
}

impl TransactionView<'_> {
    // TODO: error.
    pub fn try_new(bytes: &[u8]) -> Option<TransactionView> {
        if bytes.len() <= core::mem::size_of::<TransactionHeader>() {
            return None;
        }

        let transaction_header_ptr = bytes.as_ptr() as *const TransactionHeader;
        if !transaction_header_ptr.is_aligned() {
            return None;
        }
        let header = unsafe { &*(transaction_header_ptr) };

        // Calculate the expected total size of the transaction.
        let expected_signature_size = usize::from(header.num_required_signatures) * 64; // 64 bytes per signature
        let expected_size = usize::from(header.payload_length) + expected_signature_size;
        // TODO: May want to move this for up Vecs of transactions.
        if bytes.len() != expected_size
            || usize::from(header.payload_length) < core::mem::size_of::<TransactionHeader>()
        {
            return None;
        }

        // Check that the serialization is reasonably correct:
        // - We must have resource_mask.count_ones() resource requests (8 bytes each).
        // - We must have at least `num_addresses` addresses (32 bytes each).
        // - We must have at least `num_instructions` instructions (8 bytes each).
        let expected_resource_size =
            (header.resource_mask.count_ones() as usize) * core::mem::size_of::<u64>();
        let expected_addresses_size = usize::from(header.num_addresses) * 32;
        let expected_instruction_meta_size =
            usize::from(header.num_instructions) * core::mem::size_of::<InstructionMeta>();
        let minimum_size_check = core::mem::size_of::<TransactionHeader>()
            + expected_addresses_size
            + expected_resource_size
            + expected_instruction_meta_size
            + expected_signature_size;
        if bytes.len() < minimum_size_check {
            return None;
        }

        let view = TransactionView { bytes };

        // Check each instruction's offsets/sizes are sane.
        let instruction_metas = view.instruction_metas();
        for ix_meta in instruction_metas {
            if usize::from(ix_meta.accounts_offset) + usize::from(ix_meta.num_accounts)
                > bytes.len()
            {
                return None;
            }
            if usize::from(ix_meta.data_offset) + usize::from(ix_meta.num_data_bytes) > bytes.len()
            {
                return None;
            }
        }

        Some(view)
    }

    pub fn header(&self) -> &TransactionHeader {
        let transaction_header_ptr = self.bytes.as_ptr() as *const TransactionHeader;
        unsafe { &*transaction_header_ptr }
    }

    const fn addresses_offset() -> usize {
        core::mem::size_of::<TransactionHeader>()
    }

    fn num_addresses(&self) -> u8 {
        self.header().num_addresses
    }

    pub fn addresses(&self) -> &[[u8; 32]] {
        unsafe {
            core::slice::from_raw_parts(
                self.bytes.as_ptr().add(Self::addresses_offset()) as *const [u8; 32],
                usize::from(self.num_addresses()),
            )
        }
    }

    fn resources_offset(&self) -> usize {
        Self::addresses_offset() + usize::from(self.num_addresses()) * 32
    }

    fn num_resources(&self) -> u32 {
        self.header().resource_mask.count_ones()
    }

    pub fn resources(&self) -> &[u64] {
        unsafe {
            core::slice::from_raw_parts(
                self.bytes.as_ptr().add(self.resources_offset()) as *const u64,
                self.num_resources() as usize,
            )
        }
    }

    fn instructions_offset(&self) -> usize {
        self.resources_offset() + (self.num_resources() as usize) * core::mem::size_of::<u64>()
    }

    pub fn instruction_metas(&self) -> &[InstructionMeta] {
        let offset = self.instructions_offset();
        let num_instructions = usize::from(self.header().num_instructions);
        unsafe {
            core::slice::from_raw_parts(
                self.bytes.as_ptr().add(offset) as *const InstructionMeta,
                num_instructions,
            )
        }
    }
}
