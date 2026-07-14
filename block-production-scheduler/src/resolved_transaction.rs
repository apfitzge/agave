use {
    agave_external_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, sanitize::SanitizeConfig,
        transaction_data::TransactionData, transaction_version::TransactionVersion,
        transaction_view::SanitizedTransactionView,
    },
    agave_scheduler_bindings::{SharablePubkeys, SharableTransactionRegion},
    agave_scheduling_utils::{pubkeys_ptr::PubkeysPtr, transaction_ptr::TransactionPtr},
    agave_transaction_view::transaction_data::TransactionData as AgaveTransactionData,
    rts_alloc::Allocator,
    solana_message::v0::LoadedAddressesView,
    solana_pubkey::Pubkey,
    std::collections::HashSet,
};

const MIN_REQUESTED_HEAP_SIZE: u32 = 32 * 1024;
const MAX_REQUESTED_HEAP_SIZE: u32 = 256 * 1024;
const MAX_INSTRUCTIONS: usize = 64;
const MAX_ACCOUNTS_PER_INSTRUCTION: usize = 255;

#[allow(dead_code)]
pub(crate) fn sanitize_config(enable_instruction_accounts_limit: bool) -> SanitizeConfig {
    SanitizeConfig {
        min_requested_heap_size: MIN_REQUESTED_HEAP_SIZE,
        max_requested_heap_size: MAX_REQUESTED_HEAP_SIZE,
        max_instructions: MAX_INSTRUCTIONS,
        max_accounts_per_instruction: if enable_instruction_accounts_limit {
            MAX_ACCOUNTS_PER_INSTRUCTION
        } else {
            // The external transaction-view represents the feature-disabled case without an
            // `Option`, so use its largest possible bound.
            usize::MAX
        },
    }
}

/// A local adapter required to implement the external `TransactionData` trait for
/// [`TransactionPtr`].
pub(crate) struct ExternalTransactionData(TransactionPtr);

impl TransactionData for ExternalTransactionData {
    fn data(&self) -> &[u8] {
        AgaveTransactionData::data(&self.0)
    }
}

/// A non-owning view of a resolved-pubkey allocation retained by [`ResolvedTransaction`].
pub(crate) struct ResolvedPubkeys {
    pubkeys: Option<PubkeysPtr>,
    num_writable: usize,
}

impl ResolvedPubkeys {
    unsafe fn from_sharable_pubkeys(
        pubkeys: SharablePubkeys,
        num_writable: usize,
        allocator: &Allocator,
    ) -> Self {
        let len = pubkeys.num_pubkeys as usize;
        assert!(
            num_writable <= len,
            "check worker returned fewer resolved pubkeys than writable address lookups"
        );
        Self {
            pubkeys: (len > 0).then(|| {
                // SAFETY: a non-empty resolved-pubkey region is owned by this scheduler after
                // the check response and remains allocated for the lifetime of the external view.
                unsafe { PubkeysPtr::from_sharable_pubkeys(&pubkeys, allocator) }
            }),
            num_writable,
        }
    }

    fn as_slice(&self) -> &[Pubkey] {
        self.pubkeys.as_ref().map_or(&[], PubkeysPtr::as_slice)
    }
}

impl<'a> From<&'a ResolvedPubkeys> for LoadedAddressesView<'a> {
    fn from(pubkeys: &'a ResolvedPubkeys) -> Self {
        let (writable, readonly) = pubkeys.as_slice().split_at(pubkeys.num_writable);
        Self { writable, readonly }
    }
}

/// A parsed transaction with resolved addresses, backed directly by scheduler-owned allocations.
#[allow(dead_code)]
pub(crate) struct ResolvedTransaction {
    transaction: TransactionPtr,
    resolved_pubkeys: SharablePubkeys,
    view: ResolvedTransactionView<ExternalTransactionData, ResolvedPubkeys>,
}

/// A checked transaction that could not be parsed into an external transaction view.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct UnresolvedTransaction {
    error: agave_external_transaction_view::result::TransactionViewError,
    transaction: TransactionPtr,
    resolved_pubkeys: SharablePubkeys,
}

#[allow(dead_code)]
impl UnresolvedTransaction {
    pub(crate) fn error(&self) -> &agave_external_transaction_view::result::TransactionViewError {
        &self.error
    }

    /// # Safety
    ///
    /// `allocator` must own both retained allocations, which must not have been freed before.
    pub(crate) unsafe fn free(self, allocator: &Allocator) {
        // SAFETY: this scheduler owns the unchecked transaction and pubkey allocations.
        unsafe {
            free_transaction_and_resolved_pubkeys(
                self.transaction,
                self.resolved_pubkeys,
                allocator,
            )
        };
    }
}

#[allow(dead_code)]
impl ResolvedTransaction {
    /// Creates a parsed external transaction view without copying either shared allocation.
    ///
    /// # Safety
    ///
    /// `transaction` and non-empty `resolved_pubkeys` must be valid allocations from `allocator`
    /// that are exclusively owned by this scheduler.
    pub(crate) unsafe fn try_new(
        transaction: TransactionPtr,
        resolved_pubkeys: SharablePubkeys,
        allocator: &Allocator,
        sanitize_config: &SanitizeConfig,
        reserved_account_keys: &HashSet<Pubkey>,
    ) -> Result<Self, UnresolvedTransaction> {
        // SAFETY: `transaction` is required by this function's safety contract to be backed by
        // `allocator` and remains retained in `Self` until the external view is dropped.
        let transaction_region = unsafe { transaction.to_sharable_transaction_region(allocator) };
        // SAFETY: `transaction_region` was derived from a valid transaction allocation above.
        // This view is non-owning; `transaction` remains responsible for freeing the allocation.
        let transaction_data = ExternalTransactionData(unsafe {
            TransactionPtr::from_sharable_transaction_region(&transaction_region, allocator)
        });
        let view = (|| {
            let view =
                SanitizedTransactionView::try_new_sanitized(transaction_data, sanitize_config)?;

            let needs_resolved_pubkeys = matches!(view.version(), TransactionVersion::V0)
                || view.total_writable_lookup_accounts() != 0
                || view.total_readonly_lookup_accounts() != 0;
            let resolved_pubkeys_source = needs_resolved_pubkeys.then(|| {
                // SAFETY: the check response owns a valid writable-then-readonly pubkey allocation.
                unsafe {
                    ResolvedPubkeys::from_sharable_pubkeys(
                        resolved_pubkeys,
                        view.total_writable_lookup_accounts() as usize,
                        allocator,
                    )
                }
            });

            ResolvedTransactionView::try_new_with_source(
                view,
                resolved_pubkeys_source,
                reserved_account_keys,
            )
        })();

        match view {
            Ok(view) => Ok(Self {
                transaction,
                resolved_pubkeys,
                view,
            }),
            Err(error) => Err(UnresolvedTransaction {
                error,
                transaction,
                resolved_pubkeys,
            }),
        }
    }

    pub(crate) fn view(
        &self,
    ) -> &ResolvedTransactionView<ExternalTransactionData, ResolvedPubkeys> {
        &self.view
    }

    /// # Safety
    ///
    /// `allocator` must own this transaction allocation.
    pub(crate) unsafe fn to_sharable_transaction_region(
        &self,
        allocator: &Allocator,
    ) -> SharableTransactionRegion {
        // SAFETY: upheld by this method's safety contract.
        unsafe { self.transaction.to_sharable_transaction_region(allocator) }
    }

    /// # Safety
    ///
    /// `allocator` must own both retained allocations, which must not have been freed before.
    pub(crate) unsafe fn free(self, allocator: &Allocator) {
        let Self {
            transaction,
            resolved_pubkeys,
            view,
        } = self;
        drop(view);
        // SAFETY: this scheduler exclusively owns both allocations after receiving the checked
        // transaction and has dropped all non-owning views into them.
        unsafe { free_transaction_and_resolved_pubkeys(transaction, resolved_pubkeys, allocator) };
    }
}

/// # Safety
///
/// `allocator` must own both allocations, which must not have been freed before.
unsafe fn free_transaction_and_resolved_pubkeys(
    transaction: TransactionPtr,
    resolved_pubkeys: SharablePubkeys,
    allocator: &Allocator,
) {
    // SAFETY: upheld by this function's safety contract.
    unsafe { transaction.free(allocator) };
    if resolved_pubkeys.num_pubkeys > 0 {
        // SAFETY: upheld by this function's safety contract.
        unsafe { allocator.free_offset(resolved_pubkeys.offset) };
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::SchedulerConfig,
        agave_scheduling_utils::handshake::server::Server,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_signer::Signer,
        solana_svm_transaction::svm_message::SVMMessage,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
    };

    #[test]
    fn resolves_a_checked_transaction_without_copying_allocations() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let (session, _) = Server::setup_session(config.client_logon()).unwrap();
        let allocator = &session.tpu_to_pack.allocator;

        let payer = Keypair::new();
        let recipient = Pubkey::new_from_array([1; 32]);
        let message = Message::new(
            &[system_instruction::transfer(&payer.pubkey(), &recipient, 1)],
            Some(&payer.pubkey()),
        );
        let bytes = wincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&payer],
            message,
            Hash::default(),
        )))
        .unwrap();
        let allocation = allocator.allocate(bytes.len() as u32).unwrap();
        // SAFETY: both pointers are valid for `bytes.len()` bytes and do not overlap.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), allocation.as_ptr(), bytes.len());
        }
        // SAFETY: `allocation` was created by this allocator immediately above.
        let transaction = unsafe { TransactionPtr::from_raw_parts(allocation, bytes.len()) };

        // SAFETY: this test owns the transaction allocation and has no resolved pubkeys.
        let transaction = unsafe {
            ResolvedTransaction::try_new(
                transaction,
                SharablePubkeys {
                    offset: 0,
                    num_pubkeys: 0,
                },
                allocator,
                &sanitize_config(false),
                &HashSet::new(),
            )
        }
        .unwrap();

        assert_eq!(
            transaction.view().account_keys().get(0),
            Some(&payer.pubkey())
        );
        assert!(transaction.view().is_writable(0));
        // SAFETY: this test retains the allocation through `transaction`.
        assert_eq!(
            unsafe { transaction.to_sharable_transaction_region(allocator) }.length as usize,
            bytes.len()
        );

        // SAFETY: `transaction` owns the shared allocation created above.
        unsafe { transaction.free(allocator) };
    }

    #[test]
    fn returns_ownership_when_external_parsing_fails() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let (session, _) = Server::setup_session(config.client_logon()).unwrap();
        let allocator = &session.tpu_to_pack.allocator;
        let allocation = allocator.allocate(1).unwrap();
        // SAFETY: `allocation` points to a fresh one-byte allocation.
        unsafe { allocation.as_ptr().write(0) };
        // SAFETY: `allocation` was created by this allocator immediately above.
        let transaction = unsafe { TransactionPtr::from_raw_parts(allocation, 1) };

        // SAFETY: this test owns the malformed transaction allocation.
        let result = unsafe {
            ResolvedTransaction::try_new(
                transaction,
                SharablePubkeys {
                    offset: 0,
                    num_pubkeys: 0,
                },
                allocator,
                &sanitize_config(false),
                &HashSet::new(),
            )
        };
        let Err(unresolved) = result else {
            panic!("malformed transaction must fail external parsing");
        };
        assert_eq!(
            *unresolved.error(),
            agave_external_transaction_view::result::TransactionViewError::ParseError
        );

        // SAFETY: `unresolved` returned ownership of the allocation to this test.
        unsafe { unresolved.free(allocator) };
    }

    #[test]
    fn resolved_pubkeys_keep_the_check_worker_order() {
        let mut pubkeys = [
            Pubkey::new_from_array([1; 32]),
            Pubkey::new_from_array([2; 32]),
            Pubkey::new_from_array([3; 32]),
        ];
        let resolved_pubkeys = ResolvedPubkeys {
            // SAFETY: `pubkeys` remains allocated and unmodified until `resolved_pubkeys` is
            // dropped at the end of this test.
            pubkeys: Some(unsafe {
                PubkeysPtr::from_raw_parts(std::ptr::NonNull::from(&mut pubkeys[0]), pubkeys.len())
            }),
            num_writable: 1,
        };

        let loaded_addresses = LoadedAddressesView::from(&resolved_pubkeys);
        assert_eq!(loaded_addresses.writable, &pubkeys[..1]);
        assert_eq!(loaded_addresses.readonly, &pubkeys[1..]);
    }
}
