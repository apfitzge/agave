use {
    agave_scheduler_bindings::SharablePubkeys,
    agave_scheduling_utils::{pubkeys_ptr::PubkeysPtr, transaction_ptr::TransactionPtr},
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    rts_alloc::Allocator,
    solana_message::v0::LoadedAddressesView,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::sanitize_config::sanitize_config,
    std::collections::HashSet,
};

pub(super) struct ResolvedPubkeys {
    pubkeys: Option<PubkeysPtr>,
    num_writable: usize,
}

impl<'a> From<&'a ResolvedPubkeys> for LoadedAddressesView<'a> {
    fn from(pubkeys: &'a ResolvedPubkeys) -> Self {
        let keys = pubkeys
            .pubkeys
            .as_ref()
            .map_or(&[][..], PubkeysPtr::as_slice);
        let (writable, readonly) = keys.split_at(pubkeys.num_writable);
        Self { writable, readonly }
    }
}

/// A resolved view backed by shared allocations, released explicitly through `free`.
pub(super) struct ResolvedTransaction {
    transaction: TransactionPtr,
    resolved_pubkeys: SharablePubkeys,
    pub(super) view: ResolvedTransactionView<TransactionPtr, ResolvedPubkeys>,
}

impl ResolvedTransaction {
    /// Takes ownership on success; on failure the caller retains both allocations.
    ///
    /// # Safety
    /// The transaction and any nonempty pubkey region must be initialized, exclusively owned
    /// allocations from `allocator`. Pubkeys must be ordered writable then readonly.
    pub(super) unsafe fn try_new(
        transaction: TransactionPtr,
        resolved_pubkeys: SharablePubkeys,
        allocator: &Allocator,
        reserved_account_keys: &HashSet<Pubkey>,
    ) -> Result<Self, TransactionPtr> {
        let view = (|| {
            // SAFETY: the caller owns the allocation. This alias backs the view and is never freed
            // separately; `transaction` remains responsible for releasing the allocation.
            let data = unsafe {
                TransactionPtr::from_sharable_transaction_region(
                    &transaction.to_sharable_transaction_region(allocator),
                    allocator,
                )
            };
            let view =
                SanitizedTransactionView::try_new_sanitized(data, &sanitize_config()).ok()?;
            let num_writable = usize::from(view.total_writable_lookup_accounts());
            if num_writable > resolved_pubkeys.num_pubkeys as usize {
                return None;
            }
            let addresses = if matches!(view.version(), TransactionVersion::V0)
                || resolved_pubkeys.num_pubkeys != 0
                || view.total_readonly_lookup_accounts() != 0
            {
                Some(ResolvedPubkeys {
                    pubkeys: if resolved_pubkeys.num_pubkeys == 0 {
                        None
                    } else {
                        // SAFETY: the caller guarantees a valid, initialized pubkey allocation.
                        Some(unsafe {
                            PubkeysPtr::from_sharable_pubkeys(&resolved_pubkeys, allocator)
                        })
                    },
                    num_writable,
                })
            } else {
                None
            };
            ResolvedTransactionView::try_new_with_source(view, addresses, reserved_account_keys)
                .ok()
        })();
        match view {
            Some(view) => Ok(Self {
                transaction,
                resolved_pubkeys,
                view,
            }),
            None => Err(transaction),
        }
    }

    /// # Safety
    /// Both allocations must still be exclusively owned and belong to `allocator`.
    pub(super) unsafe fn free(self, allocator: &Allocator) {
        drop(self.view);
        // SAFETY: the caller owns both allocations and the view no longer references them.
        unsafe {
            self.transaction.free(allocator);
            free_resolved_pubkeys(self.resolved_pubkeys, allocator);
        }
    }
}

/// # Safety
/// A nonempty region must describe a live allocation exclusively owned by the caller.
pub(super) unsafe fn free_resolved_pubkeys(pubkeys: SharablePubkeys, allocator: &Allocator) {
    if pubkeys.num_pubkeys != 0 {
        // SAFETY: the caller owns this allocation.
        unsafe { allocator.free_offset(pubkeys.offset) };
    }
}
