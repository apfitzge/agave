use {
    super::{
        transaction_state::TransactionState, transaction_state_container::TransactionViewState,
    },
    crate::{
        banking_stage::{consumer::Consumer, scheduler_messages::MaxAge},
        transaction_priority::calculate_priority_and_cost,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, sanitize::SanitizeConfig,
        transaction_data::TransactionData, transaction_version::TransactionVersion,
        transaction_view::SanitizedTransactionView,
    },
    solana_accounts_db::account_locks::validate_account_locks,
    solana_address_lookup_table_interface::state::estimate_last_valid_slot,
    solana_clock::{Epoch, Slot},
    solana_message::v0::LoadedAddresses,
    solana_perf::packet::bytes::Bytes,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, sanitize_config::sanitize_config,
        transaction_meta::TransactionMeta,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::sanitized::MessageHash,
    solana_transaction_error::TransactionError,
    std::collections::HashSet,
};

#[derive(Debug)]
pub(crate) enum IngressCheckError {
    PacketHandling(PacketHandlingError),
    Transaction(TransactionError),
    FeePayer,
    BelowPriorityFloor,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PacketHandlingError {
    Sanitization,
    LockValidation,
    ComputeBudget,
    ALTResolution,
    FilterKey,
}

pub(crate) struct CheckedTransaction {
    pub(crate) state: TransactionViewState,
    pub(crate) is_validated_nonce: bool,
}

pub(crate) struct ParsedTransaction {
    state: TransactionViewState,
}

impl ParsedTransaction {
    pub(crate) fn priority(&self) -> u64 {
        self.state.priority()
    }
}

pub(crate) fn parse_transaction(
    bytes: Bytes,
    root_bank: &Bank,
    working_bank: &Bank,
    filter_keys: &HashSet<Pubkey>,
) -> Result<ParsedTransaction, IngressCheckError> {
    let sanitize_config = sanitize_config();
    let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
    let (view, deactivation_slot) = translate_to_runtime_view(
        bytes,
        root_bank,
        transaction_account_lock_limit,
        &sanitize_config,
    )
    .map_err(IngressCheckError::PacketHandling)?;

    if !filter_keys.is_empty()
        && view
            .account_keys()
            .iter()
            .any(|key| filter_keys.contains(key))
    {
        return Err(IngressCheckError::PacketHandling(
            PacketHandlingError::FilterKey,
        ));
    }

    let transaction_configuration = view
        .transaction_configuration(&working_bank.feature_set)
        .map_err(|_| IngressCheckError::PacketHandling(PacketHandlingError::ComputeBudget))?;
    let max_age = calculate_max_age(root_bank.epoch(), deactivation_slot, root_bank.slot());
    let (priority, cost) =
        calculate_priority_and_cost(working_bank, &view, &transaction_configuration);

    Ok(ParsedTransaction {
        state: TransactionState::new(view, max_age, priority, cost),
    })
}

pub(crate) fn check_parsed_transaction(
    parsed_transaction: ParsedTransaction,
    working_bank: &Bank,
) -> Result<CheckedTransaction, IngressCheckError> {
    let ParsedTransaction { state } = parsed_transaction;
    let mut error_counters = TransactionErrorMetrics::default();
    let validated_nonce_address = working_bank
        .check_transaction_without_status_cache(
            state.transaction(),
            working_bank.max_processing_age(),
            &mut error_counters,
        )
        .map_err(IngressCheckError::Transaction)?;

    Consumer::check_fee_payer_unlocked(working_bank, state.transaction(), &mut error_counters)
        .map_err(|_| IngressCheckError::FeePayer)?;

    Ok(CheckedTransaction {
        state,
        is_validated_nonce: validated_nonce_address.is_some(),
    })
}

/// Perform sanitization checks and transition from data to an executable
/// [`RuntimeTransaction`]. This additionally returns the minimum slot for
/// ALT deactivation, if any. If no minimum slot, Slot::MAX is returned.
pub(crate) fn translate_to_runtime_view<D: TransactionData>(
    data: D,
    bank: &Bank,
    transaction_account_lock_limit: usize,
    sanitize_config: &SanitizeConfig,
) -> Result<(RuntimeTransaction<ResolvedTransactionView<D>>, u64), PacketHandlingError> {
    // Parsing and basic sanitization checks
    let Ok(view) = SanitizedTransactionView::try_new_sanitized(data, sanitize_config) else {
        return Err(PacketHandlingError::Sanitization);
    };

    let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_new(
        view,
        MessageHash::Compute,
        None,
    ) else {
        return Err(PacketHandlingError::Sanitization);
    };

    // Discard non-vote packets if in vote-only mode.
    if bank.vote_only_bank() && !view.is_simple_vote_transaction() {
        return Err(PacketHandlingError::Sanitization);
    }

    if usize::from(view.total_num_accounts()) > transaction_account_lock_limit {
        return Err(PacketHandlingError::LockValidation);
    }

    let (loaded_addresses, deactivation_slot) = load_addresses_for_view(&view, bank)?;

    let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_new(
        view,
        loaded_addresses,
        bank.get_reserved_account_keys(),
    ) else {
        return Err(PacketHandlingError::Sanitization);
    };

    // Validate no duplicate accounts (must be after resolution to catch ALT duplicates)
    if validate_account_locks(view.account_keys(), transaction_account_lock_limit).is_err() {
        return Err(PacketHandlingError::LockValidation);
    }

    Ok((view, deactivation_slot))
}

/// Load addresses from ALTs (if necessary) and return the
/// [`LoadedAddresses`] with the minimum deactivation slot.
fn load_addresses_for_view<D: TransactionData>(
    view: &SanitizedTransactionView<D>,
    bank: &Bank,
) -> Result<(Option<LoadedAddresses>, Slot), PacketHandlingError> {
    match view.version() {
        TransactionVersion::Legacy | TransactionVersion::V1 => Ok((None, u64::MAX)),
        TransactionVersion::V0 => bank
            .load_addresses_from_ref(view.address_table_lookup_iter())
            .map(|(loaded_addresses, deactivation_slot)| {
                (Some(loaded_addresses), deactivation_slot)
            })
            .map_err(|_| PacketHandlingError::ALTResolution),
    }
}

/// Given the epoch, the minimum deactivation slot, and the current slot,
/// return the `MaxAge` that should be used for the transaction. This is used
/// to determine the maximum slot that a transaction will be considered valid
/// for, without re-resolving addresses or resanitizing.
///
/// This function considers the deactivation period of Address Table
/// accounts. If the deactivation period runs past the end of the epoch,
/// then the transaction is considered valid until the end of the epoch.
/// Otherwise, the transaction is considered valid until the deactivation
/// period.
///
/// Since the deactivation period technically uses blocks rather than
/// slots, the value used here is the lower-bound on the deactivation
/// period, i.e. the transaction's address lookups are valid until
/// AT LEAST this slot.
fn calculate_max_age(
    sanitized_epoch: Epoch,
    deactivation_slot: Slot,
    current_slot: Slot,
) -> MaxAge {
    let alt_min_expire_slot = estimate_last_valid_slot(deactivation_slot.min(current_slot));
    MaxAge {
        sanitized_epoch,
        alt_invalidation_slot: alt_min_expire_slot,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::{
            tests::create_slow_genesis_config,
            transaction_scheduler::transaction_state_container::RuntimeTransactionView,
        },
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_perf::packet::BytesPacket,
        solana_slot_hashes::get_entries,
        solana_system_transaction::transfer,
    };

    #[test]
    fn test_calculate_max_age() {
        let current_slot = 100;
        let sanitized_epoch = 10;

        // ALT deactivation slot is delayed
        assert_eq!(
            calculate_max_age(sanitized_epoch, current_slot - 1, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot - 1 + get_entries() as u64,
            }
        );

        // no deactivation slot
        assert_eq!(
            calculate_max_age(sanitized_epoch, u64::MAX, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot + get_entries() as u64,
            }
        );
    }

    fn assert_runtime_view(_: &RuntimeTransactionView) {}

    #[test]
    fn checked_transaction_owns_packet_bytes() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank.last_blockhash(),
        );
        let packet = BytesPacket::from_data(transaction).unwrap();
        let bytes = packet.buffer().clone();
        let bytes_ptr = bytes.as_ptr();
        drop(packet);

        let parsed_transaction = parse_transaction(bytes, &bank, &bank, &HashSet::new()).unwrap();
        let checked_transaction = check_parsed_transaction(parsed_transaction, &bank).unwrap();
        let CheckedTransaction {
            state,
            is_validated_nonce,
        } = checked_transaction;

        assert_runtime_view(state.transaction());
        assert_eq!(state.transaction().data().as_ptr(), bytes_ptr);
        assert_eq!(state.transaction().get_durable_nonce(), None);
        assert!(!is_validated_nonce);
        assert_eq!(state.nonce_address(), None);
    }
}
