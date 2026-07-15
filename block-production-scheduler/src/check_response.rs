use {
    crate::{
        resolved_transaction::ResolvedTransaction,
        state_container::{CheckedTransaction, StateContainer},
        transaction::{CheckBatch, CheckTransactionMeta, TpuTransactionMeta},
    },
    agave_external_transaction_view::sanitize::SanitizeConfig,
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, SharablePubkeys, processed_codes,
        worker_message_types::{
            CHECK_RESPONSE, CheckResponse, fee_payer_balance_flags, parsing_and_sanitization_flags,
            resolve_flags, status_check_flags,
        },
    },
    agave_scheduling_utils::responses_region::CheckResponsesPtr,
    rts_alloc::Allocator,
    solana_pubkey::Pubkey,
    std::collections::HashSet,
};

#[derive(Default)]
pub(crate) struct CheckResponseStats {
    pub(crate) checked_transactions: u64,
    pub(crate) accepted_transactions: u64,
}

/// Drain check-worker responses, dropping rejected transactions and queuing accepted ones.
pub(crate) fn drain_check_responses(
    check_worker_to_scheduler: &shaq::mpmc::Consumer<CheckWorkerToPackMessage>,
    allocator: &Allocator,
    sanitize_config: &SanitizeConfig,
    reserved_account_keys: &HashSet<Pubkey>,
    state: &mut StateContainer,
    max_batches: usize,
) -> CheckResponseStats {
    let mut stats = CheckResponseStats::default();
    for _ in 0..max_batches {
        let Some(response) = check_worker_to_scheduler.try_read() else {
            break;
        };
        let response_stats = handle_check_response(
            response,
            allocator,
            sanitize_config,
            reserved_account_keys,
            state,
        );
        stats.checked_transactions = stats
            .checked_transactions
            .wrapping_add(response_stats.checked_transactions);
        stats.accepted_transactions = stats
            .accepted_transactions
            .wrapping_add(response_stats.accepted_transactions);
    }
    stats
}

fn handle_check_response(
    response: CheckWorkerToPackMessage,
    allocator: &Allocator,
    sanitize_config: &SanitizeConfig,
    reserved_account_keys: &HashSet<Pubkey>,
    state: &mut StateContainer,
) -> CheckResponseStats {
    // SAFETY: check-worker responses return batch allocations created by TPU ingress with the
    // same metadata layout.
    let batch =
        unsafe { CheckBatch::from_sharable_transaction_batch_region(&response.batch, allocator) };

    let response_is_valid = response.processed_code == processed_codes::PROCESSED
        && response.responses.tag == CHECK_RESPONSE
        && response.responses.num_transaction_responses == response.batch.num_transactions;
    if !response_is_valid {
        let checked_transactions = discard_unprocessed_batch(batch, allocator, state);
        if response.processed_code == processed_codes::PROCESSED {
            free_check_response_region_if_present(&response, allocator);
        }
        return CheckResponseStats {
            checked_transactions,
            accepted_transactions: 0,
        };
    }

    // SAFETY: `CHECK_RESPONSE` and matching count were checked above. The response allocation is
    // owned by the external scheduler after receiving this message.
    let responses = unsafe {
        CheckResponsesPtr::from_transaction_response_region(&response.responses, allocator)
    };

    let mut stats = CheckResponseStats::default();
    for ((transaction, meta), check_response) in batch.iter().zip(responses.iter()) {
        match meta {
            CheckTransactionMeta::Tpu(meta) => {
                stats.checked_transactions = stats.checked_transactions.wrapping_add(1);
                if handle_tpu_check_response(
                    transaction,
                    meta,
                    check_response,
                    allocator,
                    sanitize_config,
                    reserved_account_keys,
                    state,
                ) {
                    stats.accepted_transactions = stats.accepted_transactions.wrapping_add(1);
                }
            }
            CheckTransactionMeta::Recheck { transaction_id } => {
                handle_recheck_response(transaction_id, check_response, allocator, state);
            }
        }
    }

    // SAFETY: both the batch container and response allocation are returned to and owned by the
    // external scheduler after check-worker completion. Valid transactions and their nested
    // resolved-pubkey allocations are retained in `state`.
    unsafe {
        batch.free();
        responses.free(allocator);
    }
    stats
}

fn handle_tpu_check_response(
    transaction: agave_scheduling_utils::transaction_ptr::TransactionPtr,
    meta: TpuTransactionMeta,
    check_response: &CheckResponse,
    allocator: &Allocator,
    sanitize_config: &SanitizeConfig,
    reserved_account_keys: &HashSet<Pubkey>,
    state: &mut StateContainer,
) -> bool {
    if initial_response_is_valid(check_response) {
        // SAFETY: successful checks transfer ownership of the transaction and resolved pubkey
        // allocations to this scheduler.
        match unsafe {
            ResolvedTransaction::try_new(
                transaction,
                check_response.resolved_pubkeys,
                allocator,
                sanitize_config,
                reserved_account_keys,
            )
        } {
            Ok(transaction) => {
                if let Some(dropped) = state.push(CheckedTransaction::new(transaction, meta)) {
                    free_checked_transaction(dropped, allocator);
                }
                true
            }
            Err(transaction) => {
                // SAFETY: this scheduler retains ownership after the external-view parse fails,
                // and must release both shared allocations.
                unsafe { transaction.free(allocator) };
                false
            }
        }
    } else {
        // SAFETY: rejected transactions are still owned by this scheduler.
        unsafe { transaction.free(allocator) };
        free_resolved_pubkeys(check_response.resolved_pubkeys, allocator);
        false
    }
}

fn handle_recheck_response(
    transaction_id: usize,
    response: &CheckResponse,
    allocator: &Allocator,
    state: &mut StateContainer,
) {
    if let Some(transaction) =
        state.complete_recheck(transaction_id, recheck_response_is_valid(response))
    {
        free_checked_transaction(transaction, allocator);
    }
}

fn initial_response_is_valid(response: &CheckResponse) -> bool {
    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED == 0
        && response.fee_payer_balance_flags & fee_payer_balance_flags::PERFORMED != 0
        && response.resolve_flags & resolve_flags::PERFORMED != 0
        && response.resolve_flags & resolve_flags::FAILED == 0
}

fn recheck_response_is_valid(response: &CheckResponse) -> bool {
    response_is_status_valid(response)
        && response.fee_payer_balance_flags & fee_payer_balance_flags::PERFORMED != 0
}

fn response_is_status_valid(response: &CheckResponse) -> bool {
    const STATUS_FAILURE_FLAGS: u8 = status_check_flags::TOO_OLD
        | status_check_flags::ALREADY_PROCESSED
        | status_check_flags::INVALID_NONCE
        | status_check_flags::UNSUPPORTED_VERSION;

    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED == 0
        && response.status_check_flags & status_check_flags::PERFORMED != 0
        && response.status_check_flags & STATUS_FAILURE_FLAGS == 0
}

fn discard_unprocessed_batch(
    batch: CheckBatch,
    allocator: &Allocator,
    state: &mut StateContainer,
) -> u64 {
    let mut checked_transactions = 0u64;
    for (transaction, meta) in batch.iter() {
        match meta {
            CheckTransactionMeta::Tpu(_) => {
                checked_transactions = checked_transactions.wrapping_add(1);
                // SAFETY: this scheduler owns transactions that were not successfully checked.
                unsafe { transaction.free(allocator) };
            }
            CheckTransactionMeta::Recheck { transaction_id } => {
                if let Some(transaction) = state.complete_recheck(transaction_id, true) {
                    free_checked_transaction(transaction, allocator);
                }
            }
        }
    }
    // SAFETY: this scheduler owns the returned batch container.
    unsafe { batch.free() };
    checked_transactions
}

fn free_check_response_region_if_present(
    response: &CheckWorkerToPackMessage,
    allocator: &Allocator,
) {
    if response.responses.tag == CHECK_RESPONSE && response.responses.num_transaction_responses > 0
    {
        // SAFETY: a non-empty check response is allocated by the check worker and returned to
        // the external scheduler for disposal.
        unsafe { allocator.free_offset(response.responses.transaction_responses_offset) };
    }
}

fn free_resolved_pubkeys(pubkeys: SharablePubkeys, allocator: &Allocator) {
    if pubkeys.num_pubkeys > 0 {
        // SAFETY: resolved pubkeys are allocated by the check worker and returned to the
        // external scheduler with the response.
        unsafe { allocator.free_offset(pubkeys.offset) };
    }
}

fn free_checked_transaction(transaction: CheckedTransaction, allocator: &Allocator) {
    // SAFETY: state-container eviction transfers ownership of this checked transaction back to
    // the response handler.
    unsafe { transaction.transaction.free(allocator) };
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            SchedulerConfig, resolved_transaction::sanitize_config, transaction::TpuTransactionMeta,
        },
        agave_scheduler_bindings::{
            SharableTransactionBatchRegion, SharableTransactionRegion, TransactionResponseRegion,
            worker_message_types,
        },
        agave_scheduling_utils::{
            handshake::{client, server::Server},
            responses_region::resolve_responses_from_iter,
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
    };

    fn make_batch(allocator: &Allocator, priority: u64) -> SharableTransactionBatchRegion {
        let payer = Keypair::new();
        let message = Message::new(
            &[system_instruction::transfer(
                &payer.pubkey(),
                &Pubkey::new_from_array([1; 32]),
                1,
            )],
            Some(&payer.pubkey()),
        );
        let bytes = wincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&payer],
            message,
            Hash::default(),
        )))
        .unwrap();
        let transaction = allocator.allocate(bytes.len() as u32).unwrap();
        // SAFETY: both pointers are valid for `bytes.len()` bytes and do not overlap.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), transaction.as_ptr(), bytes.len());
        }
        // SAFETY: `transaction` was allocated by this allocator immediately above.
        let transaction_offset = unsafe { allocator.offset(transaction) };
        let transaction = SharableTransactionRegion {
            offset: transaction_offset,
            length: bytes.len() as u32,
        };

        let batch = allocator
            .allocate(CheckBatch::TRANSACTION_META_END as u32)
            .unwrap();
        // SAFETY: `batch` was allocated by this allocator immediately above.
        let batch_offset = unsafe { allocator.offset(batch) };
        // SAFETY: `batch_offset` was obtained from this allocator immediately above.
        let transaction_regions = unsafe {
            allocator
                .ptr_from_offset(batch_offset)
                .cast::<SharableTransactionRegion>()
        };
        // SAFETY: `batch_offset` was obtained from this allocator immediately above and the
        // metadata offset is within the batch allocation.
        let transaction_metas = unsafe {
            allocator
                .ptr_from_offset(batch_offset)
                .byte_add(CheckBatch::TRANSACTION_META_START)
                .cast::<CheckTransactionMeta>()
        };
        // SAFETY: both arrays have space for at least one element.
        unsafe {
            transaction_regions.write(transaction);
            transaction_metas.write(CheckTransactionMeta::Tpu(TpuTransactionMeta {
                priority,
                cost: 0,
                flags: 0,
                src_addr: [0; 16],
            }));
        }

        SharableTransactionBatchRegion {
            num_transactions: 1,
            transactions_offset: batch_offset,
        }
    }

    fn valid_response() -> CheckResponse {
        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: 0,
            fee_payer_balance_flags: fee_payer_balance_flags::REQUESTED
                | fee_payer_balance_flags::PERFORMED,
            resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            included_slot: 0,
            balance_slot: 0,
            fee_payer_balance: 0,
            resolution_slot: 0,
            min_alt_deactivation_slot: u64::MAX,
            resolved_pubkeys: SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            },
        }
    }

    #[test]
    fn initial_responses_do_not_require_status_checks() {
        let response = valid_response();

        assert!(initial_response_is_valid(&response));
        assert!(!recheck_response_is_valid(&response));
    }

    #[test]
    fn queues_valid_transactions_and_drops_invalid_transactions() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.check_worker_count = 1;
        let logon = config.client_logon();
        let (agave_session, files) = Server::setup_session(logon).unwrap();
        let client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];

        let queue_response = |batch, check_response: CheckResponse| {
            let responses =
                resolve_responses_from_iter(allocator, std::iter::once(check_response)).unwrap();
            agave_session.check_workers[0]
                .check_worker_to_pack
                .try_write(CheckWorkerToPackMessage {
                    batch,
                    processed_code: processed_codes::PROCESSED,
                    responses,
                })
                .unwrap();
        };

        queue_response(make_batch(allocator, 1), valid_response());
        queue_response(make_batch(allocator, 2), valid_response());
        let mut invalid_response = valid_response();
        invalid_response.parsing_and_sanitization_flags = parsing_and_sanitization_flags::FAILED;
        queue_response(make_batch(allocator, 3), invalid_response);

        let mut state = StateContainer::new(1);
        let sanitize_config = sanitize_config(false);
        let reserved_account_keys = HashSet::new();
        let stats = drain_check_responses(
            &client_session.check_worker_to_pack,
            allocator,
            &sanitize_config,
            &reserved_account_keys,
            &mut state,
            2,
        );
        assert_eq!(stats.checked_transactions, 2);
        assert_eq!(stats.accepted_transactions, 2);

        assert_eq!(state.len(), 1);
        let stats = drain_check_responses(
            &client_session.check_worker_to_pack,
            allocator,
            &sanitize_config,
            &reserved_account_keys,
            &mut state,
            1,
        );
        assert_eq!(stats.checked_transactions, 1);
        assert_eq!(stats.accepted_transactions, 0);
        let first = state.pop().unwrap();
        assert_eq!(first.meta.priority, 2);
        assert!(state.pop().is_none());

        free_checked_transaction(first, allocator);
    }

    #[test]
    fn drops_unprocessed_check_responses() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.check_worker_count = 1;
        let logon = config.client_logon();
        let (agave_session, files) = Server::setup_session(logon).unwrap();
        let client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        agave_session.check_workers[0]
            .check_worker_to_pack
            .try_write(CheckWorkerToPackMessage {
                batch: make_batch(allocator, 1),
                processed_code: processed_codes::INVALID,
                responses: TransactionResponseRegion {
                    tag: worker_message_types::CHECK_RESPONSE,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .unwrap();

        let mut state = StateContainer::new(1);
        let sanitize_config = sanitize_config(false);
        let reserved_account_keys = HashSet::new();
        let stats = drain_check_responses(
            &client_session.check_worker_to_pack,
            allocator,
            &sanitize_config,
            &reserved_account_keys,
            &mut state,
            usize::MAX,
        );

        assert_eq!(stats.checked_transactions, 1);
        assert_eq!(stats.accepted_transactions, 0);
        assert!(state.pop().is_none());
    }
}
