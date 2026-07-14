use {
    crate::{
        in_flight::InFlightTracker,
        state_container::{CheckedTransaction, StateContainer},
        transaction::ExecutionBatch,
    },
    agave_scheduler_bindings::{
        ExecutionWorkerToPackMessage, processed_codes,
        worker_message_types::{EXECUTION_RESPONSE, ExecutionResponse, not_included_reasons},
    },
    agave_scheduling_utils::{
        handshake::ClientWorkerSession, responses_region::ExecutionResponsesPtr,
        thread_aware_account_locks::ThreadAwareAccountLocks,
    },
    rts_alloc::Allocator,
};

/// Drain up to `max_batches` execution responses, round-robin across workers.
pub(crate) fn drain_execution_responses(
    workers: &mut [ClientWorkerSession],
    allocator: &Allocator,
    state: &mut StateContainer,
    account_locks: &mut ThreadAwareAccountLocks,
    in_flight: &mut InFlightTracker,
    max_batches: usize,
) {
    for worker in workers.iter_mut() {
        worker.worker_to_pack.sync();
    }

    let mut remaining_batches = max_batches;
    while remaining_batches > 0 {
        let mut handled_response = false;
        for (worker_id, worker) in workers.iter_mut().enumerate() {
            if remaining_batches == 0 {
                break;
            }
            let Some(response) = worker.worker_to_pack.try_read() else {
                continue;
            };
            handle_execution_response(
                worker_id,
                *response,
                allocator,
                state,
                account_locks,
                in_flight,
            );
            remaining_batches = remaining_batches.saturating_sub(1);
            handled_response = true;
        }
        if !handled_response {
            break;
        }
    }

    for worker in workers.iter_mut() {
        worker.worker_to_pack.finalize();
    }
}

fn handle_execution_response(
    worker_id: usize,
    response: ExecutionWorkerToPackMessage,
    allocator: &Allocator,
    state: &mut StateContainer,
    account_locks: &mut ThreadAwareAccountLocks,
    in_flight: &mut InFlightTracker,
) {
    // SAFETY: execution-worker responses return batch allocations created by the scheduler with
    // the same metadata layout.
    let batch = unsafe {
        ExecutionBatch::from_sharable_transaction_batch_region(&response.batch, allocator)
    };
    let num_transactions = batch.len();
    let response_is_valid = response.processed_code == processed_codes::PROCESSED
        && response.responses.tag == EXECUTION_RESPONSE
        && response.responses.num_transaction_responses == response.batch.num_transactions;

    let mut total_scheduled_cost = 0_u64;
    if response_is_valid {
        // SAFETY: the response tag and count were checked above, and execution workers transfer
        // ownership of this response allocation to the scheduler.
        let responses = unsafe {
            ExecutionResponsesPtr::from_transaction_response_region(&response.responses, allocator)
        };
        for ((_, meta), execution_response) in batch.iter().zip(responses.iter()) {
            total_scheduled_cost = total_scheduled_cost.saturating_add(complete_transaction(
                worker_id,
                meta.transaction_id,
                retryability(execution_response),
                allocator,
                state,
                account_locks,
            ));
        }
        // SAFETY: this scheduler exclusively owns the returned response allocation.
        unsafe { responses.free(allocator) };
    } else {
        let retryability =
            (response.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED).then_some(true);
        for (_, meta) in batch.iter() {
            total_scheduled_cost = total_scheduled_cost.saturating_add(complete_transaction(
                worker_id,
                meta.transaction_id,
                retryability,
                allocator,
                state,
                account_locks,
            ));
        }
        if response.processed_code == processed_codes::PROCESSED {
            free_execution_response_region_if_present(&response, allocator);
        }
    }

    in_flight.complete_batch(worker_id, num_transactions, total_scheduled_cost);
    // SAFETY: this scheduler owns the returned batch container. Transaction allocations remain
    // owned by `state` until they are terminally dropped.
    unsafe { batch.free() };
}

fn complete_transaction(
    worker_id: usize,
    transaction_id: usize,
    retryability: Option<bool>,
    allocator: &Allocator,
    state: &mut StateContainer,
    account_locks: &mut ThreadAwareAccountLocks,
) -> u64 {
    let scheduled_cost = {
        let transaction = state.get(transaction_id);
        let locks = transaction.transaction.account_locks();
        account_locks.unlock_accounts(locks.write_locks(), locks.read_locks(), worker_id);
        transaction.meta.cost
    };

    match retryability {
        Some(immediately_retryable) => {
            state.retry(transaction_id, immediately_retryable, |transaction| {
                free_checked_transaction(transaction, allocator)
            })
        }
        None => free_checked_transaction(state.remove(transaction_id), allocator),
    }
    scheduled_cost
}

fn retryability(response: &ExecutionResponse) -> Option<bool> {
    match response.not_included_reason {
        not_included_reasons::BANK_NOT_AVAILABLE | not_included_reasons::ACCOUNT_IN_USE => {
            Some(true)
        }
        not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
        | not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
        | not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
        | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT => Some(false),
        _ => None,
    }
}

fn free_execution_response_region_if_present(
    response: &ExecutionWorkerToPackMessage,
    allocator: &Allocator,
) {
    if response.responses.tag == EXECUTION_RESPONSE
        && response.responses.num_transaction_responses > 0
    {
        // SAFETY: a non-empty execution response is allocated by the execution worker and
        // returned to this scheduler for disposal.
        unsafe { allocator.free_offset(response.responses.transaction_responses_offset) };
    }
}

fn free_checked_transaction(transaction: CheckedTransaction, allocator: &Allocator) {
    // SAFETY: terminal removal or capacity eviction transfers ownership of this checked
    // transaction's shared allocations to the response handler.
    unsafe { transaction.transaction.free(allocator) };
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            SchedulerConfig,
            resolved_transaction::{ResolvedTransaction, sanitize_config},
            transaction::{ExecutionTransactionMeta, TpuTransactionMeta},
        },
        agave_scheduler_bindings::{SharablePubkeys, worker_message_types::not_included_reasons},
        agave_scheduling_utils::{
            handshake::{client, server::Server},
            responses_region::execution_responses_from_iter,
            thread_aware_account_locks::ThreadSet,
            transaction_ptr::TransactionPtr,
        },
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
        std::collections::HashSet,
    };

    fn checked_transaction(allocator: &Allocator, cost: u64) -> CheckedTransaction {
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
        CheckedTransaction {
            transaction,
            meta: TpuTransactionMeta {
                priority: 1,
                cost,
                flags: 0,
                src_addr: [0; 16],
            },
        }
    }

    fn dispatch_transaction(
        allocator: &Allocator,
        state: &mut StateContainer,
        account_locks: &mut ThreadAwareAccountLocks,
        in_flight: &mut InFlightTracker,
    ) -> (
        agave_scheduler_bindings::SharableTransactionBatchRegion,
        Vec<Pubkey>,
        Vec<Pubkey>,
    ) {
        let cost = 10;
        assert!(state.push(checked_transaction(allocator, cost)).is_none());
        let transaction_id = state.descending_from(None).next().unwrap().id;
        state.dequeue(transaction_id);

        let (write_locks, read_locks, transaction_region) = {
            let transaction = state.get(transaction_id);
            let locks = transaction.transaction.account_locks();
            let write_locks = locks.write_locks().copied().collect::<Vec<_>>();
            let read_locks = locks.read_locks().copied().collect::<Vec<_>>();
            // SAFETY: the transaction remains in `state` until its execution response arrives.
            let transaction_region = unsafe {
                transaction
                    .transaction
                    .to_sharable_transaction_region(allocator)
            };
            (write_locks, read_locks, transaction_region)
        };
        assert_eq!(
            account_locks.try_lock_accounts(
                write_locks.iter(),
                read_locks.iter(),
                ThreadSet::any(1),
                |_| 0,
            ),
            Ok(0)
        );

        let mut batch = ExecutionBatch::allocate(allocator).unwrap();
        batch
            .push(
                transaction_region,
                ExecutionTransactionMeta { transaction_id },
            )
            .unwrap();
        in_flight.track_batch(0, 1, cost);
        (
            batch.to_sharable_transaction_batch_region(),
            write_locks,
            read_locks,
        )
    }

    fn execution_response(not_included_reason: u8) -> ExecutionResponse {
        ExecutionResponse {
            execution_slot: 1,
            not_included_reason,
            cost_units: 0,
            fee_payer_balance: 0,
        }
    }

    #[test]
    fn releases_terminal_transactions_and_account_locks() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let mut state = StateContainer::new(1);
        let mut account_locks = ThreadAwareAccountLocks::new(1);
        let mut in_flight = InFlightTracker::new(1, 1);
        let (batch, write_locks, read_locks) =
            dispatch_transaction(allocator, &mut state, &mut account_locks, &mut in_flight);
        let responses = execution_responses_from_iter(
            allocator,
            std::iter::once(execution_response(not_included_reasons::NONE)),
        )
        .unwrap();
        agave_session.workers[0]
            .worker_to_pack
            .try_write(ExecutionWorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
        agave_session.workers[0].worker_to_pack.commit();

        drain_execution_responses(
            &mut client_session.workers,
            allocator,
            &mut state,
            &mut account_locks,
            &mut in_flight,
            1,
        );

        assert!(state.is_empty());
        assert_eq!(state.buffer_len(), 0);
        assert!(in_flight.is_empty());
        assert_eq!(
            account_locks.try_lock_accounts(
                write_locks.iter(),
                read_locks.iter(),
                ThreadSet::any(1),
                |_| 0,
            ),
            Ok(0)
        );
        account_locks.unlock_accounts(write_locks.iter(), read_locks.iter(), 0);
    }

    #[test]
    fn immediately_requeues_retryable_transactions() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let mut state = StateContainer::new(1);
        let mut account_locks = ThreadAwareAccountLocks::new(1);
        let mut in_flight = InFlightTracker::new(1, 1);
        let (batch, _, _) =
            dispatch_transaction(allocator, &mut state, &mut account_locks, &mut in_flight);
        let responses = execution_responses_from_iter(
            allocator,
            std::iter::once(execution_response(not_included_reasons::BANK_NOT_AVAILABLE)),
        )
        .unwrap();
        agave_session.workers[0]
            .worker_to_pack
            .try_write(ExecutionWorkerToPackMessage {
                batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
        agave_session.workers[0].worker_to_pack.commit();

        drain_execution_responses(
            &mut client_session.workers,
            allocator,
            &mut state,
            &mut account_locks,
            &mut in_flight,
            1,
        );

        assert_eq!(state.len(), 1);
        assert_eq!(state.buffer_len(), 1);
        assert!(in_flight.is_empty());
        free_checked_transaction(state.pop().unwrap(), allocator);
    }
}
