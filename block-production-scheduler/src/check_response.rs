use {
    crate::{
        Scheduler,
        resolved_transaction::{ResolvedTransaction, free_resolved_pubkeys},
        tpu_ingress::{MAX_PACKETS_PER_CHECK_BATCH, RawCheckBatch, TpuTransactionMeta},
    },
    agave_scheduler_bindings::{
        processed_codes, tpu_message_flags,
        worker_message_types::{
            CheckResponse, fee_payer_balance_flags, parsing_and_sanitization_flags, resolve_flags,
            scheduling_details_flags, status_check_flags,
        },
    },
    agave_scheduling_utils::responses_region::CheckResponsesPtr,
};

const MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION: usize = 512 / MAX_PACKETS_PER_CHECK_BATCH;
const BURN_PERCENT: u64 = 50;
const PRIORITY_MULTIPLIER: u64 = 1_000_000;

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "metadata retained for scheduling")
)]
pub(super) struct CheckedTransaction {
    pub(super) transaction: ResolvedTransaction,
    pub(super) metadata: TpuTransactionMeta,
    pub(super) cost: u64,
    pub(super) allocated_accounts_data_size: u64,
}

impl Scheduler {
    pub(super) fn handle_check_worker_responses(&mut self) {
        for _ in 0..MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION {
            let Some(message) = self.check_receiver.try_read() else {
                break;
            };
            // SAFETY: workers return the batch sent by ingress and have finished accessing it.
            let batch = unsafe {
                RawCheckBatch::from_sharable_transaction_batch_region(
                    &message.batch,
                    &self.allocator,
                )
            };
            self.outstanding_check_packets =
                self.outstanding_check_packets.saturating_sub(batch.len());
            if message.processed_code != processed_codes::PROCESSED
                || message.responses.num_transaction_responses == 0
            {
                // SAFETY: the worker has released this batch. An unprocessed response has no
                // defined response region, so only the original allocations can be freed.
                unsafe {
                    batch.free_transactions();
                    batch.free();
                }
                continue;
            }

            // SAFETY: processed responses transfer an initialized response allocation to us.
            let responses = unsafe {
                CheckResponsesPtr::from_transaction_response_region(
                    &message.responses,
                    &self.allocator,
                )
            };
            if responses.len() != batch.len() {
                for response in responses.iter() {
                    if response.resolve_flags & resolve_flags::PERFORMED != 0 {
                        // SAFETY: only performed resolution defines a pubkey region we own.
                        unsafe {
                            free_resolved_pubkeys(response.resolved_pubkeys, &self.allocator)
                        };
                    }
                }
                // SAFETY: the worker has returned all transactions in this batch.
                unsafe { batch.free_transactions() };
            } else {
                for ((transaction, metadata), response) in batch.iter().zip(responses.iter()) {
                    if !response_is_valid(response) {
                        // SAFETY: rejected transactions and any resolved addresses are ours to free.
                        unsafe {
                            transaction.free(&self.allocator);
                            if response.resolve_flags & resolve_flags::PERFORMED != 0 {
                                free_resolved_pubkeys(response.resolved_pubkeys, &self.allocator);
                            }
                        }
                        continue;
                    }
                    // SAFETY: checks succeeded and transferred both allocations back to us.
                    let transaction = match unsafe {
                        ResolvedTransaction::try_new(
                            transaction,
                            response.resolved_pubkeys,
                            &self.allocator,
                            &self.reserved_account_keys.active,
                        )
                    } {
                        Ok(transaction) => transaction,
                        Err(transaction) => {
                            // SAFETY: constructing the view failed, leaving both allocations with us.
                            unsafe {
                                transaction.free(&self.allocator);
                                free_resolved_pubkeys(response.resolved_pubkeys, &self.allocator);
                            }
                            continue;
                        }
                    };
                    let priority = calculate_priority(response, metadata.flags);
                    let transaction = CheckedTransaction {
                        transaction,
                        metadata,
                        cost: response.estimated_cost_units,
                        allocated_accounts_data_size: response.allocated_accounts_data_size,
                    };
                    let dropped = match self.transactions.insert(priority, transaction) {
                        Ok((_, evicted)) => evicted,
                        Err(rejected) => Some(rejected),
                    };
                    if let Some(transaction) = dropped {
                        // SAFETY: rejection or eviction returns exclusive ownership.
                        unsafe { transaction.transaction.free(&self.allocator) };
                    }
                }
            }
            // SAFETY: every transaction and nested allocation has been retained or freed.
            unsafe {
                batch.free();
                responses.free(&self.allocator);
            }
        }
    }
}

fn response_is_valid(response: &CheckResponse) -> bool {
    const STATUS_FAILURE_FLAGS: u8 = status_check_flags::TOO_OLD
        | status_check_flags::ALREADY_PROCESSED
        | status_check_flags::INVALID_NONCE
        | status_check_flags::UNSUPPORTED_VERSION;

    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED == 0
        && response.status_check_flags & status_check_flags::PERFORMED != 0
        && response.status_check_flags & STATUS_FAILURE_FLAGS == 0
        && response.fee_payer_balance_flags & fee_payer_balance_flags::PERFORMED != 0
        && response.resolve_flags & resolve_flags::PERFORMED != 0
        && response.resolve_flags & resolve_flags::FAILED == 0
        && response.scheduling_details_flags & scheduling_details_flags::PERFORMED != 0
        && response.scheduling_details_flags & scheduling_details_flags::FAILED == 0
}

fn calculate_priority(response: &CheckResponse, tpu_flags: u8) -> u64 {
    if tpu_flags & tpu_message_flags::IS_SIMPLE_VOTE != 0 {
        return u64::MAX;
    }
    let reward = response.prioritization_fee.saturating_add(
        response.transaction_fee.saturating_sub(
            response
                .transaction_fee
                .saturating_mul(BURN_PERCENT)
                .wrapping_div(100),
        ),
    );
    #[expect(clippy::arithmetic_side_effects, reason = "cost plus one is nonzero")]
    reward
        .saturating_mul(PRIORITY_MULTIPLIER)
        .wrapping_div(response.estimated_cost_units.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{tests::setup, transaction_container::TransactionContainer},
        agave_scheduler_bindings::{
            CheckResponseRegion, CheckWorkerToPackMessage, PackToCheckWorkerMessage,
            SharablePubkeys, SharableTransactionRegion, TpuToPackMessage,
        },
        agave_scheduler_handshake::AgaveSession,
        agave_scheduling_utils::responses_region::resolve_responses_from_iter,
        rts_alloc::Allocator,
        solana_hash::Hash,
        solana_message::{Message, VersionedMessage, v0},
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_svm_transaction::svm_message::SVMMessage,
        solana_transaction::versioned::VersionedTransaction,
    };

    fn transaction_bytes(with_lookups: bool) -> Vec<u8> {
        let message =
            Message::new_with_blockhash(&[], Some(&Pubkey::from([1; 32])), &Hash::default());
        let message = if with_lookups {
            VersionedMessage::V0(v0::Message {
                header: message.header,
                account_keys: message.account_keys,
                recent_blockhash: message.recent_blockhash,
                instructions: message.instructions,
                address_table_lookups: vec![v0::MessageAddressTableLookup {
                    account_key: Pubkey::from([2; 32]),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
            })
        } else {
            VersionedMessage::Legacy(message)
        };
        wincode::serialize(&VersionedTransaction {
            signatures: vec![Signature::default()],
            message,
        })
        .unwrap()
    }

    fn send_to_checks(
        scheduler: &mut Scheduler,
        agave: &mut AgaveSession,
        bytes: &[u8],
        flags: u8,
    ) -> PackToCheckWorkerMessage {
        let allocator = &agave.tpu_to_pack.allocator;
        let ptr = allocator.allocate(bytes.len() as u32).unwrap();
        // SAFETY: the allocation has space for all bytes and does not overlap the source.
        let transaction = unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len());
            SharableTransactionRegion {
                offset: allocator.offset(ptr),
                length: bytes.len() as u32,
            }
        };
        agave
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction,
                flags,
                src_addr: [7; 16],
            })
            .unwrap();
        scheduler.handle_tpu_ingress();
        agave.check_workers[0]
            .pack_to_check_worker
            .try_read()
            .unwrap()
    }

    fn valid_response(allocator: &Allocator, with_lookups: bool) -> CheckResponse {
        let resolved_pubkeys = if with_lookups {
            let keys = [Pubkey::from([3; 32]), Pubkey::from([4; 32])];
            let ptr = allocator.allocate(size_of_val(&keys) as u32).unwrap();
            // SAFETY: the allocation is aligned and sized for these initialized pubkeys.
            unsafe {
                ptr.cast::<[Pubkey; 2]>().write(keys);
                SharablePubkeys {
                    offset: allocator.offset(ptr),
                    num_pubkeys: 2,
                }
            }
        } else {
            SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            }
        };
        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: status_check_flags::PERFORMED,
            fee_payer_balance_flags: fee_payer_balance_flags::PERFORMED,
            resolve_flags: resolve_flags::PERFORMED,
            scheduling_details_flags: scheduling_details_flags::PERFORMED,
            included_slot: 0,
            transaction_fee: 100,
            prioritization_fee: 50,
            estimated_cost_units: 99,
            allocated_accounts_data_size: 123,
            balance_slot: 100,
            fee_payer_balance: 1_000_000,
            resolution_slot: 100,
            min_alt_deactivation_slot: u64::MAX,
            resolved_pubkeys,
        }
    }

    fn respond(
        agave: &AgaveSession,
        request: PackToCheckWorkerMessage,
        responses: &[CheckResponse],
    ) {
        let worker = &agave.check_workers[0];
        let responses =
            resolve_responses_from_iter(&worker.allocator, responses.iter().copied()).unwrap();
        worker
            .check_worker_to_pack
            .try_write(CheckWorkerToPackMessage {
                batch: request.batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
    }

    fn assert_no_allocations(agave: &AgaveSession) {
        for allocator in [
            &agave.tpu_to_pack.allocator,
            &agave.check_workers[0].allocator,
        ] {
            allocator.clean_remote_frees();
            assert_eq!(allocator.outstanding_allocation_bytes(), 0);
        }
    }

    #[test]
    fn retains_checked_transactions_and_resolved_addresses() {
        let (mut scheduler, mut agave) = setup(2);
        let legacy = send_to_checks(&mut scheduler, &mut agave, &transaction_bytes(false), 0);
        respond(
            &agave,
            legacy,
            &[valid_response(&agave.check_workers[0].allocator, false)],
        );
        let lookup = send_to_checks(
            &mut scheduler,
            &mut agave,
            &transaction_bytes(true),
            tpu_message_flags::IS_SIMPLE_VOTE,
        );
        respond(
            &agave,
            lookup,
            &[valid_response(&agave.check_workers[0].allocator, true)],
        );
        scheduler.run_iteration();

        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(scheduler.transactions.len(), 2);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
        let id = scheduler.transactions.pop_highest().unwrap();
        let checked = scheduler.transactions.get(id).unwrap();
        assert_eq!(checked.metadata.flags, tpu_message_flags::IS_SIMPLE_VOTE);
        assert_eq!(checked.metadata.src_addr, [7; 16]);
        assert_eq!(checked.cost, 99);
        assert_eq!(checked.allocated_accounts_data_size, 123);
        assert_eq!(
            checked.transaction.view.account_keys().get(1),
            Some(&Pubkey::from([3; 32]))
        );
        assert!(checked.transaction.view.is_writable(1));
        assert!(!checked.transaction.view.is_writable(2));
        // Includes a popped transaction: teardown must drain the entire container.
        drop(scheduler);
        assert_no_allocations(&agave);
    }

    #[test]
    fn requires_successful_requested_checks() {
        let (scheduler, agave) = setup(2);
        let response = valid_response(&scheduler.allocator, false);
        assert!(response_is_valid(&response));
        assert!(!response_is_valid(&CheckResponse {
            parsing_and_sanitization_flags: parsing_and_sanitization_flags::FAILED,
            ..response
        }));
        assert!(!response_is_valid(&CheckResponse {
            status_check_flags: 0,
            ..response
        }));
        for flag in [
            status_check_flags::TOO_OLD,
            status_check_flags::ALREADY_PROCESSED,
            status_check_flags::INVALID_NONCE,
            status_check_flags::UNSUPPORTED_VERSION,
        ] {
            assert!(!response_is_valid(&CheckResponse {
                status_check_flags: status_check_flags::PERFORMED | flag,
                ..response
            }));
        }
        assert!(!response_is_valid(&CheckResponse {
            fee_payer_balance_flags: 0,
            ..response
        }));
        assert!(!response_is_valid(&CheckResponse {
            resolve_flags: 0,
            ..response
        }));
        assert!(!response_is_valid(&CheckResponse {
            resolve_flags: resolve_flags::PERFORMED | resolve_flags::FAILED,
            ..response
        }));
        assert!(!response_is_valid(&CheckResponse {
            scheduling_details_flags: 0,
            ..response
        }));
        assert!(!response_is_valid(&CheckResponse {
            scheduling_details_flags: scheduling_details_flags::PERFORMED
                | scheduling_details_flags::FAILED,
            ..response
        }));
        assert_eq!(calculate_priority(&response, 0), 1_000_000);
        assert_eq!(
            calculate_priority(&response, tpu_message_flags::IS_SIMPLE_VOTE),
            u64::MAX
        );
        assert_no_allocations(&agave);
    }

    #[test]
    fn rejected_checks_free_transactions_and_resolved_addresses() {
        let (mut scheduler, mut agave) = setup(2);
        let request = send_to_checks(&mut scheduler, &mut agave, &transaction_bytes(true), 0);
        let response = CheckResponse {
            status_check_flags: status_check_flags::PERFORMED
                | status_check_flags::ALREADY_PROCESSED,
            ..valid_response(&agave.check_workers[0].allocator, true)
        };
        respond(&agave, request, &[response]);
        let request = send_to_checks(&mut scheduler, &mut agave, &[0], 0);
        let response = CheckResponse {
            parsing_and_sanitization_flags: parsing_and_sanitization_flags::FAILED,
            resolve_flags: 0,
            resolved_pubkeys: SharablePubkeys {
                offset: usize::MAX,
                num_pubkeys: u32::MAX,
            },
            ..valid_response(&agave.check_workers[0].allocator, false)
        };
        respond(&agave, request, &[response]);
        scheduler.handle_check_worker_responses();
        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(scheduler.transactions.len(), 0);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
        assert_no_allocations(&agave);
    }

    #[test]
    fn invalid_batches_are_discarded() {
        let (mut scheduler, mut agave) = setup(2);
        let request = send_to_checks(&mut scheduler, &mut agave, &[0], 0);
        agave.check_workers[0]
            .check_worker_to_pack
            .try_write(CheckWorkerToPackMessage {
                batch: request.batch,
                processed_code: processed_codes::INVALID,
                responses: CheckResponseRegion {
                    num_transaction_responses: u8::MAX,
                    transaction_responses_offset: usize::MAX,
                },
            })
            .unwrap();
        let request = send_to_checks(&mut scheduler, &mut agave, &[0], 0);
        respond(
            &agave,
            request,
            &[
                valid_response(&agave.check_workers[0].allocator, true),
                valid_response(&agave.check_workers[0].allocator, true),
            ],
        );
        scheduler.handle_check_worker_responses();
        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(scheduler.transactions.len(), 0);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
        assert_no_allocations(&agave);
    }

    #[test]
    fn frees_evicted_and_rejected_transactions_without_evicting_for_invalid_views() {
        let (mut scheduler, mut agave) = setup(2);
        scheduler.transactions = TransactionContainer::with_capacity(1);
        let bytes = transaction_bytes(true);
        for fee in [100, 200, 200] {
            let request = send_to_checks(&mut scheduler, &mut agave, &bytes, 0);
            let response = CheckResponse {
                prioritization_fee: fee,
                allocated_accounts_data_size: fee,
                ..valid_response(&agave.check_workers[0].allocator, true)
            };
            respond(&agave, request, &[response]);
            scheduler.handle_check_worker_responses();
        }
        let id = scheduler.transactions.pop_highest().unwrap();
        assert_eq!(
            scheduler
                .transactions
                .get(id)
                .unwrap()
                .allocated_accounts_data_size,
            200
        );
        let retained = scheduler
            .transactions
            .get(id)
            .unwrap()
            .transaction
            .view
            .data()
            .as_ptr();
        scheduler.transactions.requeue(id);
        for bytes in [vec![0], transaction_bytes(false)] {
            let request = send_to_checks(&mut scheduler, &mut agave, &bytes, 0);
            let response = CheckResponse {
                prioritization_fee: 300,
                ..valid_response(&agave.check_workers[0].allocator, true)
            };
            respond(&agave, request, &[response]);
            scheduler.handle_check_worker_responses();
        }
        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(scheduler.transactions.len(), 1);
        assert_eq!(
            scheduler
                .transactions
                .get(id)
                .unwrap()
                .transaction
                .view
                .data()
                .as_ptr(),
            retained
        );
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
        drop(scheduler);
        assert_no_allocations(&agave);
    }

    #[test]
    fn limits_response_batches_per_iteration() {
        const TOTAL: usize = MAX_CHECK_RESPONSE_BATCHES_PER_ITERATION + 1;
        let (mut scheduler, mut agave) = setup(TOTAL.next_power_of_two());
        for _ in 0..TOTAL {
            let request = send_to_checks(&mut scheduler, &mut agave, &[0], 0);
            let response = CheckResponse {
                parsing_and_sanitization_flags: parsing_and_sanitization_flags::FAILED,
                ..valid_response(&agave.check_workers[0].allocator, false)
            };
            respond(&agave, request, &[response]);
        }
        scheduler.handle_check_worker_responses();
        assert_eq!(scheduler.outstanding_check_packets, 1);
        scheduler.handle_check_worker_responses();
        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
        assert_no_allocations(&agave);
    }
}
