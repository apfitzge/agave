use {
    crate::{
        progress_tracker::SchedulerState,
        transaction::{CheckBatch, MAX_PACKETS_PER_CHECK_BATCH, TpuTransactionMeta},
    },
    agave_scheduler_bindings::{
        PackToCheckWorkerMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
        TpuToPackMessage, check_message_flags, tpu_message_flags,
    },
    agave_scheduling_utils::transaction_ptr::TransactionPtr,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    rts_alloc::Allocator,
    solana_cost_model::cost_model::CostModel,
    solana_fee::FeeFeatures,
    solana_hash::Hash,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, sanitize_config::sanitize_config,
        transaction_meta::TransactionMeta,
    },
    solana_transaction::sanitized::MessageHash,
    std::{ptr::NonNull, time::Duration},
};

const TPU_RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);
const LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const BURN_PERCENT: u64 = 50;
const PRIORITY_MULTIPLIER: u64 = 1_000_000;
const CHECK_FLAGS: u16 = check_message_flags::STATUS_CHECKS
    | check_message_flags::LOAD_FEE_PAYER_BALANCE
    | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES;

pub(crate) fn drain_tpu(
    tpu_to_scheduler: &mut shaq::spsc::Consumer<TpuToPackMessage>,
    scheduler_to_check_worker: &shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    allocator: &Allocator,
    state: &SchedulerState,
    max_packets: usize,
    mut filter: impl FnMut(&SanitizedTransactionView<TransactionPtr>, &TpuTransactionMeta) -> bool,
) {
    // sleep only while not in the near-leader holding window where packets
    // should be buffered eagerly. The futex wait first checks the queue, so
    // it returns immediately if a producer has already published packets.
    if !state.should_accept_packets() {
        let _ = tpu_to_scheduler.wait_readable_timeout(TPU_RECEIVE_TIMEOUT);
    } else {
        tpu_to_scheduler.sync();
    }

    if !state.should_accept_packets() {
        drop_tpu_packets(tpu_to_scheduler, allocator, max_packets);
        return;
    }

    let sanitize_config =
        sanitize_config(state.feature_set().snapshot().limit_instruction_accounts);

    let mut remaining_packets = tpu_to_scheduler.len().min(max_packets);
    while remaining_packets > 0 {
        let num_packets = remaining_packets.min(MAX_PACKETS_PER_CHECK_BATCH);

        let Some(mut batch) = allocate_check_worker_batch(allocator) else {
            break;
        };

        let mut num_transactions = 0;
        for _ in 0..num_packets {
            let Some(message) = tpu_to_scheduler.try_read() else {
                unreachable!("queue length checked before read");
            };

            // SAFETY: the TPU queue hands ownership of this transaction allocation to the
            // scheduler. This temporary view does not free or modify that allocation.
            let transaction = unsafe {
                TransactionPtr::from_sharable_transaction_region(&message.transaction, allocator)
            };
            let meta =
                match SanitizedTransactionView::try_new_sanitized(transaction, &sanitize_config) {
                    Ok(transaction) => {
                        let (priority, cost) =
                            calculate_priority_and_cost(state, &transaction, message.flags);
                        let meta = TpuTransactionMeta {
                            priority,
                            cost,
                            flags: message.flags,
                            src_addr: message.src_addr,
                        };
                        if !filter(&transaction, &meta) {
                            // SAFETY: this packet was consumed from the TPU queue and has not been
                            // sent to a worker.
                            unsafe { allocator.free_offset(message.transaction.offset) };
                            continue;
                        }
                        meta
                    }
                    Err(_) => {
                        // Can only occur if a mismatch in sanitize configuration between sigverify
                        // and scheduler.

                        // SAFETY: this packet was consumed from the TPU queue and has not been sent
                        // to a worker.
                        unsafe { allocator.free_offset(message.transaction.offset) };
                        continue;
                    }
                };
            batch.write_transaction(num_transactions, message.transaction, meta);
            num_transactions = num_transactions.wrapping_add(1);
        }

        if num_transactions == 0 {
            batch.free(allocator);
        } else {
            match batch.reserve(scheduler_to_check_worker) {
                Ok(batch) => batch.send(num_transactions),
                Err(batch) => {
                    batch.free_with_transactions(allocator, num_transactions);
                    break;
                }
            }
        }
        remaining_packets = remaining_packets.wrapping_sub(num_packets);
    }
    tpu_to_scheduler.finalize();
}

struct CheckWorkerBatch {
    transaction_regions: NonNull<SharableTransactionRegion>,
    transaction_metas: NonNull<TpuTransactionMeta>,
    transactions_offset: usize,
}

impl CheckWorkerBatch {
    fn write_transaction(
        &mut self,
        index: usize,
        transaction: SharableTransactionRegion,
        meta: TpuTransactionMeta,
    ) {
        debug_assert!(index < MAX_PACKETS_PER_CHECK_BATCH);
        // SAFETY: `index` is bounded by the batch capacity, and both pointers reference the
        // corresponding arrays in this batch's allocation.
        unsafe {
            self.transaction_regions.add(index).write(transaction);
            self.transaction_metas.add(index).write(meta);
        }
    }

    fn reserve<'a>(
        self,
        scheduler_to_check_worker: &'a shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    ) -> Result<ReservedCheckWorkerBatch<'a>, Self> {
        // SAFETY: this scheduler is the sole producer for this queue and fully initializes the
        // reserved message before the guard is dropped.
        let Some(check_worker_message) = (unsafe { scheduler_to_check_worker.try_reserve_write() })
        else {
            return Err(self);
        };

        Ok(ReservedCheckWorkerBatch {
            check_worker_message,
            batch: self,
        })
    }

    fn free(self, allocator: &Allocator) {
        // SAFETY: this batch container was allocated by `allocate_check_worker_batch` and has
        // not been sent to a worker.
        unsafe { allocator.free_offset(self.transactions_offset) };
    }

    fn free_with_transactions(self, allocator: &Allocator, num_transactions: usize) {
        for index in 0..num_transactions {
            // SAFETY: `index` is bounded by the number of transaction regions initialized by
            // `write_transaction`.
            let transaction = unsafe { self.transaction_regions.add(index).read() };
            // SAFETY: this scheduler owns transactions until the batch is accepted by the queue.
            unsafe { allocator.free_offset(transaction.offset) };
        }
        self.free(allocator);
    }
}

struct ReservedCheckWorkerBatch<'a> {
    check_worker_message: shaq::mpmc::WriteGuard<'a, PackToCheckWorkerMessage>,
    batch: CheckWorkerBatch,
}

impl ReservedCheckWorkerBatch<'_> {
    fn send(self, num_transactions: usize) {
        self.check_worker_message.write(PackToCheckWorkerMessage {
            flags: CHECK_FLAGS,
            batch: SharableTransactionBatchRegion {
                num_transactions: num_transactions
                    .try_into()
                    .expect("batch size is at most 16"),
                transactions_offset: self.batch.transactions_offset,
            },
        });
    }
}

fn allocate_check_worker_batch(allocator: &Allocator) -> Option<CheckWorkerBatch> {
    let allocation = allocator.allocate(CheckBatch::TRANSACTION_META_END as u32)?;
    // SAFETY: `allocation` was allocated by this allocator immediately above.
    let transactions_offset = unsafe { allocator.offset(allocation) };
    // SAFETY: `transactions_offset` was obtained from this allocator immediately above.
    let transaction_regions = unsafe {
        allocator
            .ptr_from_offset(transactions_offset)
            .cast::<SharableTransactionRegion>()
    };
    // SAFETY: `transactions_offset` was obtained from this allocator immediately above and
    // `Batch::TRANSACTION_META_START` lies within the allocation.
    let transaction_metas = unsafe {
        allocator
            .ptr_from_offset(transactions_offset)
            .byte_add(CheckBatch::TRANSACTION_META_START)
            .cast::<TpuTransactionMeta>()
    };

    Some(CheckWorkerBatch {
        transaction_regions,
        transaction_metas,
        transactions_offset,
    })
}

fn calculate_priority_and_cost(
    state: &SchedulerState,
    transaction: &SanitizedTransactionView<TransactionPtr>,
    flags: u8,
) -> (u64, u64) {
    let Ok(transaction) = RuntimeTransaction::<&SanitizedTransactionView<TransactionPtr>>::try_new(
        transaction,
        MessageHash::Precomputed(Hash::default()),
        Some(flags & tpu_message_flags::IS_SIMPLE_VOTE != 0),
    ) else {
        return (0, 0);
    };
    let Ok(configuration) = transaction.transaction_configuration(state.feature_set()) else {
        return (0, 0);
    };

    let cost = CostModel::calculate_cost_for_executed_transaction(
        &transaction,
        u64::from(configuration.compute_unit_limit),
        configuration.loaded_accounts_data_size_limit,
        state.feature_set(),
    )
    .sum();
    let fee_details = solana_fee::calculate_fee_details(
        &transaction,
        LAMPORTS_PER_SIGNATURE,
        configuration.priority_fee_lamports,
        FeeFeatures::from(state.feature_set()),
    );
    let transaction_fee = fee_details.transaction_fee();
    let reward = fee_details.prioritization_fee().saturating_add(
        transaction_fee.saturating_sub(
            transaction_fee
                .saturating_mul(BURN_PERCENT)
                .wrapping_div(100),
        ),
    );

    #[allow(clippy::arithmetic_side_effects)]
    (
        reward
            .saturating_mul(PRIORITY_MULTIPLIER)
            .wrapping_div(cost.saturating_add(1)),
        cost,
    )
}

fn drop_tpu_packets(
    tpu_to_scheduler: &mut shaq::spsc::Consumer<TpuToPackMessage>,
    allocator: &Allocator,
    max_packets: usize,
) {
    for _ in 0..tpu_to_scheduler.len().min(max_packets) {
        let message = tpu_to_scheduler
            .try_read()
            .expect("queue length checked before read");
        // SAFETY: ownership of each transaction allocation transferred to the scheduler by the
        // TPU queue, and this transaction has not been sent to a worker.
        unsafe { allocator.free_offset(message.transaction.offset) };
    }
    tpu_to_scheduler.finalize();
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::SchedulerConfig,
        agave_scheduler_bindings::{LEADER_READY, ProgressMessage, scheduler_feature_flags},
        agave_scheduling_utils::handshake::{client, server::Server},
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
    };

    fn leader_ready_progress() -> ProgressMessage {
        ProgressMessage {
            leader_state: LEADER_READY,
            current_slot_progress: 0,
            epoch: 0,
            current_slot: 1,
            next_leader_slot: 10,
            leader_range_end: 13,
            remaining_cost_units: 48_000_000,
            latest_blockhash: [0; 32],
            scheduler_features: scheduler_feature_flags::NONE,
            target_bank_time_ms: 0,
        }
    }

    fn transaction_bytes(payer: &Keypair, compute_unit_price: u64) -> Vec<u8> {
        let transfer =
            system_instruction::transfer(&payer.pubkey(), &Pubkey::new_from_array([1; 32]), 1);
        let priority = ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price);
        let message = Message::new(&[transfer, priority], Some(&payer.pubkey()));
        let transaction = Transaction::new(&[payer], message, Hash::default());

        wincode::serialize(&VersionedTransaction::from(transaction)).unwrap()
    }

    fn allocate_transaction(allocator: &Allocator, bytes: &[u8]) -> SharableTransactionRegion {
        let transaction = allocator.allocate(bytes.len().try_into().unwrap()).unwrap();
        // SAFETY: `transaction` points to a fresh allocation of `bytes.len()` bytes.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), transaction.as_ptr(), bytes.len()) };
        // SAFETY: `transaction` was allocated by this allocator immediately above.
        let offset = unsafe { allocator.offset(transaction) };

        SharableTransactionRegion {
            offset,
            length: bytes.len().try_into().unwrap(),
        }
    }

    #[test]
    fn sends_bounded_batches_to_check_workers() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let mut offsets = Vec::new();
        let payer = Keypair::new();
        let transaction_bytes = transaction_bytes(&payer, 1);

        let max_packets = MAX_PACKETS_PER_CHECK_BATCH * 2;
        for packet_index in 0..=max_packets {
            let transaction = allocate_transaction(allocator, &transaction_bytes);
            offsets.push(transaction.offset);
            agave_session
                .tpu_to_pack
                .producer
                .try_write(TpuToPackMessage {
                    transaction,
                    flags: 0,
                    src_addr: u128::try_from(packet_index).unwrap().to_be_bytes(),
                })
                .unwrap();
        }
        agave_session.tpu_to_pack.producer.commit();

        let mut state = SchedulerState::new();
        state.update(&leader_ready_progress());
        drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            max_packets,
            |_, _| true,
        );

        for batch_index in 0..2 {
            let message = agave_session.check_workers[0]
                .pack_to_check_worker
                .try_read()
                .unwrap();
            assert_eq!(message.flags, CHECK_FLAGS);
            assert_eq!(
                usize::from(message.batch.num_transactions),
                MAX_PACKETS_PER_CHECK_BATCH
            );
            // SAFETY: the ingress function allocated this batch with the same metadata type.
            let batch = unsafe {
                CheckBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
            };
            for (transaction_index, (_, meta)) in batch.iter().enumerate() {
                assert!(meta.priority > 0);
                assert_eq!(meta.flags, 0);
                assert_eq!(
                    meta.src_addr,
                    u128::try_from(batch_index * MAX_PACKETS_PER_CHECK_BATCH + transaction_index)
                        .unwrap()
                        .to_be_bytes()
                );
            }

            // SAFETY: the test owns the batch after reading it from the check-worker queue.
            unsafe { batch.free() };
        }

        for offset in offsets.iter().take(max_packets) {
            // SAFETY: these transactions have not been sent to a worker in this test.
            unsafe { allocator.free_offset(*offset) };
        }

        client_session.tpu_to_pack.sync();
        let remaining = *client_session.tpu_to_pack.try_read().unwrap();
        assert_eq!(remaining.transaction.offset, offsets[max_packets]);
        client_session.tpu_to_pack.finalize();
        // SAFETY: this final queued transaction remains owned by the test.
        unsafe { allocator.free_offset(remaining.transaction.offset) };
    }

    #[test]
    fn drops_packets_when_check_worker_queue_is_full() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        config.check_worker_count = 1;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let payer = Keypair::new();
        let transaction = allocate_transaction(allocator, &transaction_bytes(&payer, 1));

        let occupied_message = PackToCheckWorkerMessage {
            flags: CHECK_FLAGS,
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: 0,
            },
        };
        while client_session
            .pack_to_check_worker
            .try_write(occupied_message)
            .is_ok()
        {}
        agave_session
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction,
                flags: 0,
                src_addr: [0; 16],
            })
            .unwrap();
        agave_session.tpu_to_pack.producer.commit();

        let mut state = SchedulerState::new();
        state.update(&leader_ready_progress());
        drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            MAX_PACKETS_PER_CHECK_BATCH,
            |_, _| true,
        );

        client_session.tpu_to_pack.sync();
        assert!(client_session.tpu_to_pack.try_read().is_none());
        client_session.tpu_to_pack.finalize();
    }

    #[test]
    fn filters_packets_before_check_worker_processing() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let payer = Keypair::new();
        let transaction = allocate_transaction(allocator, &transaction_bytes(&payer, 1));
        agave_session
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction,
                flags: 0,
                src_addr: [0; 16],
            })
            .unwrap();
        agave_session.tpu_to_pack.producer.commit();

        let mut state = SchedulerState::new();
        state.update(&leader_ready_progress());
        drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            1,
            |_, meta| meta.priority == 0,
        );

        assert!(
            agave_session.check_workers[0]
                .pack_to_check_worker
                .try_read()
                .is_none()
        );
        client_session.tpu_to_pack.sync();
        assert!(client_session.tpu_to_pack.try_read().is_none());
        client_session.tpu_to_pack.finalize();
    }

    #[test]
    fn calculates_higher_priority_for_higher_compute_unit_price() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let (agave_session, _) = Server::setup_session(config.client_logon()).unwrap();
        let allocator = &agave_session.tpu_to_pack.allocator;
        let state = SchedulerState::new();
        let payer = Keypair::new();

        let low_priority = allocate_transaction(allocator, &transaction_bytes(&payer, 1));
        let high_priority = allocate_transaction(allocator, &transaction_bytes(&payer, 1_000_000));
        // SAFETY: both regions point to transaction allocations owned by this test.
        let low_transaction =
            unsafe { TransactionPtr::from_sharable_transaction_region(&low_priority, allocator) };
        let low_transaction = SanitizedTransactionView::try_new_sanitized(
            low_transaction,
            &sanitize_config(state.feature_set().snapshot().limit_instruction_accounts),
        )
        .unwrap();
        // SAFETY: both regions point to transaction allocations owned by this test.
        let high_transaction =
            unsafe { TransactionPtr::from_sharable_transaction_region(&high_priority, allocator) };
        let high_transaction = SanitizedTransactionView::try_new_sanitized(
            high_transaction,
            &sanitize_config(state.feature_set().snapshot().limit_instruction_accounts),
        )
        .unwrap();
        let (low, _) = calculate_priority_and_cost(&state, &low_transaction, 0);
        let (high, _) = calculate_priority_and_cost(&state, &high_transaction, 0);

        assert!(high > low, "higher CU price should produce higher priority");

        // SAFETY: the test has not sent either transaction to a worker.
        unsafe {
            allocator.free_offset(low_priority.offset);
            allocator.free_offset(high_priority.offset);
        }
    }
}
