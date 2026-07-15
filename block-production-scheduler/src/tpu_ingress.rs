use {
    crate::{
        progress_tracker::SchedulerState,
        transaction::{
            CheckBatch, CheckTransactionMeta, MAX_PACKETS_PER_CHECK_BATCH, TpuTransactionMeta,
        },
    },
    agave_scheduler_bindings::{PackToCheckWorkerMessage, TpuToPackMessage, check_message_flags},
    rts_alloc::Allocator,
    std::time::Duration,
};

const TPU_RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);
const CHECK_FLAGS: u16 = check_message_flags::LOAD_FEE_PAYER_BALANCE
    | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES
    | check_message_flags::CALCULATE_SCHEDULING_DETAILS;

#[derive(Default)]
pub(crate) struct TpuIngressStats {
    pub(crate) dropped_not_accepting_packets: u64,
    pub(crate) dropped_check_worker_queue_full_packets: u64,
}

pub(crate) fn drain_tpu(
    tpu_to_scheduler: &mut shaq::spsc::Consumer<TpuToPackMessage>,
    scheduler_to_check_worker: &shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    allocator: &Allocator,
    state: &SchedulerState,
    max_packets: usize,
) -> TpuIngressStats {
    // sleep only while not in the near-leader holding window where packets
    // should be buffered eagerly. The futex wait first checks the queue, so
    // it returns immediately if a producer has already published packets.
    if !state.should_accept_packets() {
        let _ = tpu_to_scheduler.wait_readable_timeout(TPU_RECEIVE_TIMEOUT);
    } else {
        tpu_to_scheduler.sync();
    }

    if !state.should_accept_packets() {
        return TpuIngressStats {
            dropped_not_accepting_packets: drop_tpu_packets(
                tpu_to_scheduler,
                allocator,
                max_packets,
            ),
            ..TpuIngressStats::default()
        };
    }

    let mut stats = TpuIngressStats::default();
    let mut remaining_packets = tpu_to_scheduler.len().min(max_packets);
    while remaining_packets > 0 {
        let num_packets = remaining_packets.min(MAX_PACKETS_PER_CHECK_BATCH);

        let Some(mut batch) = CheckBatch::allocate(allocator) else {
            break;
        };

        for _ in 0..num_packets {
            let Some(message) = tpu_to_scheduler.try_read() else {
                unreachable!("queue length checked before read");
            };

            assert!(
                batch
                    .push(
                        message.transaction,
                        CheckTransactionMeta::Tpu(TpuTransactionMeta {
                            priority: 0,
                            cost: 0,
                            flags: message.flags,
                            src_addr: message.src_addr,
                        }),
                    )
                    .is_ok(),
                "batch is bounded by the check-worker capacity"
            );
        }

        if batch.is_empty() {
            // SAFETY: this batch was allocated locally and was not sent to a worker.
            unsafe { batch.free() };
        } else {
            if scheduler_to_check_worker
                .try_write(PackToCheckWorkerMessage {
                    flags: CHECK_FLAGS,
                    minimum_priority: 0,
                    batch: batch.to_sharable_transaction_batch_region(),
                })
                .is_err()
            {
                stats.dropped_check_worker_queue_full_packets = stats
                    .dropped_check_worker_queue_full_packets
                    .wrapping_add(batch.len() as u64);
                // SAFETY: this scheduler owns the batch and all transaction allocations until
                // the batch is accepted by the queue.
                unsafe {
                    batch.free_transactions();
                    batch.free();
                }
                break;
            }
        }
        remaining_packets = remaining_packets.wrapping_sub(num_packets);
    }
    tpu_to_scheduler.finalize();
    stats
}

fn drop_tpu_packets(
    tpu_to_scheduler: &mut shaq::spsc::Consumer<TpuToPackMessage>,
    allocator: &Allocator,
    max_packets: usize,
) -> u64 {
    let num_packets = tpu_to_scheduler.len().min(max_packets);
    for _ in 0..num_packets {
        let message = tpu_to_scheduler
            .try_read()
            .expect("queue length checked before read");
        // SAFETY: ownership of each transaction allocation transferred to the scheduler by the
        // TPU queue, and this transaction has not been sent to a worker.
        unsafe { allocator.free_offset(message.transaction.offset) };
    }
    tpu_to_scheduler.finalize();
    num_packets as u64
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::SchedulerConfig,
        agave_scheduler_bindings::{
            LEADER_READY, ProgressMessage, SharableTransactionBatchRegion,
            SharableTransactionRegion, scheduler_feature_flags,
        },
        agave_scheduling_utils::handshake::{client, server::Server},
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

    fn transaction_bytes(payer: &Keypair) -> Vec<u8> {
        let message = Message::new(
            &[system_instruction::transfer(
                &payer.pubkey(),
                &Pubkey::new_from_array([1; 32]),
                1,
            )],
            Some(&payer.pubkey()),
        );
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
        let transaction_bytes = transaction_bytes(&payer);

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
        let stats = drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            max_packets,
        );
        assert_eq!(stats.dropped_not_accepting_packets, 0);
        assert_eq!(stats.dropped_check_worker_queue_full_packets, 0);

        for batch_index in 0..2 {
            let message = agave_session.check_workers[0]
                .pack_to_check_worker
                .try_read()
                .unwrap();
            assert_eq!(message.flags, CHECK_FLAGS);
            assert_eq!(message.flags & check_message_flags::STATUS_CHECKS, 0);
            assert_ne!(
                message.flags & check_message_flags::CALCULATE_SCHEDULING_DETAILS,
                0
            );
            assert_eq!(
                usize::from(message.batch.num_transactions),
                MAX_PACKETS_PER_CHECK_BATCH
            );
            // SAFETY: the ingress function allocated this batch with the same metadata type.
            let batch = unsafe {
                CheckBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
            };
            for (transaction_index, (_, meta)) in batch.iter().enumerate() {
                let CheckTransactionMeta::Tpu(meta) = meta else {
                    panic!("TPU ingress must retain TPU metadata");
                };
                assert_eq!(meta.priority, 0);
                assert_eq!(meta.cost, 0);
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
        let transaction = allocate_transaction(allocator, &transaction_bytes(&payer));

        let occupied_message = PackToCheckWorkerMessage {
            flags: CHECK_FLAGS,
            minimum_priority: 0,
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
        let stats = drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            MAX_PACKETS_PER_CHECK_BATCH,
        );
        assert_eq!(stats.dropped_not_accepting_packets, 0);
        assert_eq!(stats.dropped_check_worker_queue_full_packets, 1);

        client_session.tpu_to_pack.sync();
        assert!(client_session.tpu_to_pack.try_read().is_none());
        client_session.tpu_to_pack.finalize();
    }

    #[test]
    fn drops_packets_when_not_accepting() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let payer = Keypair::new();
        let transaction = allocate_transaction(allocator, &transaction_bytes(&payer));
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

        let stats = drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &SchedulerState::new(),
            1,
        );

        assert_eq!(stats.dropped_not_accepting_packets, 1);
        assert_eq!(stats.dropped_check_worker_queue_full_packets, 0);
        client_session.tpu_to_pack.sync();
        assert!(client_session.tpu_to_pack.try_read().is_none());
        client_session.tpu_to_pack.finalize();
    }

    #[test]
    fn forwards_packets_without_sanitizing() {
        let mut config = SchedulerConfig::new("/unused");
        config.allocator_size = 64 * 1024 * 1024;
        let logon = config.client_logon();
        let (mut agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = &client_session.allocators[0];
        let transaction = allocate_transaction(allocator, &[0]);
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
        let stats = drain_tpu(
            &mut client_session.tpu_to_pack,
            &client_session.pack_to_check_worker,
            allocator,
            &state,
            1,
        );
        assert_eq!(stats.dropped_not_accepting_packets, 0);
        assert_eq!(stats.dropped_check_worker_queue_full_packets, 0);

        let message = agave_session.check_workers[0]
            .pack_to_check_worker
            .try_read()
            .unwrap();
        let batch = unsafe {
            CheckBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
        };
        assert_eq!(batch.transaction_region(0), transaction);
        // SAFETY: this test owns the batch after reading it from the check-worker queue.
        unsafe { batch.free() };
        client_session.tpu_to_pack.sync();
        assert!(client_session.tpu_to_pack.try_read().is_none());
        client_session.tpu_to_pack.finalize();
        // SAFETY: this test retains ownership of the raw transaction allocation.
        unsafe { allocator.free_offset(transaction.offset) };
    }
}
