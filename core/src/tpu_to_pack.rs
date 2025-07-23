use {
    agave_banking_stage_ingress_types::BankingPacketReceiver,
    agave_scheduler_bindings::{SharableTransaction, TpuToPackMessage},
    rts_alloc::Allocator,
    std::{
        ptr::NonNull,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::JoinHandle,
    },
};

/// A simple service to signature verified transactions
/// from TPU and send them to an external pack process.
/// The service will *JOIN* an allocator region and queue
/// setup by the pack process.
pub fn spawn_tpu_to_pack(
    exit_signal: Arc<AtomicBool>,
    receiver: BankingPacketReceiver,
    clean_up: impl Fn() + Send + Sync + 'static,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("solTpu2Pack".to_string())
        .spawn(move || {
            // Setup allocator and queue.
            // If setup fails, exit immediately.
            if let Some((allocator, producer)) = setup() {
                tpu_to_pack(exit_signal, receiver, allocator, producer);
            }

            // call `clean_up` unconditionally.
            // this handles cases where external pack process fails,
            // and we want to respawn a default scheduler.
            clean_up();
        })
        .expect("failed to spawn tpu_to_pack thread")
}

fn setup() -> Option<(Allocator, shaq::Producer<TpuToPackMessage>)> {
    // TODO: Pass these in.
    const ALLOCATOR_PATH: &str = "/mnt/hugepages/rts-alloc";
    const ALLOCATOR_WORKER_ID: u32 = 0;
    const TPU_TO_PACK_PATH: &str = "/mnt/hugepages/tpu_to_pack";
    let allocator = Allocator::join(ALLOCATOR_PATH, ALLOCATOR_WORKER_ID)
        .map_err(|e| {
            error!("Failed to join allocator: {e:?}");
        })
        .ok()?;
    let producer = shaq::Producer::join(TPU_TO_PACK_PATH)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some((allocator, producer))
}

fn tpu_to_pack(
    exit_signal: Arc<AtomicBool>,
    receiver: BankingPacketReceiver,
    allocator: Allocator,
    mut producer: shaq::Producer<TpuToPackMessage>,
) {
    while exit_signal.load(Ordering::Relaxed) {
        // Receive packets from the TPU.
        if let Ok(packet_batch) = receiver.try_recv() {
            // Clean all remote frees in allocator so we have as much
            // room as possible.
            allocator.clean_remote_free_lists();
            // Sync producer queue with reader so we have as much room
            // as possible.
            producer.sync();

            // Loop over all packets in the batch.
            // Allocate a shared packet, copy the bytes into it,
            // and pass along to the producer.
            'batch_loop: for batch in packet_batch.iter() {
                for packet in batch.iter() {
                    // Check if the packet is valid.
                    let packet_size = packet.meta().size;
                    let Some(packet_bytes) = packet.data(..packet_size) else {
                        // packet was marked as invalid in previous stages.
                        // skip it and do not send to pack.
                        continue;
                    };

                    // Allocate enough bytes in the allocator for the packet.
                    let Some(allocated_packet) = allocator.allocate(packet_size as u32) else {
                        // TODO: Better handling?
                        break 'batch_loop;
                    };

                    // Check if we can reserve space in the producer queue.
                    let Some(tpu_to_pack_message) = producer.reserve() else {
                        // Free the allocated packet if we cannot reserve space.
                        // SAFETY: The packet was allocated from the allocator.
                        unsafe {
                            allocator.free(allocated_packet);
                        }
                        break 'batch_loop;
                    };

                    // SAFETY:
                    // - src and dst are valid pointers.
                    // - src and dst are properly aligned (1).
                    // - src and dst do not overlap.
                    unsafe {
                        allocated_packet.copy_from_nonoverlapping(
                            NonNull::new(packet_bytes.as_ptr().cast_mut())
                                .expect("packet bytes must be non-null"),
                            packet_size,
                        );
                    }

                    // Get a sharable offset for the transaction.
                    // SAFETY: `allocated_packet` was allocated by the allocator.
                    let sharable_offset = unsafe { allocator.offset(allocated_packet) };

                    // Write the transaction into the producer queue.
                    // SAFETY: `tpu_to_pack_message` is a valid pointer to a `TpuToPackMessage`.
                    unsafe {
                        tpu_to_pack_message.write(TpuToPackMessage {
                            transaction: SharableTransaction {
                                transaction_offset: sharable_offset,
                                transaction_size: packet_size as u32,
                            },
                        });
                    }
                }
            }

            // commit the messages to the producer queue.
            // this will make them available to the pack process.
            producer.commit();
        }
    }
}
