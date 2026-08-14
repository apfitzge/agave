//! Service to send transaction packets to the external scheduler.
//!

use {
    crate::banking_trace::BankingPacketReceiver,
    agave_banking_stage_ingress_types::BankingPacketBatch,
    agave_scheduler_bindings::{SharableTransactionRegion, TpuToPackMessage, tpu_message_flags},
    agave_scheduling_utils::handshake::{AgaveTpuToPackSession, TPU_TO_PACK_WORKERS},
    crossbeam_channel::RecvTimeoutError,
    rts_alloc::Allocator,
    solana_packet::PacketFlags,
    std::{
        net::IpAddr,
        ptr::NonNull,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
        time::Duration,
    },
    tokio_util::sync::CancellationToken,
};

pub struct BankingPacketReceivers {
    pub non_vote_receiver: BankingPacketReceiver,
    pub gossip_vote_receiver: Option<BankingPacketReceiver>,
    pub tpu_vote_receiver: Option<BankingPacketReceiver>,
}

/// Spawns one thread per packet receiver to send packets to the external scheduler.
pub fn spawn(
    exit: Arc<AtomicBool>,
    shutdown_signal: CancellationToken,
    receivers: BankingPacketReceivers,
    AgaveTpuToPackSession {
        allocators,
        producer,
    }: AgaveTpuToPackSession,
) -> Vec<JoinHandle<()>> {
    let done = Arc::new(AtomicBool::new(false));
    let BankingPacketReceivers {
        non_vote_receiver,
        gossip_vote_receiver,
        tpu_vote_receiver,
    } = receivers;

    let receivers: [(&str, Option<BankingPacketReceiver>); TPU_TO_PACK_WORKERS] = [
        ("solTpu2PackNv", Some(non_vote_receiver)),
        ("solTpu2PackGsp", gossip_vote_receiver),
        ("solTpu2PackVote", tpu_vote_receiver),
    ];
    receivers
        .into_iter()
        .zip(allocators)
        .filter_map(|((thread_name, receiver), allocator)| {
            let receiver = receiver?;
            let exit = exit.clone();
            let done = done.clone();
            let shutdown_signal = shutdown_signal.clone();
            let producer = producer.clone();
            Some(
                std::thread::Builder::new()
                    .name(thread_name.to_string())
                    .spawn(move || {
                        tpu_to_pack(exit, done, shutdown_signal, receiver, allocator, producer);
                    })
                    .unwrap(),
            )
        })
        .collect()
}

fn tpu_to_pack(
    exit: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    shutdown_signal: CancellationToken,
    receiver: BankingPacketReceiver,
    allocator: Allocator,
    producer: shaq::mpmc::Producer<TpuToPackMessage>,
) {
    while !exit.load(Ordering::Relaxed) && !done.load(Ordering::Relaxed) {
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(packet_batch) => {
                handle_packet_batch(&allocator, &producer, packet_batch);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                // Any disconnected ingress terminates all tpu-to-pack workers.
                done.store(true, Ordering::Relaxed);
                shutdown_signal.cancel();
                break;
            }
        }
    }
}

fn handle_packet_batch(
    allocator: &Allocator,
    producer: &shaq::mpmc::Producer<TpuToPackMessage>,
    packet_batch: BankingPacketBatch,
) {
    // Clean all remote frees in allocator so we have as much
    // room as possible.
    allocator.clean_remote_free_lists();

    for packet in packet_batch.iter() {
        // Check if the packet is valid and get the bytes.
        let Some(packet_bytes) = packet.data(..) else {
            continue;
        };
        let packet_size = packet_bytes.len();

        // Allocate space for the packet to be copied into.
        let Some(allocated_ptr) = allocator.allocate(packet_size as u32) else {
            warn!("Failed to allocate. Dropping the rest of the batch.");
            break;
        };
        // Get the offset of the allocated pointer in the allocator.
        // SAFETY: `allocated_ptr` was allocated from `allocator`.
        let allocated_ptr_offset_in_allocator = unsafe { allocator.offset(allocated_ptr) };

        // SAFETY:
        // - `allocated_ptr` is valid for `packet_size` bytes.
        let message = unsafe {
            copy_packet_and_populate_message(
                packet_bytes,
                packet.meta(),
                allocated_ptr,
                allocated_ptr_offset_in_allocator,
            )
        };

        if producer.try_write(message).is_err() {
            // SAFETY: `allocated_ptr` was allocated by `allocator`
            //         and not previously freed.
            unsafe { allocator.free(allocated_ptr) };
        }
    }
}

/// # Safety:
/// - `allocated_ptr` must be valid for `packet_bytes.len()` bytes.
unsafe fn copy_packet_and_populate_message(
    packet_bytes: &[u8],
    packet_meta: &solana_packet::Meta,
    allocated_ptr: NonNull<u8>,
    allocated_ptr_offset_in_allocator: usize,
) -> TpuToPackMessage {
    // Copy the packet data into the allocated memory.
    // SAFETY:
    // - `allocated_ptr` is valid for `packet_size` bytes.
    // - src and dst are valid pointers that are properly aligned
    //   and do not overlap.
    unsafe {
        allocated_ptr.copy_from_nonoverlapping(
            NonNull::new(packet_bytes.as_ptr().cast_mut()).expect("packet bytes must be non-null"),
            packet_bytes.len(),
        );
    }

    // Create a sharable transaction region for the packet.
    let transaction = SharableTransactionRegion {
        offset: allocated_ptr_offset_in_allocator,
        length: packet_bytes.len() as u32,
    };

    // Translate flags from meta.
    let tpu_message_flags = flags_from_meta(packet_meta.flags);

    // Get the source address of the packet - convert to expected format.
    let src_addr = map_src_addr(packet_meta.addr);

    TpuToPackMessage {
        transaction,
        flags: tpu_message_flags,
        src_addr,
    }
}

fn flags_from_meta(flags: PacketFlags) -> u8 {
    let mut tpu_message_flags = 0;

    if flags.contains(PacketFlags::SIMPLE_VOTE_TX) {
        tpu_message_flags |= tpu_message_flags::IS_SIMPLE_VOTE;
    }
    if flags.contains(PacketFlags::FORWARDED) {
        tpu_message_flags |= tpu_message_flags::FORWARDED;
    }
    if flags.contains(PacketFlags::FROM_STAKED_NODE) {
        tpu_message_flags |= tpu_message_flags::FROM_STAKED_NODE;
    }

    tpu_message_flags
}

fn map_src_addr(addr: IpAddr) -> [u8; 16] {
    match addr {
        IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped().octets(),
        IpAddr::V6(ipv6) => ipv6.octets(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::net::Ipv4Addr};

    #[test]
    fn test_copy_packet_and_populate_message() {
        let packet_bytes = vec![1, 2, 3, 4, 5];
        let src_ip = Ipv4Addr::new(192, 168, 1, 1);
        let mut packet_meta = solana_packet::Meta::default();
        packet_meta.size = packet_bytes.len();
        packet_meta.addr = IpAddr::V4(src_ip);
        packet_meta.port = 1;
        packet_meta.flags = PacketFlags::all();

        // Buffer to simulate allocated memory
        let mut buffer = [0u8; 256];
        const DUMMY_OFFSET: usize = 42;

        let tpu_to_pack_message = unsafe {
            copy_packet_and_populate_message(
                packet_bytes.as_slice(),
                &packet_meta,
                NonNull::new(buffer.as_mut_ptr()).unwrap(),
                DUMMY_OFFSET,
            )
        };

        assert_eq!(&buffer[..packet_bytes.len()], packet_bytes.as_slice());
        assert_eq!(tpu_to_pack_message.transaction.offset, DUMMY_OFFSET);
        assert_eq!(
            tpu_to_pack_message.transaction.length,
            packet_bytes.len() as u32
        );
        assert_eq!(
            tpu_to_pack_message.flags,
            tpu_message_flags::IS_SIMPLE_VOTE
                | tpu_message_flags::FORWARDED
                | tpu_message_flags::FROM_STAKED_NODE
        );
        assert_eq!(
            tpu_to_pack_message.src_addr,
            src_ip.to_ipv6_mapped().octets()
        );
    }

    #[test]
    fn test_flags_from_meta() {
        assert_eq!(
            flags_from_meta(PacketFlags::empty()),
            tpu_message_flags::NONE
        );
        assert_eq!(
            flags_from_meta(PacketFlags::SIMPLE_VOTE_TX),
            tpu_message_flags::IS_SIMPLE_VOTE
        );
        assert_eq!(
            flags_from_meta(PacketFlags::FORWARDED),
            tpu_message_flags::FORWARDED
        );
        assert_eq!(
            flags_from_meta(PacketFlags::FROM_STAKED_NODE),
            tpu_message_flags::FROM_STAKED_NODE
        );
        assert_eq!(
            flags_from_meta(
                PacketFlags::SIMPLE_VOTE_TX
                    | PacketFlags::FORWARDED
                    | PacketFlags::FROM_STAKED_NODE
            ),
            tpu_message_flags::IS_SIMPLE_VOTE
                | tpu_message_flags::FORWARDED
                | tpu_message_flags::FROM_STAKED_NODE
        );
    }
}
