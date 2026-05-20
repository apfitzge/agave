use {
    crate::replay_event_timestamp::replay_event_timestamp_ns,
    agave_scheduler_bindings::{
        PackToWorkerMessage, ReplayBlockStatusMessage, ReplayToPackMessage,
        SharableTransactionRegion, WorkerToPackMessage,
    },
    agave_scheduling_utils::{
        replay_events::{REPLAY_EVENTS_IPC_FILE, ReplayEvent},
        shared_memory::{self, SharedMemoryError},
    },
    rts_alloc::{Allocator, FreeOnlyAllocator},
    std::{
        fs::File,
        path::{Path, PathBuf},
        sync::atomic::Ordering,
    },
};

const SESSION_ALLOCATOR_HANDLES: usize = 2;
const ALLOCATOR_SLAB_SIZE: u32 = 2 * 1024 * 1024;
const REPLAY_EVENT_CAPACITY: usize = 1024 * 1024;
pub const SIGNATURE_VERIFICATION_WORKER_COUNT: usize = 16;
const SIGNATURE_VERIFICATION_REQUEST_CAPACITY: usize = 16 * 1024;
const SIGNATURE_VERIFICATION_RESULT_CAPACITY: usize = 16 * 1024;

const SHMEM_NAME: &std::ffi::CStr = c"agave-block-verification-stage";

/// Required arguments for locally creating block-verification-stage shared resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockVerificationStageSetupConfig {
    pub allocator_size: usize,
    pub replay_to_pack_capacity: usize,
    pub replay_block_status_capacity: usize,
    pub worker_count: usize,
    pub pack_to_worker_capacity: usize,
    pub worker_to_pack_capacity: usize,
}

/// Scheduler-owned block-verification-stage queue and allocator handles.
pub struct BlockVerificationStageSession {
    pub allocator: Allocator,
    pub replay_to_pack: shaq::spsc::Consumer<ReplayToPackMessage>,
    pub replay_block_status: shaq::spsc::Producer<ReplayBlockStatusMessage>,
    pub workers: Vec<BlockVerificationStageWorkerSession>,
    pub signature_verification_requests: shaq::mpmc::Producer<SignatureVerificationRequest>,
    pub signature_verification_results: shaq::mpmc::Consumer<SignatureVerificationResult>,
}

/// Scheduler-owned queue handles for one block verification worker.
pub struct BlockVerificationStageWorkerSession {
    pub pack_to_worker: shaq::spsc::Producer<PackToWorkerMessage>,
    pub worker_to_pack: shaq::spsc::Consumer<WorkerToPackMessage>,
}

/// Replay-stage-owned queue and allocator handles.
pub struct ReplayStageSession {
    pub allocator: Allocator,
    pub replay_to_pack: shaq::spsc::Producer<ReplayToPackMessage>,
    pub replay_block_status: shaq::spsc::Consumer<ReplayBlockStatusMessage>,
}

/// Worker-thread-owned queue and allocator handles.
pub struct BlockVerificationWorkerSession {
    pub allocator: Allocator,
    pub pack_to_worker: shaq::spsc::Consumer<PackToWorkerMessage>,
    pub worker_to_pack: shaq::spsc::Producer<WorkerToPackMessage>,
}

/// Sigverify-worker-owned queue and read-only allocator handles.
pub struct SignatureVerificationWorkerSession {
    pub allocator: FreeOnlyAllocator,
    pub requests: shaq::mpmc::Consumer<SignatureVerificationRequest>,
    pub results: shaq::mpmc::Producer<SignatureVerificationResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SignatureVerificationRequest {
    pub slot: u64,
    pub bank_id: u64,
    pub transaction_index: usize,
    pub transaction: SharableTransactionRegion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SignatureVerificationResult {
    pub slot: u64,
    pub transaction_index: usize,
    pub verified: u8,
}

impl SignatureVerificationResult {
    pub fn new(slot: u64, transaction_index: usize, verified: bool) -> Self {
        Self {
            slot,
            transaction_index,
            verified: u8::from(verified),
        }
    }

    pub fn verified(self) -> bool {
        self.verified != 0
    }
}

/// Locally created session pair.
pub struct BlockVerificationStageSessions {
    pub block_verification_stage: BlockVerificationStageSession,
    pub replay_stage: ReplayStageSession,
    pub workers: Vec<BlockVerificationWorkerSession>,
    pub signature_verification_workers: Vec<SignatureVerificationWorkerSession>,
}

/// Locally created replay event broadcast.
pub struct ReplayEventBroadcast {
    path: PathBuf,
    producer: shaq::broadcast::Producer<ReplayEvent>,
}

impl ReplayEventBroadcast {
    pub fn new(ledger_path: &Path) -> Result<Self, SharedMemoryError> {
        let path = ledger_path.join(REPLAY_EVENTS_IPC_FILE);
        let producer =
            shared_memory::create_broadcast_producer_at_path(&path, REPLAY_EVENT_CAPACITY)?;

        Ok(Self { path, producer })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn emit(&self, mut event: ReplayEvent) {
        event.timestamp_ns = replay_event_timestamp_ns();
        let _ = self.producer.try_write(event, Ordering::Relaxed);
    }
}

impl BlockVerificationStageSessions {
    /// Creates the allocator and queues for an in-process block verification stage.
    pub fn setup(config: BlockVerificationStageSetupConfig) -> Result<Self, SetupError> {
        let (allocator_file, block_verification_allocator, replay_stage_allocator) =
            create_allocator(config.allocator_size, config.worker_count)?;
        let (replay_to_pack_producer, replay_to_pack_consumer) =
            create_queue_pair(config.replay_to_pack_capacity, true)?;
        let (replay_block_status_producer, replay_block_status_consumer) =
            create_queue_pair(config.replay_block_status_capacity, false)?;
        let (signature_verification_request_producer, signature_verification_request_consumer) =
            create_mpmc_queue_pair(SIGNATURE_VERIFICATION_REQUEST_CAPACITY, true)?;
        let (signature_verification_result_producer, signature_verification_result_consumer) =
            create_mpmc_queue_pair(SIGNATURE_VERIFICATION_RESULT_CAPACITY, true)?;
        let (block_verification_workers, workers) = create_worker_sessions(
            &allocator_file,
            config.worker_count,
            config.pack_to_worker_capacity,
            config.worker_to_pack_capacity,
        )?;
        let signature_verification_workers = create_signature_verification_worker_sessions(
            &allocator_file,
            SIGNATURE_VERIFICATION_WORKER_COUNT,
            signature_verification_request_consumer,
            signature_verification_result_producer,
        )?;

        Ok(Self {
            block_verification_stage: BlockVerificationStageSession {
                allocator: block_verification_allocator,
                replay_to_pack: replay_to_pack_consumer,
                replay_block_status: replay_block_status_producer,
                workers: block_verification_workers,
                signature_verification_requests: signature_verification_request_producer,
                signature_verification_results: signature_verification_result_consumer,
            },
            replay_stage: ReplayStageSession {
                allocator: replay_stage_allocator,
                replay_to_pack: replay_to_pack_producer,
                replay_block_status: replay_block_status_consumer,
            },
            workers,
            signature_verification_workers,
        })
    }
}

pub type SetupError = SharedMemoryError;

fn create_allocator(
    allocator_size: usize,
    worker_count: usize,
) -> Result<(File, Allocator, Allocator), SetupError> {
    let allocator_handles = SESSION_ALLOCATOR_HANDLES.checked_add(worker_count).unwrap();
    let (file, block_verification_allocator) = shared_memory::create_allocator(
        SHMEM_NAME,
        allocator_size,
        u32::try_from(allocator_handles).unwrap(),
        ALLOCATOR_SLAB_SIZE,
    )?;
    let replay_stage_allocator = Allocator::join(&file)?;

    Ok((file, block_verification_allocator, replay_stage_allocator))
}

fn create_queue_pair<T>(
    capacity: usize,
    huge: bool,
) -> Result<(shaq::spsc::Producer<T>, shaq::spsc::Consumer<T>), SetupError> {
    shared_memory::create_queue_pair(SHMEM_NAME, capacity, huge)
}

fn create_mpmc_queue_pair<T>(
    capacity: usize,
    huge: bool,
) -> Result<(shaq::mpmc::Producer<T>, shaq::mpmc::Consumer<T>), SetupError> {
    shared_memory::create_mpmc_queue_pair(SHMEM_NAME, capacity, huge)
}

fn create_worker_sessions(
    allocator_file: &File,
    worker_count: usize,
    pack_to_worker_capacity: usize,
    worker_to_pack_capacity: usize,
) -> Result<
    (
        Vec<BlockVerificationStageWorkerSession>,
        Vec<BlockVerificationWorkerSession>,
    ),
    SetupError,
> {
    (0..worker_count).try_fold(
        (
            Vec::with_capacity(worker_count),
            Vec::with_capacity(worker_count),
        ),
        |(mut block_verification_workers, mut workers), _| {
            let worker_allocator = Allocator::join(allocator_file)?;
            let (pack_to_worker_producer, pack_to_worker_consumer) =
                create_queue_pair(pack_to_worker_capacity, true)?;
            let (worker_to_pack_producer, worker_to_pack_consumer) =
                create_queue_pair(worker_to_pack_capacity, true)?;

            block_verification_workers.push(BlockVerificationStageWorkerSession {
                pack_to_worker: pack_to_worker_producer,
                worker_to_pack: worker_to_pack_consumer,
            });
            workers.push(BlockVerificationWorkerSession {
                allocator: worker_allocator,
                pack_to_worker: pack_to_worker_consumer,
                worker_to_pack: worker_to_pack_producer,
            });

            Ok((block_verification_workers, workers))
        },
    )
}

fn create_signature_verification_worker_sessions(
    allocator_file: &File,
    worker_count: usize,
    requests: shaq::mpmc::Consumer<SignatureVerificationRequest>,
    results: shaq::mpmc::Producer<SignatureVerificationResult>,
) -> Result<Vec<SignatureVerificationWorkerSession>, SetupError> {
    (0..worker_count)
        .map(|_| {
            Ok(SignatureVerificationWorkerSession {
                allocator: FreeOnlyAllocator::join(allocator_file)?,
                requests: requests.clone(),
                results: results.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_scheduler_bindings::{
            EntryHeader, PackToWorkerMessage, ReplayBankMessage, ReplayToPackMessagePayload,
            SharableTransactionBatchRegion, TransactionResponseRegion, WorkerToPackMessage,
            pack_message_flags, processed_codes, replay_bank_message_kinds,
            replay_block_status_codes, replay_block_status_reasons, replay_to_pack_message_types,
        },
        std::time::Instant,
    };

    fn setup(worker_count: usize) -> Result<BlockVerificationStageSessions, SetupError> {
        BlockVerificationStageSessions::setup(BlockVerificationStageSetupConfig {
            allocator_size: 64 * 1024 * 1024,
            replay_to_pack_capacity: 8,
            replay_block_status_capacity: 8,
            worker_count,
            pack_to_worker_capacity: 8,
            worker_to_pack_capacity: 8,
        })
    }

    #[test]
    fn replay_event_broadcast_stamps_timestamp() {
        let temp_dir = tempfile::tempdir().unwrap();
        let event_broadcast = ReplayEventBroadcast::new(temp_dir.path()).unwrap();
        let mut event_consumer: shaq::broadcast::Consumer<ReplayEvent> =
            shared_memory::join_broadcast_consumer_at_path(event_broadcast.path()).unwrap();
        assert_eq!(event_consumer.try_read(Ordering::Relaxed).unwrap(), None);

        event_broadcast.emit(ReplayEvent::slot_begin(0, 42));
        let event = event_consumer
            .try_read(Ordering::Relaxed)
            .unwrap()
            .expect("replay event should be broadcast");

        assert_ne!(event.timestamp_ns, 0);
        assert_eq!(event.slot(), 42);
    }

    #[test]
    fn setup_wires_replay_stage_to_block_verification_stage_ingress_queue() {
        let mut sessions = setup(1).unwrap();

        assert!(
            sessions
                .replay_stage
                .replay_to_pack
                .try_write(ReplayToPackMessage {
                    tag: replay_to_pack_message_types::BANK,
                    payload: ReplayToPackMessagePayload {
                        bank: ReplayBankMessage {
                            kind: replay_bank_message_kinds::BEGIN,
                            slot: 42,
                            bank_id: 42,
                            last_entry_hash: [0; 32],
                        },
                    },
                })
                .is_ok()
        );
        assert!(
            sessions
                .replay_stage
                .replay_to_pack
                .try_write(ReplayToPackMessage {
                    tag: replay_to_pack_message_types::ENTRY_HEADER,
                    payload: ReplayToPackMessagePayload {
                        entry_header: EntryHeader {
                            slot: 42,
                            replay_send_time: Instant::now(),
                            num_hashes: 1,
                            hash: [7; 32],
                            num_transactions: 0,
                        },
                    },
                })
                .is_ok()
        );
        sessions.replay_stage.replay_to_pack.commit();

        sessions.block_verification_stage.replay_to_pack.sync();
        assert_eq!(
            sessions
                .block_verification_stage
                .replay_to_pack
                .try_read()
                .map(|message| message.tag),
            Some(replay_to_pack_message_types::BANK)
        );
        assert_eq!(
            sessions
                .block_verification_stage
                .replay_to_pack
                .try_read()
                .map(|message| message.tag),
            Some(replay_to_pack_message_types::ENTRY_HEADER)
        );
        sessions.block_verification_stage.replay_to_pack.finalize();
    }

    #[test]
    fn setup_wires_block_verification_stage_to_replay_stage_status_queue() {
        let mut sessions = setup(1).unwrap();

        let status = ReplayBlockStatusMessage {
            slot: 42,
            status: replay_block_status_codes::SUCCESS,
            reason: replay_block_status_reasons::NONE,
        };
        sessions
            .block_verification_stage
            .replay_block_status
            .try_write(status)
            .unwrap();
        sessions
            .block_verification_stage
            .replay_block_status
            .commit();

        sessions.replay_stage.replay_block_status.sync();
        assert_eq!(
            sessions
                .replay_stage
                .replay_block_status
                .try_read()
                .copied(),
            Some(status)
        );
        sessions.replay_stage.replay_block_status.finalize();
    }

    #[test]
    fn setup_wires_block_verification_worker_queues() {
        let mut sessions = setup(2).unwrap();

        assert_eq!(sessions.block_verification_stage.workers.len(), 2);
        assert_eq!(sessions.workers.len(), 2);

        let pack_to_worker = PackToWorkerMessage {
            flags: pack_message_flags::CHECK,
            max_working_slot: 42,
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: 0,
            },
        };
        sessions.block_verification_stage.workers[0]
            .pack_to_worker
            .try_write(pack_to_worker)
            .unwrap();
        sessions.block_verification_stage.workers[0]
            .pack_to_worker
            .commit();

        sessions.workers[0].pack_to_worker.sync();
        assert_eq!(
            sessions.workers[0].pack_to_worker.try_read().copied(),
            Some(pack_to_worker)
        );
        sessions.workers[0].pack_to_worker.finalize();

        let worker_to_pack = WorkerToPackMessage {
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: 0,
            },
            processed_code: processed_codes::PROCESSED,
            responses: TransactionResponseRegion {
                tag: 0,
                num_transaction_responses: 0,
                transaction_responses_offset: 0,
            },
        };
        sessions.workers[1]
            .worker_to_pack
            .try_write(worker_to_pack)
            .unwrap();
        sessions.workers[1].worker_to_pack.commit();

        sessions.block_verification_stage.workers[1]
            .worker_to_pack
            .sync();
        assert_eq!(
            sessions.block_verification_stage.workers[1]
                .worker_to_pack
                .try_read()
                .copied(),
            Some(worker_to_pack)
        );
        sessions.block_verification_stage.workers[1]
            .worker_to_pack
            .finalize();
    }

    #[test]
    fn setup_wires_signature_verification_queues_and_read_only_allocator() {
        let sessions = setup(1).unwrap();
        let transaction_bytes = [1, 2, 3, 4];
        let transaction_ptr = sessions
            .replay_stage
            .allocator
            .allocate(transaction_bytes.len().try_into().unwrap())
            .unwrap();
        unsafe {
            // SAFETY: `transaction_ptr` references an allocation at least
            // `transaction_bytes.len()` bytes long.
            transaction_ptr
                .as_ptr()
                .copy_from_nonoverlapping(transaction_bytes.as_ptr(), transaction_bytes.len());
        }
        let transaction = SharableTransactionRegion {
            // SAFETY: `transaction_ptr` was allocated by this allocator above.
            offset: unsafe { sessions.replay_stage.allocator.offset(transaction_ptr) },
            length: transaction_bytes.len() as u32,
        };
        let request = SignatureVerificationRequest {
            slot: 42,
            bank_id: 43,
            transaction_index: 7,
            transaction,
        };

        sessions
            .block_verification_stage
            .signature_verification_requests
            .try_write(request)
            .unwrap();
        let worker_request = sessions.signature_verification_workers[0]
            .requests
            .try_read()
            .expect("signature verification worker should receive request");
        // SAFETY: The request references the transaction region allocated
        // above in the same shared allocator mapping.
        let worker_transaction_ptr = unsafe {
            sessions.signature_verification_workers[0]
                .allocator
                .ptr_from_offset(worker_request.transaction.offset)
        };
        // SAFETY: The request carries the length of the transaction region
        // allocated above.
        let worker_transaction_bytes = unsafe {
            core::slice::from_raw_parts(
                worker_transaction_ptr.as_ptr(),
                worker_request.transaction.length as usize,
            )
        };
        assert_eq!(worker_transaction_bytes, transaction_bytes);

        let result = SignatureVerificationResult::new(
            worker_request.slot,
            worker_request.transaction_index,
            true,
        );
        sessions.signature_verification_workers[0]
            .results
            .try_write(result)
            .unwrap();
        assert_eq!(
            sessions
                .block_verification_stage
                .signature_verification_results
                .try_read(),
            Some(result),
        );
    }
}
