use {
    agave_scheduler_bindings::{
        PackToWorkerMessage, ReplayBlockStatusMessage, ReplayToPackMessage, WorkerToPackMessage,
    },
    agave_scheduling_utils::shared_memory::{self, SharedMemoryError},
    rts_alloc::Allocator,
    std::fs::File,
};

const SESSION_ALLOCATOR_HANDLES: usize = 2;
const ALLOCATOR_SLAB_SIZE: u32 = 2 * 1024 * 1024;

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

/// Locally created session pair.
pub struct BlockVerificationStageSessions {
    pub block_verification_stage: BlockVerificationStageSession,
    pub replay_stage: ReplayStageSession,
    pub workers: Vec<BlockVerificationWorkerSession>,
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
        let (block_verification_workers, workers) = create_worker_sessions(
            &allocator_file,
            config.worker_count,
            config.pack_to_worker_capacity,
            config.worker_to_pack_capacity,
        )?;

        Ok(Self {
            block_verification_stage: BlockVerificationStageSession {
                allocator: block_verification_allocator,
                replay_to_pack: replay_to_pack_consumer,
                replay_block_status: replay_block_status_producer,
                workers: block_verification_workers,
            },
            replay_stage: ReplayStageSession {
                allocator: replay_stage_allocator,
                replay_to_pack: replay_to_pack_producer,
                replay_block_status: replay_block_status_consumer,
            },
            workers,
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
}
