use {
    agave_scheduler_bindings::{
        scheduler_message_types, PackToSchedulerMessage, SchedulerToPackMessage,
        MAX_TRANSACTIONS_PER_MESSAGE,
    },
    rts_alloc::Allocator,
    shaq::{Consumer, Producer},
    solana_clock::Slot,
    solana_poh::poh_recorder::PohRecorder,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
};

fn setup() -> Option<(
    Allocator,
    Consumer<PackToSchedulerMessage>,
    Producer<SchedulerToPackMessage>,
)> {
    const ALLOCATOR_PATH: &str = "/mnt/hugepages/rts-alloc";
    const ALLOCATOR_WORKER_ID: u32 = 2;

    const PACK_TO_SCHEDULER_PATH: &str = "/mnt/hugepages/pack_to_scheduler";
    const SCHEDULER_TO_PACK_PATH: &str = "/mnt/hugepages/scheduler_to_pack";

    let allocator = Allocator::join(ALLOCATOR_PATH, ALLOCATOR_WORKER_ID)
        .map_err(|e| {
            error!("Failed to join allocator: {e:?}");
        })
        .ok()?;

    let consumer = Consumer::join(PACK_TO_SCHEDULER_PATH)
        .map_err(|e| {
            error!("Failed to create consumer: {e:?}");
        })
        .ok()?;
    let producer = Producer::join(SCHEDULER_TO_PACK_PATH)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some((allocator, consumer, producer))
}

pub struct FifoScheduler {
    exit_signal: Arc<AtomicBool>,
    cached_leader_progress: CachedLeaderProgress,

    allocator: Allocator,
    pack_message_consumer: Consumer<PackToSchedulerMessage>,
    scheduler_message_producer: Producer<SchedulerToPackMessage>,
}

impl FifoScheduler {
    pub fn new(
        exit_signal: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Option<Self> {
        let (allocator, consumer, producer) = setup()?;
        Some(Self {
            exit_signal,
            cached_leader_progress: CachedLeaderProgress::new(poh_recorder),
            allocator,
            pack_message_consumer: consumer,
            scheduler_message_producer: producer,
        })
    }

    pub fn run(&mut self) {
        while !self.exit_signal.load(Ordering::Relaxed) {
            self.scheduler_message_producer.sync();
            let _progress = self.progress();
            self.receive_pack_messages();
            self.scheduler_message_producer.commit();
        }
    }

    fn progress(&mut self) -> Option<(Slot, i16)> {
        let (progress, updated) = self.cached_leader_progress.get();
        if updated {
            if let Some((slot, progress)) = progress {
                log::info!("current progress: slot {}, progress {}", slot, progress);
                if let Some(mut scheduler_message) = self.scheduler_message_producer.reserve() {
                    // SAFETY: reserved safely
                    let scheduler_message = unsafe { scheduler_message.as_mut() };
                    scheduler_message.tag = scheduler_message_types::SLOT_STATUS;
                    // SAFETY: writing the message
                    let slot_status = unsafe { &mut scheduler_message.inner.slot_status };
                    slot_status.slot = slot;
                    slot_status.progress = progress;
                }
            } else {
                log::info!("no progress available");
            }
        }

        progress
    }

    fn receive_pack_messages(&mut self) {
        self.pack_message_consumer.sync();

        while let Some(pack_message) = self.pack_message_consumer.try_read() {
            let message = unsafe { pack_message.as_ref() };

            if message.num_transactions == 0
                || message.num_transactions > MAX_TRANSACTIONS_PER_MESSAGE as u8
            {
                continue;
            }

            for (transaction_index, sharable_transaction) in message.transactions
                [..usize::from(message.num_transactions)]
                .iter()
                .enumerate()
            {
                // Pass back message that transaction was dropped.
                let ptr = self
                    .allocator
                    .ptr_from_offset(sharable_transaction.transaction_offset);

                // SAFETY: The pointer is valid as it was allocated by the allocator.
                unsafe {
                    self.allocator.free(ptr);
                }

                if let Some(mut scheduler_message) = self.scheduler_message_producer.reserve() {
                    // SAFETY: reserved safely
                    let scheduler_message = unsafe { scheduler_message.as_mut() };
                    scheduler_message.tag = scheduler_message_types::DROPPED_TRANSACTION;
                    // SAFETY: writing the message
                    let dropped_transaction =
                        unsafe { &mut scheduler_message.inner.dropped_transaction };
                    dropped_transaction.message_id = message.message_id;
                    dropped_transaction.transaction_index = transaction_index as u8;
                }
            }
        }

        self.pack_message_consumer.finalize();
    }
}

struct CachedLeaderProgress {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    progress: Option<(Slot, i16)>,
    last_progress_time: Instant,
}

impl CachedLeaderProgress {
    pub fn new(poh_recorder: Arc<RwLock<PohRecorder>>) -> Self {
        let last_progress_time = Instant::now();
        let progress = poh_recorder.read().unwrap().leader_progress();

        Self {
            poh_recorder,
            progress,
            last_progress_time,
        }
    }

    pub fn get(&mut self) -> (Option<(Slot, i16)>, bool) {
        const CACHE_DURATION: Duration = Duration::from_millis(10);
        let now = Instant::now();
        let mut updated = false;
        if now.duration_since(self.last_progress_time) >= CACHE_DURATION {
            let new_progress = self.poh_recorder.read().unwrap().leader_progress();
            self.progress = new_progress;
            self.last_progress_time = now;
            updated = true;
        }

        (self.progress, updated)
    }
}
