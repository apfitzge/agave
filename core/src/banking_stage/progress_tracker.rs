use {
    agave_scheduler_bindings::ProgressMessage,
    rts_alloc::Allocator,
    shaq::Producer,
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

fn setup() -> Option<(Allocator, Producer<ProgressMessage>)> {
    const ALLOCATOR_PATH: &str = "/mnt/hugepages/rts-alloc";
    const ALLOCATOR_WORKER_ID: u32 = 2;

    const PROGRESS_TRACKER_TO_PACK_PATH: &str = "/mnt/hugepages/progress_tracker_to_pack";

    let allocator = Allocator::join(ALLOCATOR_PATH, ALLOCATOR_WORKER_ID)
        .map_err(|e| {
            error!("Failed to join allocator: {e:?}");
        })
        .ok()?;

    let producer = Producer::join(PROGRESS_TRACKER_TO_PACK_PATH)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some((allocator, producer))
}

pub struct ProgressTracker {
    exit_signal: Arc<AtomicBool>,
    cached_leader_progress: CachedLeaderProgress,

    _allocator: Allocator,
    producer: Producer<ProgressMessage>,
}

impl ProgressTracker {
    pub fn new(
        exit_signal: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Option<Self> {
        let (allocator, producer) = setup()?;
        Some(Self {
            exit_signal,
            cached_leader_progress: CachedLeaderProgress::new(poh_recorder),
            _allocator: allocator,
            producer,
        })
    }

    pub fn run(&mut self) {
        while !self.exit_signal.load(Ordering::Relaxed) {
            let _progress = self.progress();
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn progress(&mut self) -> Option<(Slot, i16)> {
        let (progress, updated) = self.cached_leader_progress.get();
        if updated {
            if let Some((slot, progress)) = progress {
                log::info!("current progress: slot {}, progress {}", slot, progress);
                self.producer.sync();
                if let Some(mut scheduler_message) = self.producer.reserve() {
                    // SAFETY: reserved safely
                    let message = unsafe { scheduler_message.as_mut() };
                    message.slot = slot;
                    message.progress = progress;
                    message.total_compute_units = 0; // TODO: set this properly
                }
                self.producer.commit();
            } else {
                log::info!("no progress available");
            }
        }

        progress
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
