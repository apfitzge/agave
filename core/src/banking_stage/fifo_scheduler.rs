use {
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

pub struct FifoScheduler {
    exit_signal: Arc<AtomicBool>,
    cached_leader_progress: CachedLeaderProgress,
}

impl FifoScheduler {
    pub fn new(exit_signal: Arc<AtomicBool>, poh_recorder: Arc<RwLock<PohRecorder>>) -> Self {
        Self {
            exit_signal,
            cached_leader_progress: CachedLeaderProgress::new(poh_recorder),
        }
    }

    pub fn run(&mut self) {
        while !self.exit_signal.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5));
            let (progress, updated) = self.cached_leader_progress.get();
            if updated {
                if let Some((slot, progress)) = progress {
                    log::info!("current progress: slot {}, progress {}", slot, progress);
                } else {
                    log::info!("no progress available");
                }
            }
        }
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
