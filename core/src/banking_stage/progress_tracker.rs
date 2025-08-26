use {
    agave_scheduler_bindings::ProgressMessage,
    shaq::Producer,
    solana_clock::DEFAULT_TICKS_PER_SLOT,
    solana_poh::poh_recorder::{SharedLeaderFirstTickHeight, SharedTickHeight, SharedWorkingBank},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
};

fn setup() -> Option<Producer<ProgressMessage>> {
    const PROGRESS_TRACKER_TO_PACK_PATH: &str = "/mnt/hugepages/progress_tracker_to_pack";

    let producer = Producer::join(PROGRESS_TRACKER_TO_PACK_PATH)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some(producer)
}

pub struct ProgressTracker {
    exit_signal: Arc<AtomicBool>,

    shared_working_bank: SharedWorkingBank,
    shared_tick_height: SharedTickHeight,
    shared_leader_first_tick_height: SharedLeaderFirstTickHeight,

    ticks_per_slot: u64,
    producer: Producer<ProgressMessage>,
}

impl ProgressTracker {
    pub fn new(
        exit_signal: Arc<AtomicBool>,
        shared_working_bank: SharedWorkingBank,
        shared_tick_height: SharedTickHeight,
        shared_leader_first_tick_height: SharedLeaderFirstTickHeight,
    ) -> Option<Self> {
        let producer = setup()?;
        Some(Self {
            exit_signal,
            shared_working_bank,
            shared_tick_height,
            shared_leader_first_tick_height,
            ticks_per_slot: DEFAULT_TICKS_PER_SLOT,
            producer,
        })
    }

    pub fn run(&mut self) {
        let mut last_progress_message = self.get_progress_message();
        while !self.exit_signal.load(Ordering::Relaxed) {
            let progress_message = self.get_progress_message();
            if self.progress_changed(&last_progress_message, &progress_message) {
                self.producer.sync();
                let Some(message) = self.producer.reserve() else {
                    warn!("ProgressTracker: failed to reserve space in the queue");
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                };

                // copy the message into last_progress_message for next comparison
                last_progress_message = ProgressMessage {
                    slot: progress_message.slot,
                    progress: progress_message.progress,
                    total_compute_units: progress_message.total_compute_units,
                };

                // SAFETY: `message` is a valid pointer to a `ProgressMessage`.
                unsafe {
                    message.write(progress_message);
                }

                self.producer.commit();
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn get_progress_message(&mut self) -> ProgressMessage {
        if let Some(working_bank) = self.shared_working_bank.load() {
            self.ticks_per_slot = working_bank.ticks_per_slot(); // update ticks per slot
            ProgressMessage {
                slot: working_bank.slot(),
                progress: Self::calculate_leader_progress(
                    working_bank.tick_height(),
                    working_bank.max_tick_height(),
                    self.ticks_per_slot,
                ),
                total_compute_units: working_bank.read_cost_tracker().unwrap().block_cost(), // TODO avoid lock here.
            }
        } else if let Some(leader_first_tick_height) = self.shared_leader_first_tick_height.load() {
            ProgressMessage {
                slot: leader_first_tick_height / self.ticks_per_slot,
                progress: Self::calculate_progress_until_leader(
                    self.shared_tick_height.load(),
                    leader_first_tick_height,
                    self.ticks_per_slot,
                ),
                total_compute_units: 0,
            }
        } else {
            ProgressMessage {
                slot: u64::MAX,
                progress: 0,
                total_compute_units: 0,
            }
        }
    }

    fn calculate_leader_progress(
        current_tick_height: u64,
        max_tick_height: u64,
        ticks_per_slot: u64,
    ) -> i16 {
        let progress = ticks_per_slot
            .saturating_sub(max_tick_height.saturating_sub(current_tick_height))
            .saturating_mul(100)
            .saturating_div(ticks_per_slot);
        progress.try_into().unwrap_or(100)
    }

    fn calculate_progress_until_leader(
        current_tick_height: u64,
        leader_first_tick_height: u64,
        ticks_per_slot: u64,
    ) -> i16 {
        let ticks_until_leader = leader_first_tick_height.saturating_sub(current_tick_height);
        let progress = ticks_until_leader
            .saturating_mul(100)
            .saturating_div(ticks_per_slot)
            .try_into()
            .unwrap_or(100);
        -progress
    }

    fn progress_changed(&self, a: &ProgressMessage, b: &ProgressMessage) -> bool {
        a.slot != b.slot
            || a.progress != b.progress
            || a.total_compute_units != b.total_compute_units
    }
}
