use {
    agave_scheduler_bindings::{PackToSchedulerMessage, SchedulerToPackMessage},
    rts_alloc::Allocator,
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub struct ConsumerWorkerForExternal {
    exit: Arc<AtomicBool>,
    _allocator: Allocator,
    _consumer: shaq::Consumer<PackToSchedulerMessage>,
    _producer: shaq::Producer<SchedulerToPackMessage>,
}

impl ConsumerWorkerForExternal {
    pub fn new(worker_index: u32, exit: Arc<AtomicBool>) -> Option<Self> {
        let (allocator, consumer, producer) = setup(worker_index)?;
        Some(Self {
            exit,
            _allocator: allocator,
            _consumer: consumer,
            _producer: producer,
        })
    }

    pub fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {}
    }
}

fn setup(
    worker_index: u32,
) -> Option<(
    Allocator,
    shaq::Consumer<PackToSchedulerMessage>,
    shaq::Producer<SchedulerToPackMessage>,
)> {
    const ALLOCATOR_PATH: &str = "/mnt/hugepages/rts-alloc";
    const ALLOCATOR_WORKER_STARTING_ID: u32 = 4;
    let allocator_id = worker_index + ALLOCATOR_WORKER_STARTING_ID;

    const PACK_TO_WORKER_DIR: &str = "/mnt/hugepages/pack_to_worker";
    const WORKER_TO_PACK_DIR: &str = "/mnt/hugepages/worker_to_pack";

    let pack_to_worker_path = format!("{PACK_TO_WORKER_DIR}/{worker_index}");
    let worker_to_pack_path = format!("{WORKER_TO_PACK_DIR}/{worker_index}");

    let allocator = Allocator::join(ALLOCATOR_PATH, allocator_id)
        .map_err(|e| {
            error!("Failed to join allocator: {e:?}");
        })
        .ok()?;

    let consumer = shaq::Consumer::join(pack_to_worker_path)
        .map_err(|e| {
            error!("Failed to create consumer: {e:?}");
        })
        .ok()?;
    let producer = shaq::Producer::join(worker_to_pack_path)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some((allocator, consumer, producer))
}
