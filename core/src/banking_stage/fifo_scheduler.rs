use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct FifoScheduler {
    exit_signal: Arc<AtomicBool>,
}

impl FifoScheduler {
    pub fn new(exit_signal: Arc<AtomicBool>) -> Self {
        Self { exit_signal }
    }

    pub fn run(&mut self) {
        while !self.exit_signal.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
            log::info!("fifo-scheduler");
        }
    }
}
