use {
    crate::setup::BlockVerificationStageSession,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    },
};

const IDLE_SLEEP: Duration = Duration::from_millis(1);

/// Main block verification scheduler.
pub struct BlockVerificationScheduler {
    exit: Arc<AtomicBool>,
    session: BlockVerificationStageSession,
}

impl BlockVerificationScheduler {
    pub fn new(exit: Arc<AtomicBool>, session: BlockVerificationStageSession) -> Self {
        Self { exit, session }
    }

    pub fn run(self) {
        let Self {
            exit,
            session: _session,
        } = self;

        while !exit.load(Ordering::Relaxed) {
            thread::sleep(IDLE_SLEEP);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::setup::{BlockVerificationStageSessions, BlockVerificationStageSetupConfig},
    };

    #[test]
    fn run_exits_when_exit_flag_is_set() {
        let sessions = BlockVerificationStageSessions::setup(BlockVerificationStageSetupConfig {
            allocator_size: 64 * 1024 * 1024,
            replay_to_pack_capacity: 8,
            replay_block_status_capacity: 8,
            worker_count: 1,
            pack_to_worker_capacity: 8,
            worker_to_pack_capacity: 8,
        })
        .unwrap();
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler =
            BlockVerificationScheduler::new(exit.clone(), sessions.block_verification_stage);

        let thread_hdl = thread::spawn(move || scheduler.run());
        exit.store(true, Ordering::Relaxed);

        thread_hdl.join().unwrap();
    }
}
