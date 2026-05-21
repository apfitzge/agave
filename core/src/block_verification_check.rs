use {
    crate::banking_stage::{
        emit_replay_check_worker_batch_event, emit_replay_check_worker_transaction_event,
        process_replay_check_message,
    },
    agave_block_verification_stage::setup::{
        CheckWorkerResult, CheckWorkerSession, ReplayEventBroadcast, ReplayEventBuffer,
    },
    agave_scheduler_bindings::processed_codes,
    agave_scheduling_utils::replay_events::replay_event_tags,
    solana_runtime::bank_forks::BankForks,
    std::{
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const STARTING_SLEEP_DURATION: Duration = Duration::from_micros(250);
const MAX_SLEEP_DURATION: Duration = Duration::from_millis(1);
const IDLE_SLEEP_THRESHOLD: Duration = Duration::from_millis(10);
const REQUEST_WAIT_TIMEOUT: Duration = Duration::from_millis(1);

pub(crate) fn spawn_replay_check_workers(
    exit: Arc<AtomicBool>,
    workers: Vec<CheckWorkerSession>,
    bank_forks: Arc<RwLock<BankForks>>,
    event_broadcast: Option<Arc<ReplayEventBroadcast>>,
) -> Vec<JoinHandle<()>> {
    workers
        .into_iter()
        .enumerate()
        .map(|(worker_id, worker)| {
            let exit = exit.clone();
            let bank_forks = bank_forks.clone();
            let event_broadcast = event_broadcast.clone();
            Builder::new()
                .name(format!("solBvCheck{worker_id:02}"))
                .spawn(move || {
                    run_check_worker(exit, worker, worker_id, bank_forks, event_broadcast);
                })
                .unwrap()
        })
        .collect()
}

fn run_check_worker(
    exit: Arc<AtomicBool>,
    worker: CheckWorkerSession,
    worker_id: usize,
    bank_forks: Arc<RwLock<BankForks>>,
    event_broadcast: Option<Arc<ReplayEventBroadcast>>,
) {
    let mut event_buffer = ReplayEventBuffer::new(event_broadcast);

    while !exit.load(Ordering::Relaxed) {
        worker.allocator.clean_remote_free_lists();
        let message = match worker.requests.read_timeout(REQUEST_WAIT_TIMEOUT) {
            Ok(message) => message,
            Err(shaq::error::WaitError::Timeout) => continue,
        };

        emit_replay_check_worker_transaction_event(
            &worker.allocator,
            &mut event_buffer,
            worker_id
                .try_into()
                .expect("check worker id must fit in u32"),
            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
            &message,
        );
        let response = match process_replay_check_message(
            worker_id
                .try_into()
                .expect("check worker id must fit in u32"),
            &worker.allocator,
            &bank_forks,
            &mut event_buffer,
            &message,
        ) {
            Ok(response) => response,
            Err(err) => {
                event_buffer.flush();
                error!("Replay check worker error; err={err}");
                continue;
            }
        };
        if response.processed_code == processed_codes::PROCESSED {
            emit_replay_check_worker_batch_event(
                &worker.allocator,
                &mut event_buffer,
                worker_id
                    .try_into()
                    .expect("check worker id must fit in u32"),
                replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED,
                response.batch,
            );
        }
        send_result(
            &exit,
            &worker,
            CheckWorkerResult {
                worker_id,
                message: response,
            },
        );
        event_buffer.flush();
    }
}

fn send_result(exit: &AtomicBool, worker: &CheckWorkerSession, mut result: CheckWorkerResult) {
    let mut sleep_duration = STARTING_SLEEP_DURATION;
    let mut last_full_time = Instant::now();
    while !exit.load(Ordering::Relaxed) {
        match worker.results.try_write(result) {
            Ok(()) => return,
            Err(returned_result) => {
                result = returned_result;
                sleep_duration = backoff(last_full_time.elapsed(), sleep_duration);
                last_full_time = Instant::now();
            }
        }
    }
}

fn backoff(idle_duration: Duration, sleep_duration: Duration) -> Duration {
    if idle_duration < IDLE_SLEEP_THRESHOLD {
        core::hint::spin_loop();
        thread::yield_now();
        sleep_duration
    } else {
        thread::sleep(sleep_duration);
        sleep_duration.saturating_mul(2).min(MAX_SLEEP_DURATION)
    }
}
