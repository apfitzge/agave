#![cfg(target_os = "linux")]

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
use jemallocator::Jemalloc;
use {
    agave_block_production_scheduler::{SchedulerConfig, run as run_scheduler},
    agave_block_production_scheduler_load_test::{
        GreedyHarness, GreedyTpuInjectorError, Harness, HarnessConfig, LoadTestScenario,
        TransferScenario, run_scenario,
    },
    agave_scheduling_utils::handshake::{MAX_WORKERS, server::Server},
    clap::{App, Arg},
    log::info,
    std::{
        num::NonZeroUsize,
        process::exit,
        sync::{Arc, atomic::AtomicBool},
        thread,
        time::Duration,
    },
    tempfile::TempDir,
    thiserror::Error,
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, Error)]
enum RunnerError {
    #[error("slot duration must be a non-zero number of milliseconds")]
    InvalidSlotDuration,
    #[error("expired blockhash percentage must be an integer from 0 to 100")]
    InvalidExpiredBlockhashPercent,
    #[error("worker count must be an integer in 1..={MAX_WORKERS}")]
    InvalidWorkerCount,
    #[error("failed to create a temporary scheduler socket: {0}")]
    TemporarySocket(#[source] std::io::Error),
    #[error("failed to create the scheduler socket: {0}")]
    SchedulerSocket(#[source] std::io::Error),
    #[error("failed to launch the scheduler thread: {0}")]
    LaunchSchedulerThread(#[source] std::io::Error),
    #[error("scheduler handshake failed: {0}")]
    Handshake(#[from] agave_scheduling_utils::handshake::AgaveHandshakeError),
    #[error("scheduler failed: {0}")]
    Scheduler(#[from] agave_block_production_scheduler::SchedulerError),
    #[error("scheduler thread panicked")]
    SchedulerPanicked,
    #[error("failed to start the load-test harness: {0}")]
    Harness(#[from] agave_block_production_scheduler_load_test::HarnessError),
    #[error("failed to inject a generated transaction: {0}")]
    Injection(#[from] agave_block_production_scheduler_load_test::TpuInjectorError),
    #[error("failed to inject a generated transaction into the greedy scheduler: {0}")]
    GreedyInjection(#[from] GreedyTpuInjectorError),
}

fn main() {
    let exit_signal = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&exit_signal))
        .unwrap_or_else(|error| {
            eprintln!("failed to register SIGINT handler: {error}");
            exit(1);
        });
    agave_logger::setup_with_default_filter();

    if let Err(error) = run(exit_signal) {
        eprintln!("block-production-scheduler load test failed: {error}");
        exit(1);
    }
}

fn run(exit_signal: Arc<AtomicBool>) -> Result<(), RunnerError> {
    let matches = App::new("agave-block-production-scheduler-load-test")
        .about("Continuously load test Agave block-production schedulers")
        .arg(
            Arg::with_name("scheduler")
                .long("scheduler")
                .value_name("SCHEDULER")
                .help("Scheduler to run: external or in-process greedy")
                .takes_value(true)
                .possible_values(&["external", "greedy"])
                .default_value("external"),
        )
        .arg(
            Arg::with_name("slot-ms")
                .long("slot-ms")
                .value_name("MILLISECONDS")
                .help("Target leader-slot duration")
                .takes_value(true)
                .default_value("400"),
        )
        .arg(
            Arg::with_name("expired-blockhash-percent")
                .long("expired-blockhash-percent")
                .value_name("PERCENT")
                .help("Percentage of generated transactions with a non-recent blockhash")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::with_name("execution-worker-count")
                .long("execution-worker-count")
                .value_name("COUNT")
                .help("Number of execution workers")
                .takes_value(true)
                .default_value("8"),
        )
        .arg(
            Arg::with_name("check-worker-count")
                .long("check-worker-count")
                .value_name("COUNT")
                .help("Number of check workers for the external scheduler")
                .takes_value(true)
                .default_value("8"),
        )
        .arg(
            Arg::with_name("unlimited-cost-limits")
                .long("unlimited-cost-limits")
                .help("Set all bank cost limits to u64::MAX"),
        )
        .get_matches();

    let slot_duration = matches
        .value_of("slot-ms")
        .expect("clap supplies --slot-ms")
        .parse::<u64>()
        .ok()
        .filter(|milliseconds| *milliseconds != 0)
        .map(Duration::from_millis)
        .ok_or(RunnerError::InvalidSlotDuration)?;
    let expired_blockhash_percent = matches
        .value_of("expired-blockhash-percent")
        .expect("clap supplies --expired-blockhash-percent")
        .parse::<u8>()
        .ok()
        .filter(|percent| *percent <= 100)
        .ok_or(RunnerError::InvalidExpiredBlockhashPercent)?;
    let execution_worker_count = parse_worker_count(
        matches
            .value_of("execution-worker-count")
            .expect("clap supplies --execution-worker-count"),
    )?;
    let mut scenario = TransferScenario::new(expired_blockhash_percent);
    let harness_config = HarnessConfig {
        slot_duration,
        unlimited_cost_limits: matches.is_present("unlimited-cost-limits"),
        ..HarnessConfig::default()
    };
    match matches
        .value_of("scheduler")
        .expect("clap supplies --scheduler")
    {
        "external" => {
            let check_worker_count = parse_worker_count(
                matches
                    .value_of("check-worker-count")
                    .expect("clap supplies --check-worker-count"),
            )?;
            run_external(
                exit_signal,
                harness_config,
                execution_worker_count,
                check_worker_count,
                &mut scenario,
            )
        }
        "greedy" => run_greedy(
            exit_signal,
            harness_config,
            NonZeroUsize::new(execution_worker_count)
                .expect("worker count validation rejects zero"),
            &mut scenario,
        ),
        _ => unreachable!("clap restricts --scheduler to known values"),
    }
}

fn run_external(
    exit_signal: Arc<AtomicBool>,
    harness_config: HarnessConfig,
    execution_worker_count: usize,
    check_worker_count: usize,
    scenario: &mut TransferScenario,
) -> Result<(), RunnerError> {
    let socket_directory = TempDir::new().map_err(RunnerError::TemporarySocket)?;
    let socket = socket_directory.path().join("scheduler-bindings.ipc");
    let mut server = Server::new(&socket).map_err(RunnerError::SchedulerSocket)?;
    let mut scheduler_config = SchedulerConfig::new(socket);
    scheduler_config.execution_worker_count = execution_worker_count;
    scheduler_config.check_worker_count = check_worker_count;
    let scheduler_exit = Arc::clone(&exit_signal);
    let scheduler = thread::Builder::new()
        .name("solLoadScheduler".to_string())
        .spawn(move || run_scheduler(scheduler_config, scheduler_exit))
        .map_err(RunnerError::LaunchSchedulerThread)?;
    let session = server.accept()?;
    let mut harness = Harness::start(session, harness_config, exit_signal, |bank| {
        scenario.setup(bank)
    })?;
    let result = run_scenario(&mut harness, scenario, report_transactions_sent);
    harness.shutdown();
    let scheduler_result = scheduler
        .join()
        .map_err(|_| RunnerError::SchedulerPanicked)?;
    result?;
    scheduler_result?;
    Ok(())
}

fn run_greedy(
    exit_signal: Arc<AtomicBool>,
    harness_config: HarnessConfig,
    execution_worker_count: NonZeroUsize,
    scenario: &mut TransferScenario,
) -> Result<(), RunnerError> {
    let mut harness = GreedyHarness::start(
        harness_config,
        execution_worker_count,
        exit_signal,
        |bank| scenario.setup(bank),
    )?;
    let result = run_scenario(&mut harness, scenario, report_transactions_sent);
    harness.shutdown();
    result?;
    Ok(())
}

fn report_transactions_sent(transactions_sent_per_second: u64) {
    info!("transactions_sent_per_second={transactions_sent_per_second}");
}

fn parse_worker_count(value: &str) -> Result<usize, RunnerError> {
    value
        .parse()
        .ok()
        .filter(|count| (1..=MAX_WORKERS).contains(count))
        .ok_or(RunnerError::InvalidWorkerCount)
}
