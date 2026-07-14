#![cfg(target_os = "linux")]

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
use jemallocator::Jemalloc;
use {
    agave_block_production_scheduler::{SchedulerConfig, run as run_scheduler},
    agave_block_production_scheduler_load_test::{
        Harness, HarnessConfig, LoadTestScenario, TransferScenario, run_scenario,
    },
    agave_scheduling_utils::handshake::server::Server,
    clap::{App, Arg},
    std::{
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
}

fn main() {
    let exit_signal = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&exit_signal))
        .unwrap_or_else(|error| {
            eprintln!("failed to register SIGINT handler: {error}");
            exit(1);
        });

    if let Err(error) = run(exit_signal) {
        eprintln!("block-production-scheduler load test failed: {error}");
        exit(1);
    }
}

fn run(exit_signal: Arc<AtomicBool>) -> Result<(), RunnerError> {
    let matches = App::new("agave-block-production-scheduler-load-test")
        .about("Continuously load test an external Agave block-production scheduler")
        .arg(
            Arg::with_name("slot-ms")
                .long("slot-ms")
                .value_name("MILLISECONDS")
                .help("Target leader-slot duration")
                .takes_value(true)
                .default_value("400"),
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

    let socket_directory = TempDir::new().map_err(RunnerError::TemporarySocket)?;
    let socket = socket_directory.path().join("scheduler-bindings.ipc");
    let mut server = Server::new(&socket).map_err(RunnerError::SchedulerSocket)?;
    let scheduler_exit = Arc::clone(&exit_signal);
    let scheduler = thread::Builder::new()
        .name("solLoadScheduler".to_string())
        .spawn(move || run_scheduler(SchedulerConfig::new(socket), scheduler_exit))
        .map_err(RunnerError::LaunchSchedulerThread)?;
    let session = server.accept()?;

    let mut scenario = TransferScenario::new();
    let mut harness = Harness::start(
        session,
        HarnessConfig { slot_duration },
        exit_signal,
        |bank| scenario.setup(bank),
    )?;
    let result = run_scenario(&mut harness, &mut scenario, |transactions_sent| {
        eprintln!("transactions_sent_per_second={transactions_sent}");
    });
    harness.shutdown();
    let scheduler_result = scheduler
        .join()
        .map_err(|_| RunnerError::SchedulerPanicked)?;
    result?;
    scheduler_result?;
    Ok(())
}
