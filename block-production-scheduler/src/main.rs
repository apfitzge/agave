#![allow(clippy::arithmetic_side_effects)]

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
use jemallocator::Jemalloc;
use {
    agave_block_production_scheduler::{SchedulerConfig, run},
    clap::{App, Arg},
    std::{
        process::exit,
        sync::{Arc, atomic::AtomicBool},
    },
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let exit_signal = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&exit_signal))
        .unwrap_or_else(|error| {
            eprintln!("failed to register SIGINT handler: {error}");
            exit(1);
        });

    let matches = App::new("agave-block-production-scheduler")
        .about("Run the external Agave block-production scheduler")
        .arg(
            Arg::with_name("bindings-ipc")
                .long("bindings-ipc")
                .value_name("PATH")
                .help("Path to the scheduler-bindings Unix socket")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let config = SchedulerConfig::new(matches.value_of_os("bindings-ipc").unwrap());
    if let Err(error) = run(config, exit_signal) {
        eprintln!("block-production-scheduler failed: {error}");
        exit(1);
    }
}
