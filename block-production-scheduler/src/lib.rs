#![cfg(unix)]
//! External transaction scheduler implementation for Agave scheduler bindings.

use {
    agave_scheduling_utils::{
        bridge::SchedulerBindingsBridge,
        handshake::{ClientHandshakeError, client},
    },
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    },
};

mod config;

pub use config::{ConfigError, SchedulerConfig};

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("invalid scheduler config: {0}")]
    Config(#[from] ConfigError),
    #[error("scheduler bindings handshake failed: {0}")]
    Handshake(#[from] ClientHandshakeError),
}

/// Connect to Agave's scheduler bindings service and run until `exit` is set.
pub fn run(config: SchedulerConfig, exit: Arc<AtomicBool>) -> Result<(), SchedulerError> {
    config.validate()?;
    if exit.load(Ordering::Relaxed) {
        return Ok(());
    }

    let session = client::connect(
        &config.bindings_ipc,
        config.client_logon(),
        config.handshake_timeout,
    )?;
    let _bridge = SchedulerBindingsBridge::<()>::new(session);

    while !exit.load(Ordering::Relaxed) {
        thread::yield_now();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*, agave_scheduling_utils::handshake::server::Server, std::sync::atomic::AtomicBool,
        tempfile::NamedTempFile,
    };

    #[test]
    fn exits_without_connecting_when_already_signaled() {
        let exit = Arc::new(AtomicBool::new(true));

        run(SchedulerConfig::new("/does/not/exist"), exit).unwrap();
    }

    #[test]
    fn returns_typed_config_error() {
        let mut config = SchedulerConfig::new("/does/not/exist");
        config.execution_worker_count = 0;

        let error = run(config, Arc::new(AtomicBool::new(false))).unwrap_err();
        assert!(matches!(
            error,
            SchedulerError::Config(ConfigError::ExecutionWorkerCount { count: 0 })
        ));
    }

    #[test]
    fn connects_then_exits() {
        let ipc = NamedTempFile::new().unwrap();
        std::fs::remove_file(ipc.path()).unwrap();
        let mut server = Server::new(ipc.path()).unwrap();
        let mut config = SchedulerConfig::new(ipc.path());
        config.allocator_size = 64 * 1024 * 1024;

        let exit = Arc::new(AtomicBool::new(false));
        let runner_exit = Arc::clone(&exit);
        let runner = thread::spawn(move || run(config, runner_exit));

        let _session = server.accept().unwrap();
        exit.store(true, Ordering::Relaxed);
        runner.join().unwrap().unwrap();
    }
}
