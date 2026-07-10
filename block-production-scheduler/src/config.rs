use {
    agave_scheduling_utils::handshake::{ClientLogon, MAX_WORKERS},
    std::{path::PathBuf, time::Duration},
    thiserror::Error,
};

const DEFAULT_EXECUTION_WORKER_COUNT: usize = 4;
const DEFAULT_CHECK_WORKER_COUNT: usize = 8;
const DEFAULT_ALLOCATOR_SIZE: usize = 8 * 1024 * 1024 * 1024;
const DEFAULT_TPU_TO_PACK_CAPACITY: usize = 1 << 17;
const DEFAULT_PROGRESS_TRACKER_CAPACITY: usize = 128;
const DEFAULT_PACK_TO_WORKER_CAPACITY: usize = 256;
const DEFAULT_WORKER_TO_PACK_CAPACITY: usize = 256;
const DEFAULT_PACK_TO_CHECK_WORKER_CAPACITY: usize = 1 << 13;
const DEFAULT_CHECK_WORKER_TO_PACK_CAPACITY: usize = 1 << 13;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// An invalid [`SchedulerConfig`] value.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("bindings_ipc must not be empty")]
    EmptyBindingsIpc,
    #[error("execution_worker_count must be in 1..={MAX_WORKERS}; got {count}")]
    ExecutionWorkerCount { count: usize },
    #[error("check_worker_count must be in 1..={MAX_WORKERS}; got {count}")]
    CheckWorkerCount { count: usize },
    #[error("allocator_size must be greater than zero")]
    ZeroAllocatorSize,
    #[error("tpu_to_pack_capacity must be a non-zero power of two; got {capacity}")]
    TpuToPackCapacity { capacity: usize },
    #[error("progress_tracker_capacity must be a non-zero power of two; got {capacity}")]
    ProgressTrackerCapacity { capacity: usize },
    #[error("pack_to_worker_capacity must be a non-zero power of two; got {capacity}")]
    PackToWorkerCapacity { capacity: usize },
    #[error("worker_to_pack_capacity must be a non-zero power of two; got {capacity}")]
    WorkerToPackCapacity { capacity: usize },
    #[error("pack_to_check_worker_capacity must be a non-zero power of two; got {capacity}")]
    PackToCheckWorkerCapacity { capacity: usize },
    #[error("check_worker_to_pack_capacity must be a non-zero power of two; got {capacity}")]
    CheckWorkerToPackCapacity { capacity: usize },
    #[error("handshake_timeout must be greater than zero")]
    ZeroHandshakeTimeout,
}

/// Configuration for an external scheduler-bindings session.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Path to Agave's scheduler-bindings Unix socket.
    pub bindings_ipc: PathBuf,
    /// Number of execution worker queues to request.
    pub execution_worker_count: usize,
    /// Number of check workers Agave should start.
    pub check_worker_count: usize,
    /// Size of the shared-memory allocator in bytes.
    pub allocator_size: usize,
    /// Capacity of the TPU-to-scheduler queue.
    pub tpu_to_pack_capacity: usize,
    /// Capacity of the progress queue.
    pub progress_tracker_capacity: usize,
    /// Capacity of each scheduler-to-worker queue.
    pub pack_to_worker_capacity: usize,
    /// Capacity of each worker-to-scheduler queue.
    pub worker_to_pack_capacity: usize,
    /// Capacity of the scheduler-to-check-worker queue.
    pub pack_to_check_worker_capacity: usize,
    /// Capacity of the check-worker-to-scheduler queue.
    pub check_worker_to_pack_capacity: usize,
    /// Timeout for the Unix-socket scheduler-bindings handshake.
    pub handshake_timeout: Duration,
}

impl SchedulerConfig {
    #[must_use]
    pub fn new(bindings_ipc: impl Into<PathBuf>) -> Self {
        Self {
            bindings_ipc: bindings_ipc.into(),
            execution_worker_count: DEFAULT_EXECUTION_WORKER_COUNT,
            check_worker_count: DEFAULT_CHECK_WORKER_COUNT,
            allocator_size: DEFAULT_ALLOCATOR_SIZE,
            tpu_to_pack_capacity: DEFAULT_TPU_TO_PACK_CAPACITY,
            progress_tracker_capacity: DEFAULT_PROGRESS_TRACKER_CAPACITY,
            pack_to_worker_capacity: DEFAULT_PACK_TO_WORKER_CAPACITY,
            worker_to_pack_capacity: DEFAULT_WORKER_TO_PACK_CAPACITY,
            pack_to_check_worker_capacity: DEFAULT_PACK_TO_CHECK_WORKER_CAPACITY,
            check_worker_to_pack_capacity: DEFAULT_CHECK_WORKER_TO_PACK_CAPACITY,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.bindings_ipc.as_os_str().is_empty() {
            return Err(ConfigError::EmptyBindingsIpc);
        }
        validate_worker_count(self.execution_worker_count, |count| {
            ConfigError::ExecutionWorkerCount { count }
        })?;
        validate_worker_count(self.check_worker_count, |count| {
            ConfigError::CheckWorkerCount { count }
        })?;
        if self.allocator_size == 0 {
            return Err(ConfigError::ZeroAllocatorSize);
        }
        validate_queue_capacity(self.tpu_to_pack_capacity, |capacity| {
            ConfigError::TpuToPackCapacity { capacity }
        })?;
        validate_queue_capacity(self.progress_tracker_capacity, |capacity| {
            ConfigError::ProgressTrackerCapacity { capacity }
        })?;
        validate_queue_capacity(self.pack_to_worker_capacity, |capacity| {
            ConfigError::PackToWorkerCapacity { capacity }
        })?;
        validate_queue_capacity(self.worker_to_pack_capacity, |capacity| {
            ConfigError::WorkerToPackCapacity { capacity }
        })?;
        validate_queue_capacity(self.pack_to_check_worker_capacity, |capacity| {
            ConfigError::PackToCheckWorkerCapacity { capacity }
        })?;
        validate_queue_capacity(self.check_worker_to_pack_capacity, |capacity| {
            ConfigError::CheckWorkerToPackCapacity { capacity }
        })?;
        if self.handshake_timeout.is_zero() {
            return Err(ConfigError::ZeroHandshakeTimeout);
        }

        Ok(())
    }

    pub(crate) fn client_logon(&self) -> ClientLogon {
        ClientLogon {
            worker_count: self.execution_worker_count,
            check_worker_count: self.check_worker_count,
            allocator_size: self.allocator_size,
            allocator_handles: 1,
            tpu_to_pack_capacity: self.tpu_to_pack_capacity,
            progress_tracker_capacity: self.progress_tracker_capacity,
            pack_to_worker_capacity: self.pack_to_worker_capacity,
            worker_to_pack_capacity: self.worker_to_pack_capacity,
            flags: 0,
            pack_to_check_worker_capacity: self.pack_to_check_worker_capacity,
            check_worker_to_pack_capacity: self.check_worker_to_pack_capacity,
        }
    }
}

fn validate_worker_count(
    worker_count: usize,
    error: impl FnOnce(usize) -> ConfigError,
) -> Result<(), ConfigError> {
    (1..=MAX_WORKERS)
        .contains(&worker_count)
        .then_some(())
        .ok_or_else(|| error(worker_count))
}

fn validate_queue_capacity(
    capacity: usize,
    error: impl FnOnce(usize) -> ConfigError,
) -> Result<(), ConfigError> {
    (capacity != 0 && capacity.is_power_of_two())
        .then_some(())
        .ok_or_else(|| error(capacity))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_produce_valid_logon() {
        let config = SchedulerConfig::new("/tmp/scheduler-bindings.ipc");

        config.validate().unwrap();
        let logon = config.client_logon();
        assert_eq!(logon.worker_count, DEFAULT_EXECUTION_WORKER_COUNT);
        assert_eq!(logon.check_worker_count, DEFAULT_CHECK_WORKER_COUNT);
        assert_eq!(logon.allocator_handles, 1);
        assert_eq!(
            logon.pack_to_check_worker_capacity,
            DEFAULT_PACK_TO_CHECK_WORKER_CAPACITY
        );
        assert_eq!(
            logon.check_worker_to_pack_capacity,
            DEFAULT_CHECK_WORKER_TO_PACK_CAPACITY
        );
    }

    #[test]
    fn rejects_invalid_queue_capacity() {
        let mut config = SchedulerConfig::new("/tmp/scheduler-bindings.ipc");
        config.tpu_to_pack_capacity = 3;

        assert_eq!(
            config.validate(),
            Err(ConfigError::TpuToPackCapacity { capacity: 3 })
        );
    }
}
