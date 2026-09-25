use {
    super::*,
    crate::tpu_ingress::{MAX_PACKETS_PER_CHECK_BATCH, MAX_TPU_PACKETS_PER_ITERATION},
    agave_scheduler_handshake::{AgaveSession, server::Server, setup_local_session},
    std::{io::ErrorKind, path::Path, sync::Arc, thread},
};

fn config(path: &Path) -> Config {
    Config {
        ipc_path: path.to_path_buf(),
        handshake_timeout: Duration::from_secs(1),
        worker_count: 2,
        check_worker_count: 3,
        allocator_size: 64 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 16,
        progress_tracker_capacity: 32,
        pack_to_worker_capacity: 64,
        worker_to_pack_capacity: 128,
        pack_to_check_worker_capacity: 256,
        check_worker_to_pack_capacity: 512,
        transaction_state_capacity: NonZeroUsize::new(512).unwrap(),
        writable_accounts_capacity: 512,
        execution_margin: Duration::from_millis(10),
    }
}

pub(super) fn setup(check_capacity: usize) -> (Scheduler, AgaveSession) {
    let (agave, client) = setup_local_session(ClientLogon {
        worker_count: 1,
        check_worker_count: 1,
        allocator_size: 16 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: MAX_TPU_PACKETS_PER_ITERATION
            .get()
            .saturating_add(MAX_PACKETS_PER_CHECK_BATCH)
            .saturating_add(1)
            .next_power_of_two(),
        progress_tracker_capacity: 2,
        pack_to_worker_capacity: 2,
        worker_to_pack_capacity: 2,
        pack_to_check_worker_capacity: check_capacity,
        check_worker_to_pack_capacity: check_capacity,
        flags: 0,
    })
    .unwrap();
    let mut scheduler = Scheduler::new(
        client,
        NonZeroUsize::new(512).unwrap(),
        512,
        Duration::from_millis(10),
    );
    scheduler.state = SchedulerState::LeaderReady {
        slot: 100,
        remaining_cost_units: 0,
        remaining_allocated_accounts_data_size: 0,
        slot_start: Instant::now(),
        slot_duration: Duration::from_millis(400),
    };
    (scheduler, agave)
}

#[test]
fn resets_costs_only_when_entering_a_new_slot_without_outstanding_work() {
    let (mut scheduler, _agave) = setup(2);
    let start = Instant::now();
    scheduler.state = SchedulerState::LeaderReady {
        slot: 100,
        remaining_cost_units: 100,
        remaining_allocated_accounts_data_size: 50,
        slot_start: start,
        slot_duration: Duration::from_millis(400),
    };
    scheduler.handle_slot_change();
    let limits = scheduler.cost_tracker.get_limits();
    assert_eq!(limits, CostTrackerLimits::new(40, 100, 50));
    assert_eq!(
        scheduler.pacing_budget(start + Duration::from_millis(195)),
        50
    );
    scheduler
        .cost_tracker
        .try_add_cost(10, 5, core::iter::empty())
        .unwrap();

    scheduler.state = SchedulerState::LeaderReady {
        slot: 100,
        remaining_cost_units: 90,
        remaining_allocated_accounts_data_size: 45,
        slot_start: Instant::now(),
        slot_duration: Duration::from_millis(400),
    };
    scheduler.handle_slot_change();
    assert_eq!(scheduler.cost_tracker.get_limits(), limits);
    assert_eq!(scheduler.cost_tracker.block_cost(), 10);
    assert_eq!(
        scheduler.pacing_budget(start + Duration::from_millis(195)),
        40
    );

    scheduler.cost_tracker.add_transactions_in_flight(1);
    scheduler.state = SchedulerState::LeaderReady {
        slot: 101,
        remaining_cost_units: 200,
        remaining_allocated_accounts_data_size: 75,
        slot_start: Instant::now(),
        slot_duration: Duration::from_millis(400),
    };
    scheduler.handle_slot_change();
    assert_eq!(scheduler.scheduling_slot, Some(100));
    assert_eq!(
        scheduler.pacing_budget(start + Duration::from_millis(390)),
        0
    );
    assert_eq!(scheduler.cost_tracker.block_cost(), 10);

    scheduler.cost_tracker.sub_transactions_in_flight(1);
    scheduler.handle_slot_change();
    assert_eq!(scheduler.scheduling_slot, Some(101));
    assert_eq!(scheduler.cost_tracker.block_cost(), 0);
    assert_eq!(
        scheduler.cost_tracker.get_limits(),
        CostTrackerLimits::new(80, 200, 75)
    );
}

#[test]
fn connection_failure_is_returned() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(&directory.path().join("missing.ipc"));
    let error = run(config, &AtomicBool::new(false)).unwrap_err();
    assert!(
        matches!(error, ClientHandshakeError::Io(error) if error.kind() == ErrorKind::NotFound)
    );
}

#[test]
fn handshake_then_run_until_exit() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(&directory.path().join("scheduler.ipc"));
    let worker_count = config.worker_count;
    let check_worker_count = config.check_worker_count;
    let mut server = Server::new(&config.ipc_path).unwrap();
    let exit = Arc::new(AtomicBool::new(false));
    let scheduler_exit = exit.clone();
    let scheduler_thread = thread::spawn(move || run(config, &scheduler_exit));

    let session = server.accept();
    exit.store(true, Ordering::Relaxed);
    scheduler_thread.join().unwrap().unwrap();

    let session = session.unwrap();
    assert_eq!(session.flags, 0);
    assert_eq!(session.workers.len(), worker_count);
    assert_eq!(session.check_workers.len(), check_worker_count);
}
