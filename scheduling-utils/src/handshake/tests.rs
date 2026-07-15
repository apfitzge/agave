use {
    crate::handshake::{
        AgaveHandshakeError, ClientHandshakeError, ClientLogon, client::connect, server::Server,
        shared::MAX_WORKERS,
    },
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, ExecutionWorkerToPackMessage, PackToCheckWorkerMessage,
        PackToExecutionWorkerMessage, ProgressMessage, SharableTransactionBatchRegion,
        SharableTransactionRegion, TpuToPackMessage, TransactionResponseRegion,
    },
    std::time::Duration,
    tempfile::NamedTempFile,
};

#[test]
fn message_passing_on_all_queues() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    // Test messages.
    let tpu_to_pack = TpuToPackMessage {
        transaction: SharableTransactionRegion {
            offset: 10,
            length: 5,
        },
        flags: 21,
        src_addr: [4; 16],
    };
    let progress_tracker = ProgressMessage {
        leader_state: agave_scheduler_bindings::LEADER_READY,
        current_slot_progress: 32,
        epoch: 7,
        current_slot: 3,
        next_leader_slot: 12,
        leader_range_end: 16,
        remaining_cost_units: 12_000_000,
        latest_blockhash: [42; 32],
        scheduler_features: agave_scheduler_bindings::scheduler_feature_flags::NONE,
        target_bank_time_ms: 0,
    };
    let pack_to_worker = PackToExecutionWorkerMessage {
        flags: 123,
        max_working_slot: 100,
        batch: SharableTransactionBatchRegion {
            num_transactions: 5,
            transactions_offset: 100,
        },
    };
    let worker_to_pack = ExecutionWorkerToPackMessage {
        batch: SharableTransactionBatchRegion {
            num_transactions: 5,
            transactions_offset: 100,
        },
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: TransactionResponseRegion {
            tag: 3,
            num_transaction_responses: 2,
            transaction_responses_offset: 1,
        },
    };
    let pack_to_check_worker = PackToCheckWorkerMessage {
        flags: 7,
        minimum_priority: 0,
        batch: SharableTransactionBatchRegion {
            num_transactions: 3,
            transactions_offset: 200,
        },
    };
    let check_worker_to_pack = CheckWorkerToPackMessage {
        batch: SharableTransactionBatchRegion {
            num_transactions: 3,
            transactions_offset: 200,
        },
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: TransactionResponseRegion {
            tag: 3,
            num_transaction_responses: 3,
            transaction_responses_offset: 4,
        },
    };

    let server_handle = std::thread::spawn(move || {
        let mut session = server.accept().unwrap();

        // Send a tpu_to_pack message.
        session.tpu_to_pack.producer.try_write(tpu_to_pack).unwrap();
        session.tpu_to_pack.producer.commit();

        // Send a progress_tracker message.
        session
            .progress_tracker
            .try_write(progress_tracker)
            .unwrap();
        session.progress_tracker.commit();

        assert_eq!(session.check_workers.len(), 2);

        // Receive pack_to_check_worker messages.
        let mut check_messages = Vec::new();
        while check_messages.len() < session.check_workers.len() {
            for worker in &session.check_workers {
                if let Some(msg) = worker.pack_to_check_worker.try_read() {
                    check_messages.push(msg);
                }
            }
        }
        assert_eq!(
            check_messages,
            vec![
                pack_to_check_worker,
                PackToCheckWorkerMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: pack_to_check_worker.batch.num_transactions + 1,
                        ..pack_to_check_worker.batch
                    },
                    ..pack_to_check_worker
                }
            ]
        );

        // Send check_worker_to_pack messages.
        for (i, worker) in session.check_workers.iter().enumerate() {
            worker
                .check_worker_to_pack
                .try_write(CheckWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: check_worker_to_pack.batch.num_transactions + i as u8,
                        ..check_worker_to_pack.batch
                    },
                    ..check_worker_to_pack
                })
                .unwrap();
        }

        // Receive pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                worker.pack_to_worker.sync();
                if let Some(msg) = worker.pack_to_worker.try_read() {
                    break *msg;
                }
            };
            assert_eq!(
                PackToExecutionWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                },
                msg
            );
        }

        // Send worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .worker_to_pack
                .try_write(ExecutionWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                })
                .unwrap();
            worker.worker_to_pack.commit();
        }
    });
    let client_handle = std::thread::spawn(move || {
        let mut session = connect(
            ipc,
            ClientLogon {
                worker_count: 4,
                check_worker_count: 2,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        )
        .unwrap();

        // Receive tpu_to_pack message.
        let msg = loop {
            session.tpu_to_pack.sync();
            if let Some(msg) = session.tpu_to_pack.try_read() {
                break *msg;
            };
        };
        assert_eq!(msg, tpu_to_pack);

        // Receive progress_tracker message.
        let msg = loop {
            session.progress_tracker.sync();
            if let Some(msg) = session.progress_tracker.try_read() {
                break *msg;
            };
        };
        assert_eq!(msg, progress_tracker);

        // Send pack_to_check_worker messages.
        for i in 0..2 {
            session
                .pack_to_check_worker
                .try_write(PackToCheckWorkerMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: pack_to_check_worker.batch.num_transactions + i as u8,
                        ..pack_to_check_worker.batch
                    },
                    ..pack_to_check_worker
                })
                .unwrap();
        }

        // Receive check_worker_to_pack messages.
        let mut check_messages = Vec::new();
        while check_messages.len() < 2 {
            if let Some(msg) = session.check_worker_to_pack.try_read() {
                check_messages.push(msg);
            }
        }
        assert_eq!(
            check_messages,
            vec![
                check_worker_to_pack,
                CheckWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: check_worker_to_pack.batch.num_transactions + 1,
                        ..check_worker_to_pack.batch
                    },
                    ..check_worker_to_pack
                }
            ]
        );

        // Send pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .pack_to_worker
                .try_write(PackToExecutionWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                })
                .unwrap();
            worker.pack_to_worker.commit();
        }

        // Receive worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                worker.worker_to_pack.sync();
                if let Some(msg) = worker.worker_to_pack.try_read() {
                    break *msg;
                }
            };
            assert_eq!(
                ExecutionWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                },
                msg
            );
        }
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn check_worker_queues_use_dedicated_capacities() {
    const CHECK_REQUEST_CAPACITY: usize = 1 << 18;
    const CHECK_RESPONSE_CAPACITY: usize = 1 << 19;

    let logon = ClientLogon {
        worker_count: 1,
        check_worker_count: 1,
        allocator_size: 64 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 2,
        progress_tracker_capacity: 2,
        pack_to_worker_capacity: 2,
        worker_to_pack_capacity: 2,
        flags: 0,
        pack_to_check_worker_capacity: CHECK_REQUEST_CAPACITY,
        check_worker_to_pack_capacity: CHECK_RESPONSE_CAPACITY,
    };
    let (agave, files) = Server::setup_session(logon).unwrap();

    // The first five files are the global shared-memory regions. Check worker queues are
    // positions three and four; `ClientSession::setup_session` relies on this same ordering.
    assert!(
        files[3].metadata().unwrap().len()
            >= u64::try_from(shaq::mpmc::minimum_file_size::<PackToCheckWorkerMessage>(
                CHECK_REQUEST_CAPACITY
            ))
            .unwrap()
    );
    assert!(
        files[4].metadata().unwrap().len()
            >= u64::try_from(shaq::mpmc::minimum_file_size::<CheckWorkerToPackMessage>(
                CHECK_RESPONSE_CAPACITY
            ))
            .unwrap()
    );

    let client = crate::handshake::client::setup_session(&logon, files).unwrap();
    assert_eq!(agave.check_workers.len(), 1);
    assert_eq!(client.workers.len(), 1);
}

#[test]
fn accept_worker_count_max() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        assert!(res.is_ok());
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: MAX_WORKERS,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        assert!(res.is_ok());
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_low() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 0);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 0,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=0");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_high() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 100);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 100,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=100");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_check_worker_count_low() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::CheckWorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 0);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 1,
                check_worker_count: 0,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Check worker count; count=0");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_check_worker_count_high() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::CheckWorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 100);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 1,
                check_worker_count: 100,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Check worker count; count=100");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}
