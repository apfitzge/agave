use {
    crate::{
        bridge::{
            CheckScheduleBatch, KeyedTransactionMeta, ScheduleBatch, SchedulerBindingsBridge,
            TransactionKey,
        },
        handshake::{AgaveSession, ClientLogon, client, server::Server},
        responses_region::{execution_responses_from_iter, resolve_responses_from_iter},
        transaction_ptr::TransactionPtrBatch,
    },
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, ExecutionWorkerToPackMessage, ProgressMessage, SharablePubkeys,
        SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
        TransactionResponseRegion, processed_codes,
        worker_message_types::{
            CheckResponse, ExecutionResponse, fee_payer_balance_flags, not_included_reasons,
            resolve_flags, status_check_flags,
        },
    },
    solana_pubkey::Pubkey,
    solana_transaction::versioned::VersionedTransaction,
    std::ops::{Deref, DerefMut},
};

pub struct TestBridge<M>
where
    M: Copy,
{
    bridge: SchedulerBindingsBridge<M>,
    agave: AgaveSession,
}

impl<M> Deref for TestBridge<M>
where
    M: Copy,
{
    type Target = SchedulerBindingsBridge<M>;

    fn deref(&self) -> &Self::Target {
        &self.bridge
    }
}

impl<M> DerefMut for TestBridge<M>
where
    M: Copy,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bridge
    }
}

impl<M> TestBridge<M>
where
    M: Copy,
{
    #[must_use]
    pub fn new(worker_count: usize, worker_req_cap: usize) -> Self {
        assert!(
            worker_req_cap.is_power_of_two(),
            "shaq requires power of 2 queue sizes"
        );

        let logon = ClientLogon {
            worker_count,
            check_worker_count: 1,
            allocator_size: 64 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_capacity: 1024,
            progress_tracker_capacity: 256,
            pack_to_worker_capacity: worker_req_cap,
            worker_to_pack_capacity: 1024,
            flags: 0,
            pack_to_check_worker_capacity: worker_req_cap,
            check_worker_to_pack_capacity: 1024,
        };

        let (agave, files) = Server::setup_session(logon).unwrap();
        let client_session = client::setup_session(&logon, files).unwrap();

        Self {
            bridge: SchedulerBindingsBridge::new(client_session),
            agave,
        }
    }

    #[must_use]
    pub fn tx_count(&self) -> usize {
        self.bridge.state().len()
    }

    #[must_use]
    pub fn contains_tx(&self, key: TransactionKey) -> bool {
        self.bridge.state().contains_key(key)
    }

    pub fn queue_progress(&mut self, progress: ProgressMessage) {
        self.agave.progress_tracker.try_write(progress).unwrap();
        self.agave.progress_tracker.commit();
    }

    pub fn queue_tpu(&mut self, tx: &VersionedTransaction) {
        let serialized = wincode::serialize(tx).unwrap();
        let allocator = &self.agave.tpu_to_pack.allocator;

        // Allocate in shared memory and copy the transaction bytes.
        let ptr = allocator
            .allocate(serialized.len().try_into().unwrap())
            .unwrap();
        // SAFETY: `ptr` points to a fresh allocation of exactly `serialized.len()` bytes, and the
        // source slice is readable for that same length.
        unsafe {
            std::ptr::copy_nonoverlapping(serialized.as_ptr(), ptr.as_ptr(), serialized.len());
        }
        // SAFETY: `ptr` was allocated from `allocator`.
        let offset = unsafe { allocator.offset(ptr) };

        let msg = TpuToPackMessage {
            transaction: SharableTransactionRegion {
                offset,
                length: serialized.len() as u32,
            },
            flags: 0,
            src_addr: [0; 16],
        };

        self.agave.tpu_to_pack.producer.try_write(msg).unwrap();
        self.agave.tpu_to_pack.producer.commit();
    }

    pub fn queue_check_response_ok(
        &mut self,
        batch: &CheckScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        index: usize,
        keys: Option<Vec<Pubkey>>,
    ) {
        self.queue_check_response(batch, index, keys, self.check_ok());
    }

    pub fn queue_check_response(
        &mut self,
        batch: &CheckScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        index: usize,
        keys: Option<Vec<Pubkey>>,
        mut response: CheckResponse,
    ) {
        let (batch_region, responses_region) = {
            let allocator = self.bridge.allocator();

            // Allocate pubkeys in shared memory if provided.
            if let Some(keys) = keys {
                let pubkeys_ptr = allocator
                    .allocate(
                        (keys
                            .len()
                            .checked_mul(std::mem::size_of::<Pubkey>())
                            .unwrap())
                        .try_into()
                        .unwrap(),
                    )
                    .unwrap();
                // SAFETY: `pubkeys_ptr` points to a fresh allocation sized for the full `keys`
                // slice in bytes, and the source slice is valid for the same byte length.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        keys.as_ptr().cast::<u8>(),
                        pubkeys_ptr.as_ptr(),
                        keys.len()
                            .checked_mul(std::mem::size_of::<Pubkey>())
                            .unwrap(),
                    );
                }
                // SAFETY: `pubkeys_ptr` was allocated from `allocator`.
                let offset = unsafe { allocator.offset(pubkeys_ptr) };
                response.resolved_pubkeys = SharablePubkeys {
                    offset,
                    num_pubkeys: keys.len() as u32,
                };
            }

            let batch_region = self.build_single_tx_batch_region(&batch.transactions, index);
            let responses_region =
                resolve_responses_from_iter(allocator, [response].into_iter()).unwrap();
            (batch_region, responses_region)
        };

        let msg = CheckWorkerToPackMessage {
            batch: batch_region,
            processed_code: processed_codes::PROCESSED,
            responses: responses_region,
        };

        self.agave.check_workers[0]
            .check_worker_to_pack
            .try_write(msg)
            .unwrap();
    }

    pub fn queue_all_checks_ok(&mut self) {
        while let Some(batch) = self.pop_check_schedule() {
            for i in 0..batch.transactions.len() {
                self.queue_check_response_ok(&batch, i, None);
            }
        }
    }

    pub fn queue_execute_response(
        &mut self,
        batch: &ScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        index: usize,
        response: ExecutionResponse,
    ) {
        let worker_idx = batch.worker;
        let msg = {
            let allocator = self.bridge.allocator();
            let batch_region = self.build_single_tx_batch_region(&batch.transactions, index);
            let responses_region =
                execution_responses_from_iter(allocator, [response].into_iter()).unwrap();

            ExecutionWorkerToPackMessage {
                batch: batch_region,
                processed_code: processed_codes::PROCESSED,
                responses: responses_region,
            }
        };

        let worker = &mut self.agave.workers[worker_idx];
        worker.worker_to_pack.try_write(msg).unwrap();
        worker.worker_to_pack.commit();
    }

    pub fn queue_unprocessed_response(
        &mut self,
        batch: &ScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        index: usize,
    ) {
        let worker_idx = batch.worker;
        let msg = {
            let batch_region = self.build_single_tx_batch_region(&batch.transactions, index);

            ExecutionWorkerToPackMessage {
                batch: batch_region,
                processed_code: processed_codes::MAX_WORKING_SLOT_EXCEEDED,
                responses: TransactionResponseRegion {
                    tag: 0,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            }
        };

        let worker = &mut self.agave.workers[worker_idx];
        worker.worker_to_pack.try_write(msg).unwrap();
        worker.worker_to_pack.commit();
    }

    pub fn pop_schedule(&mut self) -> Option<ScheduleBatch<Vec<KeyedTransactionMeta<M>>>> {
        for (worker_idx, worker) in self.agave.workers.iter_mut().enumerate() {
            worker.pack_to_worker.sync();
            if let Some(msg) = worker.pack_to_worker.try_read() {
                let msg = *msg;
                worker.pack_to_worker.finalize();

                // SAFETY: execution schedules were allocated by the bridge in the shared
                // allocator before being sent to the Agave execution-worker queue.
                let batch = unsafe {
                    TransactionPtrBatch::<KeyedTransactionMeta<M>>::from_sharable_transaction_batch_region(
                        &msg.batch,
                        self.bridge.allocator(),
                    )
                };

                let transactions: Vec<_> = batch.iter().map(|(_tx_ptr, meta)| meta).collect();

                // SAFETY: the test helper takes ownership of the batch container after reading
                // the schedule from the queue. Individual transactions are still managed by the
                // bridge.
                unsafe { batch.free() };

                return Some(ScheduleBatch {
                    worker: worker_idx,
                    transactions,
                    max_working_slot: msg.max_working_slot,
                    flags: msg.flags,
                });
            }
        }

        None
    }

    pub fn pop_check_schedule(
        &mut self,
    ) -> Option<CheckScheduleBatch<Vec<KeyedTransactionMeta<M>>>> {
        let check_worker = &mut self.agave.check_workers[0];
        let Some(msg) = check_worker.pack_to_check_worker.try_read() else {
            return None;
        };

        // SAFETY: check schedules were allocated by the bridge in the shared allocator
        // before being sent to the Agave check-worker queue.
        let batch = unsafe {
            TransactionPtrBatch::<KeyedTransactionMeta<M>>::from_sharable_transaction_batch_region(
                &msg.batch,
                self.bridge.allocator(),
            )
        };

        let transactions: Vec<_> = batch.iter().map(|(_tx_ptr, meta)| meta).collect();

        // SAFETY: the test helper takes ownership of the batch container after reading the
        // schedule from the queue. Individual transactions are still managed by the bridge.
        unsafe { batch.free() };

        Some(CheckScheduleBatch {
            transactions,
            flags: msg.flags,
        })
    }

    pub fn check_ok(&self) -> CheckResponse {
        let progress = self.bridge.progress();

        CheckResponse {
            parsing_and_sanitization_flags: 0,
            status_check_flags: status_check_flags::REQUESTED | status_check_flags::PERFORMED,
            fee_payer_balance_flags: fee_payer_balance_flags::REQUESTED
                | fee_payer_balance_flags::PERFORMED,
            resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            scheduling_details_flags: 0,
            included_slot: progress.current_slot,
            transaction_fee: 0,
            prioritization_fee: 0,
            estimated_cost_units: 0,
            balance_slot: progress.current_slot,
            fee_payer_balance: u64::from(u32::MAX),
            resolution_slot: progress.current_slot,
            min_alt_deactivation_slot: u64::MAX,
            resolved_pubkeys: SharablePubkeys {
                offset: 0,
                num_pubkeys: 0,
            },
        }
    }

    #[must_use]
    pub fn execute_ok(&self) -> ExecutionResponse {
        ExecutionResponse {
            execution_slot: self.bridge.progress().current_slot,
            not_included_reason: not_included_reasons::NONE,
            cost_units: 0,
            fee_payer_balance: u64::from(u32::MAX),
        }
    }

    #[must_use]
    pub fn execute_err(&self, reason: u8) -> ExecutionResponse {
        ExecutionResponse {
            execution_slot: self.bridge.progress().current_slot,
            not_included_reason: reason,
            cost_units: 0,
            fee_payer_balance: u64::from(u32::MAX),
        }
    }

    fn build_single_tx_batch_region(
        &self,
        batch: &[KeyedTransactionMeta<M>],
        index: usize,
    ) -> SharableTransactionBatchRegion {
        type Batch<'a, M> = TransactionPtrBatch<'a, KeyedTransactionMeta<M>>;

        let meta = batch[index];
        let allocator = self.bridge.allocator();

        // Allocate the batch container in the bridge's shared allocator.
        let batch_ptr = allocator
            .allocate(Batch::<M>::TRANSACTION_META_END as u32)
            .unwrap();
        // SAFETY: `batch_ptr` was allocated from `allocator`.
        let batch_offset = unsafe { allocator.offset(batch_ptr) };

        // Write the transaction region (offset is relative to the shared allocator,
        // which is the same underlying file for both client and worker).
        let tx_state = self.bridge.transaction(meta.key);
        // SAFETY: the transaction state owns this transaction allocation in the bridge allocator.
        let tx_region = unsafe {
            tx_state
                .data
                .inner_data()
                .to_sharable_transaction_region(allocator)
        };
        let tx_ptr = batch_ptr.cast::<SharableTransactionRegion>();
        // SAFETY: `batch_ptr` was allocated with enough room for one transaction region.
        unsafe { tx_ptr.as_ptr().write(tx_region) };

        // Write the metadata.
        // SAFETY: `TRANSACTION_META_START` is within the allocated batch container layout.
        let meta_ptr = unsafe {
            batch_ptr
                .as_ptr()
                .byte_add(Batch::<M>::TRANSACTION_META_START)
                .cast::<KeyedTransactionMeta<M>>()
        };
        // SAFETY: `meta_ptr` points within the batch container allocation.
        unsafe { meta_ptr.write(meta) };

        SharableTransactionBatchRegion {
            num_transactions: 1,
            transactions_offset: batch_offset,
        }
    }
}
