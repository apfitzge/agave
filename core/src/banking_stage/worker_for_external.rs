use {
    crate::banking_stage::{committer::CommitTransactionDetails, consumer::Consumer},
    agave_scheduler_bindings::{
        pack_message_flags, worker_message_types, PackToWorkerMessage, SharableTransaction,
        WorkerToPackMessage, MAX_TRANSACTIONS_PER_PACK_MESSAGE,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    rts_alloc::Allocator,
    solana_poh::poh_recorder::SharedWorkingBank,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::MessageHash,
    std::{
        ptr::NonNull,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
};

struct TxPtr {
    ptr: NonNull<u8>,
    len: usize,
}

impl TxPtr {
    /// # Safety
    /// - `ptr` must be valid for reads of `len` bytes.
    unsafe fn new(ptr: NonNull<u8>, len: usize) -> Self {
        Self { ptr, len }
    }
}

impl TransactionData for TxPtr {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

type RuntimeTransactionView = RuntimeTransaction<ResolvedTransactionView<TxPtr>>;

#[allow(dead_code)]
pub struct WorkerForExternal {
    exit: Arc<AtomicBool>,
    allocator: Allocator,
    pack_message_consumer: shaq::Consumer<PackToWorkerMessage>,
    producer: shaq::Producer<WorkerToPackMessage>,

    sharable_banks: SharableBanks,
    shared_working_bank: SharedWorkingBank,
    consumer: Consumer,
}

#[allow(dead_code)]
impl WorkerForExternal {
    pub fn new(
        worker_index: u32,
        exit: Arc<AtomicBool>,
        sharable_banks: SharableBanks,
        shared_working_bank: SharedWorkingBank,
        consumer: Consumer,
    ) -> Option<Self> {
        let (allocator, pack_message_consumer, producer) = setup(worker_index)?;
        Some(Self {
            exit,
            allocator,
            pack_message_consumer,
            producer,
            sharable_banks,
            shared_working_bank,
            consumer,
        })
    }

    pub fn run(&mut self) {
        // Pre-allocate scratch space for processing messages.
        let mut current_tx_indexes = Vec::with_capacity(MAX_TRANSACTIONS_PER_PACK_MESSAGE);
        let mut current_txs = Vec::with_capacity(MAX_TRANSACTIONS_PER_PACK_MESSAGE);

        while !self.exit.load(Ordering::Relaxed) {
            self.pack_message_consumer.sync();
            self.process_loop(&mut current_tx_indexes, &mut current_txs);
            self.pack_message_consumer.finalize();
        }
    }

    fn process_loop(
        &mut self,
        current_tx_indexes: &mut Vec<usize>,
        current_txs: &mut Vec<RuntimeTransactionView>,
    ) {
        if self.pack_message_consumer.is_empty() {
            // no work - sleep for a short duration.
            const SLEEP_DURATION: Duration = Duration::from_micros(100);
            std::thread::sleep(SLEEP_DURATION);
            return;
        }

        self.producer.sync();

        // Check the exit signal between processing each message.
        while !self.exit.load(Ordering::Relaxed) {
            let Some(message_ptr) = self.pack_message_consumer.try_read() else {
                break;
            };

            // SAFETY: `message_ptr` is a valid pointer to a `PackToWorkerMessage`.
            let message = unsafe { message_ptr.as_ref() };
            if !self.check_message_validity(message) {
                continue;
            };

            // Depending on flags we may execute or proess the transaction differently.
            if message.flags & pack_message_flags::RESOLVE == 0 {
                self.execute_message(message, current_tx_indexes, current_txs);

                current_tx_indexes.clear();
                current_txs.clear();
            } else {
                self.resolve_message(message);
            }
        }

        self.producer.commit();
    }

    /// Checks the message is valid.
    /// If invalid, sends an invalid response back to pack.
    fn check_message_validity(&mut self, message: &PackToWorkerMessage) -> bool {
        // Check that the message is valid before continuing.
        if message.num_transactions == 0
            || usize::from(message.num_transactions)
                > agave_scheduler_bindings::MAX_TRANSACTIONS_PER_PACK_MESSAGE
        {
            let response = self.reserve_response();

            // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage`.
            unsafe {
                response.write(WorkerToPackMessage {
                    tag: agave_scheduler_bindings::worker_message_types::INVALID_MESSAGE,
                    inner:
                        agave_scheduler_bindings::worker_message_types::WorkerToPackMessageInner {
                            invalid: core::mem::ManuallyDrop::new(
                                agave_scheduler_bindings::worker_message_types::InvalidMessage,
                            ),
                        },
                });
            }

            return false;
        }

        true
    }

    fn execute_message(
        &mut self,
        message: &PackToWorkerMessage,
        current_tx_indexes: &mut Vec<usize>,
        current_txs: &mut Vec<RuntimeTransactionView>,
    ) {
        debug_assert_eq!(message.flags & pack_message_flags::RESOLVE, 0);

        // Get the current working bank.
        let Some(bank) = self.shared_working_bank.load() else {
            for transaction_index in 0..usize::from(message.num_transactions) {
                let sharable_transaction = &message.transactions[transaction_index];
                self.not_included_transaction(sharable_transaction);
            }
            return;
        };
        if message.slot != bank.slot() {
            for transaction_index in 0..usize::from(message.num_transactions) {
                let sharable_transaction = &message.transactions[transaction_index];
                self.not_included_transaction(sharable_transaction);
            }
            return;
        }

        for transaction_index in 0..usize::from(message.num_transactions) {
            let tx_ptr =
                self.tx_ptr_from_sharable_transaction(&message.transactions[transaction_index]);
            if let Some(resolved_view) = Self::tx_ptr_to_resolved_transaction_view(tx_ptr, &bank) {
                current_tx_indexes.push(transaction_index);
                current_txs.push(resolved_view);
            }
        }

        let output = self
            .consumer
            .process_and_record_transactions(&bank, current_txs.as_slice());

        if let Ok(results) = output
            .execute_and_commit_transactions_output
            .commit_transactions_result
        {
            let mut attempted_transaction_index = 0;
            let mut next_attempted_transaction_index =
                current_tx_indexes.iter().copied().peekable();
            for message_transaction_index in 0..usize::from(message.num_transactions) {
                let sharable_transaction = &message.transactions[message_transaction_index];
                if Some(&message_transaction_index) == next_attempted_transaction_index.peek() {
                    let result = &results[attempted_transaction_index];

                    match result {
                        CommitTransactionDetails::Committed {
                            compute_units,
                            loaded_accounts_data_size: _,
                            fee_payer_post_balance,
                            result: _,
                        } => {
                            self.included_transaction(
                                sharable_transaction,
                                compute_units,
                                fee_payer_post_balance,
                            );
                        }
                        CommitTransactionDetails::NotCommitted(_err) => {
                            self.not_included_transaction(sharable_transaction);
                        }
                    }

                    attempted_transaction_index += 1;
                    next_attempted_transaction_index.next();
                } else {
                    self.not_included_transaction(sharable_transaction);
                }
            }
        }
    }

    fn included_transaction(
        &mut self,
        tx: &SharableTransaction,
        compute_units: &u64,
        fee_payer_balance: &u64,
    ) {
        let mut response = self.reserve_response();
        // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage`.
        let response = unsafe { response.as_mut() };

        response.tag = worker_message_types::INCLUDED;
        // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage` and we've just set the tag.
        let included = unsafe { &mut response.inner.included };
        included.transaction = SharableTransaction {
            offset: tx.offset,
            length: tx.length,
        };
        included.compute_units = *compute_units;
        included.fee_payer_balance = *fee_payer_balance;
    }

    fn not_included_transaction(&mut self, tx: &SharableTransaction) {
        let mut response = self.reserve_response();
        // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage`.
        let response = unsafe { response.as_mut() };

        response.tag = worker_message_types::NOT_INCLUDED;
        // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage
        let not_included = unsafe { &mut response.inner.not_included };

        not_included.transaction = SharableTransaction {
            offset: tx.offset,
            length: tx.length,
        };
        not_included.reason = 0; // TODO: set reason
    }

    fn resolve_message(&mut self, message: &PackToWorkerMessage) {
        debug_assert_ne!(message.flags & pack_message_flags::RESOLVE, 0);

        for transaction_index in 0..usize::from(message.num_transactions) {
            let sharable_transaction = &message.transactions[transaction_index];

            // Every transaction will get a response, regardless of validity.
            let mut response = self.reserve_response();
            let response = unsafe { response.as_mut() };

            response.tag = worker_message_types::RESOLVED;
            // SAFETY: `response` is a valid pointer to a `WorkerToPackMessage` and we've just set the tag.
            let resolved_response = unsafe { &mut response.inner.resolved };
            self.resolve_transaction(sharable_transaction, resolved_response);
        }
    }

    fn resolve_transaction(
        &mut self,
        sharable_transaction: &SharableTransaction,
        resolved: &mut worker_message_types::Resolved,
    ) {
        // Set the transaction and mark unsuccessful for now.
        // Any early return will leave the response as unsuccessful.
        resolved.transaction = SharableTransaction {
            offset: sharable_transaction.offset,
            length: sharable_transaction.length,
        };
        resolved.success = false;

        let tx_ptr = self.tx_ptr_from_sharable_transaction(sharable_transaction);
        let Ok(view) = SanitizedTransactionView::try_new_sanitized(tx_ptr) else {
            return;
        };

        // Get the current root bank to resolve against.
        let root_bank = self.sharable_banks.root();

        // Load addresses for transaction.
        let Ok((loaded_addresses, deactivation_slot)) = (match view.version() {
            TransactionVersion::Legacy => Ok((None, u64::MAX)),
            TransactionVersion::V0 => root_bank
                .load_addresses_from_ref(view.address_table_lookup_iter())
                .map(|(loaded_addresses, deactivation_slot)| {
                    (Some(loaded_addresses), deactivation_slot)
                }),
        }) else {
            return;
        };

        resolved.slot = root_bank.slot();
        resolved.min_alt_deactivation_slot = deactivation_slot;

        match loaded_addresses {
            Some(loaded_addresses) => {
                // We must allocate space in the shared allocator for the resolved pubkeys.
                let num_pubkeys = loaded_addresses.writable.len() + loaded_addresses.readonly.len();
                let allocation_size = (num_pubkeys * core::mem::size_of::<Pubkey>()) as u32;

                let Some(ptr) = self.allocator.allocate(allocation_size) else {
                    panic!("WorkerForExternal: unable to allocate space for resolved pubkeys");
                };

                // Copy pointers to the allocated space.
                // SAFETY: `ptr` is valid for writes of `allocation_size` bytes.
                unsafe {
                    let pubkey_ptr = ptr.as_ptr() as *mut Pubkey;
                    for (i, pubkey) in loaded_addresses
                        .writable
                        .iter()
                        .chain(loaded_addresses.readonly.iter())
                        .enumerate()
                    {
                        pubkey_ptr.add(i).write(*pubkey);
                    }
                }
            }
            None => {
                resolved.resolved_pubkeys.num_pubkeys = 0;
                resolved.resolved_pubkeys.offset = 0;
            }
        }

        resolved.success = true;
    }

    fn tx_ptr_from_sharable_transaction(
        &self,
        sharable_transaction: &SharableTransaction,
    ) -> TxPtr {
        // This is **actually** unsafe because the offset/len may be invalid if the
        // operator has passed bad data.
        // If operators are not careful this can result in undefined behavior.
        unsafe {
            let ptr = self.allocator.ptr_from_offset(sharable_transaction.offset);
            TxPtr::new(ptr, sharable_transaction.length as usize)
        }
    }

    fn tx_ptr_to_resolved_transaction_view(
        tx_ptr: TxPtr,
        bank: &Bank,
    ) -> Option<RuntimeTransactionView> {
        let view = SanitizedTransactionView::try_new_sanitized(tx_ptr).ok()?;
        let view = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
            view,
            MessageHash::Compute,
            None,
        )
        .ok()?;

        // Load addresses for transaction.
        let load_addresses_result = match view.version() {
            TransactionVersion::Legacy => Ok((None, u64::MAX)),
            TransactionVersion::V0 => bank
                .load_addresses_from_ref(view.address_table_lookup_iter())
                .map(|(loaded_addresses, deactivation_slot)| {
                    (Some(loaded_addresses), deactivation_slot)
                }),
        };
        let (loaded_addresses, _deactivation_slot) = load_addresses_result.ok()?;

        RuntimeTransactionView::try_from(view, loaded_addresses, bank.get_reserved_account_keys())
            .ok()
    }

    // TODO: handle the case where we cannot reserve a response.
    fn reserve_response(&mut self) -> NonNull<WorkerToPackMessage> {
        self.producer
            .reserve()
            .unwrap_or_else(|| panic!("WorkerForExternal: unable to reserve response message"))
    }
}

fn setup(
    worker_index: u32,
) -> Option<(
    Allocator,
    shaq::Consumer<PackToWorkerMessage>,
    shaq::Producer<WorkerToPackMessage>,
)> {
    const ALLOCATOR_PATH: &str = "/mnt/hugepages/rts-alloc";
    const ALLOCATOR_WORKER_STARTING_ID: u32 = 2;
    let allocator_id = worker_index + ALLOCATOR_WORKER_STARTING_ID;

    const PACK_TO_WORKER_DIR: &str = "/mnt/hugepages/pack_to_worker";
    const WORKER_TO_PACK_DIR: &str = "/mnt/hugepages/worker_to_pack";

    let pack_to_worker_path = format!("{PACK_TO_WORKER_DIR}/{worker_index}");
    let worker_to_pack_path = format!("{WORKER_TO_PACK_DIR}/{worker_index}");

    let allocator = Allocator::join(ALLOCATOR_PATH, allocator_id)
        .map_err(|e| {
            error!("Failed to join allocator: {e:?}");
        })
        .ok()?;

    // SAFETY: Agave's queue is unique here for the worker.
    // - If the external pack or another process joins as consumer this is unsafe.
    let consumer = unsafe { shaq::Consumer::join(pack_to_worker_path) }
        .map_err(|e| {
            error!("Failed to create consumer: {e:?}");
        })
        .ok()?;
    let producer = shaq::Producer::join(worker_to_pack_path)
        .map_err(|e| {
            error!("Failed to create producer: {e:?}");
        })
        .ok()?;

    Some((allocator, consumer, producer))
}
