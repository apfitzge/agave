use {
    crate::banking_stage::consumer::Consumer,
    agave_scheduler_bindings::{
        pack_message_flags, worker_message_types, PackToWorkerMessage, SharableTransaction,
        WorkerToPackMessage,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    rts_alloc::Allocator,
    solana_poh::poh_recorder::SharedWorkingBank,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::SharableBanks,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
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

    current_tx_indexes: Vec<usize>,
    current_txs: Vec<RuntimeTransactionView>,
}

#[allow(dead_code)]
impl WorkerForExternal {
    pub fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            self.pack_message_consumer.sync();
            self.process_loop();
            self.pack_message_consumer.finalize();
        }
    }

    fn process_loop(&mut self) {
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
                self.execute_message(message);
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

    fn execute_message(&mut self, message: &PackToWorkerMessage) {
        debug_assert_eq!(message.flags & pack_message_flags::RESOLVE, 0);

        todo!()
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

    // TODO: handle the case where we cannot reserve a response.
    fn reserve_response(&mut self) -> NonNull<WorkerToPackMessage> {
        self.producer
            .reserve()
            .unwrap_or_else(|| panic!("WorkerForExternal: unable to reserve response message"))
    }
}
