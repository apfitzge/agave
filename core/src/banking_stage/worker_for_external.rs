use {
    crate::banking_stage::consumer::Consumer,
    agave_scheduler_bindings::{pack_message_flags, PackToWorkerMessage, WorkerToPackMessage},
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
    },
    rts_alloc::Allocator,
    solana_poh::poh_recorder::SharedWorkingBank,
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
            let Some(response) = self.producer.reserve() else {
                // TODO: gracefully handle this error - shutdown the worker.
                panic!("WorkerForExternal: unable to reserve response message");
            };

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
    }
}
