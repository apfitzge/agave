use {
    crate::banking_stage::{committer::CommitTransactionDetails, consumer::Consumer},
    agave_scheduler_bindings::{
        dropped_transaction_reasons, worker_message_types, PackToWorkerMessage,
        SharableTransaction, WorkerToPackMessage, MAX_TRANSACTIONS_PER_MESSAGE,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    rts_alloc::Allocator,
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::MessageHash,
    solana_transaction_error::TransactionError,
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

pub struct ConsumeWorkerForExternal {
    exit: Arc<AtomicBool>,
    allocator: Allocator,
    pack_message_consumer: shaq::Consumer<PackToWorkerMessage>,
    producer: shaq::Producer<WorkerToPackMessage>,

    leader_bank_notifier: Arc<LeaderBankNotifier>,
    consumer: Consumer,

    current_tx_indexes: Vec<usize>,
    current_txs: Vec<RuntimeTransactionView>,
}

impl ConsumeWorkerForExternal {
    pub fn new(
        worker_index: u32,
        exit: Arc<AtomicBool>,
        leader_bank_notifier: Arc<LeaderBankNotifier>,
        consumer: Consumer,
    ) -> Option<Self> {
        let (allocator, pack_message_consumer, producer) = setup(worker_index)?;
        Some(Self {
            exit,
            allocator,
            pack_message_consumer,
            producer,
            leader_bank_notifier,
            consumer,
            current_tx_indexes: Vec::with_capacity(MAX_TRANSACTIONS_PER_MESSAGE),
            current_txs: Vec::with_capacity(MAX_TRANSACTIONS_PER_MESSAGE),
        })
    }

    pub fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            self.pack_message_consumer.sync();
            self.process_loop();
            self.pack_message_consumer.finalize();
        }
    }

    fn process_loop(&mut self) {
        if self.pack_message_consumer.is_empty() {
            return;
        }

        // Get the bank to process transactions against.
        let Some(bank) = self
            .leader_bank_notifier
            .get_or_wait_for_in_progress(Duration::from_millis(1))
            .upgrade()
        else {
            return;
        };

        self.producer.sync();

        // check for exit signal between each message
        while !self.exit.load(Ordering::Relaxed) {
            self.current_tx_indexes.clear();
            self.current_txs.clear();

            let Some(message) = self.pack_message_consumer.try_read() else {
                break;
            };

            let message = unsafe { message.as_ref() };

            // Resolve all transactions in the message.
            for (index, tx) in message.transactions[..usize::from(message.num_transactions)]
                .iter()
                .enumerate()
            {
                let tx_ptr = TxPtr {
                    ptr: self.allocator.ptr_from_offset(tx.transaction_offset),
                    len: tx.transaction_size as usize,
                };

                if let Some(resolved) = tx_ptr_to_resolved_transaction_view(tx_ptr, &bank) {
                    self.current_tx_indexes.push(index);
                    self.current_txs.push(resolved);
                }
            }

            // TODO: Handle all or nothing.
            let output = self
                .consumer
                .process_and_record_transactions(&bank, self.current_txs.as_slice());

            if let Ok(results) = output
                .execute_and_commit_transactions_output
                .commit_transactions_result
            {
                let mut next_attempted_transaction_index =
                    self.current_tx_indexes.iter().copied().peekable();
                let mut attempted_transaction_index = 0;
                for message_transaction_index in 0..message.num_transactions {
                    let tx = &message.transactions[usize::from(message_transaction_index)];
                    if Some(&usize::from(message_transaction_index))
                        == next_attempted_transaction_index.peek()
                    {
                        // transaction attempted processing
                        let result = &results[attempted_transaction_index];

                        match result {
                            CommitTransactionDetails::Committed {
                                compute_units,
                                loaded_accounts_data_size: _,
                                result: _,
                            } => {
                                if let Some(mut msg) = self.producer.reserve() {
                                    let msg = unsafe { msg.as_mut() };
                                    msg.tag = worker_message_types::INCLUDED_TRANSACTION;
                                    let included_transaction =
                                        unsafe { &mut msg.inner.included_transaction };
                                    included_transaction.transaction.transaction_offset =
                                        tx.transaction_offset;
                                    included_transaction.transaction.transaction_size =
                                        tx.transaction_size;
                                    included_transaction.compute_units = *compute_units;

                                    // todo - this should be fed back as part of the commit results.
                                    included_transaction.fee_payer_balance = bank.get_balance(
                                        &self.current_txs[attempted_transaction_index]
                                            .static_account_keys()[0],
                                    );

                                    // todo: we should communicate status as well.
                                }
                            }
                            CommitTransactionDetails::NotCommitted(err) => {
                                Self::drop_with_reason(
                                    &mut self.producer,
                                    tx,
                                    Self::transaction_error_to_reason(err),
                                );
                            }
                        }

                        next_attempted_transaction_index.next();
                        attempted_transaction_index += 1;
                    } else {
                        Self::drop_with_reason(
                            &mut self.producer,
                            tx,
                            dropped_transaction_reasons::INVALID_FORMAT,
                        );
                    }
                }
            } else {
                let mut next_attempted_transaction_index =
                    self.current_tx_indexes.iter().copied().peekable();
                for message_transaction_index in 0..message.num_transactions {
                    let tx = &message.transactions[usize::from(message_transaction_index)];
                    if Some(&usize::from(message_transaction_index))
                        == next_attempted_transaction_index.peek()
                    {
                        Self::drop_with_reason(
                            &mut self.producer,
                            tx,
                            dropped_transaction_reasons::OTHER, // poh record failure
                        );
                        next_attempted_transaction_index.next();
                    } else {
                        Self::drop_with_reason(
                            &mut self.producer,
                            tx,
                            dropped_transaction_reasons::INVALID_FORMAT,
                        );
                    }
                }
            }

            // Respond with transaction statuses.
            // for now just drop em all.
            for tx in message.transactions[..usize::from(message.num_transactions)].iter() {
                Self::drop_with_reason(
                    &mut self.producer,
                    tx,
                    dropped_transaction_reasons::INVALID_FORMAT,
                );
            }
        }

        self.producer.commit();
    }

    // TODO: how to handle reserve failures?
    fn drop_with_reason(
        producer: &mut shaq::Producer<WorkerToPackMessage>,
        tx: &SharableTransaction,
        reason: u8,
    ) {
        if let Some(mut msg) = producer.reserve() {
            let msg = unsafe { msg.as_mut() };
            msg.tag = worker_message_types::DROPPED_TRANSACTION;
            let dropped_transaction = unsafe { &mut msg.inner.dropped_transaction };
            dropped_transaction.transaction.transaction_offset = tx.transaction_offset;
            dropped_transaction.transaction.transaction_size = tx.transaction_size;
            dropped_transaction.reason = reason;
        }
    }

    fn transaction_error_to_reason(err: &TransactionError) -> u8 {
        match err {
            TransactionError::AlreadyProcessed => dropped_transaction_reasons::ALREADY_PROCESSED,
            TransactionError::BlockhashNotFound => dropped_transaction_reasons::TOO_OLD,
            TransactionError::AccountNotFound
            | TransactionError::InvalidAccountForFee
            | TransactionError::InsufficientFundsForFee
            | TransactionError::InsufficientFundsForRent { .. } => {
                dropped_transaction_reasons::FEE_PAYER_FAILURE
            }
            TransactionError::WouldExceedMaxBlockCostLimit
            | TransactionError::WouldExceedAccountDataBlockLimit => {
                dropped_transaction_reasons::GLOBAL_BLOCK_LIMITS
            }
            TransactionError::WouldExceedMaxAccountCostLimit => {
                dropped_transaction_reasons::ACCOUNT_BLOCK_LIMITS
            }
            _ => dropped_transaction_reasons::OTHER,
        }
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

    let consumer = shaq::Consumer::join(pack_to_worker_path)
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

    RuntimeTransactionView::try_from(view, loaded_addresses, bank.get_reserved_account_keys()).ok()
}
