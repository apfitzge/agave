use {
    crate::banking_stage::{
        scheduler_messages::MaxAge,
        transaction_scheduler::receive_and_buffer::{
            PacketHandlingError, translate_to_runtime_view,
        },
    },
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, MAX_TRANSACTIONS_PER_MESSAGE, PackToCheckWorkerMessage,
        SharablePubkeys, TransactionResponseRegion, check_message_flags, processed_codes,
        worker_message_types::{
            CheckResponse, fee_payer_balance_flags, parsing_and_sanitization_flags, resolve_flags,
            status_check_flags,
        },
    },
    agave_scheduling_utils::{
        responses_region::allocate_check_response_region,
        transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, result::TransactionViewError,
        sanitize::SanitizeConfig, transaction_data::TransactionData,
        transaction_view::SanitizedTransactionView,
    },
    arrayvec::ArrayVec,
    solana_account::ReadableAccount,
    solana_clock::Slot,
    solana_message::v0::LoadedAddresses,
    solana_poh::poh_recorder::{LeaderState, SharedLeaderState},
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        bank_forks::{BankPair, SharableBanks},
    },
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, sanitize_config::sanitize_config,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_transaction::TransactionError,
    std::{
        ptr::NonNull,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    thiserror::Error,
};

type Tx = RuntimeTransaction<ResolvedTransactionView<TransactionPtr>>;
type TxView = SanitizedTransactionView<TransactionPtr>;

#[derive(Debug, Error)]
pub(crate) enum ExternalCheckWorkerError {
    #[error("Sender disconnected")]
    SenderDisconnected,
    #[error("Allocation failed")]
    AllocationFailure,
}

pub(crate) enum IterationResult {
    ProcessedMessage,
    Idle,
}

#[allow(dead_code)]
pub(crate) struct ExternalCheckWorker {
    exit: Arc<AtomicBool>,
    receiver: shaq::mpmc::Consumer<PackToCheckWorkerMessage>,
    sender: shaq::mpmc::Producer<CheckWorkerToPackMessage>,
    allocator: rts_alloc::Allocator,

    shared_leader_state: SharedLeaderState,
    sharable_banks: SharableBanks,
}

#[allow(dead_code)]
impl ExternalCheckWorker {
    const RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);

    pub fn new(
        exit: Arc<AtomicBool>,
        receiver: shaq::mpmc::Consumer<PackToCheckWorkerMessage>,
        sender: shaq::mpmc::Producer<CheckWorkerToPackMessage>,
        allocator: rts_alloc::Allocator,
        shared_leader_state: SharedLeaderState,
        sharable_banks: SharableBanks,
    ) -> Self {
        Self {
            exit,
            receiver,
            sender,
            allocator,
            shared_leader_state,
            sharable_banks,
        }
    }

    pub fn run(mut self) -> Result<(), ExternalCheckWorkerError> {
        while !self.exit.load(Ordering::Relaxed) {
            self.iterate(Self::RECEIVE_TIMEOUT)?;
        }

        Ok(())
    }

    pub(crate) fn iterate(
        &mut self,
        timeout: Duration,
    ) -> Result<IterationResult, ExternalCheckWorkerError> {
        self.allocator.clean_remote_frees();

        match self.receiver.read_timeout(timeout) {
            Ok(message) => {
                self.process_message(&message)?;
                Ok(IterationResult::ProcessedMessage)
            }
            Err(shaq::error::WaitError::Timeout) => Ok(IterationResult::Idle),
        }
    }

    fn process_message(
        &mut self,
        message: &PackToCheckWorkerMessage,
    ) -> Result<(), ExternalCheckWorkerError> {
        if !Self::validate_message(message) {
            return self.return_unprocessed_message(message, processed_codes::INVALID);
        }

        self.check_batch(message)
    }

    fn check_batch(
        &mut self,
        message: &PackToCheckWorkerMessage,
    ) -> Result<(), ExternalCheckWorkerError> {
        let BankPair {
            root_bank,
            working_bank,
        } = self.sharable_banks.load();
        // Prefer the leader bank over the highest working fork when leader.
        let working_bank = active_leader_state(&self.shared_leader_state)
            .and_then(|leader_state| leader_state.working_bank().cloned())
            .unwrap_or(working_bank);

        // SAFETY: Assumption that external scheduler does not pass messages with batch regions
        //         not pointing to valid regions in the allocator.
        let batch = unsafe {
            TransactionPtrBatch::from_sharable_transaction_batch_region(
                &message.batch,
                &self.allocator,
            )
        };

        let (responses_ptr, responses) = allocate_check_response_region(
            &self.allocator,
            usize::from(message.batch.num_transactions),
        )
        .ok_or(ExternalCheckWorkerError::AllocationFailure)?;

        // SAFETY: responses_ptr is sufficiently sized and aligned.
        let (parsing_results, parsed_transactions, response_slice) = unsafe {
            Self::parse_transactions_and_populate_initial_check_responses(
                message,
                &batch,
                &root_bank,
                responses_ptr,
            )
        };

        if message.flags & check_message_flags::LOAD_FEE_PAYER_BALANCE != 0 {
            Self::check_load_fee_payer_balance(
                &parsing_results,
                &parsed_transactions,
                response_slice,
                &working_bank,
            );
        }

        // Do resolving next since we (currently) need resolved transactions for status checks.
        let (parsing_and_resolve_results, txs, max_ages) =
            Self::translate_transaction_batch(&batch, &root_bank);

        if message.flags & check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES != 0 {
            self.check_resolve_pubkeys(
                &parsing_results,
                &parsing_and_resolve_results,
                &txs,
                &max_ages,
                response_slice,
                root_bank.slot(),
            )?;
        }

        if message.flags & check_message_flags::STATUS_CHECKS != 0 {
            Self::check_status_checks(
                &parsing_and_resolve_results,
                &txs,
                response_slice,
                &working_bank,
            );
        }

        self.sender
            .try_write(CheckWorkerToPackMessage {
                batch: message.batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .map_err(|_| ExternalCheckWorkerError::SenderDisconnected)?;

        Ok(())
    }

    fn check_resolve_pubkeys(
        &self,
        parsing_results: &[Result<(), TransactionViewError>],
        parsing_and_resolve_results: &[Result<(), PacketHandlingError>],
        txs: &[Tx],
        max_ages: &[MaxAge],
        responses: &mut [CheckResponse],
        resolution_slot: Slot,
    ) -> Result<(), ExternalCheckWorkerError> {
        assert_eq!(parsing_results.len(), parsing_and_resolve_results.len());
        assert_eq!(parsing_results.len(), responses.len());

        let mut resolved_transaction_iter = txs.iter();
        let mut max_age_iter = max_ages.iter();
        for (transaction_index, (parsing_result, parsing_and_resolve_results)) in parsing_results
            .iter()
            .zip(parsing_and_resolve_results.iter())
            .enumerate()
        {
            if parsing_result.is_err() {
                continue;
            }

            let response = &mut responses[transaction_index];
            response.resolve_flags |= resolve_flags::PERFORMED;
            if parsing_and_resolve_results.is_err() {
                response.resolve_flags |= resolve_flags::FAILED;
                continue;
            }

            let transaction = resolved_transaction_iter.next().expect(
                "resolved_transaction_iter iterator must contain element for each sent parsed \
                 transaction",
            );
            let max_age = max_age_iter.next().expect(
                "max_age_iter iterator must contain element for each sent parsed transaction",
            );

            let (sharable_keys, alt_invalidation_slot) = match transaction.loaded_addresses() {
                Some(loaded_addresses) if !loaded_addresses.is_empty() => {
                    let num_pubkeys = loaded_addresses.len();
                    let pubkeys_allocation = self
                        .allocator
                        .allocate(num_pubkeys.wrapping_mul(core::mem::size_of::<Pubkey>()) as u32)
                        .ok_or(ExternalCheckWorkerError::AllocationFailure)?
                        .cast();
                    // SAFETY: non-overlapping and appropriately sized.
                    unsafe { Self::copy_loaded_addresses(loaded_addresses, pubkeys_allocation) };
                    // SAFETY: pubkeys_allocation was allocated by allocator.
                    let offset = unsafe { self.allocator.offset(pubkeys_allocation.cast()) };
                    (
                        SharablePubkeys {
                            offset,
                            num_pubkeys: num_pubkeys as u32,
                        },
                        max_age.alt_invalidation_slot,
                    )
                }
                _ => (
                    SharablePubkeys {
                        offset: 0,
                        num_pubkeys: 0,
                    },
                    u64::MAX,
                ),
            };

            response.resolution_slot = resolution_slot;
            response.resolved_pubkeys = sharable_keys;
            response.min_alt_deactivation_slot = alt_invalidation_slot;
        }

        Ok(())
    }

    fn return_unprocessed_message(
        &mut self,
        message: &PackToCheckWorkerMessage,
        processed_code: u8,
    ) -> Result<(), ExternalCheckWorkerError> {
        assert_ne!(processed_code, processed_codes::PROCESSED);

        self.sender
            .try_write(CheckWorkerToPackMessage {
                batch: message.batch,
                processed_code,
                responses: TransactionResponseRegion {
                    tag: 0,
                    num_transaction_responses: 0,
                    transaction_responses_offset: 0,
                },
            })
            .map_err(|_| ExternalCheckWorkerError::SenderDisconnected)?;

        Ok(())
    }

    /// # Safety:
    /// - `responses_ptr` must be aligned and sufficiently sized.
    unsafe fn parse_transactions_and_populate_initial_check_responses<'a>(
        message: &PackToCheckWorkerMessage,
        batch: &TransactionPtrBatch,
        bank: &Bank,
        responses_ptr: NonNull<CheckResponse>,
    ) -> (
        ArrayVec<Result<(), TransactionViewError>, MAX_TRANSACTIONS_PER_MESSAGE>,
        ArrayVec<TxView, MAX_TRANSACTIONS_PER_MESSAGE>,
        &'a mut [CheckResponse],
    ) {
        let sanitize_config =
            sanitize_config(bank.feature_set.snapshot().limit_instruction_accounts);
        let mut parsing_results = ArrayVec::new();
        let mut parsed_transactions = ArrayVec::new();
        for (tx_ptr, _) in batch.iter() {
            match SanitizedTransactionView::try_new_sanitized(tx_ptr, &sanitize_config) {
                Ok(view) => {
                    parsing_results.push(Ok(()));
                    parsed_transactions.push(view);
                }
                Err(err) => {
                    parsing_results.push(Err(err));
                }
            }
        }

        // SAFETY: `response_ptr` is valid and of length message.batch.num_transactions.
        unsafe { Self::check_populate_initial_messages(message, &parsing_results, responses_ptr) };
        // SAFETY: `response_ptr` is valid and of length message.batch.num_transactions.
        let response_slice = unsafe {
            core::slice::from_raw_parts_mut(
                responses_ptr.as_ptr(),
                usize::from(message.batch.num_transactions),
            )
        };

        (parsing_results, parsed_transactions, response_slice)
    }

    /// # Safety
    /// - `responses_ptr` is valid ptr for a slice of [`CheckResponse`] with at least
    ///   length `message.batch.num_transactions`.
    unsafe fn check_populate_initial_messages(
        message: &PackToCheckWorkerMessage,
        parsing_results: &[Result<(), TransactionViewError>],
        responses_ptr: NonNull<CheckResponse>,
    ) {
        assert_eq!(
            parsing_results.len(),
            usize::from(message.batch.num_transactions)
        );
        let initial_status_check_flags = if message.flags & check_message_flags::STATUS_CHECKS != 0
        {
            status_check_flags::REQUESTED
        } else {
            0
        };
        let initial_fee_payer_balance_flags =
            if message.flags & check_message_flags::LOAD_FEE_PAYER_BALANCE != 0 {
                fee_payer_balance_flags::REQUESTED
            } else {
                0
            };
        let initial_resolve_flags =
            if message.flags & check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES != 0 {
                resolve_flags::REQUESTED
            } else {
                0
            };

        for (transaction_index, parsing_result) in parsing_results.iter().enumerate() {
            let parsing_and_sanitization_flags = if parsing_result.is_err() {
                parsing_and_sanitization_flags::FAILED
            } else {
                0
            };

            // SAFETY: transaction_index is in bounds.
            unsafe {
                responses_ptr.add(transaction_index).write(CheckResponse {
                    parsing_and_sanitization_flags,
                    status_check_flags: initial_status_check_flags,
                    fee_payer_balance_flags: initial_fee_payer_balance_flags,
                    resolve_flags: initial_resolve_flags,
                    included_slot: 0,
                    balance_slot: 0,
                    fee_payer_balance: 0,
                    resolution_slot: 0,
                    min_alt_deactivation_slot: 0,
                    resolved_pubkeys: SharablePubkeys {
                        offset: 0,
                        num_pubkeys: 0,
                    },
                })
            };
        }
    }

    fn validate_message(message: &PackToCheckWorkerMessage) -> bool {
        message.batch.num_transactions > 0
            && usize::from(message.batch.num_transactions) <= MAX_TRANSACTIONS_PER_MESSAGE
            && Self::validate_message_flags(message.flags)
    }

    fn validate_message_flags(flags: u16) -> bool {
        const ALLOWED_CHECK_FLAGS: u16 = check_message_flags::STATUS_CHECKS
            | check_message_flags::LOAD_FEE_PAYER_BALANCE
            | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES;

        flags != 0 && flags & !ALLOWED_CHECK_FLAGS == 0
    }

    fn check_load_fee_payer_balance<D: TransactionData>(
        parsing_results: &[Result<(), TransactionViewError>],
        parsed_transactions: &[SanitizedTransactionView<D>],
        responses: &mut [CheckResponse],
        working_bank: &Bank,
    ) {
        assert_eq!(responses.len(), parsing_results.len());

        let mut parsed_transaction_iter = parsed_transactions.iter();
        for (transaction_index, parsing_result) in parsing_results.iter().enumerate() {
            if parsing_result.is_err() {
                continue;
            }

            let transaction = parsed_transaction_iter.next().expect(
                "parsed_transaction_iter iterator must contain element for each sent parsed \
                 transaction",
            );

            let fee_payer_balance = working_bank
                .rc
                .accounts
                .load_with_fixed_root(
                    &working_bank.ancestors,
                    &transaction.static_account_keys()[0],
                )
                .map(|(account, _slot)| account.lamports())
                .unwrap_or(0);

            let response = &mut responses[transaction_index];
            response.fee_payer_balance_flags |= fee_payer_balance_flags::PERFORMED;
            response.fee_payer_balance = fee_payer_balance;
            response.balance_slot = working_bank.slot();
        }
    }

    fn check_status_checks<D: TransactionData>(
        parsing_and_resolve_results: &[Result<(), PacketHandlingError>],
        txs: &[RuntimeTransaction<ResolvedTransactionView<D>>],
        responses: &mut [CheckResponse],
        working_bank: &Bank,
    ) {
        assert_eq!(parsing_and_resolve_results.len(), responses.len());

        let mut error_counters = TransactionErrorMetrics::default();
        let (status_check_results, included_slots) = working_bank
            .check_transactions_with_processed_slots(
                txs,
                &[const { Ok(()) }; MAX_TRANSACTIONS_PER_MESSAGE],
                working_bank.max_processing_age(),
                true,
                true,
                &mut error_counters,
            );
        let included_slots = included_slots.expect("requested to collect processed slots");

        let mut status_check_results_iter = status_check_results.iter().zip(included_slots.iter());
        for (transaction_index, parsing_and_resolve_result) in
            parsing_and_resolve_results.iter().enumerate()
        {
            if parsing_and_resolve_result.is_err() {
                continue;
            }
            let (status_check_result, included_slot) = status_check_results_iter
                .next()
                .expect("status check results must have element for each sent transaction");

            let check_response = &mut responses[transaction_index];
            check_response.status_check_flags |= status_check_flags::PERFORMED;
            match status_check_result {
                Err(TransactionError::BlockhashNotFound) => {
                    check_response.status_check_flags |= status_check_flags::TOO_OLD;
                }
                Err(TransactionError::AlreadyProcessed) => {
                    check_response.status_check_flags |= status_check_flags::ALREADY_PROCESSED;
                    check_response.included_slot =
                        included_slot.expect("included_slot must be set for already processed");
                }
                Err(TransactionError::UnsupportedVersion) => {
                    check_response.status_check_flags |= status_check_flags::UNSUPPORTED_VERSION;
                }
                _ => {}
            }
        }
    }

    fn translate_transaction_batch(
        batch: &TransactionPtrBatch,
        bank: &Bank,
    ) -> (
        ArrayVec<Result<(), PacketHandlingError>, MAX_TRANSACTIONS_PER_MESSAGE>,
        ArrayVec<Tx, MAX_TRANSACTIONS_PER_MESSAGE>,
        ArrayVec<MaxAge, MAX_TRANSACTIONS_PER_MESSAGE>,
    ) {
        let sanitize_config =
            sanitize_config(bank.feature_set.snapshot().limit_instruction_accounts);
        let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();

        let mut translation_results = ArrayVec::new();
        let mut transactions = ArrayVec::new();
        let mut max_ages = ArrayVec::new();
        for (transaction_ptr, _) in batch.iter() {
            match Self::translate_transaction(
                transaction_ptr,
                bank,
                transaction_account_lock_limit,
                &sanitize_config,
            ) {
                Ok((tx, max_age)) => {
                    transactions.push(tx);
                    max_ages.push(max_age);
                    translation_results.push(Ok(()));
                }
                Err(err) => translation_results.push(Err(err)),
            }
        }

        (translation_results, transactions, max_ages)
    }

    fn translate_transaction(
        transaction_ptr: TransactionPtr,
        bank: &Bank,
        transaction_account_lock_limit: usize,
        sanitize_config: &SanitizeConfig,
    ) -> Result<(Tx, MaxAge), PacketHandlingError> {
        translate_to_runtime_view(
            transaction_ptr,
            bank,
            transaction_account_lock_limit,
            sanitize_config,
        )
        .map(|(view, deactivation_slot)| {
            (
                view,
                MaxAge {
                    sanitized_epoch: bank.epoch(),
                    alt_invalidation_slot: deactivation_slot,
                },
            )
        })
    }

    /// # Safety
    /// - destination is appropriately sized
    /// - destination does not overlap with loaded_addresses allocation
    unsafe fn copy_loaded_addresses(loaded_addresses: &LoadedAddresses, dest: NonNull<Pubkey>) {
        unsafe {
            core::ptr::copy_nonoverlapping(
                loaded_addresses.writable.as_ptr(),
                dest.as_ptr(),
                loaded_addresses.writable.len(),
            );
            core::ptr::copy_nonoverlapping(
                loaded_addresses.readonly.as_ptr(),
                dest.add(loaded_addresses.writable.len()).as_ptr(),
                loaded_addresses.readonly.len(),
            );
        }
    }
}

/// Returns an active leader state if available, otherwise None.
fn active_leader_state(
    shared_leader_state: &SharedLeaderState,
) -> Option<arc_swap::Guard<Arc<LeaderState>>> {
    let guard = shared_leader_state.load();
    if guard
        .as_ref()
        .working_bank()
        .map(|bank| bank.is_complete())
        .unwrap_or(true)
    {
        None
    } else {
        Some(guard)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::create_slow_genesis_config,
        agave_scheduler_bindings::{
            SharableTransactionBatchRegion, SharableTransactionRegion, worker_message_types,
        },
        agave_scheduling_utils::{
            handshake::{ClientLogon, client, server::Server},
            responses_region::CheckResponsesPtr,
        },
        solana_account::AccountSharedData,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_sdk_ids::system_program,
        solana_signer::Signer,
        solana_system_transaction::transfer,
        std::{
            sync::{Arc, RwLock},
            time::Duration,
        },
    };

    struct SharedBatch {
        region: SharableTransactionBatchRegion,
        transactions: Vec<SharableTransactionRegion>,
    }

    struct CheckWorkerTestFrame {
        bank: Arc<Bank>,
        _bank_forks: Arc<RwLock<BankForks>>,
        allocator: rts_alloc::Allocator,
        pack_to_check_worker: shaq::mpmc::Producer<PackToCheckWorkerMessage>,
        check_worker_to_pack: shaq::mpmc::Consumer<CheckWorkerToPackMessage>,
        worker: ExternalCheckWorker,
    }

    impl CheckWorkerTestFrame {
        fn send_message(&self, message: PackToCheckWorkerMessage) {
            self.pack_to_check_worker.try_write(message).unwrap();
        }

        fn iterate(&mut self) -> Result<(), ExternalCheckWorkerError> {
            let result = self.worker.iterate(Duration::ZERO)?;
            assert!(matches!(result, IterationResult::ProcessedMessage));
            Ok(())
        }

        fn iterate_idle(&mut self) -> Result<(), ExternalCheckWorkerError> {
            let result = self.worker.iterate(Duration::ZERO)?;
            assert!(matches!(result, IterationResult::Idle));
            Ok(())
        }

        fn recv_response(&self) -> CheckWorkerToPackMessage {
            self.check_worker_to_pack
                .read_timeout(Duration::from_secs(1))
                .unwrap()
        }

        fn check_responses(&self, region: &TransactionResponseRegion) -> Vec<CheckResponse> {
            assert_eq!(region.tag, worker_message_types::CHECK_RESPONSE);
            unsafe {
                // SAFETY: `region` was produced by this worker using the same shared allocator,
                // and the tag assertion above guarantees the pointed-to allocation contains
                // `CheckResponse` values.
                let responses =
                    CheckResponsesPtr::from_transaction_response_region(region, &self.allocator);
                let decoded = responses.iter().copied().collect();
                responses.free(&self.allocator);
                decoded
            }
        }

        fn allocate_batch(&self, transactions: &[Vec<u8>]) -> SharedBatch {
            type Batch<'a> = TransactionPtrBatch<'a>;
            assert!(transactions.len() <= MAX_TRANSACTIONS_PER_MESSAGE);

            let batch_ptr = self
                .allocator
                .allocate(Batch::TRANSACTION_META_END as u32)
                .unwrap();
            // SAFETY: `batch_ptr` came from this allocator immediately above, so translating it
            // back to an offset in the same allocator is valid.
            let batch_offset = unsafe { self.allocator.offset(batch_ptr) };
            let tx_ptr = batch_ptr.cast::<SharableTransactionRegion>();

            let mut sharable_transactions = Vec::with_capacity(transactions.len());
            for (index, transaction) in transactions.iter().enumerate() {
                let tx_allocation = self
                    .allocator
                    .allocate(transaction.len().try_into().unwrap())
                    .unwrap();
                unsafe {
                    // SAFETY: `tx_allocation` points to a fresh allocation of exactly
                    // `transaction.len()` bytes, and `transaction.as_ptr()` is readable for that
                    // same length. The regions do not overlap.
                    std::ptr::copy_nonoverlapping(
                        transaction.as_ptr(),
                        tx_allocation.as_ptr(),
                        transaction.len(),
                    );
                }
                let tx_region = SharableTransactionRegion {
                    // SAFETY: `tx_allocation` came from this allocator immediately above, so
                    // translating it back to an offset in the same allocator is valid.
                    offset: unsafe { self.allocator.offset(tx_allocation) },
                    length: transaction.len().try_into().unwrap(),
                };
                unsafe {
                    // SAFETY: the batch allocation is sized for
                    // `TransactionPtrBatch::TRANSACTION_META_END`, which includes space for up to
                    // `MAX_TRANSACTIONS_PER_MESSAGE` transaction headers, and the assert above
                    // guarantees `index` is in-bounds.
                    tx_ptr.add(index).write(tx_region)
                };
                sharable_transactions.push(tx_region);
            }

            SharedBatch {
                region: SharableTransactionBatchRegion {
                    num_transactions: transactions.len().try_into().unwrap(),
                    transactions_offset: batch_offset,
                },
                transactions: sharable_transactions,
            }
        }

        fn free_batch(&self, batch: SharedBatch) {
            for tx in batch.transactions {
                unsafe {
                    // SAFETY: each `tx.offset` was allocated by this allocator in
                    // `allocate_batch`, and `SharedBatch` owns each allocation exactly once.
                    self.allocator
                        .free(self.allocator.ptr_from_offset(tx.offset));
                }
            }
            unsafe {
                // SAFETY: `transactions_offset` is the batch container allocation created by
                // `allocate_batch`, and `SharedBatch` owns it exclusively here.
                self.allocator.free(
                    self.allocator
                        .ptr_from_offset(batch.region.transactions_offset),
                );
            }
        }
    }

    fn setup_check_worker_test_frame() -> CheckWorkerTestFrame {
        let GenesisConfigInfo { genesis_config, .. } = create_slow_genesis_config(10_000);
        let (root_bank, _root_bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let child_bank = Bank::new_from_parent(root_bank, SlotLeader::new_unique(), 1);
        let (bank, bank_forks) = child_bank.wrap_with_bank_forks_for_tests();

        let logon = ClientLogon {
            worker_count: 1,
            check_worker_count: 1,
            allocator_size: 64 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_capacity: 16,
            progress_tracker_capacity: 16,
            pack_to_worker_capacity: 16,
            worker_to_pack_capacity: 16,
            flags: 0,
        };
        let (_agave_session, files) = Server::setup_session(logon).unwrap();
        let mut client_session = client::setup_session(&logon, files).unwrap();
        let allocator = client_session.allocators.pop().unwrap();

        let (pack_to_check_worker, receiver) = shaq::mpmc::pair(16).unwrap();
        let (check_worker_to_pack, response_receiver) = shaq::mpmc::pair(16).unwrap();
        let worker_allocator = rts_alloc::Allocator::join_from_existing(&allocator)
            .expect("join allocator from test allocator");
        let shared_leader_state = SharedLeaderState::new(0, None, None);
        let worker = ExternalCheckWorker::new(
            Arc::new(AtomicBool::new(false)),
            receiver,
            check_worker_to_pack,
            worker_allocator,
            shared_leader_state,
            bank_forks.read().unwrap().sharable_banks(),
        );

        CheckWorkerTestFrame {
            bank,
            _bank_forks: bank_forks,
            allocator,
            pack_to_check_worker,
            check_worker_to_pack: response_receiver,
            worker,
        }
    }

    fn test_serialized_transaction(recent_blockhash: solana_hash::Hash) -> Vec<u8> {
        wincode::serialize(&transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            recent_blockhash,
        ))
        .unwrap()
    }

    #[test]
    fn test_idle_timeout() {
        let mut test_frame = setup_check_worker_test_frame();
        test_frame.iterate_idle().unwrap();
    }

    #[test]
    fn test_invalid_message() {
        let mut test_frame = setup_check_worker_test_frame();

        test_frame.send_message(PackToCheckWorkerMessage {
            flags: check_message_flags::STATUS_CHECKS,
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: 0,
            },
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::INVALID);
        assert_eq!(response.responses.num_transaction_responses, 0);
        assert_eq!(response.responses.transaction_responses_offset, 0);

        let batch = test_frame.allocate_batch(&[test_serialized_transaction(
            test_frame.bank.confirmed_last_blockhash(),
        )]);
        test_frame.send_message(PackToCheckWorkerMessage {
            flags: u16::MAX,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::INVALID);
        assert_eq!(response.responses.num_transaction_responses, 0);
        assert_eq!(response.responses.transaction_responses_offset, 0);

        test_frame.send_message(PackToCheckWorkerMessage {
            flags: 0,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::INVALID);
        assert_eq!(response.responses.num_transaction_responses, 0);
        assert_eq!(response.responses.transaction_responses_offset, 0);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_happy_path() {
        let mut test_frame = setup_check_worker_test_frame();
        let fee_payer = Keypair::new();
        let fee_payer_balance = 123_456;
        test_frame.bank.store_account(
            &fee_payer.pubkey(),
            &AccountSharedData::new(fee_payer_balance, 0, &system_program::ID),
        );

        let batch = test_frame.allocate_batch(&[wincode::serialize(&transfer(
            &fee_payer,
            &Pubkey::new_unique(),
            1,
            test_frame.bank.confirmed_last_blockhash(),
        ))
        .unwrap()]);
        test_frame.send_message(PackToCheckWorkerMessage {
            flags: check_message_flags::STATUS_CHECKS | check_message_flags::LOAD_FEE_PAYER_BALANCE,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        let responses = test_frame.check_responses(&response.responses);
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].status_check_flags,
            status_check_flags::REQUESTED | status_check_flags::PERFORMED
        );
        assert_eq!(
            responses[0].fee_payer_balance_flags,
            fee_payer_balance_flags::REQUESTED | fee_payer_balance_flags::PERFORMED
        );
        assert_eq!(responses[0].balance_slot, test_frame.bank.slot());
        assert_eq!(responses[0].fee_payer_balance, fee_payer_balance);

        test_frame.free_batch(batch);
    }

    #[test]
    fn test_resolve_without_loaded_addresses() {
        let mut test_frame = setup_check_worker_test_frame();
        let batch = test_frame.allocate_batch(&[test_serialized_transaction(
            test_frame.bank.confirmed_last_blockhash(),
        )]);

        test_frame.send_message(PackToCheckWorkerMessage {
            flags: check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            batch: batch.region,
        });
        test_frame.iterate().unwrap();
        let response = test_frame.recv_response();
        assert_eq!(response.processed_code, processed_codes::PROCESSED);
        let responses = test_frame.check_responses(&response.responses);
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].resolve_flags,
            resolve_flags::REQUESTED | resolve_flags::PERFORMED
        );
        assert_eq!(responses[0].resolution_slot, test_frame.bank.slot());
        assert_eq!(responses[0].resolved_pubkeys.num_pubkeys, 0);
        assert_eq!(responses[0].min_alt_deactivation_slot, u64::MAX);

        test_frame.free_batch(batch);
    }
}
