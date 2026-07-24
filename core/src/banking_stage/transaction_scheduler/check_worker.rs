use {
    super::ingress_check::{
        CheckedTransaction, IngressCheckError, check_parsed_transaction, parse_transaction,
    },
    crossbeam_channel::{Receiver, Sender, bounded},
    solana_perf::packet::bytes::Bytes,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::SharableBanks,
    std::{collections::HashSet, num::NonZeroUsize, sync::Arc, thread::Builder},
};

const CHECK_QUEUE_CAPACITY: usize = 10_000;

pub(crate) struct CheckWork {
    pub(crate) bytes: Bytes,
    pub(crate) priority_floor: u64,
}

pub(crate) struct CheckWorkerPool {
    pub(crate) work_sender: Sender<CheckWork>,
    pub(crate) result_receiver: Receiver<Result<CheckedTransaction, IngressCheckError>>,
}

impl CheckWorkerPool {
    pub(crate) fn new(
        num_workers: NonZeroUsize,
        sharable_banks: SharableBanks,
        filter_keys: Arc<HashSet<Pubkey>>,
    ) -> Self {
        Self::new_with_capacities(
            num_workers,
            sharable_banks,
            filter_keys,
            CHECK_QUEUE_CAPACITY,
            CHECK_QUEUE_CAPACITY,
        )
    }

    fn new_with_capacities(
        num_workers: NonZeroUsize,
        sharable_banks: SharableBanks,
        filter_keys: Arc<HashSet<Pubkey>>,
        work_queue_capacity: usize,
        result_queue_capacity: usize,
    ) -> Self {
        let (work_sender, work_receiver) = bounded(work_queue_capacity);
        let (result_sender, result_receiver) = bounded(result_queue_capacity);

        for index in 0..num_workers.get() {
            let work_receiver = work_receiver.clone();
            let result_sender = result_sender.clone();
            let sharable_banks = sharable_banks.clone();
            let filter_keys = filter_keys.clone();
            Builder::new()
                .name(format!("solBnkChk{index:02}"))
                .spawn(move || {
                    run_check_worker(work_receiver, result_sender, sharable_banks, filter_keys);
                })
                .expect("check worker thread must spawn");
        }

        Self {
            work_sender,
            result_receiver,
        }
    }
}

fn run_check_worker(
    work_receiver: Receiver<CheckWork>,
    result_sender: Sender<Result<CheckedTransaction, IngressCheckError>>,
    sharable_banks: SharableBanks,
    filter_keys: Arc<HashSet<Pubkey>>,
) {
    while let Ok(work) = work_receiver.recv() {
        let result = check_work(
            work.bytes,
            work.priority_floor,
            &sharable_banks,
            &filter_keys,
        );

        // A result queue at capacity applies backpressure to check workers. Accepted
        // work is never dropped by a worker.
        if result_sender.send(result).is_err() {
            return;
        }
    }
}

fn check_work(
    bytes: Bytes,
    priority_floor: u64,
    sharable_banks: &SharableBanks,
    filter_keys: &HashSet<Pubkey>,
) -> Result<CheckedTransaction, IngressCheckError> {
    let banks = sharable_banks.load();
    let parsed_transaction =
        parse_transaction(bytes, &banks.root_bank, &banks.working_bank, filter_keys)?;

    if priority_floor > 0 && parsed_transaction.priority() <= priority_floor {
        return Err(IngressCheckError::BelowPriorityFloor);
    }

    check_parsed_transaction(parsed_transaction, &banks.working_bank)
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::banking_stage::tests::create_slow_genesis_config,
        solana_ledger::genesis_utils::GenesisConfigInfo, solana_perf::packet::BytesPacket,
        solana_runtime::bank::Bank, solana_system_transaction::transfer,
    };

    fn test_banks() -> (SharableBanks, solana_keypair::Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);
        let (_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        (bank_forks.read().unwrap().sharable_banks(), mint_keypair)
    }

    fn transaction_bytes(
        sharable_banks: &SharableBanks,
        mint_keypair: &solana_keypair::Keypair,
    ) -> Bytes {
        let transaction = transfer(
            mint_keypair,
            &Pubkey::new_unique(),
            1,
            sharable_banks.working().last_blockhash(),
        );
        BytesPacket::from_data(transaction)
            .unwrap()
            .buffer()
            .clone()
    }

    #[test]
    fn worker_applies_sampled_priority_floor() {
        let (sharable_banks, mint_keypair) = test_banks();
        let bytes = transaction_bytes(&sharable_banks, &mint_keypair);
        let pool = CheckWorkerPool::new_with_capacities(
            NonZeroUsize::new(1).unwrap(),
            sharable_banks,
            Arc::default(),
            1,
            1,
        );

        pool.work_sender
            .send(CheckWork {
                bytes,
                priority_floor: u64::MAX,
            })
            .unwrap();
        let result = pool.result_receiver.recv().unwrap();
        assert!(matches!(result, Err(IngressCheckError::BelowPriorityFloor)));
    }

    #[test]
    fn bounded_result_queue_does_not_drop_results() {
        let (sharable_banks, mint_keypair) = test_banks();
        let pool = CheckWorkerPool::new_with_capacities(
            NonZeroUsize::new(2).unwrap(),
            sharable_banks.clone(),
            Arc::default(),
            8,
            1,
        );
        for _ in 0..8 {
            pool.work_sender
                .send(CheckWork {
                    bytes: transaction_bytes(&sharable_banks, &mint_keypair),
                    priority_floor: 0,
                })
                .unwrap();
        }

        for _ in 0..8 {
            let result = pool.result_receiver.recv().unwrap();
            assert!(result.is_ok());
        }
    }
}
