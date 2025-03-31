use {
    log::info,
    solana_hash::Hash,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_message::{Message, VersionedMessage},
    solana_poh::{
        poh_controller::PohController,
        poh_recorder::{PohRecorder, PohRecorderError},
        poh_service::PohService,
        record_channel::record_channels,
        transaction_recorder::TransactionRecorder,
    },
    solana_poh_config::PohConfig,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        genesis_utils::{self, GenesisConfigInfo},
        installed_scheduler_pool::BankWithScheduler,
    },
    solana_transaction::versioned::VersionedTransaction,
    std::{
        path::PathBuf,
        sync::{atomic::AtomicBool, Arc, RwLock},
        time::Duration,
    },
};

// Spawn a PoH service thread, and just spam transactions.
fn main() {
    solana_logger::setup_with_default("info");

    let ledger_dir = PathBuf::from("poh-slam-temp/ledger");
    let _ = std::fs::remove_dir_all(&ledger_dir);
    let blockstore = Arc::new(Blockstore::open(ledger_dir.as_path()).unwrap());

    let accounts_dir = PathBuf::from("poh-slam-temp/ledger/accounts");
    let GenesisConfigInfo { genesis_config, .. } = genesis_utils::create_genesis_config_with_leader(
        1_000_000_000_000,
        &Pubkey::new_unique(),
        1_000_000_000,
    );
    let exit: Arc<AtomicBool> = Arc::default();

    let mut bank = Arc::new(Bank::new_with_paths(
        &genesis_config,
        Arc::default(),
        vec![accounts_dir],
        None,
        None,
        false,
        None,
        None,
        None,
        exit.clone(),
        None,
        None,
    ));

    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
    let poh_config = PohConfig {
        target_tick_duration: Duration::from_nanos(6250),
        target_tick_count: None,
        hashes_per_tick: Some(62500),
    };
    let (poh_recorder, entry_receiver) = PohRecorder::new(
        0,
        Hash::default(),
        bank.clone(),
        Some((1, 1)),
        64,
        blockstore,
        &leader_schedule_cache,
        &poh_config,
        exit.clone(),
    );
    let poh_recorder = Arc::new(RwLock::new(poh_recorder));

    let _entry_receiver_handle =
        std::thread::Builder::new().spawn(move || for _ in entry_receiver.iter() {});

    let (record_sender, record_receiver) = record_channels(false);
    let transaction_recorder = TransactionRecorder::new(record_sender, exit.clone());
    let (poh_controller, bank_message_receiver) = PohController::new();
    let _poh_service = PohService::new(
        poh_recorder.clone(),
        &poh_config,
        exit,
        64,
        2,
        64,
        record_receiver,
        bank_message_receiver,
        poh_controller.pending_message(),
    );

    // Dummy transaction - no signatures, no instructions.
    // Let's us clone as fast as this should let us.
    let txs = vec![VersionedTransaction {
        signatures: vec![],
        message: VersionedMessage::Legacy(Message::new(&[], None)),
    }];

    let collector_id = &Pubkey::new_unique();
    let mut slot = bank.slot();
    let mut num_recorded = 0u64;
    // Set bank in poh on controller.
    poh_controller
        .set_bank(BankWithScheduler::new_without_scheduler(bank.clone()))
        .unwrap();
    loop {
        std::thread::sleep(Duration::from_micros(1));
        let result = transaction_recorder.record(slot, Hash::default(), txs.clone());
        match result {
            Ok(_) => {
                num_recorded = num_recorded.wrapping_add(1);
                if num_recorded % 1000 == 0 {
                    info!("{slot}: {num_recorded}");
                }
            }
            Err(PohRecorderError::MaxHeightReached) => {
                info!("{slot}: {num_recorded}");

                // Wait for PohRecorder to be done.
                while poh_recorder.read().unwrap().has_bank() {
                    std::thread::sleep(Duration::from_millis(1));
                }

                bank = Arc::new(Bank::new_from_parent(
                    bank,
                    collector_id,
                    slot.wrapping_add(1),
                ));
                slot = bank.slot();
                num_recorded = 0;

                // Set bank in poh on controller.
                poh_controller
                    .set_bank(BankWithScheduler::new_without_scheduler(bank.clone()))
                    .unwrap();

                info!("Starting {slot}...");
            }
            Err(err) => {
                panic!("Unexpected error: {err:?}");
            }
        }
    }
}
