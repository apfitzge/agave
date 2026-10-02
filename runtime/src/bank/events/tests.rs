use {
    super::*,
    crate::{
        bank::Bank,
        bank_forks::BankForks,
        genesis_utils::{GenesisConfigInfo, create_genesis_config_with_leader},
    },
    agave_event_system::subscriber::{StreamExplorer, Subscriber, Typed},
    solana_account::WritableAccount,
    solana_fee_calculator::FeeRateGovernor,
    solana_hash::Hash,
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_sdk_ids::system_program,
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    solana_transaction::Transaction,
    std::{sync::RwLock, thread},
};

struct Fixture {
    bank: Arc<Bank>,
    mint: Keypair,
    subscriber: Subscriber<Typed<AccountCommitted<'static>>>,
    system: EventSystem,
    _directory: tempfile::TempDir,
    _bank_forks: Arc<RwLock<BankForks>>,
}

impl Fixture {
    fn new(capacity: usize, payload_capacity: u64) -> Self {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(1_000_000_000, &Pubkey::new_unique(), 1_000_000);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(5000, 0);
        let (parent, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        // A bank created before stream initialization must share the registration.
        let bank = Arc::new(Bank::new_from_parent(parent.clone(), *parent.leader(), 1));
        let directory = tempfile::tempdir().unwrap();
        let system = EventSystem::new(directory.path()).unwrap();
        let publishers = AccountEventPublishers::with_config(
            &system,
            StreamConfig {
                capacity,
                publisher_slots: 4,
                subscriber_slots: 1,
            },
            payload_capacity,
        )
        .unwrap();
        parent.rc.account_events.set(publishers).unwrap();
        let subscriber = StreamExplorer::new(directory.path().into())
            .available_streams()
            .next()
            .unwrap()
            .try_connect_typed::<AccountCommitted>()
            .unwrap();
        system.set_stream_policy("on".parse().unwrap());
        Self {
            bank,
            mint: mint_keypair,
            subscriber,
            system,
            _directory: directory,
            _bank_forks: bank_forks,
        }
    }
}

#[test]
fn committed_account_payload_matches_stored_state_and_survives_later_commits() {
    let mut fixture = Fixture::new(16, 32 * 1024);
    let recipient = Pubkey::new_unique();
    let data = vec![0x5a; 4096];
    let mut account = AccountSharedData::new(100_000_000, data.len(), &Pubkey::new_unique());
    account.set_data_from_slice(&data);
    account.set_rent_epoch(42);
    fixture.bank.store_account(&recipient, &account);
    // Ordinary bank writes must not emit transaction commit events.
    assert!(fixture.subscriber.try_recv().is_err());

    fixture.bank.transfer(1, &fixture.mint, &recipient).unwrap();
    let payer = fixture.subscriber.try_recv().unwrap();
    let payer_event = payer.decode().unwrap();
    assert_eq!(payer_event.pubkey, fixture.mint.pubkey().to_bytes());
    assert!(payer_event.data.is_empty());
    assert_eq!(
        payer_event.lamports,
        fixture.bank.get_balance(&fixture.mint.pubkey())
    );
    drop(payer);
    let recipient_event = fixture.subscriber.try_recv().unwrap();
    let event = recipient_event.decode().unwrap();
    let stored = fixture.bank.get_account(&recipient).unwrap();
    assert_eq!(event.slot, fixture.bank.slot());
    assert_eq!(event.bank_id, fixture.bank.bank_id());
    assert_eq!(event.owner, stored.owner().to_bytes());
    assert_eq!(event.lamports, stored.lamports());
    assert_eq!(event.executable, stored.executable());
    assert_eq!(event.rent_epoch, stored.rent_epoch());
    assert_eq!(event.data, stored.data());
    assert!(event.timestamp_ns <= monotonic_timestamp_ns());
    let previous_version = event.write_version;

    fixture.bank.transfer(2, &fixture.mint, &recipient).unwrap();
    // The first payload remains valid while its receive guard is held.
    assert_eq!(recipient_event.decode().unwrap().data, data);
    drop(recipient_event);
    for _ in 0..2 {
        let message = fixture.subscriber.try_recv().unwrap();
        let event = message.decode().unwrap();
        assert!(event.write_version > previous_version);
        if event.pubkey == recipient.to_bytes() {
            assert_eq!(event.lamports, stored.lamports() + 2);
            assert_eq!(event.data, data);
        }
    }
    assert!(fixture.subscriber.try_recv().is_err());
}

#[test]
fn only_rollback_accounts_are_published_for_failed_and_fees_only_transactions() {
    let mut fixture = Fixture::new(16, 1024);
    let recipient = Pubkey::new_unique();
    let fee_payer = fixture.mint.pubkey();
    let initial_balance = fixture.bank.get_balance(&fee_payer);
    for (index, instructions) in [
        vec![
            system_instruction::transfer(&fee_payer, &recipient, 1_000_000),
            Instruction::new_with_wincode(system_program::id(), &(), vec![]),
        ],
        vec![Instruction::new_with_wincode(
            Pubkey::new_unique(),
            &(),
            vec![],
        )],
    ]
    .into_iter()
    .enumerate()
    {
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&fee_payer),
            &[&fixture.mint],
            fixture.bank.last_blockhash(),
        );
        assert!(fixture.bank.process_transaction(&tx).is_err());
        let message = fixture.subscriber.try_recv().unwrap();
        let event = message.decode().unwrap();
        assert_eq!(event.pubkey, fee_payer.to_bytes());
        assert_eq!(event.lamports, initial_balance - (index as u64 + 1) * 5000);
        assert_eq!(event.lamports, fixture.bank.get_balance(&fee_payer));
        assert!(event.data.is_empty());
        drop(message);
        assert!(fixture.subscriber.try_recv().is_err());
        assert!(fixture.bank.get_account(&recipient).is_none());
    }
}

#[test]
fn rejected_transactions_do_not_publish_accounts() {
    let mut fixture = Fixture::new(16, 1024);
    let tx = Transaction::new(
        &[&fixture.mint],
        Message::new(
            &[system_instruction::transfer(
                &fixture.mint.pubkey(),
                &Pubkey::new_unique(),
                1_000_000,
            )],
            Some(&fixture.mint.pubkey()),
        ),
        Hash::new_unique(),
    );
    assert!(fixture.bank.process_transaction(&tx).is_err());
    assert!(fixture.subscriber.try_recv().is_err());
}

#[test]
fn stream_policy_and_child_banks_share_publishers() {
    let mut fixture = Fixture::new(16, 1024);
    fixture.system.set_stream_policy("off".parse().unwrap());
    fixture
        .bank
        .transfer(1_000_000, &fixture.mint, &Pubkey::new_unique())
        .unwrap();
    assert!(fixture.subscriber.try_recv().is_err());
    fixture
        .system
        .set_stream_policy("account.committed=on".parse().unwrap());
    let child = Bank::new_from_parent(fixture.bank.clone(), *fixture.bank.leader(), 2);
    // Creating a child writes sysvars; none of those writes should be emitted.
    assert!(fixture.subscriber.try_recv().is_err());
    child
        .transfer(1_000_000, &fixture.mint, &Pubkey::new_unique())
        .unwrap();
    for _ in 0..2 {
        let message = fixture.subscriber.try_recv().unwrap();
        let event = message.decode().unwrap();
        assert_eq!(event.slot, child.slot());
        assert_eq!(event.bank_id, child.bank_id());
    }
    assert!(fixture.subscriber.try_recv().is_err());
}

#[test]
fn closing_an_account_publishes_its_zero_balance() {
    let mut fixture = Fixture::new(16, 1024);
    let sender = Keypair::new();
    fixture.bank.store_account(
        &sender.pubkey(),
        &AccountSharedData::new(1_000_000, 0, &system_program::id()),
    );
    let recipient = Pubkey::new_unique();
    let tx = Transaction::new_signed_with_payer(
        &[system_instruction::transfer(
            &sender.pubkey(),
            &recipient,
            1_000_000,
        )],
        Some(&fixture.mint.pubkey()),
        &[&fixture.mint, &sender],
        fixture.bank.last_blockhash(),
    );
    fixture.bank.process_transaction(&tx).unwrap();
    let mut saw_closed_account = false;
    for _ in 0..3 {
        let message = fixture.subscriber.try_recv().unwrap();
        let event = message.decode().unwrap();
        if event.pubkey == sender.pubkey().to_bytes() {
            assert_eq!(event.lamports, 0);
            assert!(event.data.is_empty());
            saw_closed_account = true;
        }
    }
    assert!(saw_closed_account);
    assert!(fixture.bank.get_account(&sender.pubkey()).is_none());
    assert!(fixture.subscriber.try_recv().is_err());
}

#[test]
fn insufficient_payload_space_does_not_prevent_commits() {
    let mut fixture = Fixture::new(16, 8);
    let recipient = Pubkey::new_unique();
    let mut account = AccountSharedData::new(1_000_000, 32, &Pubkey::new_unique());
    account.set_data_from_slice(&[7; 32]);
    fixture.bank.store_account(&recipient, &account);
    fixture.bank.transfer(1, &fixture.mint, &recipient).unwrap();
    assert_eq!(fixture.bank.get_balance(&recipient), 1_000_001);
    let message = fixture.subscriber.try_recv().unwrap();
    assert_eq!(
        message.decode().unwrap().pubkey,
        fixture.mint.pubkey().to_bytes()
    );
    drop(message);
    assert!(fixture.subscriber.try_recv().is_err());
}

#[test]
fn publisher_exhaustion_and_backpressure_do_not_prevent_commits() {
    let mut fixture = Fixture::new(1, 1);
    // Every new thread consumes a publisher slot for the lifetime of the stream.
    // Commits must still succeed after all four publisher slots have been used.
    for _ in 0..5 {
        let recipient = Pubkey::new_unique();
        thread::scope(|scope| {
            scope
                .spawn(|| fixture.bank.transfer(1_000_000, &fixture.mint, &recipient))
                .join()
                .unwrap()
                .unwrap();
        });
        assert_eq!(fixture.bank.get_balance(&recipient), 1_000_000);
    }
    // Each one-cell queue retained one event and dropped the second.
    let mut received = 0;
    while fixture.subscriber.try_recv().is_ok() {
        received += 1;
    }
    assert_eq!(received, 4);
}
