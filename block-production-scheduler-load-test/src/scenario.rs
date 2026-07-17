use {
    crate::{Harness, TpuInjectorError},
    agave_scheduler_bindings::tpu_message_flags,
    solana_account::AccountSharedData,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_message::Message,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_sdk_ids::system_program,
    solana_signature::Signature,
    solana_system_interface::instruction as system_instruction,
    solana_transaction::{Transaction, versioned::VersionedTransaction},
    std::{
        sync::{Arc, atomic::Ordering},
        time::{Duration, Instant},
    },
};

const ACCOUNT_PAIRS: usize = 16 * 1_024;
const ACCOUNT_PAIRS_U64: u64 = ACCOUNT_PAIRS as u64;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const ACCOUNT_BALANCE: u64 = 100 * LAMPORTS_PER_SOL;
const UNIQUE_TRANSFER_AMOUNTS_PER_PAIR: u64 = 100;
const UNIQUE_COMPUTE_UNIT_PRICES: u64 = 100;
const COMPUTE_UNIT_PRICE_STEP_MICRO_LAMPORTS: u64 = 1_000;
const TRANSFER_COMPUTE_UNIT_LIMIT: u32 = 200;
const LOADED_ACCOUNTS_DATA_SIZE_LIMIT: u32 = 32 * 1024;
const BLOCKHASH_PERCENT_DENOMINATOR: u64 = 100;
const REPORT_INTERVAL: Duration = Duration::from_secs(1);

const SIGNATURE_MARKER: [u8; 64] = [0x41; 64];
const SOURCE_MARKER: [u8; 32] = [0x42; 32];
const DESTINATION_MARKER: [u8; 32] = [0x43; 32];
const BLOCKHASH_MARKER: [u8; 32] = [0x44; 32];
const EXPIRED_BLOCKHASH_MARKER: [u8; 32] = [0x45; 32];
const LAMPORTS_MARKER: u64 = 0xf0e1_d2c3_b4a5_9687;
const COMPUTE_UNIT_PRICE_MARKER: u64 = 0x8796_a5b4_c3d2_e1f0;

/// A source of serialized transactions for [`run_scenario`].
pub trait LoadTestScenario {
    /// Initialize the initial leader bank before the harness starts its workers.
    fn setup(&mut self, bank: &Arc<Bank>);

    /// Return the next serialized transaction using `recent_blockhash`.
    fn next_transaction(&mut self, recent_blockhash: &[u8; 32]) -> Vec<u8>;
}

/// Run `scenario` continuously, filling the direct TPU queue until the harness is stopped.
///
/// The callback is invoked roughly once per second with the number of transactions sent per
/// second during that preceding interval.
pub fn run_scenario<S>(
    harness: &mut Harness,
    scenario: &mut S,
    mut report: impl FnMut(u64),
) -> Result<(), TpuInjectorError>
where
    S: LoadTestScenario,
{
    let exit = harness.exit_signal();
    let mut sent_since_report = 0u64;
    let mut last_report = Instant::now();

    while !exit.load(Ordering::Relaxed) {
        let recent_blockhash = harness.working_bank().last_blockhash().to_bytes();
        let injector = harness.injector();
        injector.sync();

        while !exit.load(Ordering::Relaxed) {
            let transaction = scenario.next_transaction(&recent_blockhash);

            match injector.try_push(&transaction, tpu_message_flags::NONE, [0; 16]) {
                Ok(()) => sent_since_report = sent_since_report.wrapping_add(1),
                Err(TpuInjectorError::QueueFull | TpuInjectorError::AllocatorFull) => break,
                Err(error) => return Err(error),
            }
        }
        injector.commit();

        let elapsed = last_report.elapsed();
        if elapsed >= REPORT_INTERVAL {
            let transactions_sent_per_second =
                (sent_since_report as f64 / elapsed.as_secs_f64()).round() as u64;
            report(transactions_sent_per_second);
            sent_since_report = 0;
            last_report = Instant::now();
        }
    }

    Ok(())
}

/// A saturating stream of simple system transfers.
///
/// The constructor is the only place this scenario uses SDK transaction types. It serializes one
/// marked transfer and records its byte offsets. [`Self::next_transaction`] only clones those
/// bytes and overwrites the signature, account keys, recent blockhash, and transfer amount.
pub struct TransferScenario {
    template: Box<[u8]>,
    signature_offset: usize,
    source_offset: usize,
    destination_offset: usize,
    blockhash_offset: usize,
    lamports_offset: usize,
    compute_unit_price_offset: usize,
    accounts: Box<[Pubkey]>,
    transaction_index: u64,
    expired_blockhash_percent: u8,
}

impl TransferScenario {
    /// Construct the one serialized transfer template used by the hot path.
    pub fn new(expired_blockhash_percent: u8) -> Self {
        assert!(
            expired_blockhash_percent <= 100,
            "expired blockhash percentage must not exceed 100"
        );
        let source_marker = Pubkey::new_from_array(SOURCE_MARKER);
        let destination_marker = Pubkey::new_from_array(DESTINATION_MARKER);
        let blockhash_marker = Hash::new_from_array(BLOCKHASH_MARKER);
        let mut transaction = Transaction::new_unsigned(Message::new_with_blockhash(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(TRANSFER_COMPUTE_UNIT_LIMIT),
                ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_UNIT_PRICE_MARKER),
                ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                    LOADED_ACCOUNTS_DATA_SIZE_LIMIT,
                ),
                system_instruction::transfer(&source_marker, &destination_marker, LAMPORTS_MARKER),
            ],
            Some(&source_marker),
            &blockhash_marker,
        ));
        transaction.signatures[0] = Signature::from(SIGNATURE_MARKER);
        let template = wincode::serialize(&VersionedTransaction::from(transaction))
            .expect("serializing a fixed transfer template must succeed")
            .into_boxed_slice();

        Self {
            signature_offset: find_unique_offset(&template, &SIGNATURE_MARKER),
            source_offset: find_unique_offset(&template, &SOURCE_MARKER),
            destination_offset: find_unique_offset(&template, &DESTINATION_MARKER),
            blockhash_offset: find_unique_offset(&template, &BLOCKHASH_MARKER),
            lamports_offset: find_unique_offset(&template, &LAMPORTS_MARKER.to_le_bytes()),
            compute_unit_price_offset: find_unique_offset(
                &template,
                &COMPUTE_UNIT_PRICE_MARKER.to_le_bytes(),
            ),
            template,
            accounts: (0..ACCOUNT_PAIRS.saturating_mul(2))
                .map(|_| Pubkey::new_unique())
                .collect(),
            transaction_index: 0,
            expired_blockhash_percent,
        }
    }
}

impl LoadTestScenario for TransferScenario {
    fn setup(&mut self, bank: &Arc<Bank>) {
        let account = AccountSharedData::new(ACCOUNT_BALANCE, 0, &system_program::id());
        for pubkey in self.accounts.iter() {
            bank.store_account(pubkey, &account);
        }
    }

    fn next_transaction(&mut self, recent_blockhash: &[u8; 32]) -> Vec<u8> {
        let transaction_index = self.transaction_index;
        self.transaction_index = self.transaction_index.wrapping_add(1);

        #[allow(clippy::arithmetic_side_effects)]
        let pair_index = (transaction_index as usize) % ACCOUNT_PAIRS;
        let transactions_for_pair = transaction_index.wrapping_div(ACCOUNT_PAIRS_U64);
        #[allow(clippy::arithmetic_side_effects)]
        let first_account = pair_index * 2;
        let second_account = first_account.wrapping_add(1);
        // Reverse direction every time a pair completes the full amount sequence so its two
        // accounts retain their original balances over each pair of cycles.
        let (source, destination) =
            if transactions_for_pair.wrapping_div(UNIQUE_TRANSFER_AMOUNTS_PER_PAIR) & 1 == 0 {
                (
                    &self.accounts[first_account],
                    &self.accounts[second_account],
                )
            } else {
                (
                    &self.accounts[second_account],
                    &self.accounts[first_account],
                )
            };
        let lamports = transfer_lamports(transactions_for_pair).to_le_bytes();
        let compute_unit_price = compute_unit_price(transaction_index).to_le_bytes();
        let blockhash = self
            .use_expired_blockhash(transaction_index)
            .then(|| expired_blockhash(transaction_index))
            .unwrap_or(*recent_blockhash);

        let mut transaction = self.template.to_vec();
        let signature = invalid_signature(transaction_index);
        transaction[self.signature_offset..self.signature_offset.wrapping_add(signature.len())]
            .copy_from_slice(&signature);
        transaction[self.source_offset..self.source_offset.wrapping_add(source.as_ref().len())]
            .copy_from_slice(source.as_ref());
        transaction[self.destination_offset
            ..self
                .destination_offset
                .wrapping_add(destination.as_ref().len())]
            .copy_from_slice(destination.as_ref());
        transaction[self.blockhash_offset..self.blockhash_offset.wrapping_add(blockhash.len())]
            .copy_from_slice(&blockhash);
        transaction[self.lamports_offset..self.lamports_offset.wrapping_add(lamports.len())]
            .copy_from_slice(&lamports);
        transaction[self.compute_unit_price_offset
            ..self
                .compute_unit_price_offset
                .wrapping_add(compute_unit_price.len())]
            .copy_from_slice(&compute_unit_price);
        transaction
    }
}

impl TransferScenario {
    fn use_expired_blockhash(&self, transaction_index: u64) -> bool {
        transaction_index.wrapping_rem(BLOCKHASH_PERCENT_DENOMINATOR)
            < u64::from(self.expired_blockhash_percent)
    }
}

fn find_unique_offset(bytes: &[u8], marker: &[u8]) -> usize {
    let mut offsets = bytes
        .windows(marker.len())
        .enumerate()
        .filter_map(|(offset, candidate)| (candidate == marker).then_some(offset));
    let offset = offsets
        .next()
        .expect("marker must occur in the serialized transfer template");
    assert!(
        offsets.next().is_none(),
        "marker must occur exactly once in the serialized transfer template"
    );
    offset
}

fn invalid_signature(transaction_index: u64) -> [u8; 64] {
    let mut signature = [0; 64];
    signature[..8].copy_from_slice(&transaction_index.to_le_bytes());
    signature[56..].copy_from_slice(&u64::MAX.to_le_bytes());
    signature
}

fn transfer_lamports(transactions_for_pair: u64) -> u64 {
    transactions_for_pair
        .wrapping_rem(UNIQUE_TRANSFER_AMOUNTS_PER_PAIR)
        .wrapping_add(1)
}

fn compute_unit_price(transaction_index: u64) -> u64 {
    transaction_index
        .wrapping_rem(UNIQUE_COMPUTE_UNIT_PRICES)
        .wrapping_add(1)
        .saturating_mul(COMPUTE_UNIT_PRICE_STEP_MICRO_LAMPORTS)
}

fn expired_blockhash(transaction_index: u64) -> [u8; 32] {
    let mut blockhash = EXPIRED_BLOCKHASH_MARKER;
    blockhash[..8].copy_from_slice(&transaction_index.to_le_bytes());
    blockhash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_generation_only_patches_the_wire_template() {
        let mut scenario = TransferScenario::new(0);
        let first = scenario.next_transaction(&[1; 32]);
        scenario.transaction_index = ACCOUNT_PAIRS_U64;
        let second = scenario.next_transaction(&[2; 32]);

        assert_ne!(first, second);
        assert_eq!(
            &first[scenario.blockhash_offset..scenario.blockhash_offset + 32],
            &[1; 32]
        );
        assert_eq!(
            &second[scenario.blockhash_offset..scenario.blockhash_offset + 32],
            &[2; 32]
        );
        assert_ne!(
            &first[scenario.signature_offset..scenario.signature_offset + 64],
            &second[scenario.signature_offset..scenario.signature_offset + 64]
        );
        assert_eq!(
            u64::from_le_bytes(
                first[scenario.lamports_offset..scenario.lamports_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            1,
        );
        assert_eq!(
            u64::from_le_bytes(
                second[scenario.lamports_offset..scenario.lamports_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            2,
        );
        assert_eq!(
            u64::from_le_bytes(
                first[scenario.compute_unit_price_offset..scenario.compute_unit_price_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            compute_unit_price(0),
        );
        assert_eq!(
            u64::from_le_bytes(
                second[scenario.compute_unit_price_offset..scenario.compute_unit_price_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            compute_unit_price(ACCOUNT_PAIRS_U64),
        );
    }

    #[test]
    fn transfer_amounts_cycle_per_pair() {
        for transaction_index in 0..UNIQUE_TRANSFER_AMOUNTS_PER_PAIR {
            assert_eq!(
                transfer_lamports(transaction_index),
                transaction_index.wrapping_add(1),
            );
        }
        assert_eq!(transfer_lamports(UNIQUE_TRANSFER_AMOUNTS_PER_PAIR), 1,);
    }

    #[test]
    fn compute_unit_prices_cycle() {
        for transaction_index in 0..UNIQUE_COMPUTE_UNIT_PRICES {
            assert_eq!(
                compute_unit_price(transaction_index),
                transaction_index
                    .wrapping_add(1)
                    .saturating_mul(COMPUTE_UNIT_PRICE_STEP_MICRO_LAMPORTS),
            );
        }
        assert_eq!(compute_unit_price(UNIQUE_COMPUTE_UNIT_PRICES), 1_000,);
    }

    #[test]
    fn expired_blockhashes_follow_the_configured_percentage() {
        let scenario = TransferScenario::new(67);
        assert!(scenario.use_expired_blockhash(0));
        assert!(scenario.use_expired_blockhash(66));
        assert!(!scenario.use_expired_blockhash(67));
        assert!(scenario.use_expired_blockhash(100));
        assert_ne!(expired_blockhash(1), [1; 32]);
        assert_ne!(expired_blockhash(1), expired_blockhash(2));
    }
}
