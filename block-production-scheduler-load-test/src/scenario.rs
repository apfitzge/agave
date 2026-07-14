use {
    crate::{Harness, TpuInjectorError},
    agave_scheduler_bindings::tpu_message_flags,
    solana_account::AccountSharedData,
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

const ACCOUNT_PAIRS: usize = 1_024;
const ACCOUNT_BALANCE: u64 = 100_000_000_000_000;
const REPORT_INTERVAL: Duration = Duration::from_secs(1);

const SIGNATURE_MARKER: [u8; 64] = [0x41; 64];
const SOURCE_MARKER: [u8; 32] = [0x42; 32];
const DESTINATION_MARKER: [u8; 32] = [0x43; 32];
const BLOCKHASH_MARKER: [u8; 32] = [0x44; 32];

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
/// bytes and overwrites the signature, account keys, and recent blockhash.
pub struct TransferScenario {
    template: Box<[u8]>,
    signature_offset: usize,
    source_offset: usize,
    destination_offset: usize,
    blockhash_offset: usize,
    accounts: Box<[Pubkey]>,
    transaction_index: u64,
}

impl TransferScenario {
    /// Construct the one serialized transfer template used by the hot path.
    pub fn new() -> Self {
        let source_marker = Pubkey::new_from_array(SOURCE_MARKER);
        let destination_marker = Pubkey::new_from_array(DESTINATION_MARKER);
        let blockhash_marker = Hash::new_from_array(BLOCKHASH_MARKER);
        let mut transaction = Transaction::new_unsigned(Message::new_with_blockhash(
            &[system_instruction::transfer(
                &source_marker,
                &destination_marker,
                1,
            )],
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
            template,
            accounts: (0..ACCOUNT_PAIRS.saturating_mul(2))
                .map(|_| Pubkey::new_unique())
                .collect(),
            transaction_index: 0,
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
        #[allow(clippy::arithmetic_side_effects)]
        let first_account = pair_index * 2;
        let second_account = first_account.wrapping_add(1);
        let (source, destination) = if transaction_index & 1 == 0 {
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
        transaction
            [self.blockhash_offset..self.blockhash_offset.wrapping_add(recent_blockhash.len())]
            .copy_from_slice(recent_blockhash);
        transaction
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_generation_only_patches_the_wire_template() {
        let mut scenario = TransferScenario::new();
        let first = scenario.next_transaction(&[1; 32]);
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
    }
}
