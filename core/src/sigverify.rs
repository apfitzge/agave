//! The `sigverify` module provides digital signature verification functions.
//! By default, signatures are verified in parallel using all available CPU
//! cores.  When perf-libs are available signature verification is offloaded
//! to the GPU.
//!

pub use solana_perf::sigverify::{
    count_packets_in_batches, ed25519_verify_cpu, ed25519_verify_disabled, init, TxOffset,
};
use {
    crate::{
        banking_trace::BankingPacketSender,
        sigverify_stage::{SigVerifier, SigVerifyServiceError},
    },
    agave_banking_stage_ingress_types::BankingPacketBatch,
    agave_feature_set::FeatureSet,
    agave_transaction_view::transaction_view::{SanitizedTransactionView, TransactionView},
    crossbeam_channel::Sender,
    rayon::iter::{IntoParallelRefMutIterator, ParallelIterator},
    solana_compute_budget_instruction::compute_budget_instruction_details::ComputeBudgetInstructionDetails,
    solana_fee::{calculate_fee, FeeFeatures},
    solana_perf::{cuda_runtime::PinnedVec, packet::PacketBatch, recycler::Recycler, sigverify},
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::signature_details::{
        get_precompile_signature_details, PrecompileSignatureDetails,
    },
    solana_svm::account_loader::validate_fee_payer_no_counters,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction_error::TransactionError,
    std::{
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

pub struct TransactionSigVerifier {
    banking_stage_sender: BankingPacketSender,
    forward_stage_sender: Option<Sender<(BankingPacketBatch, bool)>>,
    recycler: Recycler<TxOffset>,
    recycler_out: Recycler<PinnedVec<u8>>,
    reject_non_vote: bool,

    bank_forks: Option<Arc<RwLock<BankForks>>>,
    cached_working_bank: Option<Arc<Bank>>,
    last_bank_cache_time: Instant,
}

impl TransactionSigVerifier {
    pub fn new_reject_non_vote(
        packet_sender: BankingPacketSender,
        forward_stage_sender: Option<Sender<(BankingPacketBatch, bool)>>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
    ) -> Self {
        let mut new_self = Self::new(packet_sender, forward_stage_sender, bank_forks);
        new_self.reject_non_vote = true;
        new_self
    }

    pub fn new(
        banking_stage_sender: BankingPacketSender,
        forward_stage_sender: Option<Sender<(BankingPacketBatch, bool)>>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
    ) -> Self {
        init();
        let cached_working_bank = bank_forks
            .as_ref()
            .map(|bank_forks| bank_forks.read().unwrap().working_bank());
        Self {
            banking_stage_sender,
            forward_stage_sender,
            recycler: Recycler::warmed(50, 4096),
            recycler_out: Recycler::warmed(50, 4096),
            reject_non_vote: false,
            bank_forks,
            cached_working_bank,
            last_bank_cache_time: Instant::now(),
        }
    }

    fn get_working_bank(&mut self) -> Option<Arc<Bank>> {
        if let Some(bank_forks) = self.bank_forks.as_ref() {
            let now = Instant::now();
            if now.duration_since(self.last_bank_cache_time) > Duration::from_millis(25) {
                self.cached_working_bank = Some(bank_forks.read().unwrap().working_bank());
                self.last_bank_cache_time = now;
            }
            self.cached_working_bank.clone()
        } else {
            None
        }
    }
}

impl SigVerifier for TransactionSigVerifier {
    type SendType = BankingPacketBatch;

    fn send_packets(
        &mut self,
        packet_batches: Vec<PacketBatch>,
    ) -> Result<(), SigVerifyServiceError<Self::SendType>> {
        let banking_packet_batch = BankingPacketBatch::new(packet_batches);
        if let Some(forward_stage_sender) = &self.forward_stage_sender {
            self.banking_stage_sender
                .send(banking_packet_batch.clone())?;
            let _ = forward_stage_sender.try_send((banking_packet_batch, self.reject_non_vote));
        } else {
            self.banking_stage_sender.send(banking_packet_batch)?;
        }

        Ok(())
    }

    fn verify_batches(
        &mut self,
        mut batches: Vec<PacketBatch>,
        valid_packets: usize,
    ) -> Vec<PacketBatch> {
        sigverify::ed25519_verify(
            &mut batches,
            &self.recycler,
            &self.recycler_out,
            self.reject_non_vote,
            valid_packets,
        );

        if let Some(bank) = self.get_working_bank() {
            let fee_features = FeeFeatures::from(bank.feature_set.as_ref());
            batches.par_iter_mut().flatten().for_each(|mut packet| {
                let meta = packet.meta();
                if let Some(pkt_data) = packet.data(..meta.size) {
                    let Ok(view) = TransactionView::try_new_sanitized(pkt_data) else {
                        packet.meta_mut().set_discard(true);
                        return;
                    };

                    let fee = {
                        let Ok(details) = get_fee_details(&view, bank.feature_set.as_ref()) else {
                            packet.meta_mut().set_discard(true);
                            return;
                        };

                        let hack_wrapper = HackFeeWrapper {
                            view: &view,
                            details,
                        };

                        calculate_fee(
                            &hack_wrapper,
                            false, // testing on mnb - no need to have test-only shit
                            5_000, // testing on mnb - only ever 5klam
                            hack_wrapper.details.priority_fee,
                            fee_features,
                        )
                    };

                    let fee_payer = &view.static_account_keys()[0];

                    let Ok((mut fee_payer_account, _slot)) = bank
                        .rc
                        .accounts
                        .accounts_db
                        .load_with_fixed_root(&bank.ancestors, fee_payer)
                        .ok_or(TransactionError::AccountNotFound)
                    else {
                        packet.meta_mut().set_discard(true);
                        return;
                    };

                    if validate_fee_payer_no_counters(
                        fee_payer,
                        &mut fee_payer_account,
                        0,
                        bank.rent_collector(),
                        fee,
                    )
                    .is_err()
                    {
                        packet.meta_mut().set_discard(true);
                    }
                }
            })
        }

        batches
    }
}

// HACKS - don't want to resolve ALTs to get an SVMMessage, which is the only interface we have for fee.
//         implement just what we need to calculate fee!
struct HackFeeWrapper<'a> {
    view: &'a SanitizedTransactionView<&'a [u8]>,
    details: HackFeeDetails,
}

impl std::fmt::Debug for HackFeeWrapper<'_> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl SVMMessage for HackFeeWrapper<'_> {
    fn num_transaction_signatures(&self) -> u64 {
        u64::from(self.view.num_signatures())
    }

    fn num_ed25519_signatures(&self) -> u64 {
        self.details
            .precompile_details
            .num_ed25519_instruction_signatures
    }

    fn num_secp256k1_signatures(&self) -> u64 {
        self.details
            .precompile_details
            .num_secp256k1_instruction_signatures
    }

    fn num_secp256r1_signatures(&self) -> u64 {
        self.details
            .precompile_details
            .num_secp256r1_instruction_signatures
    }

    fn num_write_locks(&self) -> u64 {
        unimplemented!()
    }

    fn recent_blockhash(&self) -> &solana_hash::Hash {
        unimplemented!()
    }

    fn num_instructions(&self) -> usize {
        unimplemented!()
    }

    fn instructions_iter(
        &self,
    ) -> impl Iterator<Item = solana_svm_transaction::instruction::SVMInstruction> {
        std::iter::empty()
    }

    fn program_instructions_iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &solana_pubkey::Pubkey,
            solana_svm_transaction::instruction::SVMInstruction,
        ),
    > + Clone {
        std::iter::empty()
    }

    fn static_account_keys(&self) -> &[solana_pubkey::Pubkey] {
        unimplemented!()
    }

    fn account_keys(&self) -> solana_message::AccountKeys {
        unimplemented!()
    }

    fn fee_payer(&self) -> &solana_pubkey::Pubkey {
        unimplemented!()
    }

    fn is_writable(&self, _index: usize) -> bool {
        unimplemented!()
    }

    fn is_signer(&self, _index: usize) -> bool {
        unimplemented!()
    }

    fn is_invoked(&self, _key_index: usize) -> bool {
        unimplemented!()
    }

    fn num_lookup_tables(&self) -> usize {
        unimplemented!()
    }

    fn message_address_table_lookups(
        &self,
    ) -> impl Iterator<
        Item = solana_svm_transaction::message_address_table_lookup::SVMMessageAddressTableLookup,
    > {
        std::iter::empty()
    }
}

struct HackFeeDetails {
    priority_fee: u64,
    precompile_details: PrecompileSignatureDetails,
}

fn get_fee_details(
    view: &SanitizedTransactionView<&[u8]>,
    feature_set: &FeatureSet,
) -> Result<HackFeeDetails, TransactionError> {
    let x = ComputeBudgetInstructionDetails::try_from(view.program_instructions_iter())?;
    let compute_budget_limits = x.sanitize_and_convert_to_compute_budget_limits(feature_set)?;
    let priority_fee = u64::from(compute_budget_limits.compute_unit_limit)
        .saturating_mul(compute_budget_limits.compute_unit_price);
    let precompile_details = get_precompile_signature_details(view.program_instructions_iter());

    Ok(HackFeeDetails {
        priority_fee,
        precompile_details,
    })
}
