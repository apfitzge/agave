use {
    agave_block_verification_stage::setup::{
        ReplayEventBroadcast, SignatureVerificationRequest, SignatureVerificationResult,
        SignatureVerificationWorkerSession,
    },
    agave_scheduling_utils::{
        replay_events::{ReplayEvent, replay_event_tags},
        transaction_ptr::TransactionPtr,
    },
    agave_transaction_view::{
        transaction_version::TransactionVersion, transaction_view::UnsanitizedTransactionView,
    },
    solana_runtime::vote_sender_types::{ReplayVoteMessage, ReplayVoteSender},
    solana_transaction::simple_vote_transaction_checker::is_simple_vote_transaction_impl,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const STARTING_SLEEP_DURATION: Duration = Duration::from_micros(250);
const MAX_SLEEP_DURATION: Duration = Duration::from_millis(1);
const IDLE_SLEEP_THRESHOLD: Duration = Duration::from_millis(10);

pub(crate) fn spawn_replay_signature_verification_workers(
    exit: Arc<AtomicBool>,
    workers: Vec<SignatureVerificationWorkerSession>,
    replay_vote_sender: ReplayVoteSender,
    event_broadcast: Option<Arc<ReplayEventBroadcast>>,
) -> Vec<JoinHandle<()>> {
    workers
        .into_iter()
        .enumerate()
        .map(|(worker_id, worker)| {
            let exit = exit.clone();
            let replay_vote_sender = replay_vote_sender.clone();
            let event_broadcast = event_broadcast.clone();
            Builder::new()
                .name(format!("solBvSigvr{worker_id:02}"))
                .spawn(move || {
                    run_signature_verification_worker(
                        exit,
                        worker,
                        worker_id,
                        replay_vote_sender,
                        event_broadcast,
                    );
                })
                .unwrap()
        })
        .collect()
}

fn run_signature_verification_worker(
    exit: Arc<AtomicBool>,
    worker: SignatureVerificationWorkerSession,
    worker_id: usize,
    replay_vote_sender: ReplayVoteSender,
    event_broadcast: Option<Arc<ReplayEventBroadcast>>,
) {
    let mut sleep_duration = STARTING_SLEEP_DURATION;
    let mut did_work = false;
    let mut last_empty_time = Instant::now();

    while !exit.load(Ordering::Relaxed) {
        let Some(request) = worker.requests.try_read() else {
            let now = Instant::now();
            if did_work {
                last_empty_time = now;
            }
            did_work = false;
            sleep_duration = backoff(now.duration_since(last_empty_time), sleep_duration);
            continue;
        };

        did_work = true;
        sleep_duration = STARTING_SLEEP_DURATION;
        emit_signature_verification_worker_event(
            event_broadcast.as_deref(),
            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP,
            worker_id,
            request,
        );
        let verified = verify_transaction_signatures(
            &worker,
            worker_id,
            request,
            &replay_vote_sender,
            event_broadcast.as_deref(),
        );
        send_result(
            &exit,
            &worker,
            worker_id,
            SignatureVerificationResult::new(request.slot, request.transaction_index, verified),
            event_broadcast.as_deref(),
        );
    }
}

fn verify_transaction_signatures(
    worker: &SignatureVerificationWorkerSession,
    worker_id: usize,
    request: SignatureVerificationRequest,
    replay_vote_sender: &ReplayVoteSender,
    event_broadcast: Option<&ReplayEventBroadcast>,
) -> bool {
    let transaction = unsafe {
        // SAFETY: The scheduler only submits transaction regions backed by
        // the shared allocator mapping in this worker session.
        let ptr = worker.allocator.ptr_from_offset(request.transaction.offset);
        TransactionPtr::from_raw_parts(ptr, request.transaction.length as usize)
    };
    let Ok(view) = UnsanitizedTransactionView::try_new_unsanitized(transaction) else {
        return false;
    };
    emit_signature_verification_worker_event(
        event_broadcast,
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED,
        worker_id,
        request,
    );

    let verified = verify_signatures(&view);
    emit_signature_verification_worker_result_event(
        event_broadcast,
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE,
        worker_id,
        request.slot,
        request.transaction_index,
        verified,
    );
    if !verified {
        return false;
    }

    if is_simple_vote_transaction(&view) {
        if let Some(signature) = view.signatures().first().copied() {
            let _ = replay_vote_sender.send(ReplayVoteMessage::Verified {
                replay_bank_id: request.bank_id,
                replay_slot: request.slot,
                verified_signatures: vec![signature],
            });
        }
    }

    true
}

fn emit_signature_verification_worker_event(
    event_broadcast: Option<&ReplayEventBroadcast>,
    tag: u64,
    worker_id: usize,
    request: SignatureVerificationRequest,
) {
    let Some(event_broadcast) = event_broadcast else {
        return;
    };

    event_broadcast.emit(
        ReplayEvent::transaction_signature_verification_worker_event(
            0,
            tag,
            request.slot,
            u64::try_from(request.transaction_index).expect("transaction index must fit in u64"),
            u64::try_from(worker_id).expect("worker id must fit in u64"),
        ),
    );
}

fn emit_signature_verification_worker_result_event(
    event_broadcast: Option<&ReplayEventBroadcast>,
    tag: u64,
    worker_id: usize,
    slot: u64,
    transaction_index: usize,
    verified: bool,
) {
    let Some(event_broadcast) = event_broadcast else {
        return;
    };

    event_broadcast.emit(
        ReplayEvent::transaction_signature_verification_worker_result_event(
            0,
            tag,
            slot,
            u64::try_from(transaction_index).expect("transaction index must fit in u64"),
            u64::try_from(worker_id).expect("worker id must fit in u64"),
            verified,
        ),
    );
}

fn verify_signatures(view: &UnsanitizedTransactionView<TransactionPtr>) -> bool {
    let required_signatures = usize::from(view.num_required_signatures());
    if required_signatures == 0
        || view.signatures().len() != required_signatures
        || view.static_account_keys().len() < required_signatures
    {
        return false;
    }

    let message_data = view.message_data();
    view.signatures()
        .iter()
        .zip(view.static_account_keys().iter())
        .all(|(signature, pubkey)| signature.verify(pubkey.as_ref(), message_data))
}

fn is_simple_vote_transaction(view: &UnsanitizedTransactionView<TransactionPtr>) -> bool {
    let is_legacy = matches!(view.version(), TransactionVersion::Legacy);
    let account_keys = view.static_account_keys();
    let instruction_programs = view
        .instructions_iter()
        .filter_map(|instruction| account_keys.get(usize::from(instruction.program_id_index)));

    is_simple_vote_transaction_impl(view.signatures(), is_legacy, instruction_programs)
}

fn send_result(
    exit: &AtomicBool,
    worker: &SignatureVerificationWorkerSession,
    worker_id: usize,
    mut result: SignatureVerificationResult,
    event_broadcast: Option<&ReplayEventBroadcast>,
) {
    let mut sleep_duration = STARTING_SLEEP_DURATION;
    let mut last_full_time = Instant::now();
    let slot = result.slot;
    let transaction_index = result.transaction_index;
    let verified = result.verified();
    while !exit.load(Ordering::Relaxed) {
        match worker.results.try_write(result) {
            Ok(()) => {
                emit_signature_verification_worker_result_event(
                    event_broadcast,
                    replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT,
                    worker_id,
                    slot,
                    transaction_index,
                    verified,
                );
                return;
            }
            Err(returned_result) => {
                result = returned_result;
                sleep_duration = backoff(last_full_time.elapsed(), sleep_duration);
                last_full_time = Instant::now();
            }
        }
    }
}

fn backoff(idle_duration: Duration, sleep_duration: Duration) -> Duration {
    if idle_duration < IDLE_SLEEP_THRESHOLD {
        core::hint::spin_loop();
        sleep_duration
    } else {
        thread::sleep(sleep_duration);
        sleep_duration.saturating_mul(2).min(MAX_SLEEP_DURATION)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, core::ptr::NonNull, solana_hash::Hash, solana_keypair::Keypair,
        solana_pubkey::Pubkey, solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::versioned::VersionedTransaction,
    };

    fn transaction_view(bytes: &mut [u8]) -> UnsanitizedTransactionView<TransactionPtr> {
        let transaction = unsafe {
            TransactionPtr::from_raw_parts(NonNull::new(bytes.as_mut_ptr()).unwrap(), bytes.len())
        };
        UnsanitizedTransactionView::try_new_unsanitized(transaction).unwrap()
    }

    fn signed_transfer() -> VersionedTransaction {
        let payer = Keypair::new();
        VersionedTransaction::from(solana_transaction::Transaction::new_signed_with_payer(
            &[system_instruction::transfer(
                &payer.pubkey(),
                &Pubkey::new_unique(),
                1,
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_unique(),
        ))
    }

    #[test]
    fn direct_message_data_signature_verification_accepts_valid_transaction() {
        let mut bytes = wincode::serialize(&signed_transfer()).unwrap();
        let view = transaction_view(&mut bytes);

        assert!(verify_signatures(&view));
    }

    #[test]
    fn direct_message_data_signature_verification_rejects_mutated_signature() {
        let mut bytes = wincode::serialize(&signed_transfer()).unwrap();
        let first_signature_byte = bytes.get_mut(1).unwrap();
        *first_signature_byte = first_signature_byte.wrapping_add(1);
        let view = transaction_view(&mut bytes);

        assert!(!verify_signatures(&view));
    }
}
