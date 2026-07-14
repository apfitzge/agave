use {
    crate::{
        state_container::StateContainer,
        transaction::{CheckBatch, CheckTransactionMeta, MAX_PACKETS_PER_CHECK_BATCH},
    },
    agave_scheduler_bindings::{PackToCheckWorkerMessage, check_message_flags},
    agave_scheduling_utils::transaction_priority_queue::TransactionPriorityId,
    rts_alloc::Allocator,
};

pub(crate) const MAX_RECHECK_PACKETS_PER_ITERATION: usize = 128;

const RECHECK_FLAGS: u16 =
    check_message_flags::STATUS_CHECKS | check_message_flags::LOAD_FEE_PAYER_BALANCE;

pub(crate) struct RecheckScratch {
    cursor: Option<TransactionPriorityId>,
    transaction_ids: Vec<usize>,
}

impl RecheckScratch {
    pub(crate) fn new() -> Self {
        Self {
            cursor: None,
            transaction_ids: Vec::with_capacity(MAX_PACKETS_PER_CHECK_BATCH),
        }
    }
}

/// Send a bounded, resumable descending scan to the check workers.
///
/// Rechecks leave their transactions queued, so a high-priority transaction can be scheduled
/// before its recheck returns. Each transaction has at most one outstanding recheck request.
pub(crate) fn send_rechecks(
    scheduler_to_check_worker: &shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    allocator: &Allocator,
    transactions: &mut StateContainer,
    scratch: &mut RecheckScratch,
    max_packets: usize,
) {
    let mut remaining_packets = max_packets;
    while remaining_packets > 0 {
        let Some(mut batch) = CheckBatch::allocate(allocator) else {
            break;
        };
        let cursor_before_batch = scratch.cursor;
        scratch.transaction_ids.clear();

        let mut visited = 0;
        let mut reached_bottom = false;
        {
            let mut priority_ids = transactions.descending_from(scratch.cursor.as_ref());
            while visited < remaining_packets.min(MAX_PACKETS_PER_CHECK_BATCH) {
                let Some(priority_id) = priority_ids.next() else {
                    reached_bottom = true;
                    break;
                };
                let priority_id = *priority_id;
                scratch.cursor = Some(priority_id);
                visited = visited.wrapping_add(1);

                if transactions.has_references(priority_id.id) {
                    continue;
                }
                let transaction = transactions.get(priority_id.id);
                // SAFETY: the checked transaction remains owned by `transactions`; the check
                // worker only borrows its sharable transaction region.
                let transaction = unsafe {
                    transaction
                        .transaction
                        .to_sharable_transaction_region(allocator)
                };
                assert!(
                    batch
                        .push(
                            transaction,
                            CheckTransactionMeta::Recheck {
                                transaction_id: priority_id.id,
                            },
                        )
                        .is_ok(),
                    "recheck batches are bounded by the check-worker capacity"
                );
                scratch.transaction_ids.push(priority_id.id);
            }
        }

        if batch.is_empty() {
            // SAFETY: this empty batch was allocated locally and was not sent to a worker.
            unsafe { batch.free() };
            if reached_bottom {
                scratch.cursor = None;
                break;
            }
            remaining_packets = remaining_packets.wrapping_sub(visited);
            continue;
        }

        if scheduler_to_check_worker
            .try_write(PackToCheckWorkerMessage {
                flags: RECHECK_FLAGS,
                batch: batch.to_sharable_transaction_batch_region(),
            })
            .is_err()
        {
            // SAFETY: a failed queue write leaves the batch container under scheduler ownership.
            // Its transactions remain owned by `transactions` and must not be freed here.
            unsafe { batch.free() };
            scratch.cursor = cursor_before_batch;
            break;
        }

        for &transaction_id in &scratch.transaction_ids {
            transactions.start_recheck(transaction_id);
        }
        remaining_packets = remaining_packets.wrapping_sub(visited);
        if reached_bottom {
            scratch.cursor = None;
            break;
        }
    }
}
