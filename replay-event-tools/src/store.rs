use {
    agave_scheduling_utils::replay_events::{
        ReplayEvent, is_slot_event_tag, is_transaction_event_tag, replay_event_tags,
    },
    std::collections::BTreeMap,
};

pub(crate) struct EventStore {
    max_slots: usize,
    slots: BTreeMap<u64, SlotRecord>,
}

pub(crate) struct SlotRecord {
    pub(crate) slot: u64,
    pub(crate) slot_events: Vec<ReplayEvent>,
    pub(crate) transactions: BTreeMap<u64, TransactionRecord>,
}

pub(crate) struct TransactionRecord {
    pub(crate) index: u64,
    pub(crate) signature: Option<String>,
    pub(crate) events: Vec<ReplayEvent>,
}

impl EventStore {
    pub(crate) fn new(max_slots: usize) -> Self {
        assert!(max_slots > 0, "must retain at least one slot");
        Self {
            max_slots,
            slots: BTreeMap::new(),
        }
    }

    pub(crate) fn apply_event(&mut self, event: ReplayEvent) {
        let slot = event.slot();
        let slot_record = if event.tag == replay_event_tags::SLOT_BEGIN {
            self.slots
                .entry(slot)
                .or_insert_with(|| SlotRecord::new(slot))
        } else {
            let Some(slot_record) = self.slots.get_mut(&slot) else {
                return;
            };
            slot_record
        };

        if is_transaction_event_tag(event.tag) {
            let transaction_index = event
                .transaction_index()
                .expect("transaction event must carry transaction index");
            let transaction = slot_record
                .transactions
                .entry(transaction_index)
                .or_insert_with(|| TransactionRecord::new(transaction_index));

            if let Some(signature) = event.signature() {
                let signature = signature_string(&signature);
                transaction.signature = Some(signature);
            }

            transaction.events.push(event);
        } else if is_slot_event_tag(event.tag) {
            slot_record.slot_events.push(event);
        }

        self.prune_old_slots();
    }

    pub(crate) fn slot_ids(&self) -> Vec<u64> {
        self.slots.keys().copied().collect()
    }

    pub(crate) fn slot(&self, slot: u64) -> Option<&SlotRecord> {
        self.slots.get(&slot)
    }

    fn prune_old_slots(&mut self) {
        while self.slots.len() > self.max_slots {
            let Some(slot) = self.slots.keys().next().copied() else {
                break;
            };
            self.slots.remove(&slot);
        }
    }
}

impl SlotRecord {
    fn new(slot: u64) -> Self {
        Self {
            slot,
            slot_events: Vec::new(),
            transactions: BTreeMap::new(),
        }
    }

    pub(crate) fn status(&self) -> &'static str {
        if self
            .slot_events
            .iter()
            .any(|event| event.tag == replay_event_tags::SLOT_FAILED)
        {
            "failed"
        } else if self
            .slot_events
            .iter()
            .any(|event| event.tag == replay_event_tags::SLOT_ABORT)
        {
            "aborted"
        } else if self
            .slot_events
            .iter()
            .any(|event| event.tag == replay_event_tags::SLOT_COMPLETE)
        {
            "complete"
        } else if self
            .slot_events
            .iter()
            .any(|event| event.tag == replay_event_tags::SLOT_BEGIN)
        {
            "running"
        } else {
            "seen"
        }
    }

    pub(crate) fn transactions_by_ingest(&self) -> Vec<&TransactionRecord> {
        let mut transactions = self.transactions.values().collect::<Vec<_>>();
        transactions.sort_by_key(|transaction| {
            (
                transaction.ingest_timestamp_ns().unwrap_or(u64::MAX),
                transaction.index,
            )
        });
        transactions
    }

    pub(crate) fn duration_ns(&self) -> Option<u64> {
        let start = self.begin_timestamp_ns()?;
        let end = self
            .slot_events
            .iter()
            .find(|event| {
                matches!(
                    event.tag,
                    replay_event_tags::SLOT_ABORT
                        | replay_event_tags::SLOT_COMPLETE
                        | replay_event_tags::SLOT_FAILED
                )
            })
            .map(|event| event.timestamp_ns)?;
        Some(end.saturating_sub(start))
    }

    pub(crate) fn begin_timestamp_ns(&self) -> Option<u64> {
        self.slot_events
            .iter()
            .find(|event| event.tag == replay_event_tags::SLOT_BEGIN)
            .map(|event| event.timestamp_ns)
    }
}

impl TransactionRecord {
    fn new(index: u64) -> Self {
        Self {
            index,
            signature: None,
            events: Vec::new(),
        }
    }

    pub(crate) fn status(&self) -> &'static str {
        self.events
            .iter()
            .rev()
            .find_map(|event| match event.tag {
                replay_event_tags::TRANSACTION_FINISHED_EXEC => Some("finished"),
                replay_event_tags::TRANSACTION_EXEC_FAILED => Some("exec-failed"),
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED => {
                    Some("worker-exec-done")
                }
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY => {
                    Some("worker-exec-commit-ready")
                }
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED => {
                    Some("worker-exec-processed")
                }
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED => {
                    Some("worker-exec-translated")
                }
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED => {
                    Some("worker-exec-bank")
                }
                replay_event_tags::TRANSACTION_CHECK_FAILED => Some("check-failed"),
                replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC => Some("scheduled"),
                replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED => Some("skipped"),
                replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING => Some("ready"),
                replay_event_tags::TRANSACTION_SIGNATURES_RETURNED => {
                    if event.signature_verification_result() == Some(false) {
                        Some("sigverify-failed")
                    } else {
                        Some("sigverified")
                    }
                }
                replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT => {
                    Some("sigverify-result-sent")
                }
                replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE => {
                    Some("sigverify-worker-done")
                }
                replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED => {
                    Some("sigverify-worker-parsed")
                }
                replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP => {
                    Some("sigverify-worker-picked-up")
                }
                replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED => Some("sigverifying"),
                replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED => Some("worker-check-done"),
                replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE => {
                    Some("worker-check-status")
                }
                replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE => {
                    Some("worker-check-address-tables")
                }
                replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED => {
                    Some("worker-check-resolved")
                }
                replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE => {
                    Some("worker-check-fee-payer")
                }
                replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE => {
                    Some("worker-check-signatures")
                }
                replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED => Some("worker-check-parsed"),
                replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED => {
                    Some("worker-check-bank")
                }
                replay_event_tags::TRANSACTION_CHECK_PASSED => Some("checked"),
                replay_event_tags::TRANSACTION_WORKER_PICKED_UP => Some("worker-picked-up"),
                replay_event_tags::TRANSACTION_SENT_FOR_CHECK => Some("checking"),
                replay_event_tags::TRANSACTION_INGESTED => Some("ingested"),
                _ => None,
            })
            .unwrap_or("seen")
    }

    pub(crate) fn ingest_timestamp_ns(&self) -> Option<u64> {
        self.events
            .iter()
            .find(|event| event.tag == replay_event_tags::TRANSACTION_INGESTED)
            .map(|event| event.timestamp_ns)
    }

    pub(crate) fn terminal_timestamp_ns(&self) -> Option<u64> {
        let transaction_terminal = self
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.tag,
                    replay_event_tags::TRANSACTION_FINISHED_EXEC
                        | replay_event_tags::TRANSACTION_EXEC_FAILED
                        | replay_event_tags::TRANSACTION_CHECK_FAILED
                )
            })
            .map(|event| event.timestamp_ns)
            .max();
        let signature_submitted = self
            .events
            .iter()
            .any(|event| event.tag == replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED);
        let signature_returned = self
            .events
            .iter()
            .filter(|event| event.tag == replay_event_tags::TRANSACTION_SIGNATURES_RETURNED)
            .map(|event| event.timestamp_ns)
            .max();
        let signature_failed = self
            .events
            .iter()
            .filter(|event| {
                event.tag == replay_event_tags::TRANSACTION_SIGNATURES_RETURNED
                    && event.signature_verification_result() == Some(false)
            })
            .map(|event| event.timestamp_ns)
            .max();

        match (transaction_terminal, signature_submitted, signature_returned) {
            (Some(transaction_terminal), true, Some(signature_returned)) => {
                Some(transaction_terminal.max(signature_returned))
            }
            (Some(_), true, None) => None,
            (Some(transaction_terminal), false, _) => Some(transaction_terminal),
            (None, _, _) if signature_failed.is_some() => signature_failed,
            (None, _, _) => None,
        }
    }

    pub(crate) fn total_duration_ns(&self) -> Option<u64> {
        let start = self.ingest_timestamp_ns()?;
        let end = self.terminal_timestamp_ns()?;
        Some(end.saturating_sub(start))
    }
}

pub(crate) fn signature_string(signature: &[u8; 64]) -> String {
    bs58::encode(signature).into_string()
}

pub(crate) fn event_name(tag: u64) -> &'static str {
    match tag {
        replay_event_tags::SLOT_BEGIN => "slot-begin",
        replay_event_tags::SLOT_ABORT => "slot-abort",
        replay_event_tags::TRANSACTION_INGESTED => "tx-ingested",
        replay_event_tags::TRANSACTION_SENT_FOR_CHECK => "tx-sent-for-check",
        replay_event_tags::TRANSACTION_CHECK_FAILED => "tx-check-failed",
        replay_event_tags::TRANSACTION_CHECK_PASSED => "tx-check-passed",
        replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED => "tx-signatures-submitted",
        replay_event_tags::TRANSACTION_SIGNATURES_RETURNED => "tx-signatures-returned",
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PICKED_UP => {
            "tx-sigverify-worker-picked-up"
        }
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_PARSED => {
            "tx-sigverify-worker-parsed"
        }
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_SIGNATURES_COMPLETE => {
            "tx-sigverify-worker-signatures-complete"
        }
        replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT => {
            "tx-sigverify-worker-result-sent"
        }
        replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING => "tx-ready-for-scheduling",
        replay_event_tags::TRANSACTION_WORKER_PICKED_UP => "tx-worker-picked-up",
        replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED => "tx-worker-check-completed",
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED => {
            "tx-worker-execution-completed"
        }
        replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED => "tx-worker-check-bank",
        replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED => "tx-worker-check-parsed",
        replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE => {
            "tx-worker-check-signatures-complete"
        }
        replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE => {
            "tx-worker-check-fee-payer-balance-complete"
        }
        replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED => "tx-worker-check-resolved",
        replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE => {
            "tx-worker-check-address-tables-complete"
        }
        replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE => {
            "tx-worker-check-status-complete"
        }
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED => "tx-worker-execution-bank",
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED => {
            "tx-worker-execution-translated"
        }
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED => {
            "tx-worker-execution-processed"
        }
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY => {
            "tx-worker-execution-commit-results-ready"
        }
        replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED => "tx-scheduling-skipped",
        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC => "tx-scheduled-for-exec",
        replay_event_tags::TRANSACTION_FINISHED_EXEC => "tx-finished-exec",
        replay_event_tags::TRANSACTION_EXEC_FAILED => "tx-exec-failed",
        replay_event_tags::SLOT_COMPLETE => "slot-complete",
        replay_event_tags::SLOT_FAILED => "slot-failed",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_events_attach_to_ingested_signature() {
        let signature = [7; 64];
        let signature_string = signature_string(&signature);
        let mut store = EventStore::new(4);

        store.apply_event(ReplayEvent::slot_begin(1, 42));
        store.apply_event(ReplayEvent::transaction_ingested(10, 42, 0, signature));
        store.apply_event(ReplayEvent::transaction_event(
            20,
            replay_event_tags::TRANSACTION_SENT_FOR_CHECK,
            42,
            0,
        ));
        store.apply_event(ReplayEvent::transaction_event(
            30,
            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
            42,
            0,
        ));
        store.apply_event(ReplayEvent::transaction_event(
            40,
            replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED,
            42,
            0,
        ));
        store.apply_event(ReplayEvent::transaction_ready_for_scheduling(
            50,
            42,
            0,
            0,
        ));
        store.apply_event(ReplayEvent::transaction_event(
            60,
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            42,
            0,
        ));

        let slot = store.slot(42).unwrap();
        let transaction = slot.transactions.get(&0).unwrap();
        assert_eq!(transaction.signature.as_ref(), Some(&signature_string));
        assert_eq!(transaction.status(), "finished");
        assert_eq!(transaction.total_duration_ns(), Some(50));
        assert_eq!(transaction.events.len(), 6);
    }

    #[test]
    fn transaction_duration_waits_for_slower_signature_verification() {
        let mut store = EventStore::new(4);

        store.apply_event(ReplayEvent::slot_begin(1, 42));
        store.apply_event(ReplayEvent::transaction_ingested(10, 42, 0, [7; 64]));
        store.apply_event(ReplayEvent::transaction_signatures_submitted(20, 42, 0, 1));
        store.apply_event(ReplayEvent::transaction_event(
            30,
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            42,
            0,
        ));
        store.apply_event(ReplayEvent::transaction_signatures_returned(
            50, 42, 0, true,
        ));

        let transaction = store.slot(42).unwrap().transactions.get(&0).unwrap();
        assert_eq!(transaction.total_duration_ns(), Some(40));
    }

    #[test]
    fn transaction_duration_uses_execution_when_signature_verification_was_ready() {
        let mut store = EventStore::new(4);

        store.apply_event(ReplayEvent::slot_begin(1, 42));
        store.apply_event(ReplayEvent::transaction_ingested(10, 42, 0, [7; 64]));
        store.apply_event(ReplayEvent::transaction_signatures_submitted(12, 42, 0, 1));
        store.apply_event(ReplayEvent::transaction_signatures_returned(
            20, 42, 0, true,
        ));
        store.apply_event(ReplayEvent::transaction_event(
            50,
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            42,
            0,
        ));

        let transaction = store.slot(42).unwrap().transactions.get(&0).unwrap();
        assert_eq!(transaction.total_duration_ns(), Some(40));
    }

    #[test]
    fn prunes_slots_older_than_limit() {
        let mut store = EventStore::new(2);

        store.apply_event(ReplayEvent::slot_begin(1, 10));
        store.apply_event(ReplayEvent::slot_begin(2, 11));
        store.apply_event(ReplayEvent::slot_begin(3, 12));

        assert_eq!(store.slot_ids(), [11, 12]);
        assert!(store.slot(10).is_none());
    }

    #[test]
    fn ignores_events_for_slots_without_begin() {
        let signature = [7; 64];
        let mut store = EventStore::new(4);

        store.apply_event(ReplayEvent::transaction_ingested(10, 42, 0, signature));
        store.apply_event(ReplayEvent::slot_complete(20, 42));

        assert_eq!(store.slot_ids(), []);
        assert!(store.slot(42).is_none());

        store.apply_event(ReplayEvent::slot_begin(30, 42));
        store.apply_event(ReplayEvent::transaction_ingested(40, 42, 0, signature));

        let slot = store.slot(42).unwrap();
        assert_eq!(slot.status(), "running");
        assert_eq!(slot.slot_events.len(), 1);
        assert_eq!(slot.transactions.len(), 1);
    }

    #[test]
    fn slot_duration_uses_begin_to_terminal_slot_event() {
        let mut store = EventStore::new(2);

        store.apply_event(ReplayEvent::slot_begin(10, 42));
        store.apply_event(ReplayEvent::transaction_ingested(15, 42, 0, [1; 64]));
        store.apply_event(ReplayEvent::slot_complete(30, 42));

        assert_eq!(store.slot(42).unwrap().duration_ns(), Some(20));
    }
}
