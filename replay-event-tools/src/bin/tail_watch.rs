#[allow(dead_code)]
#[path = "../store.rs"]
mod store;

use {
    agave_scheduling_utils::{
        replay_events::{
            REPLAY_EVENTS_IPC_FILE, ReplayEvent, replay_event_tags,
            replay_scheduling_skip_reasons,
        },
        shared_memory,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        env,
        error::Error,
        path::PathBuf,
        process,
        sync::atomic::Ordering,
        thread,
        time::Duration,
    },
    store::{EventStore, SlotRecord, TransactionRecord},
};

const DEFAULT_RETAINED_SLOTS: usize = 64;
const DEFAULT_POLL_MS: u64 = 10;
const DEFAULT_TAIL_THRESHOLD_MS: u64 = 50;
const MAX_PRINTED_HANDOFF_GAPS: usize = 3;
const MAX_PRINTED_PICKUP_WAITS: usize = 3;

struct Args {
    ledger_path: PathBuf,
    retained_slots: usize,
    poll_interval: Duration,
    tail_threshold_ns: u64,
}

#[derive(Clone)]
struct TransactionAnalysis {
    index: u64,
    status: &'static str,
    signature: Option<String>,
    execution_status: Option<u64>,
    cost_units: Option<u64>,
    first_event_timestamp_ns: Option<u64>,
    ingest_timestamp_ns: Option<u64>,
    ready_timestamp_ns: Option<u64>,
    first_skip_timestamp_ns: Option<u64>,
    scheduled_timestamp_ns: Option<u64>,
    execution_terminal_timestamp_ns: Option<u64>,
    terminal_timestamp_ns: Option<u64>,
    total_duration_ns: Option<u64>,
    execution_stages: ExecutionStageTimestamps,
    skip_count: usize,
    skip_events: Vec<SkipEvent>,
    blockers: Vec<BlockerEdge>,
}

struct ChainExecutionStatusSummary {
    success: usize,
    rollback_failure: usize,
    unknown: usize,
    success_cost_units: u64,
    rollback_failure_cost_units: u64,
    unknown_cost_units: u64,
}

struct PossibleSpeculativeChainTime {
    actual_chain_exec_wall_ns: Option<u64>,
    possible_chain_time_ns: Option<u64>,
    saved_vs_actual_ns: Option<i128>,
    speculative_edges: usize,
    speculative_restarts: usize,
    missing_worker_service_txs: usize,
    missing_execution_status_txs: usize,
}

struct SpeculativeTransactionTiming {
    worker_service_ns: u64,
    failed: bool,
}

#[derive(Clone, Copy, Default)]
struct ExecutionStageTimestamps {
    worker_id: Option<u64>,
    scheduled_timestamp_ns: Option<u64>,
    picked_up_timestamp_ns: Option<u64>,
    bank_acquired_timestamp_ns: Option<u64>,
    translated_timestamp_ns: Option<u64>,
    processed_timestamp_ns: Option<u64>,
    commit_results_ready_timestamp_ns: Option<u64>,
    worker_completed_timestamp_ns: Option<u64>,
    scheduler_finished_timestamp_ns: Option<u64>,
}

#[derive(Clone)]
struct BlockerEdge {
    blocker: u64,
    timestamp_ns: u64,
    reason: Option<u64>,
    inferred: bool,
}

#[derive(Clone, Copy)]
struct BestChain {
    start_timestamp_ns: u64,
    end_timestamp_ns: u64,
    span_ns: u64,
    len: usize,
    predecessor: Option<u64>,
}

struct ChainParallelismSummary {
    scheduled_to_done_ns: u64,
    estimated_optimal_ns: u64,
    handoff_gap_ns: u64,
    handoff_edges: usize,
    explicit_handoff_edges: usize,
    inferred_handoff_edges: usize,
    queued_before_blocker_done_edges: usize,
    scheduled_after_blocker_done_edges: usize,
    prequeued_same_worker_edges: usize,
    prequeued_cross_worker_edges: usize,
    pickup_sum_ns: u64,
    pickup_busy_ns: u64,
    pickup_unattributed_ns: u64,
    worker_service_sum_ns: u64,
    translation_sum_ns: u64,
    process_sum_ns: u64,
    response_sum_ns: u64,
    top_handoff_gaps: Vec<HandoffGap>,
    top_pickup_waits: Vec<PickupWait>,
}

#[derive(Clone, Copy)]
struct HandoffGap {
    blocker: u64,
    blocked: u64,
    blocker_worker_id: Option<u64>,
    worker_id: Option<u64>,
    gap_ns: u64,
    blocker_done_timestamp_ns: u64,
    blocked_scheduled_timestamp_ns: Option<u64>,
    blocked_picked_up_timestamp_ns: u64,
    blocked_was_prequeued: bool,
    edge_reason: Option<u64>,
    edge_inferred: bool,
    worker_busy_transaction: Option<WorkerBusyTransaction>,
}

#[derive(Clone, Copy)]
struct PickupWait {
    transaction: u64,
    worker_id: Option<u64>,
    wait_ns: u64,
    scheduled_timestamp_ns: u64,
    picked_up_timestamp_ns: u64,
    worker_busy_transaction: Option<WorkerBusyTransaction>,
    worker_busy_overlap_ns: u64,
}

#[derive(Clone, Copy)]
struct WorkerBusyTransaction {
    transaction: u64,
    worker_id: u64,
    picked_up_timestamp_ns: u64,
    worker_finished_timestamp_ns: u64,
    overlap_ns: u64,
}

#[derive(Clone, Copy)]
enum TimelineActionKind {
    Finish,
    Schedule,
    Skip {
        reason: Option<u64>,
        explicit_blocker: Option<u64>,
    },
}

#[derive(Clone, Copy)]
struct TimelineAction {
    timestamp_ns: u64,
    transaction_index: u64,
    kind: TimelineActionKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayWorkerStage {
    Check,
    Execution,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let events_path = args.ledger_path.join(REPLAY_EVENTS_IPC_FILE);
    let mut consumer = shared_memory::join_broadcast_consumer_at_path::<ReplayEvent>(&events_path)?;
    let mut store = EventStore::new(args.retained_slots);
    let mut reported_slots = BTreeSet::new();

    println!(
        "listening for replay events at {} tail_threshold={} retained_slots={}",
        events_path.display(),
        format_duration_ns(args.tail_threshold_ns),
        args.retained_slots
    );

    loop {
        let mut consumed = 0usize;
        loop {
            let event = match consumer.try_read(Ordering::Relaxed) {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(skipped_events) => {
                    eprintln!("replay event consumer skipped {skipped_events} events");
                    continue;
                }
            };

            let slot = event.slot();
            let tag = event.tag;
            store.apply_event(event);
            consumed = consumed.saturating_add(1);

            if tag == replay_event_tags::SLOT_COMPLETE && reported_slots.insert(slot) {
                let Some(slot_record) = store.slot(slot) else {
                    continue;
                };
                if slot_record
                    .tail_latency_ns()
                    .is_some_and(|tail| tail > args.tail_threshold_ns)
                {
                    print_tail_report(slot_record, args.tail_threshold_ns);
                }
            }
        }

        if consumed == 0 {
            thread::sleep(args.poll_interval);
        }
    }
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut ledger_path = None;
    let mut retained_slots = DEFAULT_RETAINED_SLOTS;
    let mut poll_ms = DEFAULT_POLL_MS;
    let mut tail_threshold_ms = DEFAULT_TAIL_THRESHOLD_MS;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                process::exit(0);
            }
            "--retained-slots" => {
                retained_slots = parse_next(&mut args, "--retained-slots")?;
            }
            "--poll-ms" => {
                poll_ms = parse_next(&mut args, "--poll-ms")?;
            }
            "--threshold-ms" => {
                tail_threshold_ms = parse_next(&mut args, "--threshold-ms")?;
            }
            value if ledger_path.is_none() => {
                ledger_path = Some(PathBuf::from(value));
            }
            value => return Err(format!("unexpected argument: {value}").into()),
        }
    }

    let Some(ledger_path) = ledger_path else {
        print_usage();
        return Err("missing ledger path".into());
    };
    if retained_slots == 0 {
        return Err("--retained-slots must be greater than zero".into());
    }

    Ok(Args {
        ledger_path,
        retained_slots,
        poll_interval: Duration::from_millis(poll_ms),
        tail_threshold_ns: tail_threshold_ms.saturating_mul(1_000_000),
    })
}

fn parse_next<T>(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    let value = args
        .next()
        .ok_or_else(|| format!("missing value for {flag}"))?;
    Ok(value.parse()?)
}

fn print_usage() {
    eprintln!(
        "usage: tail_watch <ledger-path> [--threshold-ms {DEFAULT_TAIL_THRESHOLD_MS}] \
         [--retained-slots {DEFAULT_RETAINED_SLOTS}] [--poll-ms {DEFAULT_POLL_MS}]"
    );
}

fn print_tail_report(slot: &SlotRecord, threshold_ns: u64) {
    let Some(tail_latency_ns) = slot.tail_latency_ns() else {
        return;
    };
    let Some(tail_start_timestamp_ns) =
        slot_event_timestamp(slot, replay_event_tags::SLOT_INGRESS_COMPLETE)
    else {
        return;
    };
    let Some(tail_end_timestamp_ns) = slot_event_timestamp(slot, replay_event_tags::SLOT_COMPLETE)
    else {
        return;
    };

    let analysis = analyze_transactions(slot);
    let (chain, memo) = longest_living_chain(&analysis, tail_start_timestamp_ns, tail_end_timestamp_ns);
    let chain_start_timestamp_ns = chain
        .first()
        .and_then(|index| analysis.get(index))
        .map(transaction_start_timestamp_ns)
        .unwrap_or(tail_start_timestamp_ns);
    let chain_end_timestamp_ns = chain
        .last()
        .and_then(|index| analysis.get(index))
        .map(transaction_end_timestamp_ns)
        .unwrap_or(tail_end_timestamp_ns);
    let chain_span_ns = chain_end_timestamp_ns.saturating_sub(chain_start_timestamp_ns);
    let tail_overlap_ns = overlap_ns(
        chain_start_timestamp_ns,
        chain_end_timestamp_ns,
        tail_start_timestamp_ns,
        tail_end_timestamp_ns,
    );

    println!();
    println!(
        "slot={} tail={} threshold={} block={} txs={} status={}",
        slot.slot,
        format_duration_ns(tail_latency_ns),
        format_duration_ns(threshold_ns),
        slot.duration_ns()
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        slot.transactions.len(),
        slot.status()
    );
    println!(
        "  ingress_complete={} slot_complete={}",
        tail_start_timestamp_ns, tail_end_timestamp_ns
    );
    println!(
        "  longest_chain len={} span={} tail_overlap={}",
        chain.len(),
        format_duration_ns(chain_span_ns),
        format_duration_ns(tail_overlap_ns)
    );
    let execution_status = chain_execution_status_summary(&chain, &analysis);
    let chain_transaction_count = execution_status
        .success
        .saturating_add(execution_status.rollback_failure)
        .saturating_add(execution_status.unknown);
    let chain_cost_units = execution_status
        .success_cost_units
        .saturating_add(execution_status.rollback_failure_cost_units)
        .saturating_add(execution_status.unknown_cost_units);
    println!(
        "  chain_execution_status success={} rollback_failure={} unknown={} rollback_tx_pct={:.1}% rollback_cu={} total_cu={} rollback_cu_pct={:.1}%",
        execution_status.success,
        execution_status.rollback_failure,
        execution_status.unknown,
        percent(execution_status.rollback_failure as u64, chain_transaction_count as u64),
        execution_status.rollback_failure_cost_units,
        chain_cost_units,
        percent(execution_status.rollback_failure_cost_units, chain_cost_units)
    );
    let speculative = possible_speculative_chain_time(&chain, &analysis);
    println!(
        "  possible_speculative_chain_time={} actual_chain_exec_wall={} saved_vs_actual={} saved_vs_actual_pct={} speculative_edges={} speculative_restarts={} missing_worker_service_txs={} missing_execution_status_txs={}",
        format_optional_duration(speculative.possible_chain_time_ns),
        format_optional_duration(speculative.actual_chain_exec_wall_ns),
        format_optional_duration_delta(speculative.saved_vs_actual_ns),
        format_optional_percent_delta(
            speculative.saved_vs_actual_ns,
            speculative.actual_chain_exec_wall_ns
        ),
        speculative.speculative_edges,
        speculative.speculative_restarts,
        speculative.missing_worker_service_txs,
        speculative.missing_execution_status_txs
    );
    if let Some(parallelism) = chain_parallelism_summary(&chain, &analysis) {
        let estimated_optimal_percent = if parallelism.scheduled_to_done_ns == 0 {
            100.0
        } else {
            (parallelism.estimated_optimal_ns as f64 / parallelism.scheduled_to_done_ns as f64)
                * 100.0
        };
        println!(
            "  chain_parallelism scheduled_to_done={} est_no_handoff_gap={} handoff_gap={} est_efficiency={:.1}% queued_before_blocker_done={}/{}",
            format_duration_ns(parallelism.scheduled_to_done_ns),
            format_duration_ns(parallelism.estimated_optimal_ns),
            format_duration_ns(parallelism.handoff_gap_ns),
            estimated_optimal_percent,
            parallelism.queued_before_blocker_done_edges,
            parallelism.handoff_edges
        );
        let service_parallelism = if parallelism.scheduled_to_done_ns == 0 {
            0.0
        } else {
            parallelism.worker_service_sum_ns as f64 / parallelism.scheduled_to_done_ns as f64
        };
        println!(
            "  chain_edge_quality explicit={} inferred={} scheduled_after_blocker_done={} prequeued_same_worker={} prequeued_cross_worker={}",
            parallelism.explicit_handoff_edges,
            parallelism.inferred_handoff_edges,
            parallelism.scheduled_after_blocker_done_edges,
            parallelism.prequeued_same_worker_edges,
            parallelism.prequeued_cross_worker_edges
        );
        println!(
            "  chain_worker_queue pickup_busy={} pickup_unattributed={} service_parallelism={:.2}x",
            format_duration_ns(parallelism.pickup_busy_ns),
            format_duration_ns(parallelism.pickup_unattributed_ns),
            service_parallelism
        );
        println!(
            "  chain_stage_sums pickup={} worker_service={} translate_alt={} process={} response={}",
            format_duration_ns(parallelism.pickup_sum_ns),
            format_duration_ns(parallelism.worker_service_sum_ns),
            format_duration_ns(parallelism.translation_sum_ns),
            format_duration_ns(parallelism.process_sum_ns),
            format_duration_ns(parallelism.response_sum_ns)
        );
        if let Some(handoff) = parallelism.top_handoff_gaps.first().copied() {
            println!(
                "  slowest_handoff from_tx={} to_tx={} blocker_worker={} blocked_worker={} gap={} scheduled_delta={} blocker_done={} blocked_scheduled={} blocked_picked_up={} edge={}{}",
                handoff.blocker,
                handoff.blocked,
                format_optional_u64(handoff.blocker_worker_id),
                format_optional_u64(handoff.worker_id),
                format_duration_ns(handoff.gap_ns),
                handoff
                    .blocked_scheduled_timestamp_ns
                    .map(|timestamp_ns| {
                        format_relative_timestamp(timestamp_ns, handoff.blocker_done_timestamp_ns)
                    })
                    .unwrap_or_else(|| "-".to_string()),
                handoff.blocker_done_timestamp_ns,
                handoff
                    .blocked_scheduled_timestamp_ns
                    .map(|timestamp_ns| timestamp_ns.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                handoff.blocked_picked_up_timestamp_ns,
                handoff
                    .edge_reason
                    .map(skip_reason_name)
                    .unwrap_or("unknown-skip-reason"),
                if handoff.edge_inferred { " inferred" } else { "" }
            );
            if let Some(worker_busy_transaction) = handoff.worker_busy_transaction {
                println!(
                    "  slowest_handoff_worker_busy worker={} tx={} overlap={} picked_up={} worker_finished={}",
                    worker_busy_transaction.worker_id,
                    worker_busy_transaction.transaction,
                    format_duration_ns(worker_busy_transaction.overlap_ns),
                    worker_busy_transaction.picked_up_timestamp_ns,
                    worker_busy_transaction.worker_finished_timestamp_ns
                );
            } else {
                println!("  slowest_handoff_worker_busy -");
            }
        }
        if parallelism.top_handoff_gaps.len() > 1 {
            println!("  slowest_handoffs");
            for (index, handoff) in parallelism.top_handoff_gaps.iter().enumerate() {
                println!(
                    "    {}. from_tx={} to_tx={} blocker_worker={} blocked_worker={} gap={} scheduled_delta={} prequeued={} edge={}{}",
                    index + 1,
                    handoff.blocker,
                    handoff.blocked,
                    format_optional_u64(handoff.blocker_worker_id),
                    format_optional_u64(handoff.worker_id),
                    format_duration_ns(handoff.gap_ns),
                    handoff
                        .blocked_scheduled_timestamp_ns
                        .map(|timestamp_ns| format_relative_timestamp(
                            timestamp_ns,
                            handoff.blocker_done_timestamp_ns
                        ))
                        .unwrap_or_else(|| "-".to_string()),
                    handoff.blocked_was_prequeued,
                    handoff
                        .edge_reason
                        .map(skip_reason_name)
                        .unwrap_or("unknown-skip-reason"),
                    if handoff.edge_inferred { " inferred" } else { "" }
                );
                if let Some(worker_busy_transaction) = handoff.worker_busy_transaction {
                    println!(
                        "       worker_busy worker={} tx={} overlap={} picked_up={} worker_finished={}",
                        worker_busy_transaction.worker_id,
                        worker_busy_transaction.transaction,
                        format_duration_ns(worker_busy_transaction.overlap_ns),
                        worker_busy_transaction.picked_up_timestamp_ns,
                        worker_busy_transaction.worker_finished_timestamp_ns
                    );
                }
            }
        }
        if !parallelism.top_pickup_waits.is_empty() {
            println!("  slowest_pickup_waits");
            for (index, pickup_wait) in parallelism.top_pickup_waits.iter().enumerate() {
                println!(
                    "    {}. tx={} worker={} wait={} scheduled={} picked_up={} busy_overlap={}",
                    index + 1,
                    pickup_wait.transaction,
                    format_optional_u64(pickup_wait.worker_id),
                    format_duration_ns(pickup_wait.wait_ns),
                    pickup_wait.scheduled_timestamp_ns,
                    pickup_wait.picked_up_timestamp_ns,
                    format_duration_ns(pickup_wait.worker_busy_overlap_ns)
                );
                if let Some(worker_busy_transaction) = pickup_wait.worker_busy_transaction {
                    println!(
                        "       worker_busy worker={} tx={} overlap={} picked_up={} worker_finished={}",
                        worker_busy_transaction.worker_id,
                        worker_busy_transaction.transaction,
                        format_duration_ns(worker_busy_transaction.overlap_ns),
                        worker_busy_transaction.picked_up_timestamp_ns,
                        worker_busy_transaction.worker_finished_timestamp_ns
                    );
                }
            }
        }
    }

    if chain.is_empty() {
        println!("  no transactions recorded for completed slot");
        return;
    }

    for (position, transaction_index) in chain.iter().copied().enumerate() {
        let transaction = analysis
            .get(&transaction_index)
            .expect("selected chain transaction must exist");
        let edge = (position != 0)
            .then(|| edge_between(&analysis, chain[position - 1], transaction_index))
            .flatten();
        let memo_entry = memo
            .get(&transaction_index)
            .expect("selected chain memo entry must exist");
        println!(
            "  {:>2}. tx={} status={} exec_status={} total={} chain_to_here={} tail_overlap={} skips={}{}",
            position + 1,
            transaction.index,
            transaction.status,
            format_optional_u64(transaction.execution_status),
            format_optional_duration(transaction.total_duration_ns),
            format_duration_ns(memo_entry.span_ns),
            format_duration_ns(transaction_tail_overlap_ns(
                transaction,
                tail_start_timestamp_ns,
                tail_end_timestamp_ns
            )),
            transaction.skip_count,
            edge.map_or_else(String::new, edge_detail)
        );
        println!(
            "      ingest={} ready={} first_skip={} scheduled={} exec_done={} terminal={} sig={}",
            format_optional_relative(transaction.ingest_timestamp_ns, tail_start_timestamp_ns),
            format_optional_relative(transaction.ready_timestamp_ns, tail_start_timestamp_ns),
            format_optional_relative(transaction.first_skip_timestamp_ns, tail_start_timestamp_ns),
            format_optional_relative(transaction.scheduled_timestamp_ns, tail_start_timestamp_ns),
            format_optional_relative(
                transaction.execution_terminal_timestamp_ns,
                tail_start_timestamp_ns
            ),
            format_optional_relative(transaction.terminal_timestamp_ns, tail_start_timestamp_ns),
            transaction.signature.as_deref().unwrap_or("<signature-pending>")
        );
        println!(
            "      {}",
            execution_stage_delay_detail(&transaction.execution_stages)
        );
    }
}

fn analyze_transactions(slot: &SlotRecord) -> BTreeMap<u64, TransactionAnalysis> {
    let mut transactions = slot
        .transactions
        .iter()
        .map(|(index, transaction)| (*index, transaction_analysis(*index, transaction)))
        .collect::<BTreeMap<_, _>>();
    infer_multiple_lock_blockers(&mut transactions);
    transactions
}

fn transaction_analysis(
    index: u64,
    transaction: &TransactionRecord,
) -> TransactionAnalysis {
    let mut blockers = Vec::new();
    let mut skip_events = Vec::new();
    let mut first_skip_timestamp_ns = None;

    for event in &transaction.events {
        if event.tag != replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED {
            continue;
        }

        first_skip_timestamp_ns.get_or_insert(event.timestamp_ns);
        let skip_event = SkipEvent {
            timestamp_ns: event.timestamp_ns,
            reason: event.scheduling_skip_reason(),
            explicit_blocker: event.scheduling_blocked_by_transaction_index(),
        };
        skip_events.push(skip_event);
        if let Some(blocker) = skip_event.explicit_blocker {
            blockers.push(BlockerEdge {
                blocker,
                timestamp_ns: event.timestamp_ns,
                reason: skip_event.reason,
                inferred: false,
            });
        }
    }

    TransactionAnalysis {
        index,
        status: transaction.status(),
        signature: transaction.signature.clone(),
        execution_status: transaction_execution_status(transaction),
        cost_units: transaction_cost_units(transaction),
        first_event_timestamp_ns: transaction.events.first().map(|event| event.timestamp_ns),
        ingest_timestamp_ns: transaction.ingest_timestamp_ns(),
        ready_timestamp_ns: first_event_timestamp(
            transaction,
            replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING,
        ),
        first_skip_timestamp_ns,
        scheduled_timestamp_ns: first_event_timestamp(
            transaction,
            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
        ),
        execution_terminal_timestamp_ns: execution_terminal_timestamp_ns(transaction),
        terminal_timestamp_ns: transaction.terminal_timestamp_ns(),
        total_duration_ns: transaction.total_duration_ns(),
        execution_stages: execution_stage_timestamps(transaction),
        skip_count: skip_events.len(),
        skip_events,
        blockers,
    }
}

fn chain_execution_status_summary(
    chain: &[u64],
    transactions: &BTreeMap<u64, TransactionAnalysis>,
) -> ChainExecutionStatusSummary {
    let mut summary = ChainExecutionStatusSummary {
        success: 0,
        rollback_failure: 0,
        unknown: 0,
        success_cost_units: 0,
        rollback_failure_cost_units: 0,
        unknown_cost_units: 0,
    };
    for transaction_index in chain {
        let Some(transaction) = transactions.get(transaction_index) else {
            summary.unknown = summary.unknown.saturating_add(1);
            continue;
        };
        let cost_units = transaction.cost_units.unwrap_or_default();
        match transaction.execution_status {
            Some(0) => {
                summary.success = summary.success.saturating_add(1);
                summary.success_cost_units = summary.success_cost_units.saturating_add(cost_units);
            }
            Some(1) => {
                summary.rollback_failure = summary.rollback_failure.saturating_add(1);
                summary.rollback_failure_cost_units = summary
                    .rollback_failure_cost_units
                    .saturating_add(cost_units);
            }
            _ => {
                summary.unknown = summary.unknown.saturating_add(1);
                summary.unknown_cost_units = summary.unknown_cost_units.saturating_add(cost_units);
            }
        }
    }
    summary
}

fn possible_speculative_chain_time(
    chain: &[u64],
    transactions: &BTreeMap<u64, TransactionAnalysis>,
) -> PossibleSpeculativeChainTime {
    let actual_chain_exec_wall_ns = actual_chain_exec_wall_ns(chain, transactions);
    let speculative_edges = chain.len().saturating_sub(1);
    let missing_worker_service_txs = chain
        .iter()
        .filter(|transaction_index| {
            transactions
                .get(transaction_index)
                .and_then(transaction_worker_service_ns)
                .is_none()
        })
        .count();
    let missing_execution_status_txs = chain
        .iter()
        .filter(|transaction_index| {
            transactions
                .get(transaction_index)
                .and_then(transaction_execution_failed)
                .is_none()
        })
        .count();
    if missing_worker_service_txs != 0 || missing_execution_status_txs != 0 || chain.is_empty() {
        return PossibleSpeculativeChainTime {
            actual_chain_exec_wall_ns,
            possible_chain_time_ns: None,
            saved_vs_actual_ns: None,
            speculative_edges,
            speculative_restarts: 0,
            missing_worker_service_txs,
            missing_execution_status_txs,
        };
    }

    let timings: Vec<_> = chain
        .iter()
        .map(|transaction_index| {
            let transaction = transactions
                .get(transaction_index)
                .expect("missing counts should cover missing transactions");
            SpeculativeTransactionTiming {
                worker_service_ns: transaction_worker_service_ns(transaction)
                    .expect("missing count should cover missing worker timing"),
                failed: transaction_execution_failed(transaction)
                    .expect("missing count should cover missing execution status"),
            }
        })
        .collect();
    let mut start_timestamps_ns: Vec<Option<u64>> = vec![None; timings.len()];
    start_timestamps_ns[0] = Some(0);
    if start_timestamps_ns.len() > 1 {
        start_timestamps_ns[1] = Some(0);
    }
    let mut base_timestamp_ns = 0;
    let mut speculative_restarts = 0;

    for index in 0..timings.len() {
        if index > 0 && !timings[index - 1].failed {
            if start_timestamps_ns[index].is_some_and(|start_timestamp_ns| {
                start_timestamp_ns != base_timestamp_ns
            }) {
                speculative_restarts += 1;
            }
            start_timestamps_ns[index] = Some(base_timestamp_ns);
        } else if start_timestamps_ns[index].is_none() {
            start_timestamps_ns[index] = Some(base_timestamp_ns);
        }

        let start_timestamp_ns = start_timestamps_ns[index]
            .expect("current transaction start should be initialized");
        let finish_timestamp_ns =
            start_timestamp_ns.saturating_add(timings[index].worker_service_ns);
        let valid_finish_timestamp_ns = finish_timestamp_ns.max(base_timestamp_ns);

        if index + 2 < start_timestamps_ns.len() && start_timestamps_ns[index + 2].is_none() {
            start_timestamps_ns[index + 2] = Some(valid_finish_timestamp_ns);
        };
        base_timestamp_ns = valid_finish_timestamp_ns;
    }

    let modeled_possible_chain_time_ns = base_timestamp_ns;
    let possible_chain_time_ns = actual_chain_exec_wall_ns
        .map(|actual_chain_exec_wall_ns| modeled_possible_chain_time_ns.min(actual_chain_exec_wall_ns))
        .unwrap_or(modeled_possible_chain_time_ns);
    PossibleSpeculativeChainTime {
        actual_chain_exec_wall_ns,
        possible_chain_time_ns: Some(possible_chain_time_ns),
        saved_vs_actual_ns: actual_chain_exec_wall_ns.map(|actual_chain_exec_wall_ns| {
            actual_chain_exec_wall_ns as i128 - possible_chain_time_ns as i128
        }),
        speculative_edges,
        speculative_restarts,
        missing_worker_service_txs,
        missing_execution_status_txs,
    }
}

fn actual_chain_exec_wall_ns(
    chain: &[u64],
    transactions: &BTreeMap<u64, TransactionAnalysis>,
) -> Option<u64> {
    let mut first_picked_up_timestamp_ns = None;
    let mut last_finished_timestamp_ns = None;
    for transaction_index in chain {
        let transaction = transactions.get(transaction_index)?;
        let picked_up_timestamp_ns = transaction.execution_stages.picked_up_timestamp_ns?;
        let finished_timestamp_ns = transaction
            .execution_stages
            .scheduler_finished_timestamp_ns
            .or(transaction.execution_terminal_timestamp_ns)?;
        first_picked_up_timestamp_ns = Some(
            first_picked_up_timestamp_ns
                .map_or(picked_up_timestamp_ns, |timestamp_ns: u64| {
                    timestamp_ns.min(picked_up_timestamp_ns)
                }),
        );
        last_finished_timestamp_ns = Some(
            last_finished_timestamp_ns.map_or(finished_timestamp_ns, |timestamp_ns: u64| {
                timestamp_ns.max(finished_timestamp_ns)
            }),
        );
    }
    Some(last_finished_timestamp_ns?.saturating_sub(first_picked_up_timestamp_ns?))
}

fn transaction_worker_service_ns(transaction: &TransactionAnalysis) -> Option<u64> {
    stage_delay_ns(
        transaction.execution_stages.picked_up_timestamp_ns,
        transaction.execution_stages.scheduler_finished_timestamp_ns,
    )
}

fn transaction_execution_failed(transaction: &TransactionAnalysis) -> Option<bool> {
    match transaction.execution_status {
        Some(0) => Some(false),
        Some(1) => Some(true),
        _ => None,
    }
}

fn infer_multiple_lock_blockers(transactions: &mut BTreeMap<u64, TransactionAnalysis>) {
    let mut actions = Vec::new();
    for transaction in transactions.values() {
        if let Some(timestamp_ns) = transaction.scheduled_timestamp_ns {
            actions.push(TimelineAction {
                timestamp_ns,
                transaction_index: transaction.index,
                kind: TimelineActionKind::Schedule,
            });
        }
        if let Some(timestamp_ns) = transaction.execution_terminal_timestamp_ns {
            actions.push(TimelineAction {
                timestamp_ns,
                transaction_index: transaction.index,
                kind: TimelineActionKind::Finish,
            });
        }
    }

    for transaction in transactions.values() {
        for event in &transaction.skip_events {
            if event.reason != Some(replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS)
                || event.explicit_blocker.is_some()
            {
                continue;
            }

            actions.push(TimelineAction {
                timestamp_ns: event.timestamp_ns,
                transaction_index: transaction.index,
                kind: TimelineActionKind::Skip {
                    reason: event.reason,
                    explicit_blocker: None,
                },
            });
        }
    }

    actions.sort_by_key(|action| {
        (
            action.timestamp_ns,
            match action.kind {
                TimelineActionKind::Finish => 0u8,
                TimelineActionKind::Schedule => 1,
                TimelineActionKind::Skip { .. } => 2,
            },
            action.transaction_index,
        )
    });

    let mut active = BTreeMap::new();
    let mut active_key_by_transaction = BTreeMap::new();
    let mut inferred_edges = Vec::new();
    for action in actions {
        match action.kind {
            TimelineActionKind::Finish => {
                if let Some(key) = active_key_by_transaction.remove(&action.transaction_index) {
                    active.remove(&key);
                }
            }
            TimelineActionKind::Schedule => {
                let key = (action.timestamp_ns, action.transaction_index);
                active.insert(key, action.transaction_index);
                active_key_by_transaction.insert(action.transaction_index, key);
            }
            TimelineActionKind::Skip {
                reason,
                explicit_blocker,
            } => {
                if explicit_blocker.is_some() {
                    continue;
                }
                let Some((_, blocker)) = active
                    .iter()
                    .rev()
                    .find(|(_, blocker)| **blocker != action.transaction_index)
                else {
                    continue;
                };
                inferred_edges.push((
                    action.transaction_index,
                    BlockerEdge {
                        blocker: *blocker,
                        timestamp_ns: action.timestamp_ns,
                        reason,
                        inferred: true,
                    },
                ));
            }
        }
    }

    for (transaction_index, edge) in inferred_edges {
        if let Some(transaction) = transactions.get_mut(&transaction_index) {
            transaction.blockers.push(edge);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SkipEvent {
    timestamp_ns: u64,
    reason: Option<u64>,
    explicit_blocker: Option<u64>,
}

fn longest_living_chain(
    transactions: &BTreeMap<u64, TransactionAnalysis>,
    tail_start_timestamp_ns: u64,
    tail_end_timestamp_ns: u64,
) -> (Vec<u64>, BTreeMap<u64, BestChain>) {
    let mut memo = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    let mut best_end = None;
    let mut best_score = None;

    for index in transactions.keys().copied() {
        let transaction = transactions.get(&index).unwrap();
        if !transaction_overlaps_tail(transaction, tail_start_timestamp_ns, tail_end_timestamp_ns) {
            continue;
        }
        let chain = best_chain_ending_at(index, transactions, &mut memo, &mut visiting);
        let tail_overlap = overlap_ns(
            chain.start_timestamp_ns,
            chain.end_timestamp_ns,
            tail_start_timestamp_ns,
            tail_end_timestamp_ns,
        );
        let score = (tail_overlap, chain.span_ns, chain.len, chain.end_timestamp_ns);
        if best_score.is_none_or(|best_score| score > best_score) {
            best_score = Some(score);
            best_end = Some(index);
        }
    }

    let Some(mut current) = best_end else {
        return (Vec::new(), memo);
    };
    let mut chain = Vec::new();
    loop {
        chain.push(current);
        let Some(predecessor) = memo.get(&current).and_then(|chain| chain.predecessor) else {
            break;
        };
        current = predecessor;
    }
    chain.reverse();

    (chain, memo)
}

fn best_chain_ending_at(
    index: u64,
    transactions: &BTreeMap<u64, TransactionAnalysis>,
    memo: &mut BTreeMap<u64, BestChain>,
    visiting: &mut BTreeSet<u64>,
) -> BestChain {
    if let Some(chain) = memo.get(&index).copied() {
        return chain;
    }
    if !visiting.insert(index) {
        return single_transaction_chain(transactions.get(&index).unwrap());
    }

    let transaction = transactions.get(&index).unwrap();
    let mut best = single_transaction_chain(transaction);
    let mut seen_blockers = BTreeSet::new();
    for edge in &transaction.blockers {
        if !seen_blockers.insert(edge.blocker) || !transactions.contains_key(&edge.blocker) {
            continue;
        }

        let predecessor = best_chain_ending_at(edge.blocker, transactions, memo, visiting);
        let start_timestamp_ns =
            predecessor.start_timestamp_ns.min(transaction_start_timestamp_ns(transaction));
        let end_timestamp_ns =
            predecessor.end_timestamp_ns.max(transaction_end_timestamp_ns(transaction));
        let candidate = BestChain {
            start_timestamp_ns,
            end_timestamp_ns,
            span_ns: end_timestamp_ns.saturating_sub(start_timestamp_ns),
            len: predecessor.len.saturating_add(1),
            predecessor: Some(edge.blocker),
        };
        if (candidate.span_ns, candidate.len) > (best.span_ns, best.len) {
            best = candidate;
        }
    }

    visiting.remove(&index);
    memo.insert(index, best);
    best
}

fn single_transaction_chain(transaction: &TransactionAnalysis) -> BestChain {
    let start_timestamp_ns = transaction_start_timestamp_ns(transaction);
    let end_timestamp_ns = transaction_end_timestamp_ns(transaction);
    BestChain {
        start_timestamp_ns,
        end_timestamp_ns,
        span_ns: end_timestamp_ns.saturating_sub(start_timestamp_ns),
        len: 1,
        predecessor: None,
    }
}

fn edge_between(
    transactions: &BTreeMap<u64, TransactionAnalysis>,
    blocker: u64,
    blocked: u64,
) -> Option<&BlockerEdge> {
    transactions
        .get(&blocked)?
        .blockers
        .iter()
        .filter(|edge| edge.blocker == blocker)
        .min_by_key(|edge| (edge.timestamp_ns, edge.inferred))
}

fn edge_detail(edge: &BlockerEdge) -> String {
    format!(
        " blocked_by={} blocked_at={} reason={}{}",
        edge.blocker,
        edge.timestamp_ns,
        edge.reason
            .map(skip_reason_name)
            .unwrap_or("unknown-skip-reason"),
        if edge.inferred { " inferred" } else { "" }
    )
}

fn chain_parallelism_summary(
    chain: &[u64],
    transactions: &BTreeMap<u64, TransactionAnalysis>,
) -> Option<ChainParallelismSummary> {
    let first_transaction = transactions.get(chain.first()?)?;
    let last_transaction = transactions.get(chain.last()?)?;
    let first_scheduled_timestamp_ns = first_transaction
        .execution_stages
        .scheduled_timestamp_ns
        .or(first_transaction.scheduled_timestamp_ns)
        .unwrap_or_else(|| transaction_start_timestamp_ns(first_transaction));
    let last_finished_timestamp_ns = last_transaction
        .execution_stages
        .scheduler_finished_timestamp_ns
        .or(last_transaction.execution_terminal_timestamp_ns)
        .unwrap_or_else(|| transaction_end_timestamp_ns(last_transaction));
    let scheduled_to_done_ns =
        last_finished_timestamp_ns.saturating_sub(first_scheduled_timestamp_ns);

    let mut summary = ChainParallelismSummary {
        scheduled_to_done_ns,
        estimated_optimal_ns: scheduled_to_done_ns,
        handoff_gap_ns: 0,
        handoff_edges: 0,
        explicit_handoff_edges: 0,
        inferred_handoff_edges: 0,
        queued_before_blocker_done_edges: 0,
        scheduled_after_blocker_done_edges: 0,
        prequeued_same_worker_edges: 0,
        prequeued_cross_worker_edges: 0,
        pickup_sum_ns: 0,
        pickup_busy_ns: 0,
        pickup_unattributed_ns: 0,
        worker_service_sum_ns: 0,
        translation_sum_ns: 0,
        process_sum_ns: 0,
        response_sum_ns: 0,
        top_handoff_gaps: Vec::new(),
        top_pickup_waits: Vec::new(),
    };

    for transaction_index in chain {
        let Some(transaction) = transactions.get(transaction_index) else {
            continue;
        };
        let stages = &transaction.execution_stages;
        let pickup_wait_ns =
            stage_delay_ns(stages.scheduled_timestamp_ns, stages.picked_up_timestamp_ns);
        summary.pickup_sum_ns = summary
            .pickup_sum_ns
            .saturating_add(pickup_wait_ns.unwrap_or_default());
        if let (
            Some(pickup_wait_ns),
            Some(worker_id),
            Some(scheduled_timestamp_ns),
            Some(picked_up_timestamp_ns),
        ) = (
            pickup_wait_ns,
            stages.worker_id,
            stages.scheduled_timestamp_ns,
            stages.picked_up_timestamp_ns,
        ) {
            let worker_busy_overlap_ns = worker_busy_overlap_ns(
                transactions,
                transaction.index,
                worker_id,
                scheduled_timestamp_ns,
                picked_up_timestamp_ns,
            )
            .min(pickup_wait_ns);
            summary.pickup_busy_ns = summary
                .pickup_busy_ns
                .saturating_add(worker_busy_overlap_ns);
            summary.pickup_unattributed_ns = summary
                .pickup_unattributed_ns
                .saturating_add(pickup_wait_ns.saturating_sub(worker_busy_overlap_ns));
            if pickup_wait_ns != 0 {
                push_top_pickup_wait(
                    &mut summary.top_pickup_waits,
                    PickupWait {
                        transaction: transaction.index,
                        worker_id: Some(worker_id),
                        wait_ns: pickup_wait_ns,
                        scheduled_timestamp_ns,
                        picked_up_timestamp_ns,
                        worker_busy_transaction: worker_busy_during_interval(
                            transactions,
                            transaction.index,
                            worker_id,
                            scheduled_timestamp_ns,
                            picked_up_timestamp_ns,
                        ),
                        worker_busy_overlap_ns,
                    },
                );
            }
        }
        summary.worker_service_sum_ns = summary.worker_service_sum_ns.saturating_add(
            stage_delay_ns(
                stages.picked_up_timestamp_ns,
                stages.scheduler_finished_timestamp_ns,
            )
            .unwrap_or_default(),
        );
        summary.translation_sum_ns =
            summary
                .translation_sum_ns
                .saturating_add(stage_delay_ns(
                    stages.bank_acquired_timestamp_ns,
                    stages.translated_timestamp_ns,
                )
                .unwrap_or_default());
        summary.process_sum_ns = summary
            .process_sum_ns
            .saturating_add(stage_delay_ns(
                stages.translated_timestamp_ns,
                stages.processed_timestamp_ns,
            )
            .unwrap_or_default());
        summary.response_sum_ns = summary
            .response_sum_ns
            .saturating_add(stage_delay_ns(
                stages.worker_completed_timestamp_ns,
                stages.scheduler_finished_timestamp_ns,
            )
            .unwrap_or_default());
    }

    for edge in chain.windows(2) {
        let Some(blocker) = transactions.get(&edge[0]) else {
            continue;
        };
        let Some(blocked) = transactions.get(&edge[1]) else {
            continue;
        };
        let Some(blocker_done_timestamp_ns) = blocker
            .execution_stages
            .scheduler_finished_timestamp_ns
            .or(blocker.execution_terminal_timestamp_ns)
        else {
            continue;
        };
        let Some(blocked_picked_up_timestamp_ns) =
            blocked.execution_stages.picked_up_timestamp_ns
        else {
            continue;
        };
        let edge_detail = edge_between(transactions, blocker.index, blocked.index);
        if edge_detail.is_some_and(|edge| edge.inferred) {
            summary.inferred_handoff_edges = summary.inferred_handoff_edges.saturating_add(1);
        } else if edge_detail.is_some() {
            summary.explicit_handoff_edges = summary.explicit_handoff_edges.saturating_add(1);
        }

        summary.handoff_edges = summary.handoff_edges.saturating_add(1);
        let blocked_was_prequeued = blocked
            .execution_stages
            .scheduled_timestamp_ns
            .is_some_and(|timestamp_ns| timestamp_ns <= blocker_done_timestamp_ns);
        if blocked_was_prequeued {
            summary.queued_before_blocker_done_edges = summary
                .queued_before_blocker_done_edges
                .saturating_add(1);
            if blocker.execution_stages.worker_id == blocked.execution_stages.worker_id {
                summary.prequeued_same_worker_edges =
                    summary.prequeued_same_worker_edges.saturating_add(1);
            } else {
                summary.prequeued_cross_worker_edges =
                    summary.prequeued_cross_worker_edges.saturating_add(1);
            }
        } else if blocked.execution_stages.scheduled_timestamp_ns.is_some() {
            summary.scheduled_after_blocker_done_edges =
                summary.scheduled_after_blocker_done_edges.saturating_add(1);
        }

        let gap_ns = blocked_picked_up_timestamp_ns.saturating_sub(blocker_done_timestamp_ns);
        if gap_ns == 0 {
            continue;
        }
        summary.handoff_gap_ns = summary.handoff_gap_ns.saturating_add(gap_ns);
        let worker_wait_start_timestamp_ns = blocked
            .execution_stages
            .scheduled_timestamp_ns
            .map(|scheduled_timestamp_ns| scheduled_timestamp_ns.max(blocker_done_timestamp_ns))
            .unwrap_or(blocker_done_timestamp_ns);
        let handoff = HandoffGap {
            blocker: blocker.index,
            blocked: blocked.index,
            blocker_worker_id: blocker.execution_stages.worker_id,
            worker_id: blocked.execution_stages.worker_id,
            gap_ns,
            blocker_done_timestamp_ns,
            blocked_scheduled_timestamp_ns: blocked.execution_stages.scheduled_timestamp_ns,
            blocked_picked_up_timestamp_ns,
            blocked_was_prequeued,
            edge_reason: edge_detail.and_then(|edge| edge.reason),
            edge_inferred: edge_detail.is_some_and(|edge| edge.inferred),
            worker_busy_transaction: blocked.execution_stages.worker_id.and_then(|worker_id| {
                worker_busy_during_interval(
                    transactions,
                    blocked.index,
                    worker_id,
                    worker_wait_start_timestamp_ns,
                    blocked_picked_up_timestamp_ns,
                )
            }),
        };
        push_top_handoff_gap(&mut summary.top_handoff_gaps, handoff);
    }
    summary.estimated_optimal_ns = summary
        .scheduled_to_done_ns
        .saturating_sub(summary.handoff_gap_ns.min(summary.scheduled_to_done_ns));

    Some(summary)
}

fn push_top_handoff_gap(top_handoff_gaps: &mut Vec<HandoffGap>, handoff: HandoffGap) {
    top_handoff_gaps.push(handoff);
    top_handoff_gaps.sort_by_key(|handoff| {
        (
            std::cmp::Reverse(handoff.gap_ns),
            handoff.blocker,
            handoff.blocked,
        )
    });
    top_handoff_gaps.truncate(MAX_PRINTED_HANDOFF_GAPS);
}

fn push_top_pickup_wait(top_pickup_waits: &mut Vec<PickupWait>, pickup_wait: PickupWait) {
    top_pickup_waits.push(pickup_wait);
    top_pickup_waits.sort_by_key(|pickup_wait| {
        (
            std::cmp::Reverse(pickup_wait.wait_ns),
            pickup_wait.transaction,
        )
    });
    top_pickup_waits.truncate(MAX_PRINTED_PICKUP_WAITS);
}

fn worker_busy_overlap_ns(
    transactions: &BTreeMap<u64, TransactionAnalysis>,
    excluded_transaction_index: u64,
    worker_id: u64,
    start_timestamp_ns: u64,
    end_timestamp_ns: u64,
) -> u64 {
    transactions
        .values()
        .filter(|transaction| transaction.index != excluded_transaction_index)
        .filter_map(|transaction| {
            let stages = &transaction.execution_stages;
            if stages.worker_id != Some(worker_id) {
                return None;
            }
            let picked_up_timestamp_ns = stages.picked_up_timestamp_ns?;
            let worker_finished_timestamp_ns = stages
                .worker_completed_timestamp_ns
                .or(stages.scheduler_finished_timestamp_ns)?;
            Some(overlap_ns(
                picked_up_timestamp_ns,
                worker_finished_timestamp_ns,
                start_timestamp_ns,
                end_timestamp_ns,
            ))
        })
        .fold(0u64, u64::saturating_add)
}

fn worker_busy_during_interval(
    transactions: &BTreeMap<u64, TransactionAnalysis>,
    excluded_transaction_index: u64,
    worker_id: u64,
    start_timestamp_ns: u64,
    end_timestamp_ns: u64,
) -> Option<WorkerBusyTransaction> {
    transactions
        .values()
        .filter(|transaction| transaction.index != excluded_transaction_index)
        .filter_map(|transaction| {
            let stages = &transaction.execution_stages;
            if stages.worker_id != Some(worker_id) {
                return None;
            }
            let picked_up_timestamp_ns = stages.picked_up_timestamp_ns?;
            let worker_finished_timestamp_ns = stages
                .worker_completed_timestamp_ns
                .or(stages.scheduler_finished_timestamp_ns)?;
            let overlap_ns = overlap_ns(
                picked_up_timestamp_ns,
                worker_finished_timestamp_ns,
                start_timestamp_ns,
                end_timestamp_ns,
            );
            (overlap_ns != 0).then_some(WorkerBusyTransaction {
                transaction: transaction.index,
                worker_id,
                picked_up_timestamp_ns,
                worker_finished_timestamp_ns,
                overlap_ns,
            })
        })
        .max_by_key(|worker_busy_transaction| {
            (
                worker_busy_transaction.overlap_ns,
                worker_busy_transaction.worker_finished_timestamp_ns,
                worker_busy_transaction.transaction,
            )
        })
}

fn transaction_overlaps_tail(
    transaction: &TransactionAnalysis,
    tail_start_timestamp_ns: u64,
    tail_end_timestamp_ns: u64,
) -> bool {
    overlap_ns(
        transaction_start_timestamp_ns(transaction),
        transaction_end_timestamp_ns(transaction),
        tail_start_timestamp_ns,
        tail_end_timestamp_ns,
    ) != 0
}

fn transaction_tail_overlap_ns(
    transaction: &TransactionAnalysis,
    tail_start_timestamp_ns: u64,
    tail_end_timestamp_ns: u64,
) -> u64 {
    overlap_ns(
        transaction_start_timestamp_ns(transaction),
        transaction_end_timestamp_ns(transaction),
        tail_start_timestamp_ns,
        tail_end_timestamp_ns,
    )
}

fn overlap_ns(start: u64, end: u64, range_start: u64, range_end: u64) -> u64 {
    end.min(range_end).saturating_sub(start.max(range_start))
}

fn transaction_start_timestamp_ns(transaction: &TransactionAnalysis) -> u64 {
    transaction
        .ingest_timestamp_ns
        .or(transaction.first_event_timestamp_ns)
        .unwrap_or_default()
}

fn transaction_end_timestamp_ns(transaction: &TransactionAnalysis) -> u64 {
    transaction
        .terminal_timestamp_ns
        .or(transaction.execution_terminal_timestamp_ns)
        .or(transaction.first_event_timestamp_ns)
        .unwrap_or_else(|| transaction_start_timestamp_ns(transaction))
}

fn first_event_timestamp(transaction: &TransactionRecord, tag: u64) -> Option<u64> {
    transaction
        .events
        .iter()
        .find(|event| event.tag == tag)
        .map(|event| event.timestamp_ns)
}

fn transaction_execution_status(transaction: &TransactionRecord) -> Option<u64> {
    transaction
        .events
        .iter()
        .find_map(ReplayEvent::execution_status)
}

fn transaction_cost_units(transaction: &TransactionRecord) -> Option<u64> {
    transaction.events.iter().find_map(ReplayEvent::cost_units)
}

fn execution_terminal_timestamp_ns(transaction: &TransactionRecord) -> Option<u64> {
    transaction
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.tag,
                replay_event_tags::TRANSACTION_FINISHED_EXEC
                    | replay_event_tags::TRANSACTION_EXEC_FAILED
            )
        })
        .map(|event| event.timestamp_ns)
        .max()
}

fn execution_stage_timestamps(transaction: &TransactionRecord) -> ExecutionStageTimestamps {
    let mut stages = ExecutionStageTimestamps::default();
    for (event_index, event) in transaction.events.iter().enumerate() {
        match event.tag {
            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC => {
                set_first_timestamp(&mut stages.scheduled_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_PICKED_UP
                if replay_worker_stage(&transaction.events, event_index)
                    == Some(ReplayWorkerStage::Execution) =>
            {
                set_first_timestamp(&mut stages.picked_up_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED => {
                set_first_timestamp(&mut stages.bank_acquired_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED => {
                set_first_timestamp(&mut stages.translated_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED => {
                set_first_timestamp(&mut stages.processed_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY => {
                set_first_timestamp(
                    &mut stages.commit_results_ready_timestamp_ns,
                    event.timestamp_ns,
                );
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED => {
                set_first_timestamp(&mut stages.worker_completed_timestamp_ns, event.timestamp_ns);
                record_execution_worker_id(&mut stages, event);
            }
            replay_event_tags::TRANSACTION_FINISHED_EXEC
            | replay_event_tags::TRANSACTION_EXEC_FAILED => {
                stages.scheduler_finished_timestamp_ns =
                    Some(stages.scheduler_finished_timestamp_ns.map_or(
                        event.timestamp_ns,
                        |timestamp_ns| timestamp_ns.max(event.timestamp_ns),
                    ));
                record_execution_worker_id(&mut stages, event);
            }
            _ => {}
        }
    }

    stages
}

fn set_first_timestamp(timestamp: &mut Option<u64>, value: u64) {
    timestamp.get_or_insert(value);
}

fn record_execution_worker_id(stages: &mut ExecutionStageTimestamps, event: &ReplayEvent) {
    if stages.worker_id.is_none() {
        stages.worker_id = event.worker_id();
    }
}

fn replay_worker_stage(
    transaction_events: &[ReplayEvent],
    event_index: usize,
) -> Option<ReplayWorkerStage> {
    let event = transaction_events.get(event_index)?;
    replay_worker_stage_for_tag(event.tag).or_else(|| {
        (event.tag == replay_event_tags::TRANSACTION_WORKER_PICKED_UP)
            .then(|| replay_worker_pickup_stage(transaction_events, event_index))?
    })
}

fn replay_worker_pickup_stage(
    transaction_events: &[ReplayEvent],
    event_index: usize,
) -> Option<ReplayWorkerStage> {
    transaction_events
        .iter()
        .skip(event_index.saturating_add(1))
        .find_map(|event| replay_worker_stage_for_tag(event.tag))
        .or_else(|| {
            transaction_events[..event_index]
                .iter()
                .rev()
                .find_map(|event| match event.tag {
                    replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC => {
                        Some(ReplayWorkerStage::Execution)
                    }
                    replay_event_tags::TRANSACTION_SENT_FOR_CHECK => Some(ReplayWorkerStage::Check),
                    _ => None,
                })
        })
}

fn replay_worker_stage_for_tag(tag: u64) -> Option<ReplayWorkerStage> {
    match tag {
        replay_event_tags::TRANSACTION_CHECK_FAILED
        | replay_event_tags::TRANSACTION_CHECK_PASSED
        | replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED
        | replay_event_tags::TRANSACTION_WORKER_CHECK_BANK_ACQUIRED
        | replay_event_tags::TRANSACTION_WORKER_CHECK_PARSED
        | replay_event_tags::TRANSACTION_WORKER_CHECK_SIGNATURES_COMPLETE
        | replay_event_tags::TRANSACTION_WORKER_CHECK_FEE_PAYER_BALANCE_COMPLETE
        | replay_event_tags::TRANSACTION_WORKER_CHECK_RESOLVED
        | replay_event_tags::TRANSACTION_WORKER_CHECK_ADDRESS_TABLES_COMPLETE
        | replay_event_tags::TRANSACTION_WORKER_CHECK_STATUS_COMPLETE => Some(ReplayWorkerStage::Check),
        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC
        | replay_event_tags::TRANSACTION_FINISHED_EXEC
        | replay_event_tags::TRANSACTION_EXEC_FAILED
        | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED
        | replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED
        | replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED
        | replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED
        | replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY => {
            Some(ReplayWorkerStage::Execution)
        }
        _ => None,
    }
}

fn slot_event_timestamp(slot: &SlotRecord, tag: u64) -> Option<u64> {
    slot.slot_events
        .iter()
        .find(|event| event.tag == tag)
        .map(|event| event.timestamp_ns)
}

fn format_optional_duration(duration_ns: Option<u64>) -> String {
    duration_ns
        .map(format_duration_ns)
        .unwrap_or_else(|| "-".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}

fn format_optional_duration_delta(duration_ns: Option<i128>) -> String {
    match duration_ns {
        Some(duration_ns) if duration_ns < 0 => {
            format!("-{}", format_duration_ns((-duration_ns) as u64))
        }
        Some(duration_ns) => format_duration_ns(duration_ns as u64),
        None => "-".to_string(),
    }
}

fn format_optional_percent_delta(numerator: Option<i128>, denominator: Option<u64>) -> String {
    match (numerator, denominator) {
        (_, Some(0)) | (None, _) | (_, None) => "-".to_string(),
        (Some(numerator), Some(denominator)) => {
            format!("{:.1}%", (numerator as f64 / denominator as f64) * 100.0)
        }
    }
}

fn execution_stage_delay_detail(stages: &ExecutionStageTimestamps) -> String {
    format!(
        "exec_stage worker={} pickup={} bank={} translate={} process={} commit={} complete={} response={} total={}",
        stages
            .worker_id
            .map(|worker_id| worker_id.to_string())
            .unwrap_or_else(|| "-".to_string()),
        format_stage_delay(
            stages.scheduled_timestamp_ns,
            stages.picked_up_timestamp_ns
        ),
        format_stage_delay(
            stages.picked_up_timestamp_ns,
            stages.bank_acquired_timestamp_ns
        ),
        format_stage_delay(
            stages.bank_acquired_timestamp_ns,
            stages.translated_timestamp_ns
        ),
        format_stage_delay(
            stages.translated_timestamp_ns,
            stages.processed_timestamp_ns
        ),
        format_stage_delay(
            stages.processed_timestamp_ns,
            stages.commit_results_ready_timestamp_ns
        ),
        format_stage_delay(
            stages.commit_results_ready_timestamp_ns,
            stages.worker_completed_timestamp_ns
        ),
        format_stage_delay(
            stages.worker_completed_timestamp_ns,
            stages.scheduler_finished_timestamp_ns
        ),
        format_stage_delay(
            stages.scheduled_timestamp_ns,
            stages.scheduler_finished_timestamp_ns
        ),
    )
}

fn format_stage_delay(start_timestamp_ns: Option<u64>, end_timestamp_ns: Option<u64>) -> String {
    stage_delay_ns(start_timestamp_ns, end_timestamp_ns)
        .map(format_duration_ns)
        .unwrap_or_else(|| "-".to_string())
}

fn stage_delay_ns(start_timestamp_ns: Option<u64>, end_timestamp_ns: Option<u64>) -> Option<u64> {
    match (start_timestamp_ns, end_timestamp_ns) {
        (Some(start_timestamp_ns), Some(end_timestamp_ns)) => {
            Some(end_timestamp_ns.saturating_sub(start_timestamp_ns))
        }
        _ => None,
    }
}

fn format_optional_relative(timestamp_ns: Option<u64>, base_timestamp_ns: u64) -> String {
    timestamp_ns
        .map(|timestamp_ns| format_relative_timestamp(timestamp_ns, base_timestamp_ns))
        .unwrap_or_else(|| "-".to_string())
}

fn format_relative_timestamp(timestamp_ns: u64, base_timestamp_ns: u64) -> String {
    if timestamp_ns >= base_timestamp_ns {
        format!("+{}", format_duration_ns(timestamp_ns - base_timestamp_ns))
    } else {
        format!("-{}", format_duration_ns(base_timestamp_ns - timestamp_ns))
    }
}

fn format_duration_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.3}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.3}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    }
}

fn skip_reason_name(reason: u64) -> &'static str {
    match reason {
        replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS => "multiple-lock-conflicts",
        replay_scheduling_skip_reasons::TOO_MUCH_WORK_ON_THREAD => "too-much-work-on-thread",
        replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT => {
            "previously-unscheduled-conflict"
        }
        _ => "unknown-skip-reason",
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_scheduling_utils::replay_events::replay_scheduling_skip_reasons,
        store::TransactionRecord,
    };

    #[test]
    fn infers_multiple_lock_blocker_from_active_execution() {
        let slot = slot_with_transactions([
            transaction(
                1,
                [
                    ReplayEvent::transaction_ingested(10, 42, 1, [1; 64]),
                    ReplayEvent::transaction_worker_dispatch_event(
                        20,
                        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                        42,
                        1,
                        0,
                        0,
                        0,
                    ),
                    ReplayEvent::transaction_event(
                        100,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        1,
                    ),
                ],
            ),
            transaction(
                2,
                [
                    ReplayEvent::transaction_ingested(11, 42, 2, [2; 64]),
                    ReplayEvent::transaction_scheduling_skipped(
                        30,
                        42,
                        2,
                        0,
                        replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS,
                        None,
                    ),
                    ReplayEvent::transaction_worker_dispatch_event(
                        110,
                        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                        42,
                        2,
                        0,
                        0,
                        0,
                    ),
                    ReplayEvent::transaction_event(
                        150,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        2,
                    ),
                ],
            ),
        ]);

        let analysis = analyze_transactions(&slot);
        let edge = analysis.get(&2).unwrap().blockers.first().unwrap();

        assert_eq!(edge.blocker, 1);
        assert!(edge.inferred);
    }

    #[test]
    fn longest_chain_follows_explicit_and_inferred_blockers() {
        let slot = slot_with_transactions([
            transaction(
                1,
                [
                    ReplayEvent::transaction_ingested(10, 42, 1, [1; 64]),
                    ReplayEvent::transaction_worker_dispatch_event(
                        20,
                        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                        42,
                        1,
                        0,
                        0,
                        0,
                    ),
                    ReplayEvent::transaction_event(
                        100,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        1,
                    ),
                ],
            ),
            transaction(
                2,
                [
                    ReplayEvent::transaction_ingested(11, 42, 2, [2; 64]),
                    ReplayEvent::transaction_scheduling_skipped(
                        30,
                        42,
                        2,
                        0,
                        replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS,
                        None,
                    ),
                    ReplayEvent::transaction_worker_dispatch_event(
                        110,
                        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                        42,
                        2,
                        0,
                        0,
                        0,
                    ),
                    ReplayEvent::transaction_event(
                        160,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        2,
                    ),
                ],
            ),
            transaction(
                3,
                [
                    ReplayEvent::transaction_ingested(12, 42, 3, [3; 64]),
                    ReplayEvent::transaction_scheduling_skipped(
                        40,
                        42,
                        3,
                        1,
                        replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT,
                        Some(2),
                    ),
                    ReplayEvent::transaction_worker_dispatch_event(
                        170,
                        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                        42,
                        3,
                        0,
                        0,
                        0,
                    ),
                    ReplayEvent::transaction_event(
                        220,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        3,
                    ),
                ],
            ),
        ]);

        let analysis = analyze_transactions(&slot);
        let (chain, _) = longest_living_chain(&analysis, 50, 230);

        assert_eq!(chain, [1, 2, 3]);
    }

    #[test]
    fn records_execution_stage_delays() {
        let transaction = transaction(
            1,
            [
                ReplayEvent::transaction_ingested(1, 42, 1, [1; 64]),
                ReplayEvent::transaction_worker_dispatch_event(
                    10,
                    replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                    42,
                    1,
                    3,
                    0,
                    0,
                ),
                ReplayEvent::transaction_worker_event(
                    15,
                    replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    18,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_BANK_ACQUIRED,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    22,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_TRANSLATED,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    40,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_PROCESSED,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    45,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMMIT_RESULTS_READY,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    47,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
                    42,
                    1,
                    3,
                ),
                ReplayEvent::transaction_execution_result(
                    55,
                    replay_event_tags::TRANSACTION_FINISHED_EXEC,
                    42,
                    1,
                    3,
                    123,
                    0,
                ),
            ],
        );
        let analysis = transaction_analysis(1, &transaction);
        let detail = execution_stage_delay_detail(&analysis.execution_stages);

        assert_eq!(analysis.execution_stages.worker_id, Some(3));
        assert!(detail.contains("pickup=5ns"));
        assert!(detail.contains("bank=3ns"));
        assert!(detail.contains("translate=4ns"));
        assert!(detail.contains("process=18ns"));
        assert!(detail.contains("commit=5ns"));
        assert!(detail.contains("complete=2ns"));
        assert!(detail.contains("response=8ns"));
        assert!(detail.contains("total=45ns"));
    }

    #[test]
    fn chain_execution_status_counts_rollback_failures() {
        let slot = slot_with_transactions([
            execution_transaction(1, 0, 10, 10, 20, 20, vec![]),
            transaction(
                2,
                [
                    ReplayEvent::transaction_ingested(11, 42, 2, [2; 64]),
                    ReplayEvent::transaction_execution_result(
                        30,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        2,
                        0,
                        100,
                        1,
                    ),
                ],
            ),
        ]);
        let analysis = analyze_transactions(&slot);
        let summary = chain_execution_status_summary(&[1, 2, 3], &analysis);

        assert_eq!(summary.success, 1);
        assert_eq!(summary.rollback_failure, 1);
        assert_eq!(summary.unknown, 1);
        assert_eq!(summary.success_cost_units, 0);
        assert_eq!(summary.rollback_failure_cost_units, 100);
        assert_eq!(summary.unknown_cost_units, 0);
    }

    #[test]
    fn possible_speculative_chain_time_overlaps_failed_transaction_with_successor() {
        let slot = slot_with_transactions([
            execution_transaction_with_status(
                1,
                0,
                execution_timing(10, 10, 20, 20),
                vec![],
                1,
            ),
            execution_transaction_with_status(
                2,
                1,
                execution_timing(20, 20, 50, 50),
                vec![],
                0,
            ),
            execution_transaction_with_status(
                3,
                2,
                execution_timing(50, 50, 55, 55),
                vec![],
                0,
            ),
        ]);
        let analysis = analyze_transactions(&slot);

        let summary = possible_speculative_chain_time(&[1, 2, 3], &analysis);

        assert_eq!(summary.actual_chain_exec_wall_ns, Some(45));
        assert_eq!(summary.possible_chain_time_ns, Some(35));
        assert_eq!(summary.saved_vs_actual_ns, Some(10));
        assert_eq!(summary.speculative_edges, 2);
        assert_eq!(summary.speculative_restarts, 1);
        assert_eq!(summary.missing_worker_service_txs, 0);
        assert_eq!(summary.missing_execution_status_txs, 0);
    }

    #[test]
    fn possible_speculative_chain_time_chains_consecutive_failures() {
        let slot = slot_with_transactions([
            execution_transaction_with_status(
                1,
                0,
                execution_timing(10, 10, 20, 20),
                vec![],
                1,
            ),
            execution_transaction_with_status(
                2,
                1,
                execution_timing(20, 20, 50, 50),
                vec![],
                1,
            ),
            execution_transaction_with_status(
                3,
                2,
                execution_timing(50, 50, 55, 55),
                vec![],
                0,
            ),
            execution_transaction_with_status(
                4,
                3,
                execution_timing(55, 55, 62, 62),
                vec![],
                0,
            ),
        ]);
        let analysis = analyze_transactions(&slot);

        let summary = possible_speculative_chain_time(&[1, 2, 3, 4], &analysis);

        assert_eq!(summary.actual_chain_exec_wall_ns, Some(52));
        assert_eq!(summary.possible_chain_time_ns, Some(37));
        assert_eq!(summary.saved_vs_actual_ns, Some(15));
        assert_eq!(summary.speculative_edges, 3);
        assert_eq!(summary.speculative_restarts, 0);
        assert_eq!(summary.missing_worker_service_txs, 0);
        assert_eq!(summary.missing_execution_status_txs, 0);
    }

    #[test]
    fn possible_speculative_chain_time_never_reports_slower_than_actual_chain_wall() {
        let slot = slot_with_transactions([
            execution_transaction_with_status(
                1,
                0,
                execution_timing(0, 0, 50, 50),
                vec![],
                0,
            ),
            execution_transaction_with_status(
                2,
                1,
                execution_timing(0, 0, 10, 10),
                vec![],
                0,
            ),
        ]);
        let analysis = analyze_transactions(&slot);

        let summary = possible_speculative_chain_time(&[1, 2], &analysis);

        assert_eq!(summary.actual_chain_exec_wall_ns, Some(50));
        assert_eq!(summary.possible_chain_time_ns, Some(50));
        assert_eq!(summary.saved_vs_actual_ns, Some(0));
        assert_eq!(summary.speculative_edges, 1);
        assert_eq!(summary.speculative_restarts, 1);
    }

    #[test]
    fn possible_speculative_chain_time_reports_missing_worker_service_timing() {
        let slot = slot_with_transactions([
            transaction(
                1,
                [
                    ReplayEvent::transaction_ingested(10, 42, 1, [1; 64]),
                    ReplayEvent::transaction_execution_result(
                        20,
                        replay_event_tags::TRANSACTION_FINISHED_EXEC,
                        42,
                        1,
                        0,
                        0,
                        1,
                    ),
                ],
            ),
            execution_transaction_with_status(
                2,
                0,
                execution_timing(20, 20, 50, 50),
                vec![],
                0,
            ),
        ]);
        let analysis = analyze_transactions(&slot);

        let summary = possible_speculative_chain_time(&[1, 2], &analysis);

        assert_eq!(summary.actual_chain_exec_wall_ns, None);
        assert_eq!(summary.possible_chain_time_ns, None);
        assert_eq!(summary.saved_vs_actual_ns, None);
        assert_eq!(summary.speculative_edges, 1);
        assert_eq!(summary.speculative_restarts, 0);
        assert_eq!(summary.missing_worker_service_txs, 1);
        assert_eq!(summary.missing_execution_status_txs, 0);
    }

    #[test]
    fn chain_parallelism_separates_prequeued_edges_and_worker_queue_wait() {
        let slot = slot_with_transactions([
            execution_transaction(0, 1, 25, 25, 38, 38, vec![]),
            execution_transaction(1, 0, 10, 10, 20, 20, vec![]),
            execution_transaction(
                2,
                0,
                18,
                22,
                30,
                30,
                vec![ReplayEvent::transaction_scheduling_skipped(
                    15,
                    42,
                    2,
                    0,
                    replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT,
                    Some(1),
                )],
            ),
            execution_transaction(
                3,
                1,
                35,
                40,
                50,
                50,
                vec![ReplayEvent::transaction_scheduling_skipped(
                    25,
                    42,
                    3,
                    0,
                    replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT,
                    Some(2),
                )],
            ),
        ]);

        let analysis = analyze_transactions(&slot);
        let summary = chain_parallelism_summary(&[1, 2, 3], &analysis).unwrap();

        assert_eq!(summary.handoff_edges, 2);
        assert_eq!(summary.explicit_handoff_edges, 2);
        assert_eq!(summary.inferred_handoff_edges, 0);
        assert_eq!(summary.queued_before_blocker_done_edges, 1);
        assert_eq!(summary.scheduled_after_blocker_done_edges, 1);
        assert_eq!(summary.prequeued_same_worker_edges, 1);
        assert_eq!(summary.prequeued_cross_worker_edges, 0);
        assert_eq!(summary.pickup_sum_ns, 9);
        assert_eq!(summary.pickup_busy_ns, 5);
        assert_eq!(summary.pickup_unattributed_ns, 4);

        let slowest_handoff = summary.top_handoff_gaps.first().unwrap();
        assert_eq!(slowest_handoff.blocker, 2);
        assert_eq!(slowest_handoff.blocked, 3);
        assert_eq!(slowest_handoff.gap_ns, 10);
        assert_eq!(
            slowest_handoff.worker_busy_transaction.unwrap().transaction,
            0
        );

        let slowest_pickup = summary.top_pickup_waits.first().unwrap();
        assert_eq!(slowest_pickup.transaction, 3);
        assert_eq!(slowest_pickup.wait_ns, 5);
        assert_eq!(slowest_pickup.worker_busy_overlap_ns, 3);
    }

    fn slot_with_transactions<const N: usize>(
        transactions: [TransactionRecord; N],
    ) -> SlotRecord {
        SlotRecord {
            slot: 42,
            slot_events: vec![
                ReplayEvent::slot_begin(1, 42),
                ReplayEvent::slot_ingress_complete(50, 42),
                ReplayEvent::slot_complete(230, 42),
            ],
            transactions: transactions
                .into_iter()
                .map(|transaction| (transaction.index, transaction))
                .collect(),
        }
    }

    fn transaction<const N: usize>(index: u64, events: [ReplayEvent; N]) -> TransactionRecord {
        TransactionRecord {
            index,
            signature: None,
            events: events.into_iter().collect(),
        }
    }

    #[derive(Clone, Copy)]
    struct ExecutionTransactionTiming {
        scheduled_timestamp_ns: u64,
        picked_up_timestamp_ns: u64,
        worker_completed_timestamp_ns: u64,
        scheduler_finished_timestamp_ns: u64,
    }

    fn execution_timing(
        scheduled_timestamp_ns: u64,
        picked_up_timestamp_ns: u64,
        worker_completed_timestamp_ns: u64,
        scheduler_finished_timestamp_ns: u64,
    ) -> ExecutionTransactionTiming {
        ExecutionTransactionTiming {
            scheduled_timestamp_ns,
            picked_up_timestamp_ns,
            worker_completed_timestamp_ns,
            scheduler_finished_timestamp_ns,
        }
    }

    fn execution_transaction(
        index: u64,
        worker_id: u64,
        scheduled_timestamp_ns: u64,
        picked_up_timestamp_ns: u64,
        worker_completed_timestamp_ns: u64,
        scheduler_finished_timestamp_ns: u64,
        extra_events: Vec<ReplayEvent>,
    ) -> TransactionRecord {
        execution_transaction_with_status(
            index,
            worker_id,
            execution_timing(
                scheduled_timestamp_ns,
                picked_up_timestamp_ns,
                worker_completed_timestamp_ns,
                scheduler_finished_timestamp_ns,
            ),
            extra_events,
            0,
        )
    }

    fn execution_transaction_with_status(
        index: u64,
        worker_id: u64,
        timing: ExecutionTransactionTiming,
        mut extra_events: Vec<ReplayEvent>,
        execution_status: u64,
    ) -> TransactionRecord {
        let mut events = vec![ReplayEvent::transaction_ingested(
            index,
            42,
            index,
            [index as u8; 64],
        )];
        events.append(&mut extra_events);
        events.extend([
            ReplayEvent::transaction_worker_dispatch_event(
                timing.scheduled_timestamp_ns,
                replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                42,
                index,
                worker_id,
                0,
                0,
            ),
            ReplayEvent::transaction_worker_event(
                timing.picked_up_timestamp_ns,
                replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                42,
                index,
                worker_id,
            ),
            ReplayEvent::transaction_worker_event(
                timing.worker_completed_timestamp_ns,
                replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
                42,
                index,
                worker_id,
            ),
            ReplayEvent::transaction_execution_result(
                timing.scheduler_finished_timestamp_ns,
                replay_event_tags::TRANSACTION_FINISHED_EXEC,
                42,
                index,
                worker_id,
                0,
                execution_status,
            ),
        ]);

        TransactionRecord {
            index,
            signature: None,
            events,
        }
    }
}
