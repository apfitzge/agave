mod store;

use {
    agave_scheduling_utils::{
        replay_events::{
            REPLAY_EVENTS_IPC_FILE, ReplayEvent, replay_event_tags, replay_scheduling_skip_reasons,
        },
        shared_memory,
    },
    crossterm::{
        event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded},
    ratatui::{
        Frame, Terminal,
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout, Rect},
        style::{Color, Modifier, Style},
        text::{Line, Span},
        widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    },
    std::{
        env,
        error::Error,
        io,
        path::PathBuf,
        process,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
    store::{EventStore, TransactionRecord, event_name},
};

const DEFAULT_RETAINED_SLOTS: usize = 64;
const DEFAULT_POLL_MS: u64 = 0;
const DEFAULT_UI_TICK_MS: u64 = 100;
const EVENT_READER_BATCH_SIZE: usize = 4096;
const EVENT_BATCH_POOL_SIZE: usize = 16;
const EVENT_PROCESSOR_WAIT_MS: u64 = 10;
const PAGE_STEP: usize = 10;

type EventBatch = Vec<ReplayEvent>;

struct Args {
    ledger_path: PathBuf,
    retained_slots: usize,
    poll_interval: Duration,
}

#[derive(Default)]
struct ReaderStats {
    received_events: AtomicU64,
    processed_events: AtomicU64,
    skipped_events: AtomicU64,
}

#[derive(Default)]
struct App {
    selected_slot: Option<u64>,
    selected_transaction: Option<u64>,
    slot_index: usize,
    transaction_index: usize,
    tx_timeline_scroll: u16,
    worker_timeline_scroll: u16,
    worker_filter: Option<u64>,
    worker_timeline_kind: WorkerTimelineKind,
    focus: FocusPane,
    maximized_pane: Option<FocusPane>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FocusPane {
    #[default]
    Slots,
    Transactions,
    TxTimeline,
    WorkerTimeline,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum WorkerTimelineKind {
    #[default]
    Execution,
    Check,
    SignatureVerification,
    Scheduler,
    SchedulingSummary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayWorkerStage {
    Check,
    Execution,
}

struct UiSnapshot {
    received_events: u64,
    processed_events: u64,
    skipped_events: u64,
    slots: Vec<SlotSummary>,
    selected_slot: Option<SelectedSlot>,
}

struct SlotSummary {
    slot: u64,
    transaction_count: usize,
    estimated_cost_units: Option<u64>,
    cost_units: Option<u64>,
    duration_ns: Option<u64>,
    active_duration_ns: Option<u64>,
    active_session_count: usize,
    active_pending_transactions: usize,
    status: &'static str,
}

struct SelectedSlot {
    slot: u64,
    status: &'static str,
    slot_event_count: usize,
    duration_ns: Option<u64>,
    active_duration_ns: Option<u64>,
    active_pending_transactions: usize,
    active_sessions: Vec<ActiveSessionSummary>,
    slot_events: Vec<TimelineEvent>,
    worker_events: Vec<WorkerTimelineEvent>,
    check_worker_events: Vec<WorkerTimelineEvent>,
    signature_verification_worker_events: Vec<WorkerTimelineEvent>,
    scheduler_events: Vec<WorkerTimelineEvent>,
    scheduling_summary_events: Vec<WorkerTimelineEvent>,
    transactions: Vec<TransactionSummary>,
    selected_transaction: Option<TransactionTimeline>,
}

struct TransactionSummary {
    index: u64,
    status: &'static str,
    ingest_timestamp_ns: Option<u64>,
    slot_ingest_delta_ns: Option<u64>,
    estimated_cost_units: Option<u64>,
    cost_units: Option<u64>,
    check_wait_ns: Option<u64>,
    ready_wait_ns: Option<u64>,
    scheduling_wait_ns: Option<u64>,
    exec_wait_ns: Option<u64>,
    execution_duration_ns: Option<u64>,
    duration_ns: Option<u64>,
    signature: String,
}

struct TransactionTimeline {
    slot: u64,
    index: u64,
    status: &'static str,
    slot_ingest_delta_ns: Option<u64>,
    duration_ns: Option<u64>,
    signature: String,
    events: Vec<TimelineEvent>,
}

struct TimelineEvent {
    delta_ns: u64,
    timestamp_ns: u64,
    name: &'static str,
    detail: String,
}

struct ActiveSessionSummary {
    start_delta_ns: u64,
    start_timestamp_ns: u64,
    end_timestamp_ns: Option<u64>,
    duration_ns: Option<u64>,
    transaction_count: usize,
    pending_transactions: usize,
}

struct ActiveSession {
    start_timestamp_ns: u64,
    end_timestamp_ns: Option<u64>,
    transaction_count: usize,
    pending_transactions: usize,
}

struct WorkerTimelineEvent {
    delta_ns: u64,
    timestamp_ns: u64,
    worker_id: Option<u64>,
    transaction_index: Option<u64>,
    name: &'static str,
    detail: String,
}

struct TerminalSession;

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
    let store = Arc::new(Mutex::new(EventStore::new(args.retained_slots)));
    let stats = Arc::new(ReaderStats::default());
    let (event_batch_sender, event_batch_receiver) = unbounded();
    let (free_batch_sender, free_batch_receiver) = bounded(EVENT_BATCH_POOL_SIZE);
    for _ in 0..EVENT_BATCH_POOL_SIZE {
        free_batch_sender
            .send(Vec::with_capacity(EVENT_READER_BATCH_SIZE))
            .map_err(|_| "failed to initialize replay event batch pool")?;
    }
    let exit = Arc::new(AtomicBool::new(false));
    let reader = spawn_reader(
        move || consumer.try_read(Ordering::Relaxed),
        event_batch_sender,
        free_batch_receiver,
        Arc::clone(&stats),
        Arc::clone(&exit),
        args.poll_interval,
    );
    let processor = spawn_processor(
        event_batch_receiver,
        free_batch_sender,
        Arc::clone(&store),
        Arc::clone(&stats),
        Arc::clone(&exit),
    );

    let tui_result = run_tui(&store, &stats);

    exit.store(true, Ordering::Relaxed);
    reader
        .join()
        .map_err(|_| "replay event reader thread panicked")?;
    processor
        .join()
        .map_err(|_| "replay event processor thread panicked")?;
    tui_result?;
    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut ledger_path = None;
    let mut retained_slots = DEFAULT_RETAINED_SLOTS;
    let mut poll_ms = DEFAULT_POLL_MS;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                process::exit(0);
            }
            "-l" | "--ledger" => {
                let Some(path) = args.next() else {
                    return Err("--ledger requires a path".into());
                };
                ledger_path = Some(PathBuf::from(path));
            }
            "-n" | "--slots" => {
                let Some(slots) = args.next() else {
                    return Err("--slots requires a count".into());
                };
                retained_slots = slots.parse()?;
                if retained_slots == 0 {
                    return Err("--slots must be greater than zero".into());
                }
            }
            "--poll-ms" => {
                let Some(value) = args.next() else {
                    return Err("--poll-ms requires a value".into());
                };
                poll_ms = value.parse()?;
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
    }

    let Some(ledger_path) = ledger_path else {
        print_usage();
        return Err("--ledger is required".into());
    };

    Ok(Args {
        ledger_path,
        retained_slots,
        poll_interval: Duration::from_millis(poll_ms),
    })
}

fn print_usage() {
    eprintln!("usage: agave-replay-event-viewer --ledger <LEDGER_PATH> [--slots N] [--poll-ms MS]");
}

fn spawn_reader<F>(
    mut read_next: F,
    event_batch_sender: Sender<EventBatch>,
    free_batch_receiver: Receiver<EventBatch>,
    stats: Arc<ReaderStats>,
    exit: Arc<AtomicBool>,
    poll_interval: Duration,
) -> JoinHandle<()>
where
    F: FnMut() -> Result<Option<ReplayEvent>, usize> + Send + 'static,
{
    thread::spawn(move || {
        let mut events = empty_event_batch(&free_batch_receiver);
        while !exit.load(Ordering::Relaxed) {
            let mut read_any = false;
            loop {
                if exit.load(Ordering::Relaxed) {
                    flush_read_events(
                        &event_batch_sender,
                        &free_batch_receiver,
                        &stats,
                        &mut events,
                    );
                    return;
                }

                match read_next() {
                    Ok(Some(event)) => {
                        read_any = true;
                        events.push(event);
                        if events.len() >= EVENT_READER_BATCH_SIZE {
                            flush_read_events(
                                &event_batch_sender,
                                &free_batch_receiver,
                                &stats,
                                &mut events,
                            );
                        }
                    }
                    Ok(None) => break,
                    Err(skipped) => {
                        read_any = true;
                        let skipped = u64::try_from(skipped).unwrap_or(u64::MAX);
                        stats.skipped_events.fetch_add(skipped, Ordering::Relaxed);
                    }
                }
            }
            flush_read_events(
                &event_batch_sender,
                &free_batch_receiver,
                &stats,
                &mut events,
            );

            if !read_any {
                if poll_interval.is_zero() {
                    thread::yield_now();
                } else {
                    thread::sleep(poll_interval);
                }
            }
        }
    })
}

fn flush_read_events(
    event_batch_sender: &Sender<EventBatch>,
    free_batch_receiver: &Receiver<EventBatch>,
    stats: &ReaderStats,
    events: &mut Vec<ReplayEvent>,
) {
    if events.is_empty() {
        return;
    }

    let event_count = u64::try_from(events.len()).unwrap_or(u64::MAX);
    let mut next_events = empty_event_batch(free_batch_receiver);
    std::mem::swap(events, &mut next_events);
    if event_batch_sender.send(next_events).is_ok() {
        stats
            .received_events
            .fetch_add(event_count, Ordering::Relaxed);
    }
}

fn empty_event_batch(free_batch_receiver: &Receiver<EventBatch>) -> EventBatch {
    free_batch_receiver
        .try_recv()
        .unwrap_or_else(|_| Vec::with_capacity(EVENT_READER_BATCH_SIZE))
}

fn spawn_processor(
    event_batch_receiver: Receiver<EventBatch>,
    free_batch_sender: Sender<EventBatch>,
    store: Arc<Mutex<EventStore>>,
    stats: Arc<ReaderStats>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let event_batch =
                match event_batch_receiver.recv_timeout(Duration::from_millis(EVENT_PROCESSOR_WAIT_MS)) {
                    Ok(event_batch) => event_batch,
                    Err(RecvTimeoutError::Timeout) => {
                        if exit.load(Ordering::Relaxed) && event_batch_receiver.is_empty() {
                            return;
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return,
                };

            process_event_batch(event_batch, &free_batch_sender, &store, &stats);
            while let Ok(event_batch) = event_batch_receiver.try_recv() {
                process_event_batch(event_batch, &free_batch_sender, &store, &stats);
                if exit.load(Ordering::Relaxed) && event_batch_receiver.is_empty() {
                    return;
                }
            }
        }
    })
}

fn process_event_batch(
    mut events: EventBatch,
    free_batch_sender: &Sender<EventBatch>,
    store: &Mutex<EventStore>,
    stats: &ReaderStats,
) {
    let event_count = u64::try_from(events.len()).unwrap_or(u64::MAX);
    {
        let mut store = store.lock().unwrap();
        for event in events.drain(..) {
            store.apply_event(event);
        }
    }
    stats
        .processed_events
        .fetch_add(event_count, Ordering::Relaxed);
    events.clear();
    let _ = free_batch_sender.try_send(events);
}

fn run_tui(store: &Arc<Mutex<EventStore>>, stats: &Arc<ReaderStats>) -> io::Result<()> {
    let _terminal_session = TerminalSession::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::default();

    terminal.clear()?;
    loop {
        let mut ui_snapshot = snapshot(store, stats, app.selected_slot, app.selected_transaction);
        let mut selection_changed = app.sync_slots(&ui_snapshot.slots);
        if selection_changed {
            ui_snapshot = snapshot(store, stats, app.selected_slot, app.selected_transaction);
        }
        selection_changed = app.sync_transactions(&ui_snapshot) || selection_changed;
        if selection_changed {
            ui_snapshot = snapshot(store, stats, app.selected_slot, app.selected_transaction);
        }
        app.bound_timeline_scrolls(&ui_snapshot);
        store.lock().unwrap().pin_slot(app.selected_slot);

        terminal.draw(|frame| draw_ui(frame, &app, &ui_snapshot))?;

        if event::poll(Duration::from_millis(DEFAULT_UI_TICK_MS))? {
            let mut keys = Vec::new();
            loop {
                if let Event::Key(key) = event::read()? {
                    keys.push(key);
                }
                if !event::poll(Duration::ZERO)? {
                    break;
                }
            }
            if handle_key_events(&mut app, keys, &ui_snapshot) {
                break;
            }
            store.lock().unwrap().pin_slot(app.selected_slot);
        }
    }
    terminal.show_cursor()
}

fn snapshot(
    store: &Arc<Mutex<EventStore>>,
    stats: &ReaderStats,
    selected_slot: Option<u64>,
    selected_transaction: Option<u64>,
) -> UiSnapshot {
    let store = store.lock().unwrap();
    let selected_slot = selected_slot
        .and_then(|slot| store.slot(slot))
        .map(|slot_record| {
            let transactions = slot_record.transactions_by_ingest();
            let slot_begin_timestamp_ns = slot_record.begin_timestamp_ns();
            let active_sessions = slot_active_sessions(slot_record);
            let latest_timestamp_ns = slot_latest_timestamp_ns(slot_record);
            let active_duration_ns =
                active_sessions_total_duration_ns(&active_sessions, latest_timestamp_ns);
            let active_pending_transactions =
                active_sessions_pending_transactions(&active_sessions);
            let active_sessions = active_session_summaries(
                &active_sessions,
                slot_begin_timestamp_ns,
                latest_timestamp_ns,
            );
            let selected_transaction = selected_transaction
                .and_then(|selected_index| {
                    transactions
                        .iter()
                        .copied()
                        .find(|transaction| transaction.index == selected_index)
                })
                .map(|transaction| {
                    transaction_timeline(slot_record.slot, slot_begin_timestamp_ns, transaction)
                });
            let transactions = transactions
                .into_iter()
                .map(|transaction| transaction_summary(slot_begin_timestamp_ns, transaction))
                .collect::<Vec<_>>();

            SelectedSlot {
                slot: slot_record.slot,
                status: slot_record.status(),
                slot_event_count: slot_record.slot_events.len(),
                duration_ns: slot_record.duration_ns(),
                active_duration_ns,
                active_pending_transactions,
                active_sessions,
                slot_events: slot_timeline(slot_record),
                worker_events: worker_timeline(slot_record, WorkerTimelineKind::Execution),
                check_worker_events: worker_timeline(slot_record, WorkerTimelineKind::Check),
                signature_verification_worker_events: worker_timeline(
                    slot_record,
                    WorkerTimelineKind::SignatureVerification,
                ),
                scheduler_events: worker_timeline(slot_record, WorkerTimelineKind::Scheduler),
                scheduling_summary_events: worker_timeline(
                    slot_record,
                    WorkerTimelineKind::SchedulingSummary,
                ),
                transactions,
                selected_transaction,
            }
        });
    let slots = store
        .slot_ids()
        .into_iter()
        .filter_map(|slot| {
            let slot_record = store.slot(slot)?;
            let active_stats = store.active_slot_stats(slot);
            let selected_slot_summary = selected_slot
                .as_ref()
                .filter(|selected_slot| selected_slot.slot == slot);
            Some(SlotSummary {
                slot,
                transaction_count: slot_record.transactions.len(),
                estimated_cost_units: slot_estimated_cost_units(slot_record),
                cost_units: slot_cost_units(slot_record),
                duration_ns: slot_record.duration_ns(),
                active_duration_ns: selected_slot_summary
                    .and_then(|selected_slot| selected_slot.active_duration_ns)
                    .or(active_stats.active_duration_ns),
                active_session_count: selected_slot_summary.map_or(
                    active_stats.session_count,
                    |selected_slot| selected_slot.active_sessions.len(),
                ),
                active_pending_transactions: selected_slot_summary.map_or(
                    active_stats.pending_transactions,
                    |selected_slot| selected_slot.active_pending_transactions,
                ),
                status: slot_record.status(),
            })
        })
        .collect::<Vec<_>>();

    UiSnapshot {
        received_events: stats.received_events.load(Ordering::Relaxed),
        processed_events: stats.processed_events.load(Ordering::Relaxed),
        skipped_events: stats.skipped_events.load(Ordering::Relaxed),
        slots,
        selected_slot,
    }
}

fn slot_active_sessions(slot: &store::SlotRecord) -> Vec<ActiveSession> {
    let mut transitions = Vec::new();
    for transaction in slot.transactions.values() {
        if let Some(ingest_timestamp_ns) = transaction.ingest_timestamp_ns() {
            transitions.push((ingest_timestamp_ns, 1usize, 0usize));
        }
        if let Some(terminal_timestamp_ns) = transaction.terminal_timestamp_ns() {
            transitions.push((terminal_timestamp_ns, 0usize, 1usize));
        }
    }
    transitions.sort_by_key(|(timestamp_ns, starts, _)| (*timestamp_ns, usize::MAX - *starts));

    let mut sessions = Vec::new();
    let mut outstanding_transactions = 0usize;
    let mut current_session: Option<ActiveSession> = None;
    let mut index = 0usize;
    while index < transitions.len() {
        let timestamp_ns = transitions[index].0;
        let mut starts = 0usize;
        let mut stops = 0usize;
        while index < transitions.len() && transitions[index].0 == timestamp_ns {
            starts = starts.saturating_add(transitions[index].1);
            stops = stops.saturating_add(transitions[index].2);
            index += 1;
        }

        if outstanding_transactions == 0 && starts > 0 {
            current_session = Some(ActiveSession {
                start_timestamp_ns: timestamp_ns,
                end_timestamp_ns: None,
                transaction_count: 0,
                pending_transactions: 0,
            });
        }
        outstanding_transactions = outstanding_transactions.saturating_add(starts);
        if let Some(session) = &mut current_session {
            session.transaction_count = session.transaction_count.saturating_add(starts);
        }

        outstanding_transactions = outstanding_transactions.saturating_sub(stops);
        if outstanding_transactions == 0 {
            if let Some(mut session) = current_session.take() {
                session.end_timestamp_ns = Some(timestamp_ns);
                sessions.push(session);
            }
        }
    }

    if let Some(mut session) = current_session {
        session.pending_transactions = outstanding_transactions;
        sessions.push(session);
    }

    sessions
}

fn slot_latest_timestamp_ns(slot: &store::SlotRecord) -> Option<u64> {
    slot.slot_events
        .iter()
        .chain(
            slot.transactions
                .values()
                .flat_map(|transaction| transaction.events.iter()),
        )
        .map(|event| event.timestamp_ns)
        .max()
}

fn active_sessions_total_duration_ns(
    sessions: &[ActiveSession],
    open_session_end_timestamp_ns: Option<u64>,
) -> Option<u64> {
    if sessions.is_empty() {
        return None;
    }

    Some(
        sessions
            .iter()
            .filter_map(|session| active_session_duration_ns(session, open_session_end_timestamp_ns))
            .sum(),
    )
}

fn active_sessions_pending_transactions(sessions: &[ActiveSession]) -> usize {
    sessions
        .last()
        .filter(|session| session.end_timestamp_ns.is_none())
        .map_or(0, |session| session.pending_transactions)
}

fn active_session_summaries(
    sessions: &[ActiveSession],
    slot_begin_timestamp_ns: Option<u64>,
    open_session_end_timestamp_ns: Option<u64>,
) -> Vec<ActiveSessionSummary> {
    sessions
        .iter()
        .map(|session| ActiveSessionSummary {
            start_delta_ns: slot_begin_timestamp_ns
                .map(|slot_begin| session.start_timestamp_ns.saturating_sub(slot_begin))
                .unwrap_or_default(),
            start_timestamp_ns: session.start_timestamp_ns,
            end_timestamp_ns: session.end_timestamp_ns,
            duration_ns: active_session_duration_ns(session, open_session_end_timestamp_ns),
            transaction_count: session.transaction_count,
            pending_transactions: session.pending_transactions,
        })
        .collect()
}

fn active_session_duration_ns(
    session: &ActiveSession,
    open_session_end_timestamp_ns: Option<u64>,
) -> Option<u64> {
    session
        .end_timestamp_ns
        .or(open_session_end_timestamp_ns)
        .map(|end_timestamp_ns| end_timestamp_ns.saturating_sub(session.start_timestamp_ns))
}

fn transaction_summary(
    slot_begin_timestamp_ns: Option<u64>,
    transaction: &TransactionRecord,
) -> TransactionSummary {
    TransactionSummary {
        index: transaction.index,
        status: transaction.status(),
        ingest_timestamp_ns: transaction.ingest_timestamp_ns(),
        slot_ingest_delta_ns: slot_begin_timestamp_ns
            .zip(transaction.ingest_timestamp_ns())
            .map(|(slot_begin, ingest)| ingest.saturating_sub(slot_begin)),
        estimated_cost_units: transaction_estimated_cost_units(transaction),
        cost_units: transaction_cost_units(transaction),
        check_wait_ns: transaction_check_wait_ns(transaction),
        ready_wait_ns: transaction_ready_wait_ns(transaction),
        scheduling_wait_ns: transaction_scheduling_wait_ns(transaction),
        exec_wait_ns: transaction_exec_wait_ns(transaction),
        execution_duration_ns: transaction_execution_duration_ns(transaction),
        duration_ns: transaction.total_duration_ns(),
        signature: transaction
            .signature
            .clone()
            .unwrap_or_else(|| "<signature-pending>".to_string()),
    }
}

fn transaction_estimated_cost_units(transaction: &TransactionRecord) -> Option<u64> {
    transaction
        .events
        .iter()
        .find_map(ReplayEvent::estimated_cost_units)
}

fn transaction_cost_units(transaction: &TransactionRecord) -> Option<u64> {
    transaction.events.iter().find_map(ReplayEvent::cost_units)
}

fn slot_estimated_cost_units(slot: &store::SlotRecord) -> Option<u64> {
    sum_slot_transaction_values(slot, transaction_estimated_cost_units)
}

fn slot_cost_units(slot: &store::SlotRecord) -> Option<u64> {
    sum_slot_transaction_values(slot, transaction_cost_units)
}

fn sum_slot_transaction_values(
    slot: &store::SlotRecord,
    value: impl Fn(&TransactionRecord) -> Option<u64>,
) -> Option<u64> {
    let mut total = 0u64;
    let mut has_value = false;
    for value in slot.transactions.values().filter_map(value) {
        total = total.saturating_add(value);
        has_value = true;
    }

    has_value.then_some(total)
}

fn transaction_check_wait_ns(transaction: &TransactionRecord) -> Option<u64> {
    let sent_for_check = first_event_timestamp(
        transaction,
        replay_event_tags::TRANSACTION_SENT_FOR_CHECK,
    )?;
    let picked_up = first_event_timestamp_at_or_after(
        transaction,
        replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
        sent_for_check,
    )?;
    Some(picked_up.saturating_sub(sent_for_check))
}

fn transaction_ready_wait_ns(transaction: &TransactionRecord) -> Option<u64> {
    let check_passed = first_event_timestamp(transaction, replay_event_tags::TRANSACTION_CHECK_PASSED)?;
    transaction
        .events
        .iter()
        .filter(|event| {
            event.tag == replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING
                && event.timestamp_ns >= check_passed
                && event
                    .ready_released_by_transaction_index()
                    .is_some_and(|transaction_index| transaction_index != transaction.index)
        })
        .map(|event| event.timestamp_ns.saturating_sub(check_passed))
        .min()
}

fn transaction_scheduling_wait_ns(transaction: &TransactionRecord) -> Option<u64> {
    let first_skip = first_event_timestamp(
        transaction,
        replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED,
    )?;
    let scheduled = first_event_timestamp_at_or_after(
        transaction,
        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
        first_skip,
    )?;
    Some(scheduled.saturating_sub(first_skip))
}

fn transaction_exec_wait_ns(transaction: &TransactionRecord) -> Option<u64> {
    let scheduled = first_event_timestamp(
        transaction,
        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
    )?;
    let picked_up = first_event_timestamp_at_or_after(
        transaction,
        replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
        scheduled,
    )?;
    Some(picked_up.saturating_sub(scheduled))
}

fn transaction_execution_duration_ns(transaction: &TransactionRecord) -> Option<u64> {
    let scheduled = first_event_timestamp(
        transaction,
        replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
    )?;
    let picked_up = first_event_timestamp_at_or_after(
        transaction,
        replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
        scheduled,
    )?;
    let completed = first_event_timestamp_at_or_after(
        transaction,
        replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
        picked_up,
    )?;
    Some(completed.saturating_sub(picked_up))
}

fn first_event_timestamp(transaction: &TransactionRecord, tag: u64) -> Option<u64> {
    transaction
        .events
        .iter()
        .filter(|event| event.tag == tag)
        .map(|event| event.timestamp_ns)
        .min()
}

fn first_event_timestamp_at_or_after(
    transaction: &TransactionRecord,
    tag: u64,
    timestamp_ns: u64,
) -> Option<u64> {
    transaction
        .events
        .iter()
        .filter(|event| event.tag == tag && event.timestamp_ns >= timestamp_ns)
        .map(|event| event.timestamp_ns)
        .min()
}

fn transaction_timeline(
    slot: u64,
    slot_begin_timestamp_ns: Option<u64>,
    transaction: &TransactionRecord,
) -> TransactionTimeline {
    let base_timestamp_ns = transaction
        .ingest_timestamp_ns()
        .or_else(|| transaction.events.first().map(|event| event.timestamp_ns))
        .unwrap_or_default();
    let mut events = transaction
        .events
        .iter()
        .map(|event| TimelineEvent {
            delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
            timestamp_ns: event.timestamp_ns,
            name: event_name(event.tag),
            detail: timeline_event_detail(event),
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.timestamp_ns);

    TransactionTimeline {
        slot,
        index: transaction.index,
        status: transaction.status(),
        slot_ingest_delta_ns: slot_begin_timestamp_ns
            .zip(transaction.ingest_timestamp_ns())
            .map(|(slot_begin, ingest)| ingest.saturating_sub(slot_begin)),
        duration_ns: transaction.total_duration_ns(),
        signature: transaction
            .signature
            .clone()
            .unwrap_or_else(|| "<signature-pending>".to_string()),
        events,
    }
}

fn slot_timeline(slot: &store::SlotRecord) -> Vec<TimelineEvent> {
    let base_timestamp_ns = slot
        .slot_events
        .iter()
        .map(|event| event.timestamp_ns)
        .min()
        .unwrap_or_default();
    let mut events = slot
        .slot_events
        .iter()
        .filter(|event| event.tag != replay_event_tags::SLOT_SCHEDULING_SUMMARY)
        .map(|event| TimelineEvent {
            delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
            timestamp_ns: event.timestamp_ns,
            name: event_name(event.tag),
            detail: timeline_event_detail(event),
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.timestamp_ns);
    events
}

fn worker_timeline(
    slot: &store::SlotRecord,
    worker_timeline_kind: WorkerTimelineKind,
) -> Vec<WorkerTimelineEvent> {
    match worker_timeline_kind {
        WorkerTimelineKind::Scheduler => return scheduler_timeline(slot),
        WorkerTimelineKind::SchedulingSummary => return scheduling_summary_timeline(slot),
        WorkerTimelineKind::Execution
        | WorkerTimelineKind::Check
        | WorkerTimelineKind::SignatureVerification => {}
    }

    let base_timestamp_ns = slot
        .begin_timestamp_ns()
        .or_else(|| {
            slot.transactions
                .values()
                .flat_map(|transaction| {
                    transaction.events.iter().enumerate().filter_map(
                        move |(event_index, event)| {
                            worker_event_id(
                                &transaction.events,
                                event_index,
                                event,
                                worker_timeline_kind,
                            )
                            .map(|_| event.timestamp_ns)
                        },
                    )
                })
                .min()
        })
        .unwrap_or_default();
    let mut events = slot
        .transactions
        .values()
        .flat_map(|transaction| {
            transaction
                .events
                .iter()
                .enumerate()
                .filter_map(move |(event_index, event)| {
                    let worker_id = worker_event_id(
                        &transaction.events,
                        event_index,
                        event,
                        worker_timeline_kind,
                    )?;
                    let transaction_index = event.transaction_index()?;
                    Some(WorkerTimelineEvent {
                        delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
                        timestamp_ns: event.timestamp_ns,
                        worker_id: Some(worker_id),
                        transaction_index: Some(transaction_index),
                        name: event_name(event.tag),
                        detail: worker_timeline_event_detail(event),
                    })
                })
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| (event.timestamp_ns, event.worker_id, event.transaction_index));
    events
}

fn scheduler_timeline(slot: &store::SlotRecord) -> Vec<WorkerTimelineEvent> {
    let base_timestamp_ns = slot
        .begin_timestamp_ns()
        .or_else(|| {
            slot.slot_events
                .iter()
                .chain(
                    slot.transactions
                        .values()
                        .flat_map(|transaction| transaction.events.iter()),
                )
                .filter(|event| is_scheduler_event_tag(event.tag))
                .map(|event| event.timestamp_ns)
                .min()
        })
        .unwrap_or_default();
    let mut events = slot
        .slot_events
        .iter()
        .filter(|event| is_scheduler_event_tag(event.tag))
        .map(|event| WorkerTimelineEvent {
            delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
            timestamp_ns: event.timestamp_ns,
            worker_id: None,
            transaction_index: None,
            name: event_name(event.tag),
            detail: timeline_event_detail(event),
        })
        .chain(slot.transactions.values().flat_map(|transaction| {
            transaction
                .events
                .iter()
                .filter(|event| is_scheduler_event_tag(event.tag))
                .map(move |event| WorkerTimelineEvent {
                    delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
                    timestamp_ns: event.timestamp_ns,
                    worker_id: None,
                    transaction_index: event.transaction_index(),
                    name: event_name(event.tag),
                    detail: timeline_event_detail(event),
                })
        }))
        .collect::<Vec<_>>();
    events.sort_by_key(|event| (event.timestamp_ns, event.transaction_index));
    events
}

fn scheduling_summary_timeline(slot: &store::SlotRecord) -> Vec<WorkerTimelineEvent> {
    let base_timestamp_ns = slot
        .begin_timestamp_ns()
        .or_else(|| {
            slot.slot_events
                .iter()
                .filter(|event| event.tag == replay_event_tags::SLOT_SCHEDULING_SUMMARY)
                .map(|event| event.timestamp_ns)
                .min()
        })
        .unwrap_or_default();
    let mut events = slot
        .slot_events
        .iter()
        .filter(|event| event.tag == replay_event_tags::SLOT_SCHEDULING_SUMMARY)
        .map(|event| WorkerTimelineEvent {
            delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
            timestamp_ns: event.timestamp_ns,
            worker_id: None,
            transaction_index: None,
            name: event_name(event.tag),
            detail: timeline_event_detail(event),
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.timestamp_ns);
    events
}

fn is_scheduler_event_tag(tag: u64) -> bool {
    matches!(
        tag,
        replay_event_tags::SLOT_BEGIN
            | replay_event_tags::SLOT_ABORT
            | replay_event_tags::SLOT_COMPLETE
            | replay_event_tags::SLOT_FAILED
            | replay_event_tags::TRANSACTION_INGESTED
            | replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED
            | replay_event_tags::TRANSACTION_SIGNATURES_RETURNED
            | replay_event_tags::TRANSACTION_SENT_FOR_CHECK
            | replay_event_tags::TRANSACTION_CHECK_FAILED
            | replay_event_tags::TRANSACTION_CHECK_PASSED
            | replay_event_tags::TRANSACTION_READY_FOR_SCHEDULING
            | replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED
            | replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC
            | replay_event_tags::TRANSACTION_FINISHED_EXEC
            | replay_event_tags::TRANSACTION_EXEC_FAILED
    )
}

fn worker_event_id(
    transaction_events: &[ReplayEvent],
    event_index: usize,
    event: &ReplayEvent,
    worker_timeline_kind: WorkerTimelineKind,
) -> Option<u64> {
    match worker_timeline_kind {
        WorkerTimelineKind::Execution => {
            (replay_worker_stage(transaction_events, event_index)? == ReplayWorkerStage::Execution)
                .then(|| event.worker_id())
                .flatten()
        }
        WorkerTimelineKind::Check => {
            (replay_worker_stage(transaction_events, event_index)? == ReplayWorkerStage::Check)
                .then(|| event.worker_id())
                .flatten()
        }
        WorkerTimelineKind::SignatureVerification => event.signature_verification_worker_id(),
        WorkerTimelineKind::Scheduler | WorkerTimelineKind::SchedulingSummary => None,
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

fn timeline_event_detail(event: &ReplayEvent) -> String {
    let mut details = Vec::new();
    if let Some(worker_id) = event.worker_id() {
        details.push(format!("worker={worker_id}"));
    }
    if let Some(worker_id) = event.signature_verification_worker_id() {
        details.push(format!("sigverify_worker={worker_id}"));
    }
    if let Some(check_queue_len) = event.check_queue_len() {
        details.push(format!("queue_len={check_queue_len}"));
    }
    if let Some(worker_queue_len) = event.worker_queue_len() {
        details.push(format!("queue_len={worker_queue_len}"));
    }
    if let Some(unscheduled_ready_transactions_ahead) =
        event.unscheduled_ready_transactions_ahead()
    {
        details.push(format!(
            "unscheduled_ready_ahead={unscheduled_ready_transactions_ahead}"
        ));
    }
    if let Some(reason) = event.scheduling_skip_reason() {
        details.push(format!(
            "skip_reason={}",
            scheduling_skip_reason_detail(reason)
        ));
    }
    if let Some(transaction_index) = event.scheduling_blocked_by_transaction_index() {
        details.push(format!("blocked_by_tx={transaction_index}"));
    }
    if let Some(signature_verification_queue_len) = event.signature_verification_queue_len() {
        details.push(format!("queue_len={signature_verification_queue_len}"));
    }
    if let Some(estimated_cost_units) = event.estimated_cost_units() {
        details.push(format!("estimated_cost_units={estimated_cost_units}"));
    }
    if let Some(cost_units) = event.cost_units() {
        details.push(format!("cost_units={cost_units}"));
    }
    if let Some(verified) = event.signature_verification_result() {
        details.push(format!("verified={verified}"));
    }
    if let Some(verified) = event.signature_verification_worker_result() {
        details.push(format!("verified={verified}"));
    }
    if let Some(transaction_index) = event.ready_released_by_transaction_index() {
        details.push(format!("ready_released_by_tx={transaction_index}"));
    }
    if let Some(reason) = event.slot_failure_reason() {
        details.push(format!("reason={reason}"));
    }
    if let Some(end_timestamp_ns) = event.scheduling_summary_end_timestamp_ns() {
        details.push(format!("end_timestamp_ns={end_timestamp_ns}"));
        details.push(format!(
            "duration_ns={}",
            end_timestamp_ns.saturating_sub(event.timestamp_ns)
        ));
    }
    if let Some(scanned) = event.scheduling_summary_scanned() {
        details.push(format!("scanned={scanned}"));
    }
    if let Some(scheduled) = event.scheduling_summary_scheduled() {
        details.push(format!("scheduled={scheduled}"));
    }
    if let Some(conflicts) = event.scheduling_summary_conflicts() {
        details.push(format!("conflicts={conflicts}"));
    }
    details.join(" ")
}

fn worker_timeline_event_detail(event: &ReplayEvent) -> String {
    let mut details = Vec::new();
    if let Some(check_queue_len) = event.check_queue_len() {
        details.push(format!("queue_len={check_queue_len}"));
    }
    if let Some(worker_queue_len) = event.worker_queue_len() {
        details.push(format!("queue_len={worker_queue_len}"));
    }
    if let Some(unscheduled_ready_transactions_ahead) =
        event.unscheduled_ready_transactions_ahead()
    {
        details.push(format!(
            "unscheduled_ready_ahead={unscheduled_ready_transactions_ahead}"
        ));
    }
    if let Some(reason) = event.scheduling_skip_reason() {
        details.push(format!(
            "skip_reason={}",
            scheduling_skip_reason_detail(reason)
        ));
    }
    if let Some(transaction_index) = event.scheduling_blocked_by_transaction_index() {
        details.push(format!("blocked_by_tx={transaction_index}"));
    }
    if let Some(estimated_cost_units) = event.estimated_cost_units() {
        details.push(format!("estimated_cost_units={estimated_cost_units}"));
    }
    if let Some(cost_units) = event.cost_units() {
        details.push(format!("cost_units={cost_units}"));
    }
    if let Some(verified) = event.signature_verification_worker_result() {
        details.push(format!("verified={verified}"));
    }
    if let Some(reason) = event.slot_failure_reason() {
        details.push(format!("reason={reason}"));
    }
    details.join(" ")
}

fn scheduling_skip_reason_detail(reason: u64) -> String {
    match reason {
        replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS => {
            "multiple-lock-conflicts".to_string()
        }
        replay_scheduling_skip_reasons::TOO_MUCH_WORK_ON_THREAD => {
            "too-much-work-on-thread".to_string()
        }
        replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT => {
            "previously-unscheduled-conflict".to_string()
        }
        reason => format!("unknown({reason})"),
    }
}

fn handle_key(app: &mut App, key: KeyEvent, snapshot: &UiSnapshot) -> bool {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('q') => return true,
        KeyCode::Char('m' | 'M') if key.modifiers.is_empty() => app.toggle_maximized_pane(),
        KeyCode::Char('a' | 'A') if key.modifiers.is_empty() && app.clear_worker_filter() => {}
        KeyCode::Char('v' | 'V') if key.modifiers.is_empty() => {
            app.toggle_worker_timeline_kind()
        }
        KeyCode::Char(value) if key.modifiers.is_empty() && app.push_worker_filter_digit(value) => {
        }
        KeyCode::Backspace if app.pop_worker_filter_digit() => {}
        KeyCode::Esc if app.restore_maximized_pane() => {}
        KeyCode::Esc | KeyCode::Backspace | KeyCode::Left => app.move_back(),
        KeyCode::Enter | KeyCode::Right => app.move_forward(snapshot),
        KeyCode::Tab => app.next_focus(snapshot),
        KeyCode::BackTab => app.previous_focus(),
        KeyCode::Home => app.move_home(snapshot),
        KeyCode::End => app.move_end(snapshot),
        KeyCode::Up => app.move_up(snapshot),
        KeyCode::Down => app.move_down(snapshot),
        KeyCode::PageUp => app.page_up(snapshot),
        KeyCode::PageDown => app.page_down(snapshot),
        _ => {}
    }
    false
}

fn handle_key_events(
    app: &mut App,
    keys: impl IntoIterator<Item = KeyEvent>,
    snapshot: &UiSnapshot,
) -> bool {
    let mut pending_navigation = None;
    for key in keys {
        if !is_action_key(&key) {
            continue;
        }

        if is_coalescible_navigation_key(&key) {
            if pending_navigation
                .as_ref()
                .is_some_and(|pending| same_key_action(pending, &key))
            {
                pending_navigation = Some(key);
            } else {
                if let Some(pending_navigation) = pending_navigation.take() {
                    if handle_key(app, pending_navigation, snapshot) {
                        return true;
                    }
                }
                pending_navigation = Some(key);
            }
            continue;
        }

        if let Some(pending_navigation) = pending_navigation.take() {
            if handle_key(app, pending_navigation, snapshot) {
                return true;
            }
        }
        if handle_key(app, key, snapshot) {
            return true;
        }
    }

    pending_navigation.is_some_and(|key| handle_key(app, key, snapshot))
}

fn is_action_key(key: &KeyEvent) -> bool {
    matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
}

fn is_coalescible_navigation_key(key: &KeyEvent) -> bool {
    matches!(
        key.code,
        KeyCode::Up | KeyCode::Down | KeyCode::PageUp | KeyCode::PageDown
    )
}

fn same_key_action(left: &KeyEvent, right: &KeyEvent) -> bool {
    left.code == right.code && left.modifiers == right.modifiers
}

impl App {
    fn set_focus(&mut self, focus: FocusPane) {
        self.focus = focus;
        if self.maximized_pane.is_some() {
            self.maximized_pane = Some(focus);
        }
    }

    fn toggle_maximized_pane(&mut self) {
        self.maximized_pane = if self.maximized_pane.is_some() {
            None
        } else {
            Some(self.focus)
        };
    }

    fn restore_maximized_pane(&mut self) -> bool {
        self.maximized_pane.take().is_some()
    }

    fn set_selected_slot(&mut self, selected_slot: Option<u64>) -> bool {
        let changed = self.selected_slot != selected_slot;
        self.selected_slot = selected_slot;
        if changed {
            self.selected_transaction = None;
            self.transaction_index = 0;
            self.tx_timeline_scroll = 0;
            self.worker_timeline_scroll = 0;
            self.worker_filter = None;
        }
        changed
    }

    fn push_worker_filter_digit(&mut self, value: char) -> bool {
        if self.focus != FocusPane::WorkerTimeline
            || !self.worker_timeline_kind.supports_worker_filter()
            || !value.is_ascii_digit()
        {
            return false;
        }

        let digit = value.to_digit(10).expect("ascii digit must parse");
        self.worker_filter = Some(
            self.worker_filter
                .unwrap_or_default()
                .saturating_mul(10)
                .saturating_add(u64::from(digit)),
        );
        self.worker_timeline_scroll = 0;
        true
    }

    fn pop_worker_filter_digit(&mut self) -> bool {
        if self.focus != FocusPane::WorkerTimeline
            || !self.worker_timeline_kind.supports_worker_filter()
        {
            return false;
        }
        let Some(worker_filter) = self.worker_filter else {
            return false;
        };

        self.worker_filter = if worker_filter < 10 {
            None
        } else {
            Some(worker_filter / 10)
        };
        self.worker_timeline_scroll = 0;
        true
    }

    fn clear_worker_filter(&mut self) -> bool {
        if self.focus != FocusPane::WorkerTimeline || self.worker_filter.is_none() {
            return false;
        }

        self.worker_filter = None;
        self.worker_timeline_scroll = 0;
        true
    }

    fn toggle_worker_timeline_kind(&mut self) {
        if self.focus != FocusPane::WorkerTimeline {
            return;
        }

        self.worker_timeline_kind = match self.worker_timeline_kind {
            WorkerTimelineKind::Execution => WorkerTimelineKind::Check,
            WorkerTimelineKind::Check => WorkerTimelineKind::SignatureVerification,
            WorkerTimelineKind::SignatureVerification => WorkerTimelineKind::Scheduler,
            WorkerTimelineKind::Scheduler => WorkerTimelineKind::SchedulingSummary,
            WorkerTimelineKind::SchedulingSummary => WorkerTimelineKind::Execution,
        };
        self.worker_timeline_scroll = 0;
    }

    fn sync_slots(&mut self, slots: &[SlotSummary]) -> bool {
        if slots.is_empty() {
            let changed = self.selected_slot.take().is_some()
                || self.selected_transaction.take().is_some()
                || self.worker_filter.take().is_some();
            self.slot_index = 0;
            self.transaction_index = 0;
            self.tx_timeline_scroll = 0;
            self.worker_timeline_scroll = 0;
            return changed;
        }

        if let Some(selected_slot) = self.selected_slot {
            if let Some(index) = slots.iter().position(|slot| slot.slot == selected_slot) {
                self.slot_index = index;
                return false;
            }
        }

        self.slot_index = slots.len().saturating_sub(1);
        self.set_selected_slot(Some(slots[self.slot_index].slot));
        true
    }

    fn sync_transactions(&mut self, snapshot: &UiSnapshot) -> bool {
        let Some(slot) = &snapshot.selected_slot else {
            let changed = self.selected_transaction.take().is_some();
            self.transaction_index = 0;
            self.tx_timeline_scroll = 0;
            self.worker_timeline_scroll = 0;
            return changed;
        };

        if slot.transactions.is_empty() {
            let changed = self.selected_transaction.take().is_some();
            self.transaction_index = 0;
            self.tx_timeline_scroll = 0;
            return changed;
        }

        if let Some(selected_transaction) = self.selected_transaction {
            if let Some(index) = slot
                .transactions
                .iter()
                .position(|transaction| transaction.index == selected_transaction)
            {
                self.transaction_index = index;
                return false;
            }
        }

        self.transaction_index = self
            .transaction_index
            .min(slot.transactions.len().saturating_sub(1));
        let selected_transaction = slot.transactions[self.transaction_index].index;
        let changed = self.selected_transaction != Some(selected_transaction);
        self.selected_transaction = Some(selected_transaction);
        self.tx_timeline_scroll = 0;
        changed
    }

    fn move_back(&mut self) {
        let focus = match self.focus {
            FocusPane::Slots => FocusPane::Slots,
            FocusPane::Transactions => FocusPane::Slots,
            FocusPane::TxTimeline => FocusPane::Transactions,
            FocusPane::WorkerTimeline => FocusPane::TxTimeline,
        };
        self.set_focus(focus);
    }

    fn move_forward(&mut self, snapshot: &UiSnapshot) {
        let focus = match self.focus {
            FocusPane::Slots => FocusPane::Transactions,
            FocusPane::Transactions if snapshot.selected_transaction().is_some() => {
                FocusPane::TxTimeline
            }
            FocusPane::Transactions if snapshot.selected_slot.is_some() => {
                FocusPane::WorkerTimeline
            }
            FocusPane::Transactions => self.focus,
            FocusPane::TxTimeline => FocusPane::WorkerTimeline,
            FocusPane::WorkerTimeline => self.focus,
        };
        self.set_focus(focus);
    }

    fn next_focus(&mut self, _snapshot: &UiSnapshot) {
        let focus = match self.focus {
            FocusPane::Slots => FocusPane::Transactions,
            FocusPane::Transactions => FocusPane::TxTimeline,
            FocusPane::TxTimeline => FocusPane::WorkerTimeline,
            FocusPane::WorkerTimeline => FocusPane::Slots,
        };
        self.set_focus(focus);
    }

    fn previous_focus(&mut self) {
        let focus = match self.focus {
            FocusPane::Slots => FocusPane::WorkerTimeline,
            FocusPane::Transactions => FocusPane::Slots,
            FocusPane::TxTimeline => FocusPane::Transactions,
            FocusPane::WorkerTimeline => FocusPane::TxTimeline,
        };
        self.set_focus(focus);
    }

    fn move_home(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                self.slot_index = 0;
                self.set_selected_slot(snapshot.slots.first().map(|slot| slot.slot));
            }
            FocusPane::Transactions => {
                self.transaction_index = 0;
                self.selected_transaction = snapshot.selected_slot.as_ref().and_then(|slot| {
                    slot.transactions
                        .first()
                        .map(|transaction| transaction.index)
                });
                self.tx_timeline_scroll = 0;
            }
            FocusPane::TxTimeline => {
                self.tx_timeline_scroll = 0;
            }
            FocusPane::WorkerTimeline => {
                self.worker_timeline_scroll = 0;
            }
        }
    }

    fn move_end(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                self.slot_index = snapshot.slots.len().saturating_sub(1);
                self.set_selected_slot(snapshot.slots.get(self.slot_index).map(|slot| slot.slot));
            }
            FocusPane::Transactions => {
                if let Some(slot) = &snapshot.selected_slot {
                    self.transaction_index = slot.transactions.len().saturating_sub(1);
                    self.selected_transaction = slot
                        .transactions
                        .get(self.transaction_index)
                        .map(|transaction| transaction.index);
                    self.tx_timeline_scroll = 0;
                }
            }
            FocusPane::TxTimeline => {
                self.tx_timeline_scroll = tx_timeline_line_count(snapshot)
                    .saturating_sub(1)
                    .try_into()
                    .unwrap_or(u16::MAX);
            }
            FocusPane::WorkerTimeline => {
                self.worker_timeline_scroll = worker_timeline_line_count(
                    snapshot,
                    self.worker_filter,
                    self.worker_timeline_kind,
                )
                .saturating_sub(1)
                .try_into()
                .unwrap_or(u16::MAX);
            }
        }
    }

    fn move_up(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                self.slot_index = self.slot_index.saturating_sub(1);
                self.set_selected_slot(snapshot.slots.get(self.slot_index).map(|slot| slot.slot));
            }
            FocusPane::Transactions => {
                self.transaction_index = self.transaction_index.saturating_sub(1);
                self.selected_transaction = snapshot.selected_slot.as_ref().and_then(|slot| {
                    slot.transactions
                        .get(self.transaction_index)
                        .map(|transaction| transaction.index)
                });
                self.tx_timeline_scroll = 0;
            }
            FocusPane::TxTimeline => scroll_value(&mut self.tx_timeline_scroll, -1),
            FocusPane::WorkerTimeline => scroll_value(&mut self.worker_timeline_scroll, -1),
        }
    }

    fn move_down(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                if !snapshot.slots.is_empty() {
                    self.slot_index = self
                        .slot_index
                        .saturating_add(1)
                        .min(snapshot.slots.len().saturating_sub(1));
                    self.set_selected_slot(
                        snapshot.slots.get(self.slot_index).map(|slot| slot.slot),
                    );
                }
            }
            FocusPane::Transactions => {
                if let Some(slot) = &snapshot.selected_slot {
                    self.transaction_index = self
                        .transaction_index
                        .saturating_add(1)
                        .min(slot.transactions.len().saturating_sub(1));
                    self.selected_transaction = slot
                        .transactions
                        .get(self.transaction_index)
                        .map(|transaction| transaction.index);
                    self.tx_timeline_scroll = 0;
                }
            }
            FocusPane::TxTimeline => scroll_value(&mut self.tx_timeline_scroll, 1),
            FocusPane::WorkerTimeline => scroll_value(&mut self.worker_timeline_scroll, 1),
        }
    }

    fn page_up(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                self.slot_index = self.slot_index.saturating_sub(PAGE_STEP);
                self.set_selected_slot(snapshot.slots.get(self.slot_index).map(|slot| slot.slot));
            }
            FocusPane::Transactions => {
                self.transaction_index = self.transaction_index.saturating_sub(PAGE_STEP);
                self.selected_transaction = snapshot.selected_slot.as_ref().and_then(|slot| {
                    slot.transactions
                        .get(self.transaction_index)
                        .map(|transaction| transaction.index)
                });
                self.tx_timeline_scroll = 0;
            }
            FocusPane::TxTimeline => {
                scroll_value(&mut self.tx_timeline_scroll, -(PAGE_STEP as i16));
            }
            FocusPane::WorkerTimeline => {
                scroll_value(&mut self.worker_timeline_scroll, -(PAGE_STEP as i16));
            }
        }
    }

    fn page_down(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                if !snapshot.slots.is_empty() {
                    self.slot_index = self
                        .slot_index
                        .saturating_add(PAGE_STEP)
                        .min(snapshot.slots.len().saturating_sub(1));
                    self.set_selected_slot(
                        snapshot.slots.get(self.slot_index).map(|slot| slot.slot),
                    );
                }
            }
            FocusPane::Transactions => {
                if let Some(slot) = &snapshot.selected_slot {
                    self.transaction_index = self
                        .transaction_index
                        .saturating_add(PAGE_STEP)
                        .min(slot.transactions.len().saturating_sub(1));
                    self.selected_transaction = slot
                        .transactions
                        .get(self.transaction_index)
                        .map(|transaction| transaction.index);
                    self.tx_timeline_scroll = 0;
                }
            }
            FocusPane::TxTimeline => {
                scroll_value(&mut self.tx_timeline_scroll, PAGE_STEP as i16);
            }
            FocusPane::WorkerTimeline => {
                scroll_value(&mut self.worker_timeline_scroll, PAGE_STEP as i16);
            }
        }
    }

    fn bound_timeline_scrolls(&mut self, snapshot: &UiSnapshot) {
        bound_scroll(
            &mut self.tx_timeline_scroll,
            tx_timeline_line_count(snapshot),
        );
        bound_scroll(
            &mut self.worker_timeline_scroll,
            worker_timeline_line_count(snapshot, self.worker_filter, self.worker_timeline_kind),
        );
    }
}

fn scroll_value(scroll: &mut u16, delta: i16) {
    if delta.is_negative() {
        *scroll = (*scroll).saturating_sub(delta.unsigned_abs());
    } else {
        *scroll = (*scroll).saturating_add(delta.unsigned_abs());
    }
}

fn bound_scroll(scroll: &mut u16, line_count: usize) {
    let max_scroll = line_count.saturating_sub(1).try_into().unwrap_or(u16::MAX);
    *scroll = (*scroll).min(max_scroll);
}

impl UiSnapshot {
    fn selected_transaction(&self) -> Option<&TransactionTimeline> {
        self.selected_slot
            .as_ref()
            .and_then(|slot| slot.selected_transaction.as_ref())
    }
}

impl SelectedSlot {
    fn worker_events(&self, worker_timeline_kind: WorkerTimelineKind) -> &[WorkerTimelineEvent] {
        match worker_timeline_kind {
            WorkerTimelineKind::Execution => &self.worker_events,
            WorkerTimelineKind::Check => &self.check_worker_events,
            WorkerTimelineKind::SignatureVerification => &self.signature_verification_worker_events,
            WorkerTimelineKind::Scheduler => &self.scheduler_events,
            WorkerTimelineKind::SchedulingSummary => &self.scheduling_summary_events,
        }
    }
}

impl FocusPane {
    fn name(self) -> &'static str {
        match self {
            FocusPane::Slots => "slots",
            FocusPane::Transactions => "transactions",
            FocusPane::TxTimeline => "tx-timeline",
            FocusPane::WorkerTimeline => "worker-timeline",
        }
    }
}

impl WorkerTimelineKind {
    fn label(self) -> &'static str {
        match self {
            WorkerTimelineKind::Execution => "exec",
            WorkerTimelineKind::Check => "check",
            WorkerTimelineKind::SignatureVerification => "sigverify",
            WorkerTimelineKind::Scheduler => "scheduler",
            WorkerTimelineKind::SchedulingSummary => "scheduling-summary",
        }
    }

    fn supports_worker_filter(self) -> bool {
        !matches!(self, Self::Scheduler | Self::SchedulingSummary)
    }
}

impl TerminalSession {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        if let Err(err) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(err);
        }
        Ok(Self)
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn draw_ui(frame: &mut Frame<'_>, app: &App, snapshot: &UiSnapshot) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(frame.area());
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(layout[1]);
    let details = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(body[1]);
    let timelines = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(details[1]);

    render_header(frame, layout[0], app, snapshot);
    if let Some(maximized_pane) = app.maximized_pane {
        render_pane(frame, layout[1], maximized_pane, app, snapshot);
        render_footer(frame, layout[2]);
        return;
    }

    render_slots(frame, body[0], app, snapshot);
    render_transactions(frame, details[0], app, snapshot);
    render_tx_timeline(frame, timelines[0], app, snapshot);
    render_worker_timeline(frame, timelines[1], app, snapshot);
    render_footer(frame, layout[2]);
}

fn render_pane(
    frame: &mut Frame<'_>,
    area: Rect,
    pane: FocusPane,
    app: &App,
    snapshot: &UiSnapshot,
) {
    match pane {
        FocusPane::Slots => render_slots(frame, area, app, snapshot),
        FocusPane::Transactions => render_transactions(frame, area, app, snapshot),
        FocusPane::TxTimeline => render_tx_timeline(frame, area, app, snapshot),
        FocusPane::WorkerTimeline => render_worker_timeline(frame, area, app, snapshot),
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    let selected_slot = app
        .selected_slot
        .map(|slot| slot.to_string())
        .unwrap_or_else(|| "-".to_string());
    let maximized = app
        .maximized_pane
        .map(|pane| format!(" maximized={}", pane.name()))
        .unwrap_or_default();
    let line = Line::from(vec![
        Span::styled(
            "Replay events",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  received={} processed={} queued={} skipped={} retained={} selected_slot={} focus={}{}",
            snapshot.received_events,
            snapshot.processed_events,
            snapshot
                .received_events
                .saturating_sub(snapshot.processed_events),
            snapshot.skipped_events,
            snapshot.slots.len(),
            selected_slot,
            app.focus.name(),
            maximized,
        )),
    ]);
    frame.render_widget(
        Paragraph::new(line).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_slots(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    let rows = if snapshot.slots.is_empty() {
        vec![Row::new(["waiting", "", "", "", "", "", "", ""])]
    } else {
        snapshot
            .slots
            .iter()
            .map(|slot| {
                Row::new([
                    Cell::from(slot.slot.to_string()),
                    Cell::from(slot.transaction_count.to_string()),
                    Cell::from(format_optional_cost_units(slot.estimated_cost_units)),
                    Cell::from(format_optional_cost_units(slot.cost_units)),
                    Cell::from(
                        slot.duration_ns
                            .map(format_duration_ns)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::from(
                        slot.active_duration_ns
                            .map(format_duration_ns)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::from(format!(
                        "{}{}",
                        slot.active_session_count,
                        if slot.active_pending_transactions == 0 {
                            String::new()
                        } else {
                            format!("+{}", slot.active_pending_transactions)
                        }
                    )),
                    Cell::from(slot.status),
                ])
            })
            .collect()
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Min(8),
        ],
    )
    .header(
        Row::new([
            "slot", "txs", "est CUs", "cost CUs", "block", "active", "sessions", "status",
        ])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(focused_block("Slots", app.focus == FocusPane::Slots))
    .row_highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan));
    let mut state = TableState::default();
    if !snapshot.slots.is_empty() {
        state.select(Some(app.slot_index));
    }
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_transactions(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    let (rows, selected_row) = if let Some(slot) = &snapshot.selected_slot {
        if slot.transactions.is_empty() {
            (vec![placeholder_transaction_row("no txs")], None)
        } else {
            (
                transaction_table_rows(slot),
                Some(app.transaction_index.min(slot.transactions.len() - 1)),
            )
        }
    } else {
        (vec![placeholder_transaction_row("no slot")], None)
    };

    let title = snapshot
        .selected_slot
        .as_ref()
        .map(|slot| {
            format!(
                "Transactions slot={} status={} active={} sessions={} pending={} slot_events={}",
                slot.slot,
                slot.status,
                slot.active_duration_ns
                    .map(format_duration_ns)
                    .unwrap_or_else(|| "-".to_string()),
                slot.active_sessions.len(),
                slot.active_pending_transactions,
                slot.slot_event_count
            )
        })
        .unwrap_or_else(|| "Transactions".to_string());
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Min(12),
        ],
    )
    .header(
        Row::new([
            "index",
            "status",
            "ingest",
            "est-cost",
            "cost",
            "chk-wait",
            "ready-wait",
            "sched-wait",
            "exec-wait",
            "exec",
            "ns/CU",
            "total",
            "signature",
        ])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(focused_block(title, app.focus == FocusPane::Transactions))
    .row_highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan));
    let mut state = TableState::default();
    state.select(selected_row);
    frame.render_stateful_widget(table, area, &mut state);
}

fn transaction_table_rows(slot: &SelectedSlot) -> Vec<Row<'static>> {
    let fresh_start_transaction_indices = fresh_start_transaction_indices(slot);
    slot.transactions
        .iter()
        .map(|transaction| {
            transaction_table_row(
                transaction,
                fresh_start_transaction_indices.contains(&transaction.index),
            )
        })
        .collect()
}

fn fresh_start_transaction_indices(slot: &SelectedSlot) -> Vec<u64> {
    let mut indices = Vec::with_capacity(slot.active_sessions.len());
    let mut transaction_index = 0usize;
    for session in &slot.active_sessions {
        while transaction_index < slot.transactions.len() {
            let transaction = &slot.transactions[transaction_index];
            let Some(ingest_timestamp_ns) = transaction.ingest_timestamp_ns else {
                transaction_index += 1;
                continue;
            };
            if ingest_timestamp_ns < session.start_timestamp_ns {
                transaction_index += 1;
                continue;
            }
            if session
                .end_timestamp_ns
                .is_none_or(|end_timestamp_ns| ingest_timestamp_ns <= end_timestamp_ns)
            {
                indices.push(transaction.index);
            }
            break;
        }
    }
    indices
}

fn transaction_table_row(transaction: &TransactionSummary, fresh_start: bool) -> Row<'static> {
    Row::new([
        transaction.index.to_string(),
        transaction.status.to_string(),
        transaction
            .slot_ingest_delta_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        format_optional_u64(transaction.estimated_cost_units),
        format_optional_u64(transaction.cost_units),
        transaction
            .check_wait_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        transaction
            .ready_wait_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        transaction
            .scheduling_wait_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        transaction
            .exec_wait_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        transaction
            .execution_duration_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        format_time_per_cost_unit_ns(transaction.execution_duration_ns, transaction.cost_units),
        transaction
            .duration_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        transaction.signature.clone(),
    ])
    .style(if fresh_start {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    })
}

fn placeholder_transaction_row(label: &str) -> Row<'static> {
    Row::new([
        label.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ])
}

fn render_tx_timeline(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    frame.render_widget(
        Paragraph::new(tx_timeline_lines(snapshot))
            .block(focused_block(
                "Tx Timeline",
                app.focus == FocusPane::TxTimeline,
            ))
            .scroll((app.tx_timeline_scroll, 0)),
        area,
    );
}

fn render_worker_timeline(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    frame.render_widget(
        Paragraph::new(worker_timeline_lines(
            snapshot,
            app.worker_filter,
            app.worker_timeline_kind,
        ))
            .block(focused_block(
                worker_timeline_title(app.worker_filter, app.worker_timeline_kind),
                app.focus == FocusPane::WorkerTimeline,
            ))
            .scroll((app.worker_timeline_scroll, 0)),
        area,
    );
}

fn tx_timeline_line_count(snapshot: &UiSnapshot) -> usize {
    tx_timeline_lines(snapshot).len()
}

fn worker_timeline_line_count(
    snapshot: &UiSnapshot,
    worker_filter: Option<u64>,
    worker_timeline_kind: WorkerTimelineKind,
) -> usize {
    worker_timeline_lines(snapshot, worker_filter, worker_timeline_kind).len()
}

fn tx_timeline_lines(snapshot: &UiSnapshot) -> Vec<Line<'static>> {
    let Some(slot) = &snapshot.selected_slot else {
        return vec![Line::from("waiting for replay events")];
    };

    let mut lines = vec![
        Line::from(format!(
            "slot={} status={} slot_duration={} active={} sessions={} pending={} transactions={}",
            slot.slot,
            slot.status,
            slot.duration_ns
                .map(format_duration_ns)
                .unwrap_or_else(|| "-".to_string()),
            slot.active_duration_ns
                .map(format_duration_ns)
                .unwrap_or_else(|| "-".to_string()),
            slot.active_sessions.len(),
            slot.active_pending_transactions,
            slot.transactions.len()
        )),
        Line::from(""),
        Line::from(format!("{:>14} {:>22} slot event", "delta", "timestamp_ns")),
    ];

    if slot.slot_events.is_empty() {
        lines.push(Line::from("no slot events recorded"));
    } else {
        lines.extend(slot.slot_events.iter().map(timeline_event_line));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(format!(
        "{:>14} {:>22} active session",
        "delta", "timestamp_ns"
    )));
    if slot.active_sessions.is_empty() {
        lines.push(Line::from("no active sessions inferred"));
    } else {
        lines.extend(slot.active_sessions.iter().map(active_session_line));
    }

    lines.push(Line::from(""));

    if let Some(transaction) = &slot.selected_transaction {
        lines.extend([
            Line::from(format!(
                "slot={} transaction_index={} status={} slot_begin_to_ingest={} \
                 total_ingest_to_done={}",
                transaction.slot,
                transaction.index,
                transaction.status,
                transaction
                    .slot_ingest_delta_ns
                    .map(format_duration_ns)
                    .unwrap_or_else(|| "-".to_string()),
                transaction
                    .duration_ns
                    .map(format_duration_ns)
                    .unwrap_or_else(|| "-".to_string())
            )),
            Line::from(format!("signature={}", transaction.signature)),
            Line::from(""),
            Line::from(format!("{:>14} {:>22} tx event", "delta", "timestamp_ns")),
        ]);
        lines.extend(transaction.events.iter().map(timeline_event_line));
    } else if slot.transactions.is_empty() {
        lines.push(Line::from("no transactions recorded for selected slot"));
    } else {
        lines.push(Line::from("select a transaction to inspect its timeline"));
    }

    lines
}

fn worker_timeline_lines(
    snapshot: &UiSnapshot,
    worker_filter: Option<u64>,
    worker_timeline_kind: WorkerTimelineKind,
) -> Vec<Line<'static>> {
    let Some(slot) = &snapshot.selected_slot else {
        return vec![Line::from("waiting for replay events")];
    };

    let worker_filter = worker_timeline_kind
        .supports_worker_filter()
        .then_some(worker_filter)
        .flatten();
    let worker_label = if worker_timeline_kind.supports_worker_filter() {
        worker_filter
            .map(|worker_id| worker_id.to_string())
            .unwrap_or_else(|| "all".to_string())
    } else {
        worker_timeline_kind.label().to_string()
    };
    let mut lines = vec![Line::from(format!(
        "slot={} status={} slot_duration={} transactions={} mode={} worker={}",
        slot.slot,
        slot.status,
        slot.duration_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        slot.transactions.len(),
        worker_timeline_kind.label(),
        worker_label
    ))];
    lines.push(Line::from(""));
    lines.push(Line::from(format!(
        "{:>14} {:>22} {:>6} {:>8} {} event",
        "delta",
        "timestamp_ns",
        "worker",
        "tx",
        worker_timeline_kind.label()
    )));
    let worker_events = slot
        .worker_events(worker_timeline_kind)
        .iter()
        .filter(|event| worker_filter.is_none_or(|worker_id| event.worker_id == Some(worker_id)));
    let mut has_events = false;
    for event in worker_events {
        has_events = true;
        lines.push(worker_timeline_event_line(event));
    }
    if !has_events {
        lines.push(Line::from(match worker_filter {
            Some(worker_id) => {
                format!(
                    "no {} events recorded for worker {worker_id} in selected slot",
                    worker_timeline_kind.label(),
                )
            }
            None => format!(
                "no {} events recorded for selected slot",
                worker_timeline_kind.label(),
            ),
        }));
    }

    lines
}

fn worker_timeline_title(
    worker_filter: Option<u64>,
    worker_timeline_kind: WorkerTimelineKind,
) -> String {
    let title = match worker_timeline_kind {
        WorkerTimelineKind::Execution => "Exec Timeline",
        WorkerTimelineKind::Check => "Check Timeline",
        WorkerTimelineKind::SignatureVerification => "Sigverify Timeline",
        WorkerTimelineKind::Scheduler => "Scheduler Timeline",
        WorkerTimelineKind::SchedulingSummary => "Scheduling Summary",
    };
    if !worker_timeline_kind.supports_worker_filter() {
        return title.to_string();
    }
    match worker_filter {
        Some(worker_id) => format!("{title} worker={worker_id}"),
        None => format!("{title} all"),
    }
}

fn timeline_event_line(event: &TimelineEvent) -> Line<'static> {
    let detail = if event.detail.is_empty() {
        String::new()
    } else {
        format!(" {}", event.detail)
    };
    Line::from(format!(
        "{:>14} {:>22} {}{}",
        format_duration_ns(event.delta_ns),
        event.timestamp_ns,
        event.name,
        detail
    ))
}

fn active_session_line(session: &ActiveSessionSummary) -> Line<'static> {
    let end = session
        .end_timestamp_ns
        .map(|timestamp_ns| timestamp_ns.to_string())
        .unwrap_or_else(|| "open".to_string());
    let duration = session
        .duration_ns
        .map(format_duration_ns)
        .unwrap_or_else(|| "-".to_string());
    Line::from(format!(
        "{:>14} {:>22} active-session duration={} end={} txs={} pending={}",
        format_duration_ns(session.start_delta_ns),
        session.start_timestamp_ns,
        duration,
        end,
        session.transaction_count,
        session.pending_transactions
    ))
}

fn worker_timeline_event_line(event: &WorkerTimelineEvent) -> Line<'static> {
    let detail = if event.detail.is_empty() {
        String::new()
    } else {
        format!(" {}", event.detail)
    };
    Line::from(format!(
        "{:>14} {:>22} {:>6} {:>8} {}{}",
        format_duration_ns(event.delta_ns),
        event.timestamp_ns,
        event
            .worker_id
            .map(|worker_id| worker_id.to_string())
            .unwrap_or_else(|| "-".to_string()),
        event
            .transaction_index
            .map(|transaction_index| transaction_index.to_string())
            .unwrap_or_else(|| "-".to_string()),
        event.name,
        detail
    ))
}

fn render_footer(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(
            "Up/Down select  Enter/Right open  Esc/Left back  Tab pane  Home/End jump  PgUp/PgDn \
             page  m maximize  Worker pane: digits filter, a all, v view  q quit",
        )
        .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn focused_block<T>(title: T, focused: bool) -> Block<'static>
where
    T: Into<String>,
{
    let block = Block::default().title(title.into()).borders(Borders::ALL);
    if focused {
        block.border_style(Style::default().fg(Color::Cyan))
    } else {
        block
    }
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_optional_cost_units(value: Option<u64>) -> String {
    value
        .map(format_cost_units)
        .unwrap_or_else(|| "-".to_string())
}

fn format_time_per_cost_unit_ns(duration_ns: Option<u64>, cost_units: Option<u64>) -> String {
    let Some(duration_ns) = duration_ns else {
        return "-".to_string();
    };
    let Some(cost_units) = cost_units else {
        return "-".to_string();
    };
    if cost_units == 0 {
        return "-".to_string();
    }

    let ns_per_cost_unit = duration_ns as f64 / cost_units as f64;
    if ns_per_cost_unit >= 1_000.0 {
        format!("{:.1}k", ns_per_cost_unit / 1_000.0)
    } else if ns_per_cost_unit >= 100.0 {
        format!("{ns_per_cost_unit:.0}")
    } else if ns_per_cost_unit >= 10.0 {
        format!("{ns_per_cost_unit:.1}")
    } else {
        format!("{ns_per_cost_unit:.2}")
    }
}

fn format_cost_units(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_duration_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!(
            "{}.{:03}s",
            ns / 1_000_000_000,
            ns % 1_000_000_000 / 1_000_000
        )
    } else if ns >= 1_000_000 {
        format!("{}.{:03}ms", ns / 1_000_000, ns % 1_000_000 / 1_000)
    } else if ns >= 1_000 {
        format!("{}.{:03}us", ns / 1_000, ns % 1_000)
    } else {
        format!("{ns}ns")
    }
}

#[cfg(test)]
mod tests {
    use {super::*, agave_scheduling_utils::replay_events::replay_event_tags};

    #[test]
    fn sync_slots_selects_newest_slot_and_clears_transaction() {
        let mut app = App {
            selected_transaction: Some(7),
            ..App::default()
        };
        let slots = vec![slot_summary(10), slot_summary(11)];

        assert!(app.sync_slots(&slots));
        assert_eq!(app.selected_slot, Some(11));
        assert_eq!(app.selected_transaction, None);
        assert_eq!(app.slot_index, 1);
        assert_eq!(app.transaction_index, 0);
    }

    #[test]
    fn sync_transactions_preserves_selected_transaction_id() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(7),
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&[3, 7, 11]);

        assert!(!app.sync_transactions(&snapshot));
        assert_eq!(app.selected_transaction, Some(7));
        assert_eq!(app.transaction_index, 1);
    }

    #[test]
    fn snapshot_uses_aggregate_active_stats_for_unselected_slots() {
        let mut event_store = EventStore::new(4);
        event_store.apply_event(ReplayEvent::slot_begin(1, 41));
        event_store.apply_event(ReplayEvent::transaction_ingested(10, 41, 0, [1; 64]));
        event_store.apply_event(ReplayEvent::transaction_event(
            50,
            replay_event_tags::TRANSACTION_FINISHED_EXEC,
            41,
            0,
        ));
        event_store.apply_event(ReplayEvent::slot_begin(60, 42));
        event_store.apply_event(ReplayEvent::transaction_ingested(70, 42, 0, [2; 64]));
        let store = Arc::new(Mutex::new(event_store));
        let stats = ReaderStats::default();

        let snapshot = snapshot(&store, &stats, Some(42), None);

        let unselected_slot = snapshot
            .slots
            .iter()
            .find(|slot| slot.slot == 41)
            .expect("unselected slot must be present");
        assert_eq!(unselected_slot.active_duration_ns, Some(40));
        assert_eq!(unselected_slot.active_session_count, 1);
        assert_eq!(unselected_slot.active_pending_transactions, 0);
    }

    #[test]
    fn sync_transactions_replaces_missing_selection_with_current_row() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(99),
            transaction_index: 1,
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&[3, 7, 11]);

        assert!(app.sync_transactions(&snapshot));
        assert_eq!(app.selected_transaction, Some(7));
        assert_eq!(app.transaction_index, 1);
    }

    #[test]
    fn transaction_navigation_updates_selected_transaction_id() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(3),
            focus: FocusPane::Transactions,
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&[3, 7, 11]);

        app.move_down(&snapshot);
        assert_eq!(app.selected_transaction, Some(7));
        assert_eq!(app.transaction_index, 1);
    }

    #[test]
    fn page_keys_move_slot_selection_when_slots_are_focused() {
        let mut app = App {
            selected_slot: Some(0),
            selected_transaction: Some(7),
            focus: FocusPane::Slots,
            ..App::default()
        };
        let snapshot = UiSnapshot {
            received_events: 0,
            processed_events: 0,
            skipped_events: 0,
            slots: (0..15).map(slot_summary).collect(),
            selected_slot: None,
        };

        app.page_down(&snapshot);
        assert_eq!(app.slot_index, PAGE_STEP);
        assert_eq!(app.selected_slot, Some(PAGE_STEP as u64));
        assert_eq!(app.selected_transaction, None);
        assert_eq!(app.transaction_index, 0);

        app.page_up(&snapshot);
        assert_eq!(app.slot_index, 0);
        assert_eq!(app.selected_slot, Some(0));
    }

    #[test]
    fn page_keys_move_transaction_selection_when_transactions_are_focused() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(0),
            focus: FocusPane::Transactions,
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&(0..15).collect::<Vec<_>>());

        app.page_down(&snapshot);
        assert_eq!(app.transaction_index, PAGE_STEP);
        assert_eq!(app.selected_transaction, Some(PAGE_STEP as u64));

        app.page_up(&snapshot);
        assert_eq!(app.transaction_index, 0);
        assert_eq!(app.selected_transaction, Some(0));
    }

    #[test]
    fn repeated_navigation_keys_are_coalesced_per_event_batch() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(0),
            focus: FocusPane::Transactions,
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&(0..15).collect::<Vec<_>>());

        assert!(!handle_key_events(
            &mut app,
            [
                key_event(KeyCode::Down),
                key_event(KeyCode::Down),
                key_event(KeyCode::Down),
            ],
            &snapshot,
        ));
        assert_eq!(app.transaction_index, 1);
        assert_eq!(app.selected_transaction, Some(1));
    }

    #[test]
    fn different_navigation_keys_are_not_coalesced_together() {
        let mut app = App {
            selected_slot: Some(42),
            selected_transaction: Some(1),
            transaction_index: 1,
            focus: FocusPane::Transactions,
            ..App::default()
        };
        let snapshot = snapshot_with_transactions(&(0..15).collect::<Vec<_>>());

        assert!(!handle_key_events(
            &mut app,
            [key_event(KeyCode::Down), key_event(KeyCode::Up)],
            &snapshot,
        ));
        assert_eq!(app.transaction_index, 1);
        assert_eq!(app.selected_transaction, Some(1));
    }

    #[test]
    fn focus_cycle_includes_separate_timeline_panes() {
        let snapshot = snapshot_with_transactions(&[7]);
        let mut app = App::default();

        app.next_focus(&snapshot);
        assert_eq!(app.focus, FocusPane::Transactions);
        app.next_focus(&snapshot);
        assert_eq!(app.focus, FocusPane::TxTimeline);
        app.next_focus(&snapshot);
        assert_eq!(app.focus, FocusPane::WorkerTimeline);
        app.next_focus(&snapshot);
        assert_eq!(app.focus, FocusPane::Slots);

        app.previous_focus();
        assert_eq!(app.focus, FocusPane::WorkerTimeline);
    }

    #[test]
    fn maximized_pane_toggles_and_tracks_focus_changes() {
        let snapshot = snapshot_with_transactions(&[7]);
        let mut app = App {
            focus: FocusPane::Transactions,
            ..App::default()
        };

        app.toggle_maximized_pane();
        assert_eq!(app.maximized_pane, Some(FocusPane::Transactions));

        app.next_focus(&snapshot);
        assert_eq!(app.focus, FocusPane::TxTimeline);
        assert_eq!(app.maximized_pane, Some(FocusPane::TxTimeline));

        assert!(app.restore_maximized_pane());
        assert_eq!(app.maximized_pane, None);
        assert!(!app.restore_maximized_pane());

        app.toggle_maximized_pane();
        assert_eq!(app.maximized_pane, Some(FocusPane::TxTimeline));
        app.toggle_maximized_pane();
        assert_eq!(app.maximized_pane, None);
    }

    #[test]
    fn page_keys_scroll_tx_timeline_only_when_tx_timeline_is_focused() {
        let snapshot = snapshot_with_transactions(&[7]);
        let mut app = App {
            focus: FocusPane::TxTimeline,
            ..App::default()
        };

        app.page_down(&snapshot);
        assert_eq!(app.tx_timeline_scroll, PAGE_STEP as u16);
        assert_eq!(app.worker_timeline_scroll, 0);

        app.page_up(&snapshot);
        assert_eq!(app.tx_timeline_scroll, 0);
    }

    #[test]
    fn page_keys_scroll_worker_timeline_only_when_worker_timeline_is_focused() {
        let snapshot = snapshot_with_transactions(&[7]);
        let mut app = App {
            focus: FocusPane::WorkerTimeline,
            ..App::default()
        };

        app.page_down(&snapshot);
        assert_eq!(app.worker_timeline_scroll, PAGE_STEP as u16);
        assert_eq!(app.tx_timeline_scroll, 0);

        app.page_up(&snapshot);
        assert_eq!(app.worker_timeline_scroll, 0);
    }

    #[test]
    fn worker_timeline_digit_input_filters_worker() {
        let mut app = App {
            focus: FocusPane::WorkerTimeline,
            worker_timeline_scroll: 9,
            ..App::default()
        };

        assert!(app.push_worker_filter_digit('1'));
        assert!(app.push_worker_filter_digit('2'));
        assert_eq!(app.worker_filter, Some(12));
        assert_eq!(app.worker_timeline_scroll, 0);

        assert!(app.pop_worker_filter_digit());
        assert_eq!(app.worker_filter, Some(1));
        assert!(app.clear_worker_filter());
        assert_eq!(app.worker_filter, None);
    }

    #[test]
    fn worker_timeline_toggle_cycles_worker_views() {
        let mut app = App {
            focus: FocusPane::WorkerTimeline,
            worker_timeline_scroll: 9,
            ..App::default()
        };

        assert_eq!(app.worker_timeline_kind, WorkerTimelineKind::Execution);
        app.toggle_worker_timeline_kind();
        assert_eq!(app.worker_timeline_kind, WorkerTimelineKind::Check);
        assert_eq!(app.worker_timeline_scroll, 0);

        app.toggle_worker_timeline_kind();
        assert_eq!(
            app.worker_timeline_kind,
            WorkerTimelineKind::SignatureVerification
        );
        app.toggle_worker_timeline_kind();
        assert_eq!(app.worker_timeline_kind, WorkerTimelineKind::Scheduler);
        app.toggle_worker_timeline_kind();
        assert_eq!(
            app.worker_timeline_kind,
            WorkerTimelineKind::SchedulingSummary
        );
        app.toggle_worker_timeline_kind();
        assert_eq!(app.worker_timeline_kind, WorkerTimelineKind::Execution);
    }

    #[test]
    fn worker_filter_resets_when_slot_changes() {
        let snapshot = UiSnapshot {
            received_events: 0,
            processed_events: 0,
            skipped_events: 0,
            slots: (0..3).map(slot_summary).collect(),
            selected_slot: None,
        };
        let mut app = App {
            selected_slot: Some(1),
            slot_index: 1,
            worker_filter: Some(7),
            focus: FocusPane::Slots,
            ..App::default()
        };

        app.move_down(&snapshot);

        assert_eq!(app.selected_slot, Some(2));
        assert_eq!(app.worker_filter, None);
    }

    #[test]
    fn worker_timeline_lines_filter_by_worker() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.worker_events = vec![
            WorkerTimelineEvent {
                delta_ns: 0,
                timestamp_ns: 10,
                worker_id: Some(3),
                transaction_index: Some(7),
                name: "worker-three",
                detail: String::new(),
            },
            WorkerTimelineEvent {
                delta_ns: 1,
                timestamp_ns: 11,
                worker_id: Some(4),
                transaction_index: Some(8),
                name: "worker-four",
                detail: String::new(),
            },
        ];

        let rendered = rendered_lines(worker_timeline_lines(
            &snapshot,
            Some(3),
            WorkerTimelineKind::Execution,
        ));

        assert!(rendered.iter().any(|line| line.contains("worker=3")));
        assert!(rendered.iter().any(|line| line.contains("worker-three")));
        assert!(!rendered.iter().any(|line| line.contains("worker-four")));
    }

    #[test]
    fn worker_timeline_lines_can_show_signature_verification_workers() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.worker_events = vec![WorkerTimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            worker_id: Some(3),
            transaction_index: Some(7),
            name: "regular-worker",
            detail: String::new(),
        }];
        slot.signature_verification_worker_events = vec![WorkerTimelineEvent {
            delta_ns: 1,
            timestamp_ns: 11,
            worker_id: Some(4),
            transaction_index: Some(8),
            name: "sigverify-worker",
            detail: "verified=true".to_string(),
        }];

        let rendered = rendered_lines(worker_timeline_lines(
            &snapshot,
            None,
            WorkerTimelineKind::SignatureVerification,
        ));

        assert!(rendered.iter().any(|line| line.contains("mode=sigverify")));
        assert!(rendered.iter().any(|line| line.contains("sigverify-worker")));
        assert!(!rendered.iter().any(|line| line.contains("regular-worker")));
    }

    #[test]
    fn worker_timeline_lines_can_show_check_workers() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.worker_events = vec![WorkerTimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            worker_id: Some(3),
            transaction_index: Some(7),
            name: "exec-worker",
            detail: String::new(),
        }];
        slot.check_worker_events = vec![WorkerTimelineEvent {
            delta_ns: 1,
            timestamp_ns: 11,
            worker_id: Some(4),
            transaction_index: Some(8),
            name: "check-worker",
            detail: String::new(),
        }];

        let rendered = rendered_lines(worker_timeline_lines(
            &snapshot,
            None,
            WorkerTimelineKind::Check,
        ));

        assert!(rendered.iter().any(|line| line.contains("mode=check")));
        assert!(rendered.iter().any(|line| line.contains("check-worker")));
        assert!(!rendered.iter().any(|line| line.contains("exec-worker")));
    }

    #[test]
    fn worker_timeline_lines_can_show_scheduler_events() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.worker_events = vec![WorkerTimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            worker_id: Some(3),
            transaction_index: Some(7),
            name: "exec-worker",
            detail: String::new(),
        }];
        slot.scheduler_events = vec![WorkerTimelineEvent {
            delta_ns: 1,
            timestamp_ns: 11,
            worker_id: None,
            transaction_index: Some(8),
            name: "tx-signatures-submitted",
            detail: "queue_len=2".to_string(),
        }];

        let rendered = rendered_lines(worker_timeline_lines(
            &snapshot,
            Some(3),
            WorkerTimelineKind::Scheduler,
        ));

        assert!(rendered.iter().any(|line| line.contains("mode=scheduler")));
        assert!(rendered.iter().any(|line| line.contains("worker=scheduler")));
        assert!(rendered.iter().any(|line| line.contains("tx-signatures-submitted")));
        assert!(!rendered.iter().any(|line| line.contains("exec-worker")));
    }

    #[test]
    fn worker_timeline_lines_can_show_scheduling_summaries() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.scheduler_events = vec![WorkerTimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            worker_id: None,
            transaction_index: Some(7),
            name: "tx-signatures-submitted",
            detail: String::new(),
        }];
        slot.scheduling_summary_events = vec![WorkerTimelineEvent {
            delta_ns: 1,
            timestamp_ns: 11,
            worker_id: None,
            transaction_index: None,
            name: "slot-scheduling-summary",
            detail: "duration_ns=3 scanned=2 scheduled=1 conflicts=1".to_string(),
        }];

        let rendered = rendered_lines(worker_timeline_lines(
            &snapshot,
            Some(3),
            WorkerTimelineKind::SchedulingSummary,
        ));

        assert!(rendered.iter().any(|line| line.contains("mode=scheduling-summary")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("worker=scheduling-summary")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("slot-scheduling-summary")));
        assert!(!rendered
            .iter()
            .any(|line| line.contains("tx-signatures-submitted")));
    }

    #[test]
    fn timeline_lines_include_slot_events_without_transaction() {
        let mut snapshot = snapshot_with_transactions(&[]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.duration_ns = Some(20);
        slot.slot_events.push(TimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            name: "slot-begin",
            detail: String::new(),
        });
        slot.slot_events.push(TimelineEvent {
            delta_ns: 20,
            timestamp_ns: 30,
            name: "slot-complete",
            detail: String::new(),
        });

        let rendered = rendered_lines(tx_timeline_lines(&snapshot));
        assert!(
            rendered
                .iter()
                .any(|line| line.contains("slot_duration=20ns"))
        );
        assert!(rendered.iter().any(|line| line.contains("slot-begin")));
        assert!(rendered.iter().any(|line| line.contains("slot-complete")));
    }

    #[test]
    fn timeline_lines_include_slot_begin_to_ingest_delta() {
        let mut snapshot = snapshot_with_transactions(&[7]);
        snapshot
            .selected_slot
            .as_mut()
            .unwrap()
            .selected_transaction = Some(TransactionTimeline {
            slot: 42,
            index: 7,
            status: "ready",
            slot_ingest_delta_ns: Some(15),
            duration_ns: None,
            signature: "signature-7".to_string(),
            events: Vec::new(),
        });

        let rendered = rendered_lines(tx_timeline_lines(&snapshot));
        assert!(
            rendered
                .iter()
                .any(|line| line.contains("slot_begin_to_ingest=15ns"))
        );
    }

    #[test]
    fn timeline_lines_include_worker_id_for_worker_events() {
        let mut snapshot = snapshot_with_transactions(&[7]);
        snapshot
            .selected_slot
            .as_mut()
            .unwrap()
            .selected_transaction = Some(TransactionTimeline {
            slot: 42,
            index: 7,
            status: "checking",
            slot_ingest_delta_ns: None,
            duration_ns: None,
            signature: "signature-7".to_string(),
            events: vec![TimelineEvent {
                delta_ns: 10,
                timestamp_ns: 20,
                name: "tx-worker-picked-up",
                detail: timeline_event_detail(&ReplayEvent::transaction_worker_event(
                    20,
                    agave_scheduling_utils::replay_events::replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                    42,
                    7,
                    3,
                )),
            }],
        });

        let rendered = rendered_lines(tx_timeline_lines(&snapshot));
        assert!(
            rendered
                .iter()
                .any(|line| { line.contains("tx-worker-picked-up") && line.contains("worker=3") })
        );
    }

    #[test]
    fn transaction_timeline_includes_dispatch_queue_len() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                ReplayEvent::transaction_sent_for_check(20, 42, 7, 4),
                ReplayEvent::transaction_worker_event(
                    40,
                    replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                    42,
                    7,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    60,
                    replay_event_tags::TRANSACTION_CHECK_PASSED,
                    42,
                    7,
                    3,
                ),
            ],
        };

        let timeline = transaction_timeline(42, Some(0), &transaction);
        let sent_for_check = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-sent-for-check")
            .unwrap();

        assert!(sent_for_check.detail.contains("queue_len=4"));
    }

    #[test]
    fn transaction_timeline_includes_unscheduled_ready_transactions_ahead() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                ReplayEvent::transaction_scheduling_skipped(
                    20,
                    42,
                    7,
                    3,
                    replay_scheduling_skip_reasons::MULTIPLE_LOCK_CONFLICTS,
                    Some(5),
                ),
                ReplayEvent::transaction_worker_dispatch_event(
                    30,
                    replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                    42,
                    7,
                    1,
                    2,
                    4,
                ),
            ],
        };

        let timeline = transaction_timeline(42, Some(0), &transaction);
        let skipped = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-scheduling-skipped")
            .unwrap();
        let scheduled = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-scheduled-for-exec")
            .unwrap();

        assert!(skipped.detail.contains("unscheduled_ready_ahead=3"));
        assert!(
            skipped
                .detail
                .contains("skip_reason=multiple-lock-conflicts")
        );
        assert!(skipped.detail.contains("blocked_by_tx=5"));
        assert!(scheduled.detail.contains("unscheduled_ready_ahead=4"));
    }

    #[test]
    fn transaction_timeline_includes_signature_verification_queue_len() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                ReplayEvent::transaction_signatures_submitted(20, 42, 7, 4),
            ],
        };

        let timeline = transaction_timeline(42, Some(0), &transaction);
        let submitted = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-signatures-submitted")
            .unwrap();

        assert!(submitted.detail.contains("queue_len=4"));
    }

    #[test]
    fn transaction_timeline_includes_ready_releasing_transaction() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                ReplayEvent::transaction_ready_for_scheduling(20, 42, 7, 3),
            ],
        };

        let timeline = transaction_timeline(42, Some(0), &transaction);
        let ready = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-ready-for-scheduling")
            .unwrap();

        assert!(ready.detail.contains("ready_released_by_tx=3"));
    }

    #[test]
    fn transaction_summary_includes_costs_and_wait_times() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(105, 42, 7, [1; 64]),
                ReplayEvent::transaction_sent_for_check(110, 42, 7, 4),
                ReplayEvent::transaction_worker_event(
                    130,
                    replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                    42,
                    7,
                    3,
                ),
                ReplayEvent::transaction_check_passed(150, 42, 7, 3, 123),
                ReplayEvent::transaction_ready_for_scheduling(180, 42, 7, 8),
                ReplayEvent::transaction_scheduling_skipped(
                    190,
                    42,
                    7,
                    2,
                    replay_scheduling_skip_reasons::PREVIOUSLY_UNSCHEDULED_CONFLICT,
                    None,
                ),
                ReplayEvent::transaction_worker_dispatch_event(
                    220,
                    replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                    42,
                    7,
                    3,
                    5,
                    1,
                ),
                ReplayEvent::transaction_worker_event(
                    250,
                    replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                    42,
                    7,
                    3,
                ),
                ReplayEvent::transaction_worker_event(
                    330,
                    replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
                    42,
                    7,
                    3,
                ),
                ReplayEvent::transaction_execution_result(
                    340,
                    replay_event_tags::TRANSACTION_FINISHED_EXEC,
                    42,
                    7,
                    3,
                    456,
                ),
            ],
        };

        let summary = transaction_summary(Some(100), &transaction);

        assert_eq!(summary.slot_ingest_delta_ns, Some(5));
        assert_eq!(summary.estimated_cost_units, Some(123));
        assert_eq!(summary.cost_units, Some(456));
        assert_eq!(summary.check_wait_ns, Some(20));
        assert_eq!(summary.ready_wait_ns, Some(30));
        assert_eq!(summary.scheduling_wait_ns, Some(30));
        assert_eq!(summary.exec_wait_ns, Some(30));
        assert_eq!(summary.execution_duration_ns, Some(80));
    }

    #[test]
    fn cost_unit_formatter_uses_compact_suffixes() {
        assert_eq!(format_optional_cost_units(None), "-");
        assert_eq!(format_optional_cost_units(Some(999)), "999");
        assert_eq!(format_optional_cost_units(Some(1_000)), "1.0k");
        assert_eq!(format_optional_cost_units(Some(12_345)), "12.3k");
        assert_eq!(format_optional_cost_units(Some(1_000_000)), "1.0M");
        assert_eq!(format_optional_cost_units(Some(12_345_678)), "12.3M");
    }

    #[test]
    fn time_per_cost_unit_formatter_uses_execution_duration_and_actual_cost_units() {
        assert_eq!(format_time_per_cost_unit_ns(None, Some(10)), "-");
        assert_eq!(format_time_per_cost_unit_ns(Some(10), None), "-");
        assert_eq!(format_time_per_cost_unit_ns(Some(10), Some(0)), "-");
        assert_eq!(format_time_per_cost_unit_ns(Some(80), Some(456)), "0.18");
        assert_eq!(format_time_per_cost_unit_ns(Some(12_345), Some(1)), "12.3k");
    }

    #[test]
    fn slot_cost_unit_summaries_sum_transaction_costs() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(0, 42)],
            transactions: std::collections::BTreeMap::from([
                (
                    1,
                    TransactionRecord {
                        index: 1,
                        signature: None,
                        events: vec![
                            ReplayEvent::transaction_check_passed(10, 42, 1, 0, 100),
                            ReplayEvent::transaction_execution_result(
                                20,
                                replay_event_tags::TRANSACTION_FINISHED_EXEC,
                                42,
                                1,
                                0,
                                70,
                            ),
                        ],
                    },
                ),
                (
                    2,
                    TransactionRecord {
                        index: 2,
                        signature: None,
                        events: vec![
                            ReplayEvent::transaction_check_passed(30, 42, 2, 0, 200),
                            ReplayEvent::transaction_execution_result(
                                40,
                                replay_event_tags::TRANSACTION_FINISHED_EXEC,
                                42,
                                2,
                                0,
                                80,
                            ),
                        ],
                    },
                ),
            ]),
        };

        assert_eq!(slot_estimated_cost_units(&slot), Some(300));
        assert_eq!(slot_cost_units(&slot), Some(150));
    }

    #[test]
    fn active_sessions_wait_for_signature_verification_after_execution() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(0, 42)],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: None,
                    events: vec![
                        ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                        ReplayEvent::transaction_signatures_submitted(12, 42, 7, 1),
                        ReplayEvent::transaction_event(
                            30,
                            replay_event_tags::TRANSACTION_FINISHED_EXEC,
                            42,
                            7,
                        ),
                        ReplayEvent::transaction_signatures_returned(50, 42, 7, true),
                    ],
                },
            )]),
        };

        let sessions = slot_active_sessions(&slot);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].start_timestamp_ns, 10);
        assert_eq!(sessions[0].end_timestamp_ns, Some(50));
        assert_eq!(
            active_sessions_total_duration_ns(&sessions, slot_latest_timestamp_ns(&slot)),
            Some(40)
        );
    }

    #[test]
    fn active_sessions_close_at_execution_when_signature_verification_was_ready() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(0, 42)],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: None,
                    events: vec![
                        ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                        ReplayEvent::transaction_signatures_submitted(12, 42, 7, 1),
                        ReplayEvent::transaction_signatures_returned(20, 42, 7, true),
                        ReplayEvent::transaction_event(
                            50,
                            replay_event_tags::TRANSACTION_FINISHED_EXEC,
                            42,
                            7,
                        ),
                    ],
                },
            )]),
        };

        let sessions = slot_active_sessions(&slot);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].start_timestamp_ns, 10);
        assert_eq!(sessions[0].end_timestamp_ns, Some(50));
        assert_eq!(
            active_sessions_total_duration_ns(&sessions, slot_latest_timestamp_ns(&slot)),
            Some(40)
        );
    }

    #[test]
    fn active_sessions_merge_overlapping_transactions() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(0, 42)],
            transactions: std::collections::BTreeMap::from([
                (
                    7,
                    TransactionRecord {
                        index: 7,
                        signature: None,
                        events: vec![
                            ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                            ReplayEvent::transaction_event(
                                50,
                                replay_event_tags::TRANSACTION_FINISHED_EXEC,
                                42,
                                7,
                            ),
                        ],
                    },
                ),
                (
                    8,
                    TransactionRecord {
                        index: 8,
                        signature: None,
                        events: vec![
                            ReplayEvent::transaction_ingested(30, 42, 8, [2; 64]),
                            ReplayEvent::transaction_event(
                                70,
                                replay_event_tags::TRANSACTION_FINISHED_EXEC,
                                42,
                                8,
                            ),
                        ],
                    },
                ),
            ]),
        };

        let sessions = slot_active_sessions(&slot);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].start_timestamp_ns, 10);
        assert_eq!(sessions[0].end_timestamp_ns, Some(70));
        assert_eq!(sessions[0].transaction_count, 2);
    }

    #[test]
    fn transaction_table_rows_mark_first_transaction_in_active_session() {
        let selected_slot = SelectedSlot {
            slot: 42,
            status: "running",
            slot_event_count: 0,
            duration_ns: None,
            active_duration_ns: Some(60),
            active_pending_transactions: 0,
            active_sessions: vec![ActiveSessionSummary {
                start_delta_ns: 10,
                start_timestamp_ns: 10,
                end_timestamp_ns: Some(70),
                duration_ns: Some(60),
                transaction_count: 2,
                pending_transactions: 0,
            }],
            slot_events: Vec::new(),
            worker_events: Vec::new(),
            check_worker_events: Vec::new(),
            signature_verification_worker_events: Vec::new(),
            scheduler_events: Vec::new(),
            scheduling_summary_events: Vec::new(),
            transactions: vec![
                TransactionSummary {
                    index: 7,
                    status: "finished",
                    ingest_timestamp_ns: Some(10),
                    slot_ingest_delta_ns: Some(10),
                    estimated_cost_units: None,
                    cost_units: None,
                    check_wait_ns: None,
                    ready_wait_ns: None,
                    scheduling_wait_ns: None,
                    exec_wait_ns: None,
                    execution_duration_ns: None,
                    duration_ns: Some(40),
                    signature: "signature-7".to_string(),
                },
                TransactionSummary {
                    index: 8,
                    status: "finished",
                    ingest_timestamp_ns: Some(30),
                    slot_ingest_delta_ns: Some(30),
                    estimated_cost_units: None,
                    cost_units: None,
                    check_wait_ns: None,
                    ready_wait_ns: None,
                    scheduling_wait_ns: None,
                    exec_wait_ns: None,
                    execution_duration_ns: None,
                    duration_ns: Some(40),
                    signature: "signature-8".to_string(),
                },
            ],
            selected_transaction: None,
        };

        let rows = transaction_table_rows(&selected_slot);
        let fresh_start_indices = fresh_start_transaction_indices(&selected_slot);

        assert_eq!(fresh_start_indices, [7]);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn transaction_ready_wait_ignores_self_released_ready_events() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_check_passed(10, 42, 7, 3, 123),
                ReplayEvent::transaction_ready_for_scheduling(20, 42, 7, 7),
            ],
        };

        let summary = transaction_summary(Some(0), &transaction);

        assert_eq!(summary.ready_wait_ns, None);
    }

    #[test]
    fn transaction_timeline_includes_cost_units() {
        let transaction = TransactionRecord {
            index: 7,
            signature: Some("signature-7".to_string()),
            events: vec![
                ReplayEvent::transaction_ingested(10, 42, 7, [1; 64]),
                ReplayEvent::transaction_check_passed(20, 42, 7, 3, 123),
                ReplayEvent::transaction_execution_result(
                    30,
                    replay_event_tags::TRANSACTION_FINISHED_EXEC,
                    42,
                    7,
                    3,
                    456,
                ),
            ],
        };

        let timeline = transaction_timeline(42, Some(0), &transaction);
        let check_passed = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-check-passed")
            .unwrap();
        let finished_exec = timeline
            .events
            .iter()
            .find(|event| event.name == "tx-finished-exec")
            .unwrap();

        assert!(
            check_passed
                .detail
                .contains("estimated_cost_units=123")
        );
        assert!(finished_exec.detail.contains("cost_units=456"));
    }

    #[test]
    fn worker_timeline_shows_execution_worker_events() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(10, 42)],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: Some("signature-7".to_string()),
                    events: vec![
                        ReplayEvent::transaction_worker_dispatch_event(
                            20,
                            replay_event_tags::TRANSACTION_SCHEDULED_FOR_EXEC,
                            42,
                            7,
                            3,
                            1,
                            0,
                        ),
                        ReplayEvent::transaction_worker_event(
                            30,
                            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                            42,
                            7,
                            3,
                        ),
                        ReplayEvent::transaction_worker_event(
                            40,
                            replay_event_tags::TRANSACTION_WORKER_EXECUTION_COMPLETED,
                            42,
                            7,
                            3,
                        ),
                    ],
                },
            )]),
        };
        let timeline = worker_timeline(&slot, WorkerTimelineKind::Execution);

        assert_eq!(timeline.len(), 3);
        assert_eq!(timeline[0].delta_ns, 10);
        assert_eq!(timeline[0].worker_id, Some(3));
        assert_eq!(timeline[0].transaction_index, Some(7));
        assert_eq!(timeline[0].name, "tx-scheduled-for-exec");
        assert_eq!(timeline[1].name, "tx-worker-picked-up");
        assert_eq!(timeline[2].name, "tx-worker-execution-completed");
    }

    #[test]
    fn worker_timeline_shows_check_worker_events() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(10, 42)],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: Some("signature-7".to_string()),
                    events: vec![
                        ReplayEvent::transaction_worker_event(
                            20,
                            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                            42,
                            7,
                            3,
                        ),
                        ReplayEvent::transaction_worker_event(
                            30,
                            replay_event_tags::TRANSACTION_WORKER_CHECK_COMPLETED,
                            42,
                            7,
                            3,
                        ),
                    ],
                },
            )]),
        };
        let timeline = worker_timeline(&slot, WorkerTimelineKind::Check);

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].delta_ns, 10);
        assert_eq!(timeline[0].worker_id, Some(3));
        assert_eq!(timeline[0].transaction_index, Some(7));
        assert_eq!(timeline[0].name, "tx-worker-picked-up");
        assert_eq!(timeline[1].name, "tx-worker-check-completed");
    }

    #[test]
    fn worker_timeline_shows_signature_verification_worker_events() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![ReplayEvent::slot_begin(10, 42)],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: Some("signature-7".to_string()),
                    events: vec![
                        ReplayEvent::transaction_worker_event(
                            20,
                            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                            42,
                            7,
                            3,
                        ),
                        ReplayEvent::transaction_signature_verification_worker_result_event(
                            30,
                            replay_event_tags::TRANSACTION_SIGNATURE_VERIFICATION_WORKER_RESULT_SENT,
                            42,
                            7,
                            4,
                            true,
                        ),
                    ],
                },
            )]),
        };
        let timeline = worker_timeline(&slot, WorkerTimelineKind::SignatureVerification);

        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].delta_ns, 20);
        assert_eq!(timeline[0].worker_id, Some(4));
        assert_eq!(timeline[0].transaction_index, Some(7));
        assert_eq!(timeline[0].name, "tx-sigverify-worker-result-sent");
        assert_eq!(timeline[0].detail, "verified=true");
    }

    #[test]
    fn worker_timeline_shows_scheduler_emitted_events() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![
                ReplayEvent::slot_begin(10, 42),
                ReplayEvent::slot_scheduling_summary(15, 42, 18, 1, 0, 1),
            ],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: Some("signature-7".to_string()),
                    events: vec![
                        ReplayEvent::transaction_event(
                            20,
                            replay_event_tags::TRANSACTION_SIGNATURES_SUBMITTED,
                            42,
                            7,
                        ),
                        ReplayEvent::transaction_worker_event(
                            30,
                            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                            42,
                            7,
                            3,
                        ),
                        ReplayEvent::transaction_worker_event(
                            40,
                            replay_event_tags::TRANSACTION_CHECK_PASSED,
                            42,
                            7,
                            3,
                        ),
                    ],
                },
            )]),
        };
        let timeline = worker_timeline(&slot, WorkerTimelineKind::Scheduler);

        assert_eq!(timeline.len(), 3);
        assert_eq!(timeline[0].transaction_index, None);
        assert_eq!(timeline[0].name, "slot-begin");
        assert_eq!(timeline[1].transaction_index, Some(7));
        assert_eq!(timeline[1].name, "tx-signatures-submitted");
        assert_eq!(timeline[2].transaction_index, Some(7));
        assert_eq!(timeline[2].name, "tx-check-passed");
        assert!(!timeline
            .iter()
            .any(|event| event.name == "tx-worker-picked-up"));
        assert!(!timeline
            .iter()
            .any(|event| event.name == "slot-scheduling-summary"));
    }

    #[test]
    fn worker_timeline_can_show_only_scheduling_summaries() {
        let slot = store::SlotRecord {
            slot: 42,
            slot_events: vec![
                ReplayEvent::slot_begin(10, 42),
                ReplayEvent::slot_scheduling_summary(20, 42, 30, 7, 3, 4),
                ReplayEvent::slot_complete(40, 42),
            ],
            transactions: std::collections::BTreeMap::from([(
                7,
                TransactionRecord {
                    index: 7,
                    signature: Some("signature-7".to_string()),
                    events: vec![ReplayEvent::transaction_event(
                        25,
                        replay_event_tags::TRANSACTION_SCHEDULING_SKIPPED,
                        42,
                        7,
                    )],
                },
            )]),
        };
        let timeline = worker_timeline(&slot, WorkerTimelineKind::SchedulingSummary);

        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].transaction_index, None);
        assert_eq!(timeline[0].name, "slot-scheduling-summary");
        assert!(timeline[0].detail.contains("scanned=7"));
        assert!(timeline[0].detail.contains("scheduled=3"));
        assert!(timeline[0].detail.contains("conflicts=4"));
    }

    #[test]
    fn timeline_scrolls_are_bounded() {
        let mut app = App {
            tx_timeline_scroll: 100,
            worker_timeline_scroll: 100,
            ..App::default()
        };
        let mut snapshot = snapshot_with_transactions(&[7]);
        let slot = snapshot.selected_slot.as_mut().unwrap();
        slot.selected_transaction = Some(TransactionTimeline {
            slot: 42,
            index: 7,
            status: "checking",
            slot_ingest_delta_ns: None,
            duration_ns: None,
            signature: "signature-7".to_string(),
            events: vec![TimelineEvent {
                delta_ns: 0,
                timestamp_ns: 10,
                name: "tx-ingested",
                detail: String::new(),
            }],
        });
        slot.worker_events.push(WorkerTimelineEvent {
            delta_ns: 0,
            timestamp_ns: 10,
            worker_id: Some(3),
            transaction_index: Some(7),
            name: "tx-sent-for-check",
            detail: String::new(),
        });

        app.bound_timeline_scrolls(&snapshot);
        let expected_tx_scroll = tx_timeline_line_count(&snapshot)
            .saturating_sub(1)
            .try_into()
            .unwrap();
        let expected_worker_scroll = worker_timeline_line_count(
            &snapshot,
            app.worker_filter,
            app.worker_timeline_kind,
        )
        .saturating_sub(1)
        .try_into()
        .unwrap();
        assert_eq!(app.tx_timeline_scroll, expected_tx_scroll);
        assert_eq!(app.worker_timeline_scroll, expected_worker_scroll);
    }

    fn snapshot_with_transactions(indices: &[u64]) -> UiSnapshot {
        UiSnapshot {
            received_events: 0,
            processed_events: 0,
            skipped_events: 0,
            slots: vec![slot_summary(42)],
            selected_slot: Some(SelectedSlot {
                slot: 42,
                status: "running",
                slot_event_count: 0,
                duration_ns: None,
                active_duration_ns: None,
                active_pending_transactions: 0,
                active_sessions: Vec::new(),
                slot_events: Vec::new(),
                worker_events: Vec::new(),
                check_worker_events: Vec::new(),
                signature_verification_worker_events: Vec::new(),
                scheduler_events: Vec::new(),
                scheduling_summary_events: Vec::new(),
                transactions: indices
                    .iter()
                    .map(|index| TransactionSummary {
                        index: *index,
                        status: "ingested",
                        ingest_timestamp_ns: None,
                        slot_ingest_delta_ns: None,
                        estimated_cost_units: None,
                        cost_units: None,
                        check_wait_ns: None,
                        ready_wait_ns: None,
                        scheduling_wait_ns: None,
                        exec_wait_ns: None,
                        execution_duration_ns: None,
                        duration_ns: None,
                        signature: format!("signature-{index}"),
                    })
                    .collect(),
                selected_transaction: None,
            }),
        }
    }

    fn rendered_lines(lines: Vec<Line<'static>>) -> Vec<String> {
        lines
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect()
    }

    fn slot_summary(slot: u64) -> SlotSummary {
        SlotSummary {
            slot,
            transaction_count: 0,
            estimated_cost_units: None,
            cost_units: None,
            duration_ns: None,
            active_duration_ns: None,
            active_session_count: 0,
            active_pending_transactions: 0,
            status: "running",
        }
    }

    fn key_event(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }
}
