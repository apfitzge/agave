mod store;

use {
    agave_scheduling_utils::{
        replay_events::{REPLAY_EVENTS_IPC_FILE, ReplayEvent},
        shared_memory,
    },
    crossterm::{
        event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
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
const DEFAULT_POLL_MS: u64 = 10;
const DEFAULT_UI_TICK_MS: u64 = 100;
const PAGE_STEP: usize = 10;

struct Args {
    ledger_path: PathBuf,
    retained_slots: usize,
    poll_interval: Duration,
}

#[derive(Default)]
struct ReaderStats {
    received_events: AtomicU64,
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
    focus: FocusPane,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FocusPane {
    #[default]
    Slots,
    Transactions,
    TxTimeline,
    WorkerTimeline,
}

struct UiSnapshot {
    received_events: u64,
    skipped_events: u64,
    slots: Vec<SlotSummary>,
    selected_slot: Option<SelectedSlot>,
}

struct SlotSummary {
    slot: u64,
    transaction_count: usize,
    duration_ns: Option<u64>,
    status: &'static str,
}

struct SelectedSlot {
    slot: u64,
    status: &'static str,
    slot_event_count: usize,
    duration_ns: Option<u64>,
    slot_events: Vec<TimelineEvent>,
    worker_events: Vec<WorkerTimelineEvent>,
    transactions: Vec<TransactionSummary>,
    selected_transaction: Option<TransactionTimeline>,
}

struct TransactionSummary {
    index: u64,
    status: &'static str,
    slot_ingest_delta_ns: Option<u64>,
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

struct WorkerTimelineEvent {
    delta_ns: u64,
    timestamp_ns: u64,
    worker_id: u64,
    transaction_index: u64,
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
    let exit = Arc::new(AtomicBool::new(false));
    let reader = spawn_reader(
        move || consumer.try_read(Ordering::Relaxed),
        Arc::clone(&store),
        Arc::clone(&stats),
        Arc::clone(&exit),
        args.poll_interval,
    );

    let tui_result = run_tui(&store, &stats);

    exit.store(true, Ordering::Relaxed);
    reader
        .join()
        .map_err(|_| "replay event reader thread panicked")?;
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
    store: Arc<Mutex<EventStore>>,
    stats: Arc<ReaderStats>,
    exit: Arc<AtomicBool>,
    poll_interval: Duration,
) -> JoinHandle<()>
where
    F: FnMut() -> Result<Option<ReplayEvent>, usize> + Send + 'static,
{
    thread::spawn(move || {
        while !exit.load(Ordering::Relaxed) {
            let mut read_any = false;
            loop {
                if exit.load(Ordering::Relaxed) {
                    return;
                }

                match read_next() {
                    Ok(Some(event)) => {
                        read_any = true;
                        stats.received_events.fetch_add(1, Ordering::Relaxed);
                        store.lock().unwrap().apply_event(event);
                    }
                    Ok(None) => break,
                    Err(skipped) => {
                        read_any = true;
                        let skipped = u64::try_from(skipped).unwrap_or(u64::MAX);
                        stats.skipped_events.fetch_add(skipped, Ordering::Relaxed);
                    }
                }
            }

            if !read_any {
                thread::sleep(poll_interval);
            }
        }
    })
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

        terminal.draw(|frame| draw_ui(frame, &app, &ui_snapshot))?;

        if event::poll(Duration::from_millis(DEFAULT_UI_TICK_MS))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind == KeyEventKind::Press && handle_key(&mut app, key, &ui_snapshot) {
                break;
            }
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
    let slots = store
        .slot_ids()
        .into_iter()
        .filter_map(|slot| {
            let slot_record = store.slot(slot)?;
            Some(SlotSummary {
                slot,
                transaction_count: slot_record.transactions.len(),
                duration_ns: slot_record.duration_ns(),
                status: slot_record.status(),
            })
        })
        .collect::<Vec<_>>();

    let selected_slot = selected_slot
        .and_then(|slot| store.slot(slot))
        .map(|slot_record| {
            let transactions = slot_record.transactions_by_ingest();
            let slot_begin_timestamp_ns = slot_record.begin_timestamp_ns();
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
                slot_events: slot_timeline(slot_record),
                worker_events: worker_timeline(slot_record),
                transactions,
                selected_transaction,
            }
        });

    UiSnapshot {
        received_events: stats.received_events.load(Ordering::Relaxed),
        skipped_events: stats.skipped_events.load(Ordering::Relaxed),
        slots,
        selected_slot,
    }
}

fn transaction_summary(
    slot_begin_timestamp_ns: Option<u64>,
    transaction: &TransactionRecord,
) -> TransactionSummary {
    TransactionSummary {
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
    }
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

fn worker_timeline(slot: &store::SlotRecord) -> Vec<WorkerTimelineEvent> {
    let base_timestamp_ns = slot
        .begin_timestamp_ns()
        .or_else(|| {
            slot.transactions
                .values()
                .flat_map(|transaction| transaction.events.iter())
                .filter(|event| event.worker_id().is_some())
                .map(|event| event.timestamp_ns)
                .min()
        })
        .unwrap_or_default();
    let mut events = slot
        .transactions
        .values()
        .flat_map(|transaction| {
            transaction.events.iter().filter_map(move |event| {
                let worker_id = event.worker_id()?;
                let transaction_index = event.transaction_index()?;
                Some(WorkerTimelineEvent {
                    delta_ns: event.timestamp_ns.saturating_sub(base_timestamp_ns),
                    timestamp_ns: event.timestamp_ns,
                    worker_id,
                    transaction_index,
                    name: event_name(event.tag),
                    detail: worker_timeline_event_detail(event),
                })
            })
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| (event.timestamp_ns, event.worker_id, event.transaction_index));
    events
}

fn timeline_event_detail(event: &ReplayEvent) -> String {
    let mut details = Vec::new();
    if let Some(worker_id) = event.worker_id() {
        details.push(format!("worker={worker_id}"));
    }
    if let Some(worker_queue_len) = event.worker_queue_len() {
        details.push(format!("queue_len={worker_queue_len}"));
    }
    if let Some(verified) = event.signature_verification_result() {
        details.push(format!("verified={verified}"));
    }
    if let Some(reason) = event.slot_failure_reason() {
        details.push(format!("reason={reason}"));
    }
    details.join(" ")
}

fn worker_timeline_event_detail(event: &ReplayEvent) -> String {
    let mut details = Vec::new();
    if let Some(worker_queue_len) = event.worker_queue_len() {
        details.push(format!("queue_len={worker_queue_len}"));
    }
    if let Some(reason) = event.slot_failure_reason() {
        details.push(format!("reason={reason}"));
    }
    details.join(" ")
}

fn handle_key(app: &mut App, key: KeyEvent, snapshot: &UiSnapshot) -> bool {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('q') => return true,
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

impl App {
    fn sync_slots(&mut self, slots: &[SlotSummary]) -> bool {
        if slots.is_empty() {
            let changed =
                self.selected_slot.take().is_some() || self.selected_transaction.take().is_some();
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
        self.selected_slot = Some(slots[self.slot_index].slot);
        self.selected_transaction = None;
        self.transaction_index = 0;
        self.tx_timeline_scroll = 0;
        self.worker_timeline_scroll = 0;
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
        self.focus = match self.focus {
            FocusPane::Slots => FocusPane::Slots,
            FocusPane::Transactions => FocusPane::Slots,
            FocusPane::TxTimeline => FocusPane::Transactions,
            FocusPane::WorkerTimeline => FocusPane::TxTimeline,
        };
    }

    fn move_forward(&mut self, snapshot: &UiSnapshot) {
        self.focus = match self.focus {
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
    }

    fn next_focus(&mut self, _snapshot: &UiSnapshot) {
        self.focus = match self.focus {
            FocusPane::Slots => FocusPane::Transactions,
            FocusPane::Transactions => FocusPane::TxTimeline,
            FocusPane::TxTimeline => FocusPane::WorkerTimeline,
            FocusPane::WorkerTimeline => FocusPane::Slots,
        };
    }

    fn previous_focus(&mut self) {
        self.focus = match self.focus {
            FocusPane::Slots => FocusPane::WorkerTimeline,
            FocusPane::Transactions => FocusPane::Slots,
            FocusPane::TxTimeline => FocusPane::Transactions,
            FocusPane::WorkerTimeline => FocusPane::TxTimeline,
        };
    }

    fn move_home(&mut self, snapshot: &UiSnapshot) {
        match self.focus {
            FocusPane::Slots => {
                self.slot_index = 0;
                self.selected_slot = snapshot.slots.first().map(|slot| slot.slot);
                self.selected_transaction = None;
                self.transaction_index = 0;
                self.tx_timeline_scroll = 0;
                self.worker_timeline_scroll = 0;
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
                self.selected_slot = snapshot.slots.get(self.slot_index).map(|slot| slot.slot);
                self.selected_transaction = None;
                self.transaction_index = 0;
                self.tx_timeline_scroll = 0;
                self.worker_timeline_scroll = 0;
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
                self.worker_timeline_scroll = worker_timeline_line_count(snapshot)
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
                self.selected_slot = snapshot.slots.get(self.slot_index).map(|slot| slot.slot);
                self.selected_transaction = None;
                self.transaction_index = 0;
                self.tx_timeline_scroll = 0;
                self.worker_timeline_scroll = 0;
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
                    self.selected_slot = snapshot.slots.get(self.slot_index).map(|slot| slot.slot);
                    self.selected_transaction = None;
                    self.transaction_index = 0;
                    self.tx_timeline_scroll = 0;
                    self.worker_timeline_scroll = 0;
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
                self.selected_slot = snapshot.slots.get(self.slot_index).map(|slot| slot.slot);
                self.selected_transaction = None;
                self.transaction_index = 0;
                self.tx_timeline_scroll = 0;
                self.worker_timeline_scroll = 0;
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
                    self.selected_slot = snapshot.slots.get(self.slot_index).map(|slot| slot.slot);
                    self.selected_transaction = None;
                    self.transaction_index = 0;
                    self.tx_timeline_scroll = 0;
                    self.worker_timeline_scroll = 0;
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
            worker_timeline_line_count(snapshot),
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
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
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
    render_slots(frame, body[0], app, snapshot);
    render_transactions(frame, details[0], app, snapshot);
    render_tx_timeline(frame, timelines[0], app, snapshot);
    render_worker_timeline(frame, timelines[1], app, snapshot);
    render_footer(frame, layout[2]);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    let selected_slot = app
        .selected_slot
        .map(|slot| slot.to_string())
        .unwrap_or_else(|| "-".to_string());
    let line = Line::from(vec![
        Span::styled(
            "Replay events",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  received={} skipped={} retained={} selected_slot={} focus={}",
            snapshot.received_events,
            snapshot.skipped_events,
            snapshot.slots.len(),
            selected_slot,
            app.focus.name()
        )),
    ]);
    frame.render_widget(
        Paragraph::new(line).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_slots(frame: &mut Frame<'_>, area: Rect, app: &App, snapshot: &UiSnapshot) {
    let rows = if snapshot.slots.is_empty() {
        vec![Row::new(["waiting", "", "", ""])]
    } else {
        snapshot
            .slots
            .iter()
            .map(|slot| {
                Row::new([
                    Cell::from(slot.slot.to_string()),
                    Cell::from(slot.transaction_count.to_string()),
                    Cell::from(
                        slot.duration_ns
                            .map(format_duration_ns)
                            .unwrap_or_else(|| "-".to_string()),
                    ),
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
            Constraint::Length(12),
            Constraint::Min(8),
        ],
    )
    .header(
        Row::new(["slot", "txs", "block", "status"]).style(
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
    let rows = if let Some(slot) = &snapshot.selected_slot {
        if slot.transactions.is_empty() {
            vec![Row::new([
                "no txs".to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ])]
        } else {
            slot.transactions
                .iter()
                .map(|transaction| {
                    Row::new([
                        transaction.index.to_string(),
                        transaction.status.to_string(),
                        transaction
                            .slot_ingest_delta_ns
                            .map(format_duration_ns)
                            .unwrap_or_else(|| "-".to_string()),
                        transaction
                            .duration_ns
                            .map(format_duration_ns)
                            .unwrap_or_else(|| "-".to_string()),
                        short_signature(&transaction.signature),
                    ])
                })
                .collect()
        }
    } else {
        vec![Row::new([
            "no slot".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ])]
    };

    let title = snapshot
        .selected_slot
        .as_ref()
        .map(|slot| {
            format!(
                "Transactions slot={} status={} slot_events={}",
                slot.slot, slot.status, slot.slot_event_count
            )
        })
        .unwrap_or_else(|| "Transactions".to_string());
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Min(12),
        ],
    )
    .header(
        Row::new(["index", "status", "ingest", "total", "signature"]).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(focused_block(title, app.focus == FocusPane::Transactions))
    .row_highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan));
    let mut state = TableState::default();
    if snapshot
        .selected_slot
        .as_ref()
        .is_some_and(|slot| !slot.transactions.is_empty())
    {
        state.select(Some(app.transaction_index));
    }
    frame.render_stateful_widget(table, area, &mut state);
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
        Paragraph::new(worker_timeline_lines(snapshot))
            .block(focused_block(
                "Worker Timeline",
                app.focus == FocusPane::WorkerTimeline,
            ))
            .scroll((app.worker_timeline_scroll, 0)),
        area,
    );
}

fn tx_timeline_line_count(snapshot: &UiSnapshot) -> usize {
    tx_timeline_lines(snapshot).len()
}

fn worker_timeline_line_count(snapshot: &UiSnapshot) -> usize {
    worker_timeline_lines(snapshot).len()
}

fn tx_timeline_lines(snapshot: &UiSnapshot) -> Vec<Line<'static>> {
    let Some(slot) = &snapshot.selected_slot else {
        return vec![Line::from("waiting for replay events")];
    };

    let mut lines = vec![
        Line::from(format!(
            "slot={} status={} slot_duration={} transactions={}",
            slot.slot,
            slot.status,
            slot.duration_ns
                .map(format_duration_ns)
                .unwrap_or_else(|| "-".to_string()),
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

fn worker_timeline_lines(snapshot: &UiSnapshot) -> Vec<Line<'static>> {
    let Some(slot) = &snapshot.selected_slot else {
        return vec![Line::from("waiting for replay events")];
    };

    let mut lines = vec![Line::from(format!(
        "slot={} status={} slot_duration={} transactions={}",
        slot.slot,
        slot.status,
        slot.duration_ns
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string()),
        slot.transactions.len()
    ))];
    lines.push(Line::from(""));
    lines.push(Line::from(format!(
        "{:>14} {:>22} {:>6} {:>8} worker event",
        "delta", "timestamp_ns", "worker", "tx"
    )));
    if slot.worker_events.is_empty() {
        lines.push(Line::from("no worker events recorded for selected slot"));
    } else {
        lines.extend(slot.worker_events.iter().map(worker_timeline_event_line));
    }

    lines
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
        event.worker_id,
        event.transaction_index,
        event.name,
        detail
    ))
}

fn render_footer(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(
            "Up/Down select  Enter/Right open  Esc/Left back  Tab pane  Home/End jump  PgUp/PgDn \
             page  q quit",
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

fn short_signature(signature: &str) -> String {
    if signature == "<signature-pending>" || signature.len() <= 20 {
        signature.to_string()
    } else {
        format!("{}..{}", &signature[..8], &signature[signature.len() - 8..])
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

        let rendered = tx_timeline_lines(&snapshot)
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
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

        let rendered = tx_timeline_lines(&snapshot)
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
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

        let rendered = tx_timeline_lines(&snapshot)
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
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
                ReplayEvent::transaction_worker_dispatch_event(
                    20,
                    replay_event_tags::TRANSACTION_SENT_FOR_CHECK,
                    42,
                    7,
                    3,
                    4,
                ),
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

        assert!(sent_for_check.detail.contains("worker=3"));
        assert!(sent_for_check.detail.contains("queue_len=4"));
    }

    #[test]
    fn worker_timeline_shows_worker_transaction_and_queue_len() {
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
                            replay_event_tags::TRANSACTION_SENT_FOR_CHECK,
                            42,
                            7,
                            3,
                            4,
                        ),
                        ReplayEvent::transaction_worker_event(
                            30,
                            replay_event_tags::TRANSACTION_WORKER_PICKED_UP,
                            42,
                            7,
                            3,
                        ),
                    ],
                },
            )]),
        };
        let timeline = worker_timeline(&slot);

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].delta_ns, 10);
        assert_eq!(timeline[0].worker_id, 3);
        assert_eq!(timeline[0].transaction_index, 7);
        assert_eq!(timeline[0].name, "tx-sent-for-check");
        assert_eq!(timeline[0].detail, "queue_len=4");
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
            worker_id: 3,
            transaction_index: 7,
            name: "tx-sent-for-check",
            detail: String::new(),
        });

        app.bound_timeline_scrolls(&snapshot);
        let expected_tx_scroll = tx_timeline_line_count(&snapshot)
            .saturating_sub(1)
            .try_into()
            .unwrap();
        let expected_worker_scroll = worker_timeline_line_count(&snapshot)
            .saturating_sub(1)
            .try_into()
            .unwrap();
        assert_eq!(app.tx_timeline_scroll, expected_tx_scroll);
        assert_eq!(app.worker_timeline_scroll, expected_worker_scroll);
    }

    fn snapshot_with_transactions(indices: &[u64]) -> UiSnapshot {
        UiSnapshot {
            received_events: 0,
            skipped_events: 0,
            slots: vec![slot_summary(42)],
            selected_slot: Some(SelectedSlot {
                slot: 42,
                status: "running",
                slot_event_count: 0,
                duration_ns: None,
                slot_events: Vec::new(),
                worker_events: Vec::new(),
                transactions: indices
                    .iter()
                    .map(|index| TransactionSummary {
                        index: *index,
                        status: "ingested",
                        slot_ingest_delta_ns: None,
                        duration_ns: None,
                        signature: format!("signature-{index}"),
                    })
                    .collect(),
                selected_transaction: None,
            }),
        }
    }

    fn slot_summary(slot: u64) -> SlotSummary {
        SlotSummary {
            slot,
            transaction_count: 0,
            duration_ns: None,
            status: "running",
        }
    }
}
