use {
    crate::setup::BlockVerificationStageSession,
    agave_scheduler_bindings::{
        EntryHeader, ReplayBankMessage, ReplayBlockStatusMessage, SharableTransactionRegion,
        replay_bank_message_kinds, replay_block_status_codes, replay_block_status_reasons,
        replay_to_pack_message_types,
    },
    std::{
        collections::{HashMap, HashSet},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    },
};

const IDLE_SLEEP: Duration = Duration::from_millis(1);
const INGRESS_MESSAGE_LIMIT: usize = 1024;

/// Main block verification scheduler.
pub struct BlockVerificationScheduler {
    exit: Arc<AtomicBool>,
    session: BlockVerificationStageSession,
    scheduling_states: HashMap<u64, SchedulingState>,
    scheduling_state_pool: Vec<SchedulingState>,
    aborted_slots: HashSet<u64>,
}

struct SchedulingState {
    slot: u64,
    entry_headers: Vec<EntryHeader>,
    transactions: Vec<SharableTransactionRegion>,
    in_flight_worker_messages: usize,
    aborted: bool,
}

impl SchedulingState {
    fn new(slot: u64) -> Self {
        Self {
            slot,
            entry_headers: Vec::new(),
            transactions: Vec::new(),
            in_flight_worker_messages: 0,
            aborted: false,
        }
    }

    fn reset_for_slot(&mut self, slot: u64) {
        self.slot = slot;
        self.entry_headers.clear();
        self.transactions.clear();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }

    fn clear_for_pool(&mut self) {
        self.entry_headers.clear();
        self.transactions.clear();
        self.in_flight_worker_messages = 0;
        self.aborted = false;
    }
}

impl BlockVerificationScheduler {
    pub fn new(exit: Arc<AtomicBool>, session: BlockVerificationStageSession) -> Self {
        Self {
            exit,
            session,
            scheduling_states: HashMap::new(),
            scheduling_state_pool: Vec::new(),
            aborted_slots: HashSet::new(),
        }
    }

    pub fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            if self.service_ingress_queue(INGRESS_MESSAGE_LIMIT) == 0 {
                thread::sleep(IDLE_SLEEP);
            }
        }
    }

    pub fn service_ingress_queue(&mut self, max_messages: usize) -> usize {
        if max_messages == 0 {
            return 0;
        }

        self.session.replay_to_pack.sync();

        let mut consumed = 0;
        while consumed < max_messages {
            let Some(message) = self.session.replay_to_pack.try_read().copied() else {
                break;
            };

            consumed += 1;
            match message.tag {
                replay_to_pack_message_types::BANK => {
                    // SAFETY: The replay ingress protocol is owned by Agave, and
                    // we trust Agave to set the BANK tag only when the bank
                    // payload field is active.
                    let bank_message = unsafe { message.payload.bank };
                    self.handle_bank_message(bank_message);
                }
                replay_to_pack_message_types::ENTRY_HEADER => {
                    // SAFETY: The replay ingress protocol is owned by Agave, and
                    // we trust Agave to set the ENTRY_HEADER tag only when the
                    // entry_header payload field is active.
                    let entry_header = unsafe { message.payload.entry_header };
                    consumed += self.handle_entry(entry_header);
                }
                replay_to_pack_message_types::TRANSACTION => {
                    // Entry transactions are consumed by `handle_entry` in the
                    // inner loop immediately following their entry header, so a
                    // transaction here is a malformed naked transaction.
                    panic!("transaction message without entry header");
                }
                tag => panic!("unknown replay ingress message tag: {tag}"),
            }
        }

        self.session.replay_to_pack.finalize();

        consumed
    }

    fn handle_bank_message(&mut self, message: ReplayBankMessage) {
        match message.kind {
            replay_bank_message_kinds::BEGIN => self.handle_bank_begin(message.slot),
            replay_bank_message_kinds::ABORT => self.handle_bank_abort(message.slot),
            kind => panic!("unknown replay bank message kind: {kind}"),
        }
    }

    fn handle_bank_begin(&mut self, slot: u64) {
        assert!(
            !self.aborted_slots.contains(&slot),
            "begin received for aborted slot: {slot}",
        );
        assert!(
            !self.scheduling_states.contains_key(&slot),
            "slot already has scheduling state: {slot}",
        );

        let mut state = self
            .scheduling_state_pool
            .pop()
            .unwrap_or_else(|| SchedulingState::new(slot));
        state.reset_for_slot(slot);

        let previous = self.scheduling_states.insert(slot, state);
        assert!(
            previous.is_none(),
            "slot already has scheduling state: {slot}"
        );
    }

    fn handle_bank_abort(&mut self, slot: u64) {
        let state = self
            .scheduling_states
            .get_mut(&slot)
            .expect("abort received for unknown slot");
        state.aborted = true;
        self.aborted_slots.insert(slot);

        self.try_cleanup_aborted_slot(slot);
    }

    fn handle_entry(&mut self, entry_header: EntryHeader) -> usize {
        let slot = entry_header.slot;
        if !self.is_slot_aborted(slot) {
            self.scheduling_state_mut(slot)
                .entry_headers
                .push(entry_header);
        }

        let mut consumed = 0;
        for _ in 0..entry_header.num_transactions {
            let message = self
                .session
                .replay_to_pack
                .try_read()
                .copied()
                .expect("entry header missing transaction message");
            assert_eq!(
                message.tag,
                replay_to_pack_message_types::TRANSACTION,
                "entry header followed by non-transaction message",
            );

            // SAFETY: We asserted that this message is tagged as TRANSACTION,
            // and we trust Agave to make the transaction payload active for
            // that tag.
            let transaction = unsafe { message.payload.transaction };
            self.handle_transaction(slot, transaction);
            consumed += 1;
        }

        consumed
    }

    fn handle_transaction(&mut self, slot: u64, transaction: SharableTransactionRegion) {
        if self.is_slot_aborted(slot) {
            self.free_transaction_allocation(transaction);
            return;
        }

        self.scheduling_state_mut(slot)
            .transactions
            .push(transaction);
    }

    fn is_slot_aborted(&self, slot: u64) -> bool {
        if self.aborted_slots.contains(&slot) {
            return true;
        }

        self.scheduling_states
            .get(&slot)
            .expect("replay ingress received for unknown slot")
            .aborted
    }

    fn scheduling_state_mut(&mut self, slot: u64) -> &mut SchedulingState {
        self.scheduling_states
            .get_mut(&slot)
            .expect("replay ingress received for unknown slot")
    }

    fn try_cleanup_aborted_slot(&mut self, slot: u64) -> bool {
        let Some(state) = self.scheduling_states.get(&slot) else {
            return false;
        };
        if !state.aborted || state.in_flight_worker_messages != 0 {
            return false;
        }

        let mut state = self.scheduling_states.remove(&slot).unwrap();
        self.free_scheduling_state_allocations(&mut state);
        state.clear_for_pool();
        self.scheduling_state_pool.push(state);
        self.send_replay_block_status(ReplayBlockStatusMessage {
            slot,
            status: replay_block_status_codes::ABORTED,
            reason: replay_block_status_reasons::NONE,
        });

        true
    }

    fn free_scheduling_state_allocations(&mut self, state: &mut SchedulingState) {
        for transaction in state.transactions.drain(..) {
            self.free_transaction_allocation(transaction);
        }
    }

    fn free_transaction_allocation(&mut self, transaction: SharableTransactionRegion) {
        // SAFETY: Replay transaction messages transfer ownership to the
        // scheduler. We only call this for transactions still owned by this
        // scheduler state, or for aborted-slot transactions we intentionally
        // drop instead of retaining.
        unsafe {
            self.session.allocator.free_offset(transaction.offset);
        }
    }

    fn send_replay_block_status(&mut self, message: ReplayBlockStatusMessage) {
        self.session
            .replay_block_status
            .try_write(message)
            .expect("replay block status queue full");
        self.session.replay_block_status.commit();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::setup::{
            BlockVerificationStageSessions, BlockVerificationStageSetupConfig, ReplayStageSession,
        },
        agave_scheduler_bindings::{ReplayToPackMessage, ReplayToPackMessagePayload},
    };

    fn setup_sessions() -> BlockVerificationStageSessions {
        BlockVerificationStageSessions::setup(BlockVerificationStageSetupConfig {
            allocator_size: 64 * 1024 * 1024,
            replay_to_pack_capacity: 8,
            replay_block_status_capacity: 8,
            worker_count: 1,
            pack_to_worker_capacity: 8,
            worker_to_pack_capacity: 8,
        })
        .unwrap()
    }

    fn setup_scheduler_and_replay_stage() -> (BlockVerificationScheduler, ReplayStageSession) {
        let sessions = setup_sessions();
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler = BlockVerificationScheduler::new(exit, sessions.block_verification_stage);

        (scheduler, sessions.replay_stage)
    }

    fn write_replay_messages(
        replay_stage: &mut ReplayStageSession,
        messages: impl IntoIterator<Item = ReplayToPackMessage>,
    ) {
        for message in messages {
            assert!(replay_stage.replay_to_pack.try_write(message).is_ok());
        }
        replay_stage.replay_to_pack.commit();
    }

    fn bank_message(kind: u8, slot: u64) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::BANK,
            payload: ReplayToPackMessagePayload {
                bank: ReplayBankMessage { kind, slot },
            },
        }
    }

    fn begin(slot: u64) -> ReplayToPackMessage {
        bank_message(replay_bank_message_kinds::BEGIN, slot)
    }

    fn abort(slot: u64) -> ReplayToPackMessage {
        bank_message(replay_bank_message_kinds::ABORT, slot)
    }

    fn entry(slot: u64, num_transactions: u32) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::ENTRY_HEADER,
            payload: ReplayToPackMessagePayload {
                entry_header: EntryHeader {
                    slot,
                    num_hashes: 1,
                    hash: [slot as u8; 32],
                    num_transactions,
                },
            },
        }
    }

    fn transaction(offset: usize) -> ReplayToPackMessage {
        ReplayToPackMessage {
            tag: replay_to_pack_message_types::TRANSACTION,
            payload: ReplayToPackMessagePayload {
                transaction: SharableTransactionRegion { offset, length: 0 },
            },
        }
    }

    fn allocate_transaction(
        allocator: &rts_alloc::Allocator,
        data: &[u8],
    ) -> SharableTransactionRegion {
        let ptr = allocator.allocate(data.len().try_into().unwrap()).unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        }

        SharableTransactionRegion {
            offset: unsafe { allocator.offset(ptr) },
            length: data.len().try_into().unwrap(),
        }
    }

    fn read_replay_block_status(
        replay_stage: &mut ReplayStageSession,
    ) -> Option<ReplayBlockStatusMessage> {
        replay_stage.replay_block_status.sync();
        let message = replay_stage.replay_block_status.try_read().copied();
        replay_stage.replay_block_status.finalize();

        message
    }

    #[test]
    fn run_exits_when_exit_flag_is_set() {
        let sessions = setup_sessions();
        let exit = Arc::new(AtomicBool::new(false));
        let scheduler =
            BlockVerificationScheduler::new(exit.clone(), sessions.block_verification_stage);

        let thread_hdl = thread::spawn(move || scheduler.run());
        exit.store(true, Ordering::Relaxed);

        thread_hdl.join().unwrap();
    }

    #[test]
    fn service_ingress_queue_respects_message_limit_between_items() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(1), begin(2), begin(3)]);

        assert_eq!(scheduler.service_ingress_queue(2), 2);
        assert!(scheduler.scheduling_states.contains_key(&1));
        assert!(scheduler.scheduling_states.contains_key(&2));
        assert!(!scheduler.scheduling_states.contains_key(&3));

        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert!(scheduler.scheduling_states.contains_key(&3));
    }

    #[test]
    fn service_ingress_queue_finishes_entry_over_message_limit() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 2),
                transaction(11),
                transaction(12),
                begin(43),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(2), 4);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert_eq!(state.entry_headers.len(), 1);
        assert_eq!(state.transactions.len(), 2);
        assert_eq!(state.transactions[0].offset, 11);
        assert_eq!(state.transactions[1].offset, 12);
        assert!(!scheduler.scheduling_states.contains_key(&43));

        assert_eq!(scheduler.service_ingress_queue(1), 1);
        assert!(scheduler.scheduling_states.contains_key(&43));
    }

    #[test]
    fn service_ingress_queue_routes_entries_by_slot() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(
            &mut replay_stage,
            [
                begin(1),
                begin(2),
                entry(2, 1),
                transaction(21),
                entry(1, 0),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(5), 5);
        let state_1 = scheduler.scheduling_states.get(&1).unwrap();
        let state_2 = scheduler.scheduling_states.get(&2).unwrap();
        assert_eq!(state_1.entry_headers.len(), 1);
        assert!(state_1.transactions.is_empty());
        assert_eq!(state_2.entry_headers.len(), 1);
        assert_eq!(state_2.transactions.len(), 1);
        assert_eq!(state_2.transactions[0].offset, 21);
    }

    #[test]
    #[should_panic(expected = "entry header followed by non-transaction message")]
    fn service_ingress_queue_panics_if_entry_is_not_followed_by_transactions() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), entry(42, 1), begin(43)]);

        scheduler.service_ingress_queue(3);
    }

    #[test]
    fn abort_without_in_flight_work_cleans_up_and_reports_status() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        let transaction = allocate_transaction(&replay_stage.allocator, &[1, 2, 3, 4]);
        write_replay_messages(
            &mut replay_stage,
            [
                begin(42),
                entry(42, 1),
                ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload { transaction },
                },
                abort(42),
            ],
        );

        assert_eq!(scheduler.service_ingress_queue(4), 4);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert!(scheduler.scheduling_state_pool[0].entry_headers.is_empty());
        assert!(scheduler.scheduling_state_pool[0].transactions.is_empty());
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::ABORTED,
                reason: replay_block_status_reasons::NONE,
            }),
        );

        let dropped_transaction = allocate_transaction(&replay_stage.allocator, &[5, 6, 7]);
        write_replay_messages(
            &mut replay_stage,
            [
                entry(42, 1),
                ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload {
                        transaction: dropped_transaction,
                    },
                },
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(2), 2);
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert_eq!(read_replay_block_status(&mut replay_stage), None);
    }

    #[test]
    fn abort_with_in_flight_work_waits_for_cleanup() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        scheduler
            .scheduling_states
            .get_mut(&42)
            .unwrap()
            .in_flight_worker_messages = 1;

        write_replay_messages(&mut replay_stage, [abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.aborted);
        assert_eq!(state.in_flight_worker_messages, 1);
        assert!(scheduler.scheduling_state_pool.is_empty());
        assert_eq!(read_replay_block_status(&mut replay_stage), None);

        let dropped_transaction = allocate_transaction(&replay_stage.allocator, &[9, 10, 11]);
        write_replay_messages(
            &mut replay_stage,
            [
                entry(42, 1),
                ReplayToPackMessage {
                    tag: replay_to_pack_message_types::TRANSACTION,
                    payload: ReplayToPackMessagePayload {
                        transaction: dropped_transaction,
                    },
                },
            ],
        );
        assert_eq!(scheduler.service_ingress_queue(2), 2);
        let state = scheduler.scheduling_states.get(&42).unwrap();
        assert!(state.entry_headers.is_empty());
        assert!(state.transactions.is_empty());

        scheduler
            .scheduling_states
            .get_mut(&42)
            .unwrap()
            .in_flight_worker_messages = 0;
        assert!(scheduler.try_cleanup_aborted_slot(42));
        assert!(!scheduler.scheduling_states.contains_key(&42));
        assert_eq!(scheduler.scheduling_state_pool.len(), 1);
        assert_eq!(
            read_replay_block_status(&mut replay_stage),
            Some(ReplayBlockStatusMessage {
                slot: 42,
                status: replay_block_status_codes::ABORTED,
                reason: replay_block_status_reasons::NONE,
            }),
        );
    }

    #[test]
    fn cleanup_returns_state_to_pool_for_reuse() {
        let (mut scheduler, mut replay_stage) = setup_scheduler_and_replay_stage();
        write_replay_messages(&mut replay_stage, [begin(42), entry(42, 0), abort(42)]);
        assert_eq!(scheduler.service_ingress_queue(3), 3);

        let pooled_entry_capacity = scheduler.scheduling_state_pool[0].entry_headers.capacity();
        assert!(pooled_entry_capacity > 0);

        write_replay_messages(&mut replay_stage, [begin(43)]);
        assert_eq!(scheduler.service_ingress_queue(1), 1);

        assert!(scheduler.scheduling_state_pool.is_empty());
        let state = scheduler.scheduling_states.get(&43).unwrap();
        assert_eq!(state.slot, 43);
        assert!(state.entry_headers.is_empty());
        assert!(state.transactions.is_empty());
        assert!(state.entry_headers.capacity() >= pooled_entry_capacity);
    }
}
