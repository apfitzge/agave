use {
    agave_feature_set::{
        FeatureSet, bls_pubkey_management_in_vote_account, create_account_allow_prefund,
        limit_instruction_accounts,
    },
    agave_reserved_account_keys::ReservedAccountKeys,
    agave_scheduler_bindings::{
        LEADER_READY, LEADER_STARTING, NOT_LEADER, ProgressMessage, scheduler_feature_flags,
    },
    solana_clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
    solana_pubkey::Pubkey,
    std::{collections::HashSet, time::Duration},
};

/// Scheduler state derived from the latest progress message.
pub(crate) struct SchedulerState {
    leader_state: u8,
    current_slot: u64,
    next_leader_slot: u64,
    remaining_cost_units: u64,
    initial_remaining_cost_units: u64,
    target_scheduled_cus: u64,
    target_bank_time_ms: u16,
    initial_bank_progress: u8,
    feature_set: FeatureSet,
    reserved_account_keys: ReservedAccountKeys,
}

impl SchedulerState {
    pub(crate) fn new() -> Self {
        Self {
            leader_state: NOT_LEADER,
            current_slot: 0,
            next_leader_slot: u64::MAX,
            remaining_cost_units: 0,
            initial_remaining_cost_units: 0,
            target_scheduled_cus: 0,
            target_bank_time_ms: 0,
            initial_bank_progress: 0,
            feature_set: FeatureSet::default(),
            reserved_account_keys: ReservedAccountKeys::default(),
        }
    }

    pub(crate) fn update(&mut self, progress: &ProgressMessage) {
        let is_first_ready_progress = progress.leader_state == LEADER_READY
            && (self.leader_state != LEADER_READY || self.current_slot != progress.current_slot);
        self.leader_state = progress.leader_state;
        self.current_slot = progress.current_slot;
        self.next_leader_slot = progress.next_leader_slot;
        self.remaining_cost_units = progress.remaining_cost_units;
        if is_first_ready_progress {
            self.initial_remaining_cost_units = progress.remaining_cost_units;
            self.target_scheduled_cus = progress.remaining_cost_units / 4;
            self.target_bank_time_ms = progress.target_bank_time_ms;
            self.initial_bank_progress = progress.current_slot_progress;
        }
        self.feature_set = feature_set_from_scheduler_features(progress.scheduler_features);
        self.reserved_account_keys = ReservedAccountKeys::default();
        self.reserved_account_keys
            .update_active_set(&self.feature_set);
        debug_assert!(
            !self.can_process_transactions() || self.should_accept_packets(),
            "a scheduler that can process transactions must accept packets"
        );
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.leader_state, LEADER_STARTING | LEADER_READY)
    }

    pub(crate) fn can_process_transactions(&self) -> bool {
        self.leader_state == LEADER_READY
    }

    pub(crate) fn should_accept_packets(&self) -> bool {
        if self.is_leader() {
            return true;
        }

        match self.leader_state {
            NOT_LEADER => {
                let slots_until_leader = self.next_leader_slot.saturating_sub(self.current_slot);
                slots_until_leader < FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET
            }
            _ => false,
        }
    }

    pub(crate) fn feature_set(&self) -> &FeatureSet {
        &self.feature_set
    }

    pub(crate) fn reserved_account_keys(&self) -> &HashSet<Pubkey> {
        &self.reserved_account_keys.active
    }

    pub(crate) fn current_slot(&self) -> u64 {
        self.current_slot
    }

    pub(crate) fn remaining_cost_units(&self) -> u64 {
        self.remaining_cost_units
    }

    pub(crate) fn initial_remaining_cost_units(&self) -> u64 {
        self.initial_remaining_cost_units
    }

    pub(crate) fn target_scheduled_cus(&self) -> u64 {
        self.target_scheduled_cus
    }

    pub(crate) fn target_bank_time_ms(&self) -> u16 {
        self.target_bank_time_ms
    }

    /// The bank time that elapsed before the scheduler first observed this leader bank.
    pub(crate) fn initial_bank_elapsed_time(&self) -> Duration {
        Duration::from_millis(
            u64::from(self.target_bank_time_ms)
                .saturating_mul(u64::from(self.initial_bank_progress))
                .saturating_div(100),
        )
    }
}

pub(crate) fn drain_progress(
    progress_messages: &mut shaq::spsc::Consumer<ProgressMessage>,
    state: &mut SchedulerState,
) -> bool {
    progress_messages.sync();

    let num_progress_messages = progress_messages.len();
    if num_progress_messages == 0 {
        return false;
    }
    for _ in 0..num_progress_messages.wrapping_sub(1) {
        // Quick ptr access, no copying of bytes since we immediately discard.
        let _ = progress_messages.try_read_ptr();
    }
    if let Some(progress_message) = progress_messages.try_read() {
        state.update(progress_message);
    } else {
        unreachable!("checked there is one remaining progress message, but failed to read it");
    }

    progress_messages.finalize();
    true
}

fn feature_set_from_scheduler_features(scheduler_features: u64) -> FeatureSet {
    let mut feature_set = FeatureSet::default();
    if scheduler_features & scheduler_feature_flags::LIMIT_INSTRUCTION_ACCOUNTS != 0 {
        feature_set.activate(&limit_instruction_accounts::id(), 0);
    }
    if scheduler_features & scheduler_feature_flags::CREATE_ACCOUNT_ALLOW_PREFUND != 0 {
        feature_set.activate(&create_account_allow_prefund::id(), 0);
    }
    if scheduler_features & scheduler_feature_flags::BLS_PUBKEY_MANAGEMENT_IN_VOTE_ACCOUNT != 0 {
        feature_set.activate(&bls_pubkey_management_in_vote_account::id(), 0);
    }

    feature_set
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress_message(
        leader_state: u8,
        current_slot: u64,
        current_slot_progress: u8,
        next_leader_slot: u64,
    ) -> ProgressMessage {
        ProgressMessage {
            leader_state,
            current_slot_progress,
            epoch: 42,
            current_slot,
            next_leader_slot,
            leader_range_end: next_leader_slot.saturating_add(3),
            remaining_cost_units: 48_000_000,
            latest_blockhash: [7; 32],
            scheduler_features: scheduler_feature_flags::NONE,
            target_bank_time_ms: 400,
        }
    }

    #[test]
    fn classifies_leader_and_packet_acceptance() {
        let mut state = SchedulerState::new();
        assert!(!state.is_leader());
        assert!(!state.can_process_transactions());
        assert!(!state.should_accept_packets());

        state.update(&progress_message(LEADER_STARTING, 100, 50, 101));
        assert!(state.is_leader());
        assert!(!state.can_process_transactions());
        assert!(state.should_accept_packets());

        state.update(&progress_message(LEADER_READY, 101, 0, 110));
        assert!(state.is_leader());
        assert!(state.can_process_transactions());
        assert!(state.should_accept_packets());

        state.update(&progress_message(NOT_LEADER, 100, 0, 101));
        assert!(!state.is_leader());
        assert!(!state.can_process_transactions());
        assert!(state.should_accept_packets());

        state.update(&progress_message(NOT_LEADER, 100, 0, 102));
        assert!(!state.should_accept_packets());
    }

    #[test]
    fn scheduler_feature_flags_build_feature_set() {
        let feature_set = feature_set_from_scheduler_features(
            scheduler_feature_flags::LIMIT_INSTRUCTION_ACCOUNTS
                | scheduler_feature_flags::CREATE_ACCOUNT_ALLOW_PREFUND
                | scheduler_feature_flags::BLS_PUBKEY_MANAGEMENT_IN_VOTE_ACCOUNT,
        );
        let snapshot = feature_set.snapshot();

        assert!(snapshot.limit_instruction_accounts);
        assert!(snapshot.create_account_allow_prefund);
        assert!(snapshot.bls_pubkey_management_in_vote_account);
    }

    #[test]
    fn retains_target_scheduled_cus_for_the_leader_bank() {
        let mut state = SchedulerState::new();
        let mut starting = progress_message(LEADER_STARTING, 100, 0, 104);
        starting.remaining_cost_units = 0;
        state.update(&starting);
        assert_eq!(state.target_scheduled_cus, 0);

        let mut first_ready = progress_message(LEADER_READY, 100, 50, 104);
        first_ready.remaining_cost_units = 40;
        state.update(&first_ready);
        assert_eq!(state.initial_remaining_cost_units(), 40);
        assert_eq!(state.target_scheduled_cus, 10);
        assert_eq!(state.target_bank_time_ms(), 400);
        assert_eq!(
            state.initial_bank_elapsed_time(),
            Duration::from_millis(200)
        );

        let mut later = progress_message(LEADER_READY, 100, 60, 104);
        later.remaining_cost_units = 4;
        later.target_bank_time_ms = 300;
        state.update(&later);
        assert_eq!(state.initial_remaining_cost_units(), 40);
        assert_eq!(state.target_scheduled_cus, 10);
        assert_eq!(state.target_bank_time_ms(), 400);
        assert_eq!(
            state.initial_bank_elapsed_time(),
            Duration::from_millis(200)
        );

        let mut next_bank = progress_message(LEADER_STARTING, 101, 0, 105);
        next_bank.remaining_cost_units = 0;
        state.update(&next_bank);
        assert_eq!(state.target_scheduled_cus, 10);

        let mut next_ready = progress_message(LEADER_READY, 101, 1, 105);
        next_ready.remaining_cost_units = 80;
        next_ready.target_bank_time_ms = 500;
        state.update(&next_ready);
        assert_eq!(state.initial_remaining_cost_units(), 80);
        assert_eq!(state.target_scheduled_cus, 20);
        assert_eq!(state.target_bank_time_ms(), 500);
        assert_eq!(state.initial_bank_elapsed_time(), Duration::from_millis(5));
    }

    #[test]
    fn drains_only_the_latest_progress_message() {
        let (mut producer, mut consumer) = shaq::spsc::pair(2).unwrap();
        let mut state = SchedulerState::new();

        let mut not_leader = progress_message(NOT_LEADER, 100, 0, 101);
        not_leader.scheduler_features =
            scheduler_feature_flags::BLS_PUBKEY_MANAGEMENT_IN_VOTE_ACCOUNT;
        producer.try_write(not_leader).unwrap();
        let mut ready = progress_message(LEADER_READY, 101, 12, 200);
        ready.scheduler_features = scheduler_feature_flags::LIMIT_INSTRUCTION_ACCOUNTS;
        producer.try_write(ready).unwrap();
        producer.commit();

        drain_progress(&mut consumer, &mut state);
        assert!(state.is_leader());
        assert!(state.can_process_transactions());
        assert!(state.feature_set.snapshot().limit_instruction_accounts);
        assert!(
            !state
                .feature_set
                .snapshot()
                .bls_pubkey_management_in_vote_account
        );
    }
}
