use {
    agave_feature_set::{
        FeatureSet, bls_pubkey_management_in_vote_account, create_account_allow_prefund,
        limit_instruction_accounts,
    },
    agave_scheduler_bindings::{
        LEADER_READY, LEADER_STARTING, NOT_LEADER, ProgressMessage, scheduler_feature_flags,
    },
    solana_clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
};

/// Scheduler state derived from the latest progress message.
pub(crate) struct SchedulerState {
    leader_state: u8,
    current_slot: u64,
    next_leader_slot: u64,
    feature_set: FeatureSet,
}

impl SchedulerState {
    pub(crate) fn new() -> Self {
        Self {
            leader_state: NOT_LEADER,
            current_slot: 0,
            next_leader_slot: u64::MAX,
            feature_set: FeatureSet::default(),
        }
    }

    pub(crate) fn update(&mut self, progress: &ProgressMessage) {
        self.leader_state = progress.leader_state;
        self.current_slot = progress.current_slot;
        self.next_leader_slot = progress.next_leader_slot;
        self.feature_set = feature_set_from_scheduler_features(progress.scheduler_features);
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
}

pub(crate) fn drain_progress(
    progress_messages: &mut shaq::spsc::Consumer<ProgressMessage>,
    state: &mut SchedulerState,
) {
    progress_messages.sync();

    let num_progress_messages = progress_messages.len();
    if num_progress_messages == 0 {
        return;
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
