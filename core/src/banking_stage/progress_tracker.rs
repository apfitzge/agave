//! Service to send progress updates to the external scheduler.
//!

use {
    crate::banking_stage::consume_worker::ConsumeWorkerMetrics,
    agave_feature_set::FeatureSet,
    agave_scheduler_bindings::{ProgressMessage, scheduler_feature_flags},
    solana_clock::{Epoch, Slot},
    solana_cost_model::cost_tracker::SharedBlockCost,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
    },
};

/// Spawns a thread to track and send progress updates.
pub fn spawn(
    exit: Arc<AtomicBool>,
    mut producer: shaq::spsc::Producer<ProgressMessage>,
    shared_leader_state: SharedLeaderState,
    sharable_banks: SharableBanks,
    worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
    ticks_per_slot: u64,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("solProgTrker".to_string())
        .spawn(move || {
            ProgressTracker::new(
                exit,
                shared_leader_state,
                sharable_banks,
                worker_metrics,
                ticks_per_slot,
            )
            .run(&mut producer);
        })
        .unwrap()
}

struct ProgressTracker {
    exit: Arc<AtomicBool>,
    shared_leader_state: SharedLeaderState,
    sharable_banks: SharableBanks,
    worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
    ticks_per_slot: u64,

    last_observed_leader_slot: Option<Slot>,
    limit_and_shared_block_cost: Option<(u64, SharedBlockCost)>,
    last_observed_scheduler_features_epoch: Option<Epoch>,
    scheduler_features: u64,
}

impl ProgressTracker {
    fn new(
        exit: Arc<AtomicBool>,
        shared_leader_state: SharedLeaderState,
        sharable_banks: SharableBanks,
        worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
        ticks_per_slot: u64,
    ) -> Self {
        Self {
            exit,
            shared_leader_state,
            sharable_banks,
            worker_metrics,
            ticks_per_slot,

            last_observed_leader_slot: None,
            limit_and_shared_block_cost: None,
            last_observed_scheduler_features_epoch: None,
            scheduler_features: scheduler_feature_flags::NONE,
        }
    }

    fn run(mut self, producer: &mut shaq::spsc::Producer<ProgressMessage>) {
        let mut last_published_tick_height = u64::MAX;
        while !self.exit.load(Ordering::Relaxed) {
            let (message, tick_height) = self.produce_progress_message();
            if tick_height != last_published_tick_height {
                last_published_tick_height = tick_height;
                if !self.publish(producer, message) {
                    break; // external scheduler is so far behind we could not publish a message.
                }
            }

            self.worker_metrics
                .iter()
                .for_each(|metrics| metrics.maybe_report_and_reset());

            // Yield to other threads. Sleeping isn't that accurate and we want to avoid
            // missing updates and delaying progress messages to the external.
            std::thread::yield_now();
        }
    }

    /// returns true if a message was published
    fn publish(
        &mut self,
        producer: &mut shaq::spsc::Producer<ProgressMessage>,
        message: ProgressMessage,
    ) -> bool {
        producer.sync();
        if producer.try_write(message).is_ok() {
            producer.commit();
            true
        } else {
            false
        }
    }

    /// Gets current progress and formats into expected message type.
    /// Returns the tick height to avoid publishing the same message multiple times.
    fn produce_progress_message(&mut self) -> (ProgressMessage, u64) {
        let leader_state = self.shared_leader_state.load();
        let tick_height = leader_state.tick_height();
        let leader_bank = leader_state.working_bank();
        let scheduler_features = self.scheduler_features(leader_bank);
        let (next_leader_range_start, next_leader_range_end) = leader_state
            .next_leader_slot_range()
            .unwrap_or((u64::MAX, u64::MAX));
        let progress_message = if let Some(working_bank) = leader_bank {
            // If new leader slot grab the cost tracker lock to get limit and shared cost.
            // This avoid needing to lock except on new leader slots.
            if self.last_observed_leader_slot != Some(working_bank.slot()) {
                let cost_tracker = working_bank.read_cost_tracker().unwrap();
                self.limit_and_shared_block_cost = Some((
                    cost_tracker.get_block_limit(),
                    cost_tracker.shared_block_cost(),
                ));
                self.last_observed_leader_slot = Some(working_bank.slot());
            }

            ProgressMessage {
                leader_state: agave_scheduler_bindings::LEADER_READY,
                current_slot_progress: progress(
                    working_bank.slot(),
                    tick_height,
                    self.ticks_per_slot,
                ),
                epoch: working_bank.epoch(),
                current_slot: working_bank.slot(),
                next_leader_slot: next_leader_range_start,
                leader_range_end: next_leader_range_end,
                remaining_cost_units: self.remaining_block_cost(),
                latest_blockhash: working_bank.last_blockhash().to_bytes(),
                scheduler_features,
                target_bank_time_ms: target_bank_time_ms(working_bank.ns_per_slot),
            }
        } else {
            let current_slot = slot_from_tick_height(tick_height, self.ticks_per_slot);

            // No bank yet but we may already be inside our leader window.
            let leader_state =
                if (next_leader_range_start..=next_leader_range_end).contains(&current_slot) {
                    agave_scheduler_bindings::LEADER_STARTING
                } else {
                    agave_scheduler_bindings::NOT_LEADER
                };

            ProgressMessage {
                leader_state,
                current_slot_progress: progress(current_slot, tick_height, self.ticks_per_slot),
                epoch: 0,
                current_slot,
                next_leader_slot: next_leader_range_start,
                leader_range_end: next_leader_range_end,
                remaining_cost_units: 0,
                latest_blockhash: [0; 32],
                scheduler_features,
                target_bank_time_ms: 0,
            }
        };

        (progress_message, tick_height)
    }

    /// If leader get the remaining block cost. Otherwise 0.
    fn remaining_block_cost(&self) -> u64 {
        self.limit_and_shared_block_cost
            .as_ref()
            .map(|(limit, shared_block_cost)| limit.saturating_sub(shared_block_cost.load()))
            .unwrap_or(0)
    }

    fn scheduler_features(&mut self, leader_bank: Option<&Arc<Bank>>) -> u64 {
        match leader_bank {
            Some(bank) => self.update_scheduler_features(bank.epoch(), &bank.feature_set),
            None => {
                let bank = self.sharable_banks.working();
                self.update_scheduler_features(bank.epoch(), &bank.feature_set)
            }
        }
    }

    fn update_scheduler_features(&mut self, epoch: Epoch, feature_set: &FeatureSet) -> u64 {
        if self.last_observed_scheduler_features_epoch != Some(epoch) {
            self.last_observed_scheduler_features_epoch = Some(epoch);
            self.scheduler_features = scheduler_features_from_feature_set(feature_set);
        }
        self.scheduler_features
    }
}

fn target_bank_time_ms(ns_per_slot: u128) -> u16 {
    let milliseconds = ns_per_slot.wrapping_div(1_000_000);
    u16::try_from(milliseconds).unwrap_or(u16::MAX)
}

fn scheduler_features_from_feature_set(feature_set: &FeatureSet) -> u64 {
    // DEVELOPER NOTE:
    //  When features are removed after activation on all clusters, we should not
    //  simply remove them here. Please take the following actions:
    //      1. Mark the appropriate `scheduler_feature_flags` as deprecated
    //      2. Unconditionally set the flag here so external schedulers see it as active
    //
    // Justification for this approach:
    // - We don't need to break ABI everytime a feature is removed. Schedulers that update
    //   to latest versions of the code will see the feature is deprecated and can update
    //   their handling accordingly. Schedulers that do not update will see it as active
    //   and not break.

    let snapshot = feature_set.snapshot();
    let mut scheduler_features = scheduler_feature_flags::NONE;
    if snapshot.limit_instruction_accounts {
        scheduler_features |= scheduler_feature_flags::LIMIT_INSTRUCTION_ACCOUNTS;
    }
    if snapshot.create_account_allow_prefund {
        scheduler_features |= scheduler_feature_flags::CREATE_ACCOUNT_ALLOW_PREFUND;
    }
    if snapshot.bls_pubkey_management_in_vote_account {
        scheduler_features |= scheduler_feature_flags::BLS_PUBKEY_MANAGEMENT_IN_VOTE_ACCOUNT;
    }

    scheduler_features
}

/// Calculate progress through a slot based on tick-height.
fn progress(slot: Slot, tick_height: u64, ticks_per_slot: u64) -> u8 {
    debug_assert!(ticks_per_slot < u8::MAX as u64 && ticks_per_slot > 0);

    ((100 * tick_height.saturating_sub(slot * ticks_per_slot)) / ticks_per_slot) as u8
}

/// Calculate a slot based on tick-height - optimistic on boundaries.
/// i.e. tick_height 64 = slot 1 (with 0 progress) rather than slot 0
/// being complete.
fn slot_from_tick_height(tick_height: u64, ticks_per_slot: u64) -> u64 {
    tick_height / ticks_per_slot
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_clock::DEFAULT_TICKS_PER_SLOT,
        solana_epoch_schedule::MINIMUM_SLOTS_PER_EPOCH,
        solana_leader_schedule::SlotLeader,
        solana_poh::poh_recorder::LeaderState,
        solana_runtime::{bank::Bank, bank_forks::SharableBanks},
    };

    fn test_sharable_banks() -> SharableBanks {
        let (_bank, bank_forks) =
            Bank::new_for_tests(&solana_genesis_config::create_genesis_config(1).0)
                .wrap_with_bank_forks_for_tests();
        bank_forks.read().unwrap().sharable_banks()
    }

    #[test]
    fn test_progress_tracker_produce_progress_message() {
        let mut shared_leader_state = SharedLeaderState::new(0, None, None);
        let ticks_per_slot = DEFAULT_TICKS_PER_SLOT;

        let mut progress_tracker = ProgressTracker::new(
            Arc::default(),
            shared_leader_state.clone(),
            test_sharable_banks(),
            vec![],
            ticks_per_slot,
        );

        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, 0);
        assert_eq!(message.leader_state, agave_scheduler_bindings::NOT_LEADER);
        assert_eq!(message.current_slot, 0);
        assert_eq!(message.current_slot_progress, 0);
        assert_eq!(message.next_leader_slot, u64::MAX);
        assert_eq!(message.leader_range_end, u64::MAX);
        assert_eq!(message.epoch, 0);
        assert_eq!(message.latest_blockhash, [0; 32]);
        assert_eq!(message.target_bank_time_ms, 0);

        let expected_tick_height = 2 * ticks_per_slot;
        shared_leader_state.store(Arc::new(LeaderState::new(
            None,
            expected_tick_height,
            None,
            None,
        )));
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, expected_tick_height);
        assert_eq!(message.leader_state, agave_scheduler_bindings::NOT_LEADER);
        assert_eq!(message.current_slot, 2);
        assert_eq!(message.next_leader_slot, u64::MAX);
        assert_eq!(message.leader_range_end, u64::MAX);
        assert_eq!(message.current_slot_progress, 0);
        assert_eq!(message.epoch, 0);
        assert_eq!(message.latest_blockhash, [0; 32]);
        assert_eq!(message.target_bank_time_ms, 0);

        // Next leader slot is in the future - should be NOT_LEADER.
        shared_leader_state.store(Arc::new(LeaderState::new(
            None,
            expected_tick_height,
            Some(4 * ticks_per_slot),
            Some((4, 7)),
        )));
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, expected_tick_height);
        assert_eq!(message.leader_state, agave_scheduler_bindings::NOT_LEADER);
        assert_eq!(message.current_slot, 2);
        assert_eq!(message.next_leader_slot, 4);
        assert_eq!(message.leader_range_end, 7);
        assert_eq!(message.current_slot_progress, 0);
        assert_eq!(message.epoch, 0);
        assert_eq!(message.latest_blockhash, [0; 32]);
        assert_eq!(message.target_bank_time_ms, 0);

        // In leader slot but no bank yet - should be LEADER_STARTING.
        // leader_first_tick_height is at start of slot 4, and we're at tick_height
        // that puts us in slot 4.
        let leader_first_tick = 4 * ticks_per_slot + 1;
        shared_leader_state.store(Arc::new(LeaderState::new(
            None,
            leader_first_tick, // tick_height >= leader_first_tick_height
            Some(leader_first_tick),
            Some((4, 7)),
        )));
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, leader_first_tick);
        assert_eq!(
            message.leader_state,
            agave_scheduler_bindings::LEADER_STARTING
        );
        assert_eq!(message.current_slot, 4);
        assert_eq!(message.next_leader_slot, 4);
        assert_eq!(message.leader_range_end, 7);
        assert_eq!(message.current_slot_progress, 1);
        assert_eq!(message.epoch, 0);
        assert_eq!(message.latest_blockhash, [0; 32]);
        assert_eq!(message.target_bank_time_ms, 0);

        // Slot boundary mid-window: tick_height one tick before leader_first_tick_height.
        let slot_5_boundary = 5 * ticks_per_slot;
        shared_leader_state.store(Arc::new(LeaderState::new(
            None,
            slot_5_boundary,
            Some(slot_5_boundary + 1),
            Some((5, 7)),
        )));
        let (message, _) = progress_tracker.produce_progress_message();
        assert_eq!(message.current_slot, 5);
        assert_eq!(
            message.leader_state,
            agave_scheduler_bindings::LEADER_STARTING
        );

        let (bank, _bank_forks) =
            Bank::new_for_tests(&solana_genesis_config::create_genesis_config(1).0)
                .wrap_with_bank_forks_for_tests();
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(bank.clone()),
            bank.tick_height(),
            Some(4 * ticks_per_slot),
            Some((4, 7)),
        )));

        // With a working bank - should be LEADER_READY.
        assert!(!bank.is_complete());
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, bank.tick_height());
        assert_eq!(message.leader_state, agave_scheduler_bindings::LEADER_READY);
        assert_eq!(message.current_slot, bank.slot());
        assert_eq!(message.next_leader_slot, 4);
        assert_eq!(message.leader_range_end, 7);
        assert_eq!(message.current_slot_progress, 0);
        assert_eq!(message.epoch, bank.epoch());
        assert_eq!(message.latest_blockhash, bank.last_blockhash().to_bytes());
        assert_eq!(
            message.target_bank_time_ms,
            target_bank_time_ms(bank.ns_per_slot)
        );

        bank.fill_bank_with_ticks_for_tests();
        assert!(bank.is_complete());
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(bank.clone()),
            bank.tick_height(),
            Some(4 * ticks_per_slot),
            Some((4, 7)),
        )));
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, bank.tick_height());
        assert_eq!(message.leader_state, agave_scheduler_bindings::LEADER_READY);
        assert_eq!(message.current_slot, bank.slot());
        assert_eq!(message.next_leader_slot, 4);
        assert_eq!(message.leader_range_end, 7);
        assert_eq!(message.current_slot_progress, 100);
        assert_eq!(message.epoch, bank.epoch());
        assert_eq!(message.latest_blockhash, bank.last_blockhash().to_bytes());
        assert_eq!(
            message.target_bank_time_ms,
            target_bank_time_ms(bank.ns_per_slot)
        );

        // Child bank past the first epoch boundary - epoch should advance.
        let child_bank = Arc::new(Bank::new_from_parent(
            bank,
            SlotLeader::new_unique(),
            MINIMUM_SLOTS_PER_EPOCH,
        ));
        assert_eq!(child_bank.epoch(), 1);
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(child_bank.clone()),
            child_bank.tick_height(),
            Some(MINIMUM_SLOTS_PER_EPOCH),
            Some((MINIMUM_SLOTS_PER_EPOCH, MINIMUM_SLOTS_PER_EPOCH + 3)),
        )));
        let (message, tick_height) = progress_tracker.produce_progress_message();
        assert_eq!(tick_height, child_bank.tick_height());
        assert_eq!(message.leader_state, agave_scheduler_bindings::LEADER_READY);
        assert_eq!(message.current_slot, child_bank.slot());
        assert_eq!(message.next_leader_slot, MINIMUM_SLOTS_PER_EPOCH);
        assert_eq!(message.leader_range_end, MINIMUM_SLOTS_PER_EPOCH + 3);
        assert_eq!(message.current_slot_progress, 0);
        assert_eq!(message.epoch, child_bank.epoch());
        assert_eq!(
            message.latest_blockhash,
            child_bank.last_blockhash().to_bytes()
        );
        assert_eq!(
            message.target_bank_time_ms,
            target_bank_time_ms(child_bank.ns_per_slot)
        );
    }

    #[test]
    fn test_progress_tracker_remaining_block_cost() {
        let mut progress_tracker = ProgressTracker::new(
            Arc::default(),
            SharedLeaderState::new(0, None, None),
            test_sharable_banks(),
            vec![],
            DEFAULT_TICKS_PER_SLOT,
        );

        // No bank - no block cost set (0).
        assert_eq!(0, progress_tracker.remaining_block_cost());

        let block_limit = 10_000;
        progress_tracker.limit_and_shared_block_cost = Some((block_limit, SharedBlockCost::new(0)));
        assert_eq!(block_limit, progress_tracker.remaining_block_cost());
        progress_tracker.limit_and_shared_block_cost =
            Some((block_limit, SharedBlockCost::new(block_limit / 2)));
        assert_eq!(block_limit / 2, progress_tracker.remaining_block_cost());
    }

    #[test]
    fn test_progress() {
        let ticks_per_slot = DEFAULT_TICKS_PER_SLOT;
        assert_eq!(0, progress(0, 0, ticks_per_slot));
        assert_eq!(1, progress(0, 1, ticks_per_slot));
        assert_eq!(3, progress(0, 2, ticks_per_slot));
        assert_eq!(98, progress(0, ticks_per_slot - 1, ticks_per_slot));
        assert_eq!(100, progress(0, ticks_per_slot, ticks_per_slot));
        assert_eq!(0, progress(1, ticks_per_slot, ticks_per_slot));
        assert_eq!(3, progress(1, ticks_per_slot + 2, ticks_per_slot));
    }

    #[test]
    fn test_scheduler_features() {
        let mut progress_tracker = ProgressTracker::new(
            Arc::default(),
            SharedLeaderState::new(0, None, None),
            test_sharable_banks(),
            vec![],
            DEFAULT_TICKS_PER_SLOT,
        );
        let disabled = FeatureSet::default();
        let enabled = FeatureSet::all_enabled();
        let expected = scheduler_feature_flags::LIMIT_INSTRUCTION_ACCOUNTS
            | scheduler_feature_flags::CREATE_ACCOUNT_ALLOW_PREFUND
            | scheduler_feature_flags::BLS_PUBKEY_MANAGEMENT_IN_VOTE_ACCOUNT;

        assert_eq!(
            progress_tracker.update_scheduler_features(0, &disabled),
            scheduler_feature_flags::NONE
        );
        assert_eq!(
            progress_tracker.update_scheduler_features(0, &enabled),
            scheduler_feature_flags::NONE
        );
        assert_eq!(
            progress_tracker.update_scheduler_features(1, &enabled),
            expected
        );
    }

    #[test]
    fn test_scheduler_features_prefer_leader_bank() {
        let genesis_config = solana_genesis_config::create_genesis_config(1).0;
        let mut fallback_bank = Bank::new_for_tests(&genesis_config);
        fallback_bank.deactivate_feature(&agave_feature_set::limit_instruction_accounts::id());
        let (_fallback_bank, fallback_bank_forks) = fallback_bank.wrap_with_bank_forks_for_tests();
        let sharable_banks = fallback_bank_forks.read().unwrap().sharable_banks();

        let mut leader_bank = Bank::new_for_tests(&genesis_config);
        leader_bank.activate_feature(&agave_feature_set::limit_instruction_accounts::id());
        let (leader_bank, _leader_bank_forks) = leader_bank.wrap_with_bank_forks_for_tests();
        let mut shared_leader_state = SharedLeaderState::new(0, None, None);
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(leader_bank.clone()),
            leader_bank.tick_height(),
            None,
            None,
        )));

        let mut progress_tracker = ProgressTracker::new(
            Arc::default(),
            shared_leader_state,
            sharable_banks.clone(),
            vec![],
            DEFAULT_TICKS_PER_SLOT,
        );
        let expected = scheduler_features_from_feature_set(&leader_bank.feature_set);
        assert_ne!(
            expected,
            scheduler_features_from_feature_set(&sharable_banks.working().feature_set)
        );

        let (message, _) = progress_tracker.produce_progress_message();
        assert_eq!(message.scheduler_features, expected);
    }

    #[test]
    fn test_slot_from_tick_height() {
        let ticks_per_slot = DEFAULT_TICKS_PER_SLOT;
        assert_eq!(0, slot_from_tick_height(0, ticks_per_slot));
        assert_eq!(0, slot_from_tick_height(ticks_per_slot - 1, ticks_per_slot));
        assert_eq!(1, slot_from_tick_height(ticks_per_slot, ticks_per_slot));
        assert_eq!(1, slot_from_tick_height(ticks_per_slot + 1, ticks_per_slot));
        assert_eq!(
            1,
            slot_from_tick_height(2 * ticks_per_slot - 1, ticks_per_slot)
        );
        assert_eq!(2, slot_from_tick_height(2 * ticks_per_slot, ticks_per_slot));
        assert_eq!(
            2,
            slot_from_tick_height(2 * ticks_per_slot + 1, ticks_per_slot)
        );
    }
}
