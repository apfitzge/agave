#![allow(
    dead_code,
    reason = "the following scheduling stage dispatches and completes external execution batches"
)]

/// Tracks execution work dispatched to each worker that has not yet completed.
///
/// The execution response returns the original transaction metadata, so callers provide the
/// completed batch's transaction count and total cost directly. No per-batch lookup is needed.
pub(crate) struct InFlightTracker {
    scheduling_slot: Option<u64>,
    num_in_flight_per_worker: Vec<usize>,
    cost_units_in_flight_per_worker: Vec<u64>,
}

impl InFlightTracker {
    pub(crate) fn new(num_workers: usize) -> Self {
        assert!(num_workers > 0, "must have at least one execution worker");
        Self {
            scheduling_slot: None,
            num_in_flight_per_worker: vec![0; num_workers],
            cost_units_in_flight_per_worker: vec![0; num_workers],
        }
    }

    /// Begin scheduling `slot`, unless work for the preceding slot is still outstanding.
    pub(crate) fn enter_slot(&mut self, slot: u64) -> bool {
        match self.scheduling_slot {
            None => {
                self.scheduling_slot = Some(slot);
                true
            }
            Some(current_slot) if current_slot == slot => true,
            Some(_) if !self.is_empty() => false,
            Some(_) => {
                self.scheduling_slot = Some(slot);
                true
            }
        }
    }

    pub(crate) fn num_in_flight_per_worker(&self) -> &[usize] {
        &self.num_in_flight_per_worker
    }

    pub(crate) fn cost_units_in_flight_per_worker(&self) -> &[u64] {
        &self.cost_units_in_flight_per_worker
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.num_in_flight_per_worker
            .iter()
            .zip(&self.cost_units_in_flight_per_worker)
            .all(|(&count, &cost_units)| count == 0 && cost_units == 0)
    }

    pub(crate) fn track_batch(
        &mut self,
        worker_index: usize,
        num_transactions: usize,
        total_cost_units: u64,
    ) {
        self.num_in_flight_per_worker[worker_index] =
            self.num_in_flight_per_worker[worker_index].wrapping_add(num_transactions);
        self.cost_units_in_flight_per_worker[worker_index] =
            self.cost_units_in_flight_per_worker[worker_index].wrapping_add(total_cost_units);
    }

    pub(crate) fn complete_batch(
        &mut self,
        worker_index: usize,
        num_transactions: usize,
        total_cost_units: u64,
    ) {
        debug_assert!(
            self.num_in_flight_per_worker[worker_index] >= num_transactions,
            "completed batch has more transactions than the worker has in flight"
        );
        debug_assert!(
            self.cost_units_in_flight_per_worker[worker_index] >= total_cost_units,
            "completed batch has more cost units than the worker has in flight"
        );
        self.num_in_flight_per_worker[worker_index] =
            self.num_in_flight_per_worker[worker_index].wrapping_sub(num_transactions);
        self.cost_units_in_flight_per_worker[worker_index] =
            self.cost_units_in_flight_per_worker[worker_index].wrapping_sub(total_cost_units);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_and_completes_worker_load() {
        let mut tracker = InFlightTracker::new(2);

        tracker.track_batch(0, 2, 10_000);
        tracker.track_batch(1, 1, 15_000);
        assert_eq!(tracker.num_in_flight_per_worker(), &[2, 1]);
        assert_eq!(tracker.cost_units_in_flight_per_worker(), &[10_000, 15_000]);

        tracker.complete_batch(0, 2, 10_000);
        assert_eq!(tracker.num_in_flight_per_worker(), &[0, 1]);
        assert_eq!(tracker.cost_units_in_flight_per_worker(), &[0, 15_000]);

        tracker.complete_batch(1, 1, 15_000);
        assert!(tracker.is_empty());
    }

    #[test]
    fn waits_for_all_previous_slot_work_to_complete() {
        let mut tracker = InFlightTracker::new(2);
        assert!(tracker.enter_slot(10));
        tracker.track_batch(0, 1, 0);
        tracker.track_batch(1, 1, 10);

        assert!(!tracker.enter_slot(11));

        tracker.complete_batch(0, 1, 0);
        assert!(!tracker.enter_slot(11));

        tracker.complete_batch(1, 1, 10);
        assert!(tracker.enter_slot(11));
    }
}
