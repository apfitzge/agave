use {core::time::Duration, std::time::Instant};

pub(crate) struct CostPacer {
    budget: u64,
    slot_start: Instant,
    fill_duration: Duration,
}

impl CostPacer {
    pub(crate) fn new(
        budget: u64,
        slot_start: Instant,
        slot_duration: Duration,
        execution_margin: Duration,
    ) -> Self {
        Self {
            budget,
            slot_start,
            fill_duration: slot_duration.saturating_sub(execution_margin),
        }
    }

    /// Returns released cost units not already consumed or reserved for execution.
    pub(crate) fn available_budget(&self, now: Instant, consumed_cost: u64) -> u64 {
        let elapsed = now.saturating_duration_since(self.slot_start);
        let released = if elapsed >= self.fill_duration {
            self.budget
        } else {
            u128::from(self.budget)
                .saturating_mul(elapsed.as_nanos())
                .checked_div(self.fill_duration.as_nanos())
                .unwrap() as u64
        };
        released.saturating_sub(consumed_cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn releases_budget_linearly_before_slot_end() {
        let start = Instant::now();
        let pacer = CostPacer::new(
            100,
            start,
            Duration::from_millis(400),
            Duration::from_millis(10),
        );
        assert_eq!(pacer.available_budget(start, 0), 0);
        assert_eq!(
            pacer.available_budget(start + Duration::from_millis(195), 0),
            50
        );
        assert_eq!(
            pacer.available_budget(start + Duration::from_millis(195), 30),
            20
        );
        assert_eq!(
            pacer.available_budget(start + Duration::from_millis(195), 60),
            0
        );
        assert_eq!(
            pacer.available_budget(start + Duration::from_millis(390), 0),
            100
        );
        assert_eq!(
            pacer.available_budget(start + Duration::from_millis(500), 100),
            0
        );
    }

    #[test]
    fn margin_can_be_zero_or_exceed_slot_duration() {
        let start = Instant::now();
        let duration = Duration::from_millis(400);
        let pacer = CostPacer::new(100, start, duration, Duration::ZERO);
        assert_eq!(pacer.available_budget(start + duration, 0), 100);
        let pacer = CostPacer::new(100, start, duration, Duration::from_millis(500));
        assert_eq!(pacer.available_budget(start, 0), 100);
    }
}
