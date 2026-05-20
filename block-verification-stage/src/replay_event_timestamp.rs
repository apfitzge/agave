use std::mem::MaybeUninit;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

pub(crate) fn replay_event_timestamp_ns() -> u64 {
    let mut timestamp = MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: `timestamp.as_mut_ptr()` is valid for `clock_gettime` to write a
    // `timespec` into. We only assume initialization after the call succeeds.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, timestamp.as_mut_ptr()) };
    debug_assert_eq!(result, 0, "clock_gettime(CLOCK_MONOTONIC) failed");
    // SAFETY: `clock_gettime` succeeded, so it initialized `timestamp`.
    let timestamp = unsafe { timestamp.assume_init() };

    let seconds = timestamp.tv_sec as u64;
    let nanoseconds = timestamp.tv_nsec as u64;
    // u64 nanoseconds would not wrap until roughly year 2554 even for an
    // epoch-based clock, and CLOCK_MONOTONIC is uptime-based.
    seconds
        .wrapping_mul(NANOS_PER_SECOND)
        .wrapping_add(nanoseconds)
}

#[cfg(test)]
mod tests {
    use {super::*, std::thread};

    #[test]
    fn timestamp_returns_nondecreasing_monotonic_ns() {
        let first_timestamp_ns = replay_event_timestamp_ns();
        let second_timestamp_ns = replay_event_timestamp_ns();

        assert_ne!(first_timestamp_ns, 0);
        assert!(first_timestamp_ns <= second_timestamp_ns);
    }

    #[test]
    fn timestamp_advances_with_time() {
        let first_timestamp_ns = replay_event_timestamp_ns();
        thread::sleep(std::time::Duration::from_millis(1));
        let second_timestamp_ns = replay_event_timestamp_ns();

        assert!(first_timestamp_ns < second_timestamp_ns);
    }
}
