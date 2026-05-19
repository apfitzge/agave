use std::{
    num::NonZeroU64,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const TSC_SCALE_SHIFT: u32 = 64;
const TSC_FALLBACK_CALIBRATION_SLEEP: Duration = Duration::from_millis(1);

pub(crate) struct ReplayEventTimestampSource {
    base_counter: u64,
    base_timestamp_ns: u64,
    ns_per_counter_unit_q64: u128,
}

impl ReplayEventTimestampSource {
    pub(crate) fn new() -> Self {
        let counter_frequency_hz = timestamp_counter_frequency_hz();
        let counter_before = read_timestamp_counter();
        let base_timestamp_ns = unix_timestamp_ns();
        let counter_after = read_timestamp_counter();
        let base_counter =
            counter_before.wrapping_add(counter_after.wrapping_sub(counter_before) / 2);

        Self {
            base_counter,
            base_timestamp_ns,
            ns_per_counter_unit_q64: ns_per_counter_unit_q64(counter_frequency_hz),
        }
    }

    pub(crate) fn timestamp_ns(&self) -> u64 {
        let elapsed_counter_units = read_timestamp_counter().wrapping_sub(self.base_counter);
        let elapsed_ns = u128::from(elapsed_counter_units)
            .saturating_mul(self.ns_per_counter_unit_q64)
            .checked_shr(TSC_SCALE_SHIFT)
            .unwrap_or(u128::MAX);
        self.base_timestamp_ns
            .saturating_add(u64::try_from(elapsed_ns).unwrap_or(u64::MAX))
    }
}

fn unix_timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn ns_per_counter_unit_q64(counter_frequency_hz: NonZeroU64) -> u128 {
    (u128::from(NANOS_PER_SECOND) << TSC_SCALE_SHIFT)
        .checked_div(u128::from(counter_frequency_hz.get()))
        .expect("nonzero counter frequency")
}

fn timestamp_counter_frequency_hz() -> NonZeroU64 {
    timestamp_counter_frequency_hz_from_cpuid()
        .or_else(calibrate_timestamp_counter_frequency_hz)
        .unwrap_or_else(|| NonZeroU64::new(NANOS_PER_SECOND).expect("nanoseconds per second"))
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn timestamp_counter_frequency_hz_from_cpuid() -> Option<NonZeroU64> {
    let (max_basic_leaf, _, _, _) = cpuid_count_registers(0, 0);

    if max_basic_leaf >= 0x15 {
        let (denominator, numerator, crystal_frequency_hz, _) = cpuid_count_registers(0x15, 0);
        if denominator != 0 && numerator != 0 && crystal_frequency_hz != 0 {
            let tsc_frequency_hz = u128::from(crystal_frequency_hz)
                .saturating_mul(u128::from(numerator))
                .checked_div(u128::from(denominator))?;
            if let Some(tsc_frequency_hz) = u64::try_from(tsc_frequency_hz)
                .ok()
                .and_then(NonZeroU64::new)
            {
                return Some(tsc_frequency_hz);
            }
        }
    }

    if max_basic_leaf >= 0x16 {
        let (base_frequency_mhz, _, _, _) = cpuid_count_registers(0x16, 0);
        return u64::from(base_frequency_mhz)
            .checked_mul(1_000_000)
            .and_then(NonZeroU64::new);
    }

    None
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn timestamp_counter_frequency_hz_from_cpuid() -> Option<NonZeroU64> {
    None
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn calibrate_timestamp_counter_frequency_hz() -> Option<NonZeroU64> {
    let start_counter = read_timestamp_counter();
    let start = Instant::now();
    thread::sleep(TSC_FALLBACK_CALIBRATION_SLEEP);
    let elapsed_ns = start.elapsed().as_nanos();
    let elapsed_counter_units = read_timestamp_counter().wrapping_sub(start_counter);
    if elapsed_ns == 0 || elapsed_counter_units == 0 {
        return None;
    }

    let counter_frequency_hz = u128::from(elapsed_counter_units)
        .checked_mul(u128::from(NANOS_PER_SECOND))?
        .checked_div(elapsed_ns)?;
    u64::try_from(counter_frequency_hz)
        .ok()
        .and_then(NonZeroU64::new)
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn calibrate_timestamp_counter_frequency_hz() -> Option<NonZeroU64> {
    NonZeroU64::new(NANOS_PER_SECOND)
}

#[cfg(target_arch = "x86")]
fn read_timestamp_counter() -> u64 {
    // SAFETY: `_rdtsc` reads the processor timestamp counter and has no memory
    // safety preconditions.
    unsafe { std::arch::x86::_rdtsc() }
}

#[cfg(target_arch = "x86_64")]
fn read_timestamp_counter() -> u64 {
    // SAFETY: `_rdtsc` reads the processor timestamp counter and has no memory
    // safety preconditions.
    unsafe { std::arch::x86_64::_rdtsc() }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn read_timestamp_counter() -> u64 {
    unix_timestamp_ns()
}

#[cfg(target_arch = "x86")]
fn cpuid_count_registers(leaf: u32, sub_leaf: u32) -> (u32, u32, u32, u32) {
    let result = std::arch::x86::__cpuid_count(leaf, sub_leaf);
    (result.eax, result.ebx, result.ecx, result.edx)
}

#[cfg(target_arch = "x86_64")]
fn cpuid_count_registers(leaf: u32, sub_leaf: u32) -> (u32, u32, u32, u32) {
    let result = std::arch::x86_64::__cpuid_count(leaf, sub_leaf);
    (result.eax, result.ebx, result.ecx, result.edx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_source_returns_nondecreasing_unix_ns() {
        let source = ReplayEventTimestampSource::new();
        let first_timestamp_ns = source.timestamp_ns();
        let second_timestamp_ns = source.timestamp_ns();

        assert_ne!(first_timestamp_ns, 0);
        assert!(first_timestamp_ns <= second_timestamp_ns);
    }

    #[test]
    fn timestamp_counter_scale_is_nonzero() {
        let counter_frequency_hz = timestamp_counter_frequency_hz();

        assert_ne!(ns_per_counter_unit_q64(counter_frequency_hz), 0);
    }
}
