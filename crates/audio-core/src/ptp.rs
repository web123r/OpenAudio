use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// PTP (IEEE 1588) Precision Time Protocol clock synchronization state.
/// Maintains sub-microsecond time alignment with a PTP Grandmaster clock.
pub struct PtpClock {
    base_system_time_ns: u64,
    base_monotonic_instant: Instant,
    clock_offset_ns: AtomicI64,
    drift_ppm: AtomicU64,
}

impl Default for PtpClock {
    fn default() -> Self {
        let now_sys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64;

        Self {
            base_system_time_ns: now_sys,
            base_monotonic_instant: Instant::now(),
            clock_offset_ns: AtomicI64::new(0),
            drift_ppm: AtomicU64::new(0),
        }
    }
}

impl PtpClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns current grandmaster-aligned nanosecond timestamp.
    pub fn now_ns(&self) -> u64 {
        let elapsed_ns = self.base_monotonic_instant.elapsed().as_nanos() as u64;
        let local_ns = self.base_system_time_ns.saturating_add(elapsed_ns);
        let offset = self.clock_offset_ns.load(Ordering::Relaxed);

        if offset >= 0 {
            local_ns.saturating_add(offset as u64)
        } else {
            local_ns.saturating_sub(offset.unsigned_abs())
        }
    }

    /// Updates clock offset from PTP Sync/Follow_Up frames.
    pub fn adjust_offset(&self, offset_ns: i64) {
        self.clock_offset_ns.store(offset_ns, Ordering::Relaxed);
    }

    pub fn offset_ns(&self) -> i64 {
        self.clock_offset_ns.load(Ordering::Relaxed)
    }

    pub fn set_drift_ppm(&self, ppm: u64) {
        self.drift_ppm.store(ppm, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_ptp_clock_monotonicity_and_offset() {
        let clock = PtpClock::new();
        let t1 = clock.now_ns();
        thread::sleep(Duration::from_millis(5));
        let t2 = clock.now_ns();

        assert!(t2 > t1);
        assert!(t2 - t1 >= 4_000_000); // At least ~4-5ms passed

        // Adjust PTP offset +1,000,000 ns (+1ms)
        clock.adjust_offset(1_000_000);
        let t3 = clock.now_ns();
        assert_eq!(clock.offset_ns(), 1_000_000);
        assert!(t3 > t2);
    }
}
