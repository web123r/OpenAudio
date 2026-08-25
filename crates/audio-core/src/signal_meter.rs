//! Lock-free audio signal metering shared by every OpenAudio engine.
//!
//! Audio processing threads update existing `SignalMeter` handles without
//! locking the global registry. The registry mutex is only used when a meter
//! starts, stops, or when the desktop UI requests a snapshot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const SILENCE_FLOOR_DB: f32 = -90.0;
const SIGNAL_THRESHOLD_LINEAR: f32 = 0.001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalDirection {
    Input,
    Output,
    Diagnostic,
}

#[derive(Debug, Clone)]
pub struct SignalMeterSnapshot {
    pub id: String,
    pub label: String,
    pub direction: SignalDirection,
    pub channel_peaks_db: Vec<f32>,
    pub last_signal_age_ms: u64,
}

pub struct SignalMeter {
    id: String,
    label: String,
    direction: SignalDirection,
    channel_peaks: Vec<AtomicU32>,
    last_signal_ms: AtomicU64,
}

pub struct SignalMeterGuard {
    id: String,
    meter: Arc<SignalMeter>,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<SignalMeter>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, Arc<SignalMeter>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

impl SignalMeter {
    fn new(id: String, label: String, direction: SignalDirection, channel_count: usize) -> Self {
        Self {
            id,
            label,
            direction,
            channel_peaks: (0..channel_count.max(1))
                .map(|_| AtomicU32::new(0.0f32.to_bits()))
                .collect(),
            last_signal_ms: AtomicU64::new(0),
        }
    }

    pub fn channel_count(&self) -> usize {
        self.channel_peaks.len()
    }

    /// Observes interleaved Float32 samples.
    ///
    /// This function does not allocate and does not acquire a mutex.
    pub fn observe_interleaved(&self, samples: &[f32], channel_count: usize) {
        if samples.is_empty() || channel_count == 0 {
            return;
        }

        let measured_channels = channel_count.min(self.channel_peaks.len());

        let mut signal_detected = false;

        for frame in samples.chunks(channel_count) {
            for channel in 0..measured_channels {
                let Some(sample) = frame.get(channel) else {
                    continue;
                };

                let peak = sanitize_peak(*sample);

                if peak >= SIGNAL_THRESHOLD_LINEAR {
                    signal_detected = true;
                }

                atomic_max_f32(&self.channel_peaks[channel], peak);
            }
        }

        if signal_detected {
            self.last_signal_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    /// Observes one non-interleaved logical channel.
    pub fn observe_channel(&self, channel: usize, samples: &[f32]) {
        let Some(destination) = self.channel_peaks.get(channel) else {
            return;
        };

        let mut peak = 0.0f32;

        for sample in samples {
            peak = peak.max(sanitize_peak(*sample));
        }

        atomic_max_f32(destination, peak);

        if peak >= SIGNAL_THRESHOLD_LINEAR {
            self.last_signal_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    /// Observes a peak value already calculated by an audio engine.
    pub fn observe_channel_peak(&self, channel: usize, peak: f32) {
        let Some(destination) = self.channel_peaks.get(channel) else {
            return;
        };

        let peak = sanitize_peak(peak);

        atomic_max_f32(destination, peak);

        if peak >= SIGNAL_THRESHOLD_LINEAR {
            self.last_signal_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    fn snapshot_and_reset(&self) -> SignalMeterSnapshot {
        let channel_peaks_db = self
            .channel_peaks
            .iter()
            .map(|peak| {
                let linear = f32::from_bits(peak.swap(0.0f32.to_bits(), Ordering::Relaxed));

                linear_to_dbfs(linear)
            })
            .collect();

        let last_signal_ms = self.last_signal_ms.load(Ordering::Relaxed);

        let last_signal_age_ms = if last_signal_ms == 0 {
            u64::MAX
        } else {
            now_ms().saturating_sub(last_signal_ms)
        };

        SignalMeterSnapshot {
            id: self.id.clone(),
            label: self.label.clone(),
            direction: self.direction,
            channel_peaks_db,
            last_signal_age_ms,
        }
    }
}

impl Drop for SignalMeterGuard {
    fn drop(&mut self) {
        let mut meters = match registry().lock() {
            Ok(meters) => meters,
            Err(poisoned) => poisoned.into_inner(),
        };

        let should_remove = meters
            .get(&self.id)
            .map(|current| Arc::ptr_eq(current, &self.meter))
            .unwrap_or(false);

        if should_remove {
            meters.remove(&self.id);
        }
    }
}

pub fn register_signal_meter(
    id: impl Into<String>,
    label: impl Into<String>,
    direction: SignalDirection,
    channel_count: usize,
) -> Arc<SignalMeter> {
    let id = id.into();

    let meter = Arc::new(SignalMeter::new(
        id.clone(),
        label.into(),
        direction,
        channel_count,
    ));

    match registry().lock() {
        Ok(mut meters) => {
            meters.insert(id, meter.clone());
        }
        Err(poisoned) => {
            poisoned.into_inner().insert(id, meter.clone());
        }
    }

    meter
}

/// Registers a meter and returns an automatic cleanup guard.
///
/// Keep the returned guard alive for the entire audio session. When the
/// guard is dropped, its meter is removed from the global monitor.
pub fn register_scoped_signal_meter(
    id: impl Into<String>,
    label: impl Into<String>,
    direction: SignalDirection,
    channel_count: usize,
) -> (Arc<SignalMeter>, SignalMeterGuard) {
    let meter = register_signal_meter(id, label, direction, channel_count);

    let guard = SignalMeterGuard {
        id: meter.id.clone(),
        meter: meter.clone(),
    };

    (meter, guard)
}

pub fn remove_signal_meter(id: &str) {
    match registry().lock() {
        Ok(mut meters) => {
            meters.remove(id);
        }
        Err(poisoned) => {
            poisoned.into_inner().remove(id);
        }
    }
}

/// Returns the latest peak snapshot for every registered meter.
///
/// Reading a snapshot resets each stored peak to silence. New peaks then
/// accumulate until the next UI refresh.
pub fn signal_meter_snapshots() -> Vec<SignalMeterSnapshot> {
    // Clone meter handles first so the registry lock is released before
    // scanning channel atomics.
    let meters = match registry().lock() {
        Ok(meters) => meters.values().cloned().collect::<Vec<_>>(),
        Err(poisoned) => poisoned.into_inner().values().cloned().collect::<Vec<_>>(),
    };

    let mut snapshots = meters
        .iter()
        .map(|meter| meter.snapshot_and_reset())
        .collect::<Vec<_>>();

    snapshots.sort_by(|left, right| left.label.to_lowercase().cmp(&right.label.to_lowercase()));

    snapshots
}

fn sanitize_peak(sample: f32) -> f32 {
    if sample.is_finite() {
        sample.abs().min(16.0)
    } else {
        0.0
    }
}

fn atomic_max_f32(destination: &AtomicU32, value: f32) {
    let mut current = destination.load(Ordering::Relaxed);

    loop {
        let current_value = f32::from_bits(current);

        if value <= current_value {
            return;
        }

        match destination.compare_exchange_weak(
            current,
            value.to_bits(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn linear_to_dbfs(value: f32) -> f32 {
    if !value.is_finite() || value <= 0.000_031_622_78 {
        SILENCE_FLOOR_DB
    } else {
        (20.0 * value.log10()).clamp(SILENCE_FLOOR_DB, 24.0)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
