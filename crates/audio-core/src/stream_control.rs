use std::time::{Duration, Instant};

const MIN_JITTER_TARGET_MS: f64 = 6.0;
const MAX_JITTER_TARGET_MS: f64 = 120.0;
const DEFAULT_JITTER_TARGET_MS: f64 = 20.0;
const MAX_CLOCK_CORRECTION_PPM: f64 = 500.0;

#[derive(Debug, Clone)]
pub struct AdaptiveJitterController {
    target_frames: f64,
    sample_rate: u32,
    underruns: u64,
    overruns: u64,
    last_adjustment: Instant,
}

impl AdaptiveJitterController {
    pub fn new(sample_rate: u32) -> Self {
        let mut controller = Self {
            target_frames: 0.0,
            sample_rate: sample_rate.max(1),
            underruns: 0,
            overruns: 0,
            last_adjustment: Instant::now(),
        };
        controller.target_frames = controller.frames_for_ms(DEFAULT_JITTER_TARGET_MS);
        controller
    }

    pub fn target_frames(&self) -> usize {
        self.target_frames.round().max(1.0) as usize
    }

    pub fn target_duration(&self) -> Duration {
        Duration::from_secs_f64(self.target_frames / self.sample_rate as f64)
    }

    pub fn observe(&mut self, buffered_frames: usize, underrun: bool, overrun: bool) {
        self.underruns += u64::from(underrun);
        self.overruns += u64::from(overrun);

        if self.last_adjustment.elapsed() < Duration::from_millis(250) {
            return;
        }

        let target = self.target_frames();
        if underrun || buffered_frames < target / 2 {
            self.target_frames *= 1.15;
        } else if overrun || buffered_frames > target.saturating_mul(2) {
            self.target_frames *= 0.92;
        } else {
            let error = buffered_frames as f64 - self.target_frames;
            self.target_frames += error * 0.05;
        }

        let minimum = self.frames_for_ms(MIN_JITTER_TARGET_MS);
        let maximum = self.frames_for_ms(MAX_JITTER_TARGET_MS);
        self.target_frames = self.target_frames.clamp(minimum, maximum);
        self.last_adjustment = Instant::now();
    }

    pub fn underruns(&self) -> u64 {
        self.underruns
    }

    pub fn overruns(&self) -> u64 {
        self.overruns
    }

    fn frames_for_ms(&self, milliseconds: f64) -> f64 {
        self.sample_rate as f64 * milliseconds / 1_000.0
    }
}

#[derive(Debug, Clone)]
pub struct ClockSynchronizer {
    offset_ns: f64,
    correction_ppm: f64,
    samples: u64,
}

impl Default for ClockSynchronizer {
    fn default() -> Self {
        Self {
            offset_ns: 0.0,
            correction_ppm: 0.0,
            samples: 0,
        }
    }
}

impl ClockSynchronizer {
    pub fn observe(&mut self, sender_timestamp_ns: u64, receiver_elapsed: Duration) {
        let receiver_ns = receiver_elapsed.as_nanos() as f64;
        let error = sender_timestamp_ns as f64 - receiver_ns;
        self.offset_ns += (error - self.offset_ns) * 0.02;
        self.samples = self.samples.saturating_add(1);

        if self.samples > 1 {
            let estimated = error - self.offset_ns;
            self.correction_ppm = (self.correction_ppm + estimated / 1_000_000.0 * 0.01)
                .clamp(-MAX_CLOCK_CORRECTION_PPM, MAX_CLOCK_CORRECTION_PPM);
        }
    }

    pub fn offset(&self) -> Duration {
        if self.offset_ns >= 0.0 {
            Duration::from_nanos(self.offset_ns.min(u64::MAX as f64) as u64)
        } else {
            Duration::ZERO
        }
    }

    pub fn correction_ppm(&self) -> f64 {
        self.correction_ppm
    }
}

pub fn resample_interleaved_linear(
    input: &[f32],
    channels: usize,
    input_rate: u32,
    output_rate: u32,
) -> Vec<f32> {
    if channels == 0 || input_rate == 0 || output_rate == 0 || input.is_empty() {
        return Vec::new();
    }
    if input_rate == output_rate {
        return input.to_vec();
    }

    let input_frames = input.len() / channels;
    if input_frames < 2 {
        return input.to_vec();
    }

    let output_frames = ((input_frames as u64 * output_rate as u64) / input_rate as u64)
        .max(1) as usize;
    let ratio = input_rate as f64 / output_rate as f64;
    let mut output = Vec::with_capacity(output_frames * channels);

    for output_frame in 0..output_frames {
        let position = output_frame as f64 * ratio;
        let first = position.floor() as usize;
        let second = (first + 1).min(input_frames - 1);
        let fraction = (position - first as f64) as f32;

        for channel in 0..channels {
            let a = input[first * channels + channel];
            let b = input[second * channels + channel];
            output.push(a + (b - a) * fraction);
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::{resample_interleaved_linear, AdaptiveJitterController, ClockSynchronizer};
    use std::time::Duration;

    #[test]
    fn resampler_preserves_channel_interleaving() {
        let input = [0.0, 1.0, 1.0, 2.0, 2.0, 3.0];
        let output = resample_interleaved_linear(&input, 2, 2, 4);
        assert_eq!(output.len(), 12);
        assert_eq!(&output[..2], &[0.0, 1.0]);
        assert_eq!(&output[10..], &[2.0, 3.0]);
    }

    #[test]
    fn jitter_target_stays_within_bounds() {
        let mut controller = AdaptiveJitterController::new(48_000);
        for _ in 0..20 {
            controller.observe(0, true, false);
        }
        assert!(controller.target_duration() <= Duration::from_millis(120));
        assert!(controller.target_duration() >= Duration::from_millis(6));
    }

    #[test]
    fn clock_synchronizer_tracks_offset() {
        let mut synchronizer = ClockSynchronizer::default();
        synchronizer.observe(1_000_000, Duration::ZERO);
        assert!(synchronizer.offset() > Duration::ZERO);
    }
}
