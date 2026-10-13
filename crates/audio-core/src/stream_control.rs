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

    let output_frames =
        ((input_frames as u64 * output_rate as u64) / input_rate as u64).max(1) as usize;
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

/// Stateful linear resampler for packetized streams. Keeps the fractional
/// source-frame position and the final source frame between calls so packet
/// boundaries do not discard fractional output frames or repeat edge samples.
#[derive(Debug, Clone)]
pub struct StreamingLinearResampler {
    channels: usize,
    input_rate: u32,
    output_rate: u32,
    phase: f64,
    pending: Vec<f32>,
}

impl StreamingLinearResampler {
    pub fn new(channels: usize, input_rate: u32, output_rate: u32) -> Result<Self, String> {
        if channels == 0 {
            return Err("resampler channel count must be greater than zero".to_string());
        }
        if input_rate == 0 || output_rate == 0 {
            return Err("resampler sample rates must be greater than zero".to_string());
        }

        Ok(Self {
            channels,
            input_rate,
            output_rate,
            phase: 0.0,
            pending: Vec::new(),
        })
    }

    pub fn process(&mut self, input: &[f32]) -> Result<Vec<f32>, String> {
        if input.len() % self.channels != 0 {
            return Err(format!(
                "resampler input has {} samples, not aligned to {} channels",
                input.len(),
                self.channels
            ));
        }
        if input.is_empty() {
            return Ok(Vec::new());
        }

        if self.input_rate == self.output_rate && self.pending.is_empty() {
            return Ok(input.to_vec());
        }

        self.pending.extend_from_slice(input);

        let input_frames = self.pending.len() / self.channels;
        let step = self.input_rate as f64 / self.output_rate as f64;
        let mut output = Vec::new();

        loop {
            let first_frame = self.phase.floor() as usize;
            let fraction = self.phase - first_frame as f64;
            if first_frame >= input_frames
                || (first_frame + 1 >= input_frames && fraction > f64::EPSILON)
            {
                break;
            }

            let second_frame = (first_frame + 1).min(input_frames - 1);
            for channel in 0..self.channels {
                let first = self.pending[first_frame * self.channels + channel];
                let second = self.pending[second_frame * self.channels + channel];
                output.push(first + (second - first) * fraction as f32);
            }
            self.phase += step;
        }

        let frames_to_consume = (self.phase.floor() as usize).min(input_frames.saturating_sub(1));
        if frames_to_consume > 0 {
            self.pending.drain(..frames_to_consume * self.channels);
            self.phase -= frames_to_consume as f64;
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        resample_interleaved_linear, AdaptiveJitterController, ClockSynchronizer,
        StreamingLinearResampler,
    };
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
    fn streaming_resampler_preserves_fractional_frames_across_packets() {
        let mut resampler = StreamingLinearResampler::new(2, 48_000, 44_100).unwrap();
        let mut output = Vec::new();
        let input = (0..48_000)
            .flat_map(|frame| [frame as f32, -(frame as f32)])
            .collect::<Vec<_>>();

        for chunk in input.chunks(58 * 2) {
            output.extend(resampler.process(chunk).unwrap());
        }

        let expected = resample_interleaved_linear(&input, 2, 48_000, 44_100);
        assert_eq!(output.len() / 2, 44_100);
        assert!(output.chunks_exact(2).all(|frame| frame[1] == -frame[0]));
        assert!(output
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() < 0.01));
    }

    #[test]
    fn streaming_resampler_rejects_partial_interleaved_frames() {
        let mut resampler = StreamingLinearResampler::new(2, 48_000, 44_100).unwrap();
        assert!(resampler.process(&[0.0]).is_err());
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
