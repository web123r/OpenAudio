/// Packet Loss Concealment (PLC) for real-time audio streams.
///
/// When an audio packet is dropped and cannot be recovered by FEC or retransmission,
/// PLC synthesizes smooth continuation audio rather than abruptly cutting off or
/// skipping forward in the timeline. When valid audio resumes, it applies a micro-crossfade
/// to eliminate boundary transients (clicks/pops).
#[derive(Debug, Clone)]
pub struct PacketLossConcealer {
    last_samples: Vec<f32>,
    channel_count: usize,
    in_loss_state: bool,
    decay_factor: f32,
}

impl PacketLossConcealer {
    pub fn new(channels: usize) -> Self {
        Self {
            last_samples: vec![0.0; channels.max(1)],
            channel_count: channels.max(1),
            in_loss_state: false,
            decay_factor: 0.85,
        }
    }

    /// Update the channel count if stream format changes.
    pub fn set_channel_count(&mut self, channels: usize) {
        let ch = channels.max(1);
        if self.channel_count != ch {
            self.channel_count = ch;
            self.last_samples = vec![0.0; ch];
            self.in_loss_state = false;
        }
    }

    /// Observes valid received audio samples to keep the continuity state fresh.
    /// If recovering from a loss state, automatically applies a micro-fade-in
    /// to the first few frames to eliminate step discontinuities.
    pub fn process_incoming(&mut self, samples: &mut [f32]) {
        if samples.is_empty() || self.channel_count == 0 {
            return;
        }

        if self.in_loss_state {
            // Apply micro-fade-in over up to 32 frames (approx 0.67ms @ 48kHz)
            let fade_frames = (samples.len() / self.channel_count).min(32);
            if fade_frames > 1 {
                for frame in 0..fade_frames {
                    let gain = frame as f32 / fade_frames as f32;
                    let offset = frame * self.channel_count;
                    for c in 0..self.channel_count {
                        samples[offset + c] *= gain;
                    }
                }
            }
            self.in_loss_state = false;
        }

        // Save last frame of audio for future extrapolation
        let total_frames = samples.len() / self.channel_count;
        if total_frames > 0 {
            let last_frame_offset = (total_frames - 1) * self.channel_count;
            self.last_samples
                .copy_from_slice(&samples[last_frame_offset..last_frame_offset + self.channel_count]);
        }
    }

    /// Synthesizes `frames` of concealment audio using exponential attenuation
    /// from the last known audio frame. This fills the missing time slice so the
    /// audio timeline does not skip forward.
    pub fn synthesize_concealment(&mut self, frames: usize) -> Vec<f32> {
        let mut concealment = Vec::with_capacity(frames * self.channel_count);
        let mut current_frame = self.last_samples.clone();
        if current_frame.len() != self.channel_count {
            current_frame = vec![0.0; self.channel_count];
        }

        self.in_loss_state = true;

        for _ in 0..frames {
            for c in 0..self.channel_count {
                current_frame[c] *= self.decay_factor;
                // Clamp denormals
                if current_frame[c].abs() < 1e-6 {
                    current_frame[c] = 0.0;
                }
                concealment.push(current_frame[c]);
            }
        }

        self.last_samples.copy_from_slice(&current_frame);
        concealment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plc_synthesizes_decaying_audio_and_fades_in() {
        let mut plc = PacketLossConcealer::new(2);

        // 1. Feed a normal audio block
        let mut audio = vec![1.0f32, -1.0f32, 0.8f32, -0.8f32];
        plc.process_incoming(&mut audio);
        assert_eq!(plc.last_samples, vec![0.8f32, -0.8f32]);

        // 2. Synthesize concealment for 4 missing frames
        let concealed = plc.synthesize_concealment(4);
        assert_eq!(concealed.len(), 8);
        assert!(concealed[0] < 0.8 && concealed[0] > 0.0);
        assert!(concealed[1] > -0.8 && concealed[1] < 0.0);
        // Successive frames must decay monotonically towards zero
        assert!(concealed[2].abs() < concealed[0].abs());
        assert!(concealed[4].abs() < concealed[2].abs());

        // 3. Process new incoming audio after loss - first frame must be scaled down (fade-in)
        let mut fresh_audio = vec![1.0f32; 8];
        plc.process_incoming(&mut fresh_audio);
        assert!(fresh_audio[0] < 0.1); // first frame faded in near zero
        assert!(fresh_audio[6] > fresh_audio[0]); // ramped up towards 1.0
    }
}
