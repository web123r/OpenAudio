//! Opus audio codec integration for OpenAudio.
//!
//! Provides lightweight `OpusChannelEncoder` and `OpusChannelDecoder` wrappers
//! around `rusty_opus` (a pure-Rust RFC 6716 Opus implementation).
//! Both are designed for the real-time streaming path:
//! - **Encoder**: accepts interleaved Float32 PCM frames and returns a
//!   variable-length Opus packet with a 2-byte big-endian length prefix.
//! - **Decoder**: accepts the prefixed packet and returns interleaved Float32.
//!
//! Frame size is fixed at 480 samples (10 ms @ 48 kHz) for low latency.
//! The encoder accumulates input until a full frame is available.

#[cfg(feature = "opus")]
pub mod inner {
    use rusty_opus::{Application, OpusDecoder, OpusEncoder};

    /// 10 ms frame @ 48 kHz. Opus mandates 2.5 / 5 / 10 / 20 / 40 / 60 ms
    /// frames; 10 ms is the ideal trade-off between latency and efficiency for
    /// live audio-over-IP.
    pub const OPUS_FRAME_SAMPLES: usize = 480; // 10 ms @ 48 kHz

    /// Maximum encoded Opus packet size (bytes). Actual size is always much
    /// smaller; this is just a safe allocation ceiling.
    const MAX_OPUS_PACKET_BYTES: usize = 1_500;

    // ─────────────────────────────────────────────────────────────────────────
    // Encoder
    // ─────────────────────────────────────────────────────────────────────────

    /// Stateful Opus encoder for a single mono or stereo channel stream.
    pub struct OpusChannelEncoder {
        encoder: OpusEncoder,
        pending: Vec<f32>,
        _channels: usize,
        frame_samples: usize,
        encode_buf: Vec<u8>,
    }

    impl OpusChannelEncoder {
        /// Creates a new encoder.
        ///
        /// * `sample_rate` — 48000 Hz canonical OpenAudio rate (or other Opus rates).
        /// * `channel_count` — 1 (mono) or 2 (stereo).
        /// * `bitrate_kbps` — target bitrate, e.g. 64 for mono, 128 for stereo.
        pub fn new(
            sample_rate: u32,
            channel_count: usize,
            bitrate_kbps: i32,
        ) -> Result<Self, String> {
            if channel_count != 1 && channel_count != 2 {
                return Err(format!(
                    "Opus only supports 1 or 2 channels per stream; got {channel_count}"
                ));
            }

            let mut encoder = OpusEncoder::new(
                sample_rate as i32,
                channel_count,
                Application::Audio,
            )
            .map_err(|e| format!("failed to create Opus encoder: {e}"))?;

            encoder.bitrate_bps = bitrate_kbps * 1_000;

            Ok(Self {
                encoder,
                pending: Vec::new(),
                _channels: channel_count,
                frame_samples: OPUS_FRAME_SAMPLES * channel_count,
                encode_buf: vec![0u8; MAX_OPUS_PACKET_BYTES],
            })
        }

        /// Feeds interleaved Float32 samples into the encoder.
        ///
        /// Returns `Ok(Some(packet))` whenever a complete 10 ms frame has been
        /// encoded. Returns `Ok(None)` when more samples are needed. The
        /// returned packet includes a 2-byte big-endian length prefix.
        pub fn push_samples(&mut self, samples: &[f32]) -> Result<Option<Vec<u8>>, String> {
            self.pending.extend_from_slice(samples);

            if self.pending.len() < self.frame_samples {
                return Ok(None);
            }

            let frame = &self.pending[..self.frame_samples];
            let encoded_len = self
                .encoder
                .encode(frame, OPUS_FRAME_SAMPLES, &mut self.encode_buf)
                .map_err(|e| format!("Opus encode error: {e}"))?;

            self.pending.drain(..self.frame_samples);

            let payload = &self.encode_buf[..encoded_len];
            let mut packet = Vec::with_capacity(2 + encoded_len);
            let len_u16 = encoded_len as u16;
            packet.push((len_u16 >> 8) as u8);
            packet.push((len_u16 & 0xFF) as u8);
            packet.extend_from_slice(payload);

            Ok(Some(packet))
        }

        /// Returns the number of pending (not yet encoded) samples.
        pub fn pending_samples(&self) -> usize {
            self.pending.len()
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Decoder
    // ─────────────────────────────────────────────────────────────────────────

    /// Stateful Opus decoder for a single mono or stereo channel stream.
    pub struct OpusChannelDecoder {
        decoder: OpusDecoder,
        channels: usize,
        output_buf: Vec<f32>,
    }

    impl OpusChannelDecoder {
        pub fn new(sample_rate: u32, channel_count: usize) -> Result<Self, String> {
            if channel_count != 1 && channel_count != 2 {
                return Err(format!(
                    "Opus only supports 1 or 2 channels per stream; got {channel_count}"
                ));
            }

            let decoder = OpusDecoder::new(sample_rate as i32, channel_count)
                .map_err(|e| format!("failed to create Opus decoder: {e}"))?;

            Ok(Self {
                decoder,
                channels: channel_count,
                output_buf: vec![0.0f32; OPUS_FRAME_SAMPLES * channel_count],
            })
        }

        /// Decodes an Opus packet with a 2-byte big-endian length prefix.
        ///
        /// Returns interleaved Float32 PCM for `OPUS_FRAME_SAMPLES` frames.
        pub fn decode_packet(&mut self, packet: &[u8]) -> Result<Vec<f32>, String> {
            if packet.len() < 2 {
                return Err("Opus packet too short (missing 2-byte length prefix)".to_string());
            }

            let declared_len = u16::from_be_bytes([packet[0], packet[1]]) as usize;
            if packet.len() < 2 + declared_len {
                return Err(format!(
                    "Opus packet truncated: declared {declared_len} bytes, got {}",
                    packet.len() - 2
                ));
            }

            let opus_data = &packet[2..2 + declared_len];

            let decoded_frames = self
                .decoder
                .decode(opus_data, OPUS_FRAME_SAMPLES, &mut self.output_buf)
                .map_err(|e| format!("Opus decode error: {e}"))?;

            let total_samples = decoded_frames * self.channels;
            Ok(self.output_buf[..total_samples].to_vec())
        }

        /// Generates a comfort-noise PLC frame when a packet is lost.
        pub fn decode_lost(&mut self) -> Result<Vec<f32>, String> {
            let decoded_frames = self
                .decoder
                .decode(&[], OPUS_FRAME_SAMPLES, &mut self.output_buf)
                .map_err(|e| format!("Opus PLC decode error: {e}"))?;

            let total_samples = decoded_frames * self.channels;
            Ok(self.output_buf[..total_samples].to_vec())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn opus_mono_encode_decode_roundtrip() {
            let mut encoder = OpusChannelEncoder::new(48_000, 1, 64).unwrap();
            let mut decoder = OpusChannelDecoder::new(48_000, 1).unwrap();

            // Warm-up frame to settle Opus MDCT filter state.
            let warmup: Vec<f32> = (0..OPUS_FRAME_SAMPLES)
                .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 48_000.0).sin() * 0.5)
                .collect();
            let p1 = encoder.push_samples(&warmup).unwrap().expect("packet 1");
            let _ = decoder.decode_packet(&p1).unwrap();

            // Test frame 2 (steady-state).
            let pcm: Vec<f32> = (OPUS_FRAME_SAMPLES..OPUS_FRAME_SAMPLES * 2)
                .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 48_000.0).sin() * 0.5)
                .collect();

            let packet = encoder.push_samples(&pcm).unwrap().expect("should produce a packet");
            let recovered = decoder.decode_packet(&packet).unwrap();

            assert_eq!(recovered.len(), OPUS_FRAME_SAMPLES);

            // SNR check: decoded steady-state should approximate input
            let mse: f32 = pcm.iter().zip(recovered.iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f32>() / pcm.len() as f32;
            assert!(mse < 0.05, "Opus round-trip MSE too high: {mse}");
        }

        #[test]
        fn opus_stereo_encode_decode_roundtrip() {
            let mut encoder = OpusChannelEncoder::new(48_000, 2, 128).unwrap();
            let mut decoder = OpusChannelDecoder::new(48_000, 2).unwrap();

            let pcm: Vec<f32> = (0..OPUS_FRAME_SAMPLES * 2)
                .map(|i| {
                    let frame = i / 2;
                    (2.0 * std::f32::consts::PI * 440.0 * frame as f32 / 48_000.0).sin() * 0.5
                })
                .collect();

            let packet = encoder.push_samples(&pcm).unwrap().expect("should produce a packet");
            let recovered = decoder.decode_packet(&packet).unwrap();

            assert_eq!(recovered.len(), OPUS_FRAME_SAMPLES * 2);
        }

        #[test]
        fn encoder_buffers_partial_frame() {
            let mut encoder = OpusChannelEncoder::new(48_000, 1, 64).unwrap();
            let half: Vec<f32> = vec![0.0f32; OPUS_FRAME_SAMPLES / 2];
            let result = encoder.push_samples(&half).unwrap();
            assert!(result.is_none(), "should not emit packet for partial frame");
            assert_eq!(encoder.pending_samples(), OPUS_FRAME_SAMPLES / 2);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stub exports when the opus feature is disabled
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(not(feature = "opus"))]
pub mod inner {
    pub struct OpusChannelEncoder;
    pub struct OpusChannelDecoder;

    impl OpusChannelEncoder {
        pub fn new(_sr: u32, _ch: usize, _kbps: i32) -> Result<Self, String> {
            Err("OpenAudio was compiled without Opus support. \
                 Rebuild with --features opus."
                .to_string())
        }

        pub fn push_samples(&mut self, _samples: &[f32]) -> Result<Option<Vec<u8>>, String> {
            Err("Opus not available".to_string())
        }

        pub fn pending_samples(&self) -> usize { 0 }
    }

    impl OpusChannelDecoder {
        pub fn new(_sr: u32, _ch: usize) -> Result<Self, String> {
            Err("OpenAudio was compiled without Opus support.".to_string())
        }

        pub fn decode_packet(&mut self, _pkt: &[u8]) -> Result<Vec<f32>, String> {
            Err("Opus not available".to_string())
        }

        pub fn decode_lost(&mut self) -> Result<Vec<f32>, String> {
            Err("Opus not available".to_string())
        }
    }
}

pub use inner::{OpusChannelDecoder, OpusChannelEncoder};
#[cfg(feature = "opus")]
pub use inner::OPUS_FRAME_SAMPLES;
