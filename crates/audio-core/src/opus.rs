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

    /// Stateful Opus encoder for mono, stereo, or multichannel streams (up to 64 channels).
    pub struct OpusChannelEncoder {
        encoders: Vec<OpusEncoder>,
        pending: Vec<f32>,
        channels: usize,
        substream_channels: Vec<usize>,
        frame_samples: usize,
        encode_buf: Vec<u8>,
        substream_buf: Vec<f32>,
    }

    impl OpusChannelEncoder {
        /// Creates a new encoder supporting 1 to 64 channels.
        ///
        /// For multichannel streams (>2 channels), the encoder partitions channels
        /// into stereo pairs (with an optional mono trailer for odd channel counts)
        /// and sets bitrates appropriately so network packets remain compact and MTU-safe.
        pub fn new(
            sample_rate: u32,
            channel_count: usize,
            bitrate_kbps: i32,
        ) -> Result<Self, String> {
            if channel_count == 0 || channel_count > 64 {
                return Err(format!(
                    "Opus channel count must be between 1 and 64; got {channel_count}"
                ));
            }

            let mut substream_channels = Vec::new();
            if channel_count <= 2 {
                substream_channels.push(channel_count);
            } else {
                let mut remaining = channel_count;
                while remaining > 0 {
                    if remaining >= 2 {
                        substream_channels.push(2);
                        remaining -= 2;
                    } else {
                        substream_channels.push(1);
                        remaining -= 1;
                    }
                }
            }

            let num_substreams = substream_channels.len();
            let per_substream_kbps = if channel_count <= 2 {
                bitrate_kbps.clamp(16, 256)
            } else if bitrate_kbps >= (32 * num_substreams as i32) {
                (bitrate_kbps / num_substreams as i32).clamp(24, 128)
            } else {
                // 32 kbps per stereo pair is broadcast-grade and fits 48 channels into ~1000 bytes (< 1200 MTU)
                32
            };

            let mut encoders = Vec::with_capacity(num_substreams);
            for &sub_ch in &substream_channels {
                let mut enc = OpusEncoder::new(
                    sample_rate as i32,
                    sub_ch,
                    Application::Audio,
                )
                .map_err(|e| format!("failed to create Opus encoder: {e}"))?;
                enc.bitrate_bps = per_substream_kbps * 1_000;
                encoders.push(enc);
            }

            Ok(Self {
                encoders,
                pending: Vec::new(),
                channels: channel_count,
                substream_channels,
                frame_samples: OPUS_FRAME_SAMPLES * channel_count,
                encode_buf: vec![0u8; MAX_OPUS_PACKET_BYTES],
                substream_buf: Vec::with_capacity(OPUS_FRAME_SAMPLES * 2),
            })
        }

        /// Feeds interleaved Float32 samples into the encoder.
        ///
        /// Returns `Ok(Some(packet))` whenever a complete 10 ms frame has been
        /// encoded. Returns `Ok(None)` when more samples are needed.
        pub fn push_samples(&mut self, samples: &[f32]) -> Result<Option<Vec<u8>>, String> {
            self.pending.extend_from_slice(samples);

            if self.pending.len() < self.frame_samples {
                return Ok(None);
            }

            let frame = &self.pending[..self.frame_samples];
            let mut packet = Vec::with_capacity(256 * self.encoders.len());

            let mut ch_offset = 0;
            for (s, &sub_ch) in self.substream_channels.iter().enumerate() {
                self.substream_buf.clear();
                for f in 0..OPUS_FRAME_SAMPLES {
                    let base = f * self.channels + ch_offset;
                    self.substream_buf.push(frame[base]);
                    if sub_ch == 2 {
                        self.substream_buf.push(frame[base + 1]);
                    }
                }
                ch_offset += sub_ch;

                let encoded_len = self.encoders[s]
                    .encode(&self.substream_buf, OPUS_FRAME_SAMPLES, &mut self.encode_buf)
                    .map_err(|e| format!("Opus encode error on sub-stream {s}: {e}"))?;

                let payload = &self.encode_buf[..encoded_len];
                let len_u16 = encoded_len as u16;
                packet.push((len_u16 >> 8) as u8);
                packet.push((len_u16 & 0xFF) as u8);
                packet.extend_from_slice(payload);
            }

            self.pending.drain(..self.frame_samples);
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

    /// Stateful Opus decoder for mono, stereo, or multichannel streams (up to 64 channels).
    pub struct OpusChannelDecoder {
        decoders: Vec<OpusDecoder>,
        channels: usize,
        substream_channels: Vec<usize>,
        output_buf: Vec<f32>,
        substream_buf: Vec<f32>,
    }

    impl OpusChannelDecoder {
        pub fn new(sample_rate: u32, channel_count: usize) -> Result<Self, String> {
            if channel_count == 0 || channel_count > 64 {
                return Err(format!(
                    "Opus channel count must be between 1 and 64; got {channel_count}"
                ));
            }

            let mut substream_channels = Vec::new();
            if channel_count <= 2 {
                substream_channels.push(channel_count);
            } else {
                let mut remaining = channel_count;
                while remaining > 0 {
                    if remaining >= 2 {
                        substream_channels.push(2);
                        remaining -= 2;
                    } else {
                        substream_channels.push(1);
                        remaining -= 1;
                    }
                }
            }

            let mut decoders = Vec::with_capacity(substream_channels.len());
            for &sub_ch in &substream_channels {
                let dec = OpusDecoder::new(sample_rate as i32, sub_ch)
                    .map_err(|e| format!("failed to create Opus decoder: {e}"))?;
                decoders.push(dec);
            }

            Ok(Self {
                decoders,
                channels: channel_count,
                substream_channels,
                output_buf: vec![0.0f32; OPUS_FRAME_SAMPLES * channel_count],
                substream_buf: vec![0.0f32; OPUS_FRAME_SAMPLES * 2],
            })
        }

        /// Decodes an Opus packet with length-prefixed sub-streams.
        ///
        /// Returns interleaved Float32 PCM for `OPUS_FRAME_SAMPLES` frames.
        pub fn decode_packet(&mut self, packet: &[u8]) -> Result<Vec<f32>, String> {
            let mut offset = 0;
            let mut ch_offset = 0;

            for (s, &sub_ch) in self.substream_channels.iter().enumerate() {
                if offset + 2 > packet.len() {
                    return Err("Opus packet truncated: missing 2-byte length prefix".to_string());
                }
                let declared_len = u16::from_be_bytes([packet[offset], packet[offset + 1]]) as usize;
                offset += 2;
                if offset + declared_len > packet.len() {
                    return Err(format!(
                        "Opus packet truncated on sub-stream {s}: declared {declared_len} bytes, got {}",
                        packet.len() - offset
                    ));
                }
                let opus_data = &packet[offset..offset + declared_len];
                offset += declared_len;

                let decoded_frames = self.decoders[s]
                    .decode(opus_data, OPUS_FRAME_SAMPLES, &mut self.substream_buf)
                    .map_err(|e| format!("Opus decode error on sub-stream {s}: {e}"))?;

                for f in 0..decoded_frames {
                    let out_base = f * self.channels + ch_offset;
                    self.output_buf[out_base] = self.substream_buf[f * sub_ch];
                    if sub_ch == 2 {
                        self.output_buf[out_base + 1] = self.substream_buf[f * sub_ch + 1];
                    }
                }
                ch_offset += sub_ch;
            }

            Ok(self.output_buf.clone())
        }

        /// Generates a comfort-noise PLC frame when a packet is lost.
        pub fn decode_lost(&mut self) -> Result<Vec<f32>, String> {
            let mut ch_offset = 0;
            for (s, &sub_ch) in self.substream_channels.iter().enumerate() {
                let decoded_frames = self.decoders[s]
                    .decode(&[], OPUS_FRAME_SAMPLES, &mut self.substream_buf)
                    .map_err(|e| format!("Opus PLC decode error on sub-stream {s}: {e}"))?;

                for f in 0..decoded_frames {
                    let out_base = f * self.channels + ch_offset;
                    self.output_buf[out_base] = self.substream_buf[f * sub_ch];
                    if sub_ch == 2 {
                        self.output_buf[out_base + 1] = self.substream_buf[f * sub_ch + 1];
                    }
                }
                ch_offset += sub_ch;
            }

            Ok(self.output_buf.clone())
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

        #[test]
        fn opus_48_channel_encode_decode_roundtrip() {
            let channels = 48;
            let mut encoder = OpusChannelEncoder::new(48_000, channels, 128).unwrap();
            let mut decoder = OpusChannelDecoder::new(48_000, channels).unwrap();

            // Warm up
            let warmup = vec![0.0f32; OPUS_FRAME_SAMPLES * channels];
            let _ = encoder.push_samples(&warmup).unwrap();

            // 48-channel interleaved test frame
            let pcm: Vec<f32> = (0..OPUS_FRAME_SAMPLES * channels)
                .map(|i| {
                    let frame = i / channels;
                    let ch = i % channels;
                    (2.0 * std::f32::consts::PI * (220.0 + ch as f32 * 20.0) * frame as f32 / 48_000.0).sin() * 0.4
                })
                .collect();

            let packet = encoder.push_samples(&pcm).unwrap().expect("should produce a packet");
            // Verify that 48 channels of Opus fits comfortably within 1200 bytes MTU!
            assert!(packet.len() < 1200, "Opus 48ch packet size {} exceeds 1200 bytes limit", packet.len());

            let recovered = decoder.decode_packet(&packet).unwrap();
            assert_eq!(recovered.len(), OPUS_FRAME_SAMPLES * channels);
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
