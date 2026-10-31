use opus_rs::{Application, OpusDecoder, OpusEncoder};
use std::collections::HashSet;

pub const OPUS_SAMPLE_RATE: i32 = 48_000;
/// Low-latency Opus frame duration used by the OpenAudio wire profile.
pub const OPUS_FRAME_MS: usize = 10;
pub const OPUS_FRAME_SAMPLES: usize = OPUS_SAMPLE_RATE as usize * OPUS_FRAME_MS / 1_000;
pub const OPUS_FRAME_DURATION_NS: u64 = OPUS_FRAME_MS as u64 * 1_000_000;

pub struct OpusChannelEncoder {
    encoder: OpusEncoder,
    channels: usize,
    pcm_scratch: Vec<f32>,
}

impl OpusChannelEncoder {
    pub fn new(channels: usize, bitrate_bps: i32) -> Result<Self, String> {
        if !(1..=2).contains(&channels) {
            return Err(format!(
                "Opus encoder supports only 1 or 2 channels, got {channels}"
            ));
        }

        let mut encoder = OpusEncoder::new(OPUS_SAMPLE_RATE, channels, Application::Audio)
            .map_err(|error| format!("failed to create Opus encoder: {error}"))?;
        encoder.bitrate_bps = bitrate_bps.max(6_000);
        encoder.use_inband_fec = true;
        encoder.packet_loss_perc = 10;
        encoder.complexity = 5;

        Ok(Self {
            encoder,
            channels,
            pcm_scratch: Vec::with_capacity(OPUS_FRAME_SAMPLES * channels),
        })
    }

    pub fn encode(&mut self, interleaved_pcm: &[f32]) -> Result<Vec<u8>, String> {
        let expected_samples = OPUS_FRAME_SAMPLES * self.channels;
        if interleaved_pcm.len() != expected_samples {
            return Err(format!(
                "Opus encoder requires {expected_samples} PCM samples for {} channels, got {}",
                self.channels,
                interleaved_pcm.len()
            ));
        }
        Self::encode_frame(&mut self.encoder, interleaved_pcm)
    }

    fn encode_frame(encoder: &mut OpusEncoder, interleaved_pcm: &[f32]) -> Result<Vec<u8>, String> {
        let mut packet = vec![0_u8; 1276];
        let encoded_len = encoder
            .encode(interleaved_pcm, OPUS_FRAME_SAMPLES, &mut packet)
            .map_err(|error| format!("Opus encode failed: {error}"))?;
        packet.truncate(encoded_len);
        Ok(packet)
    }

    fn encode_multichannel(
        &mut self,
        interleaved_pcm: &[f32],
        source_channels: usize,
        source_channel: usize,
    ) -> Result<Vec<u8>, String> {
        self.pcm_scratch.clear();
        for frame in interleaved_pcm.chunks_exact(source_channels) {
            self.pcm_scratch
                .extend_from_slice(&frame[source_channel..source_channel + self.channels]);
        }
        Self::encode_frame(&mut self.encoder, &self.pcm_scratch)
    }
}

pub struct MultichannelOpusEncoder {
    encoders: Vec<OpusChannelEncoder>,
    channel_count: usize,
}

impl MultichannelOpusEncoder {
    pub fn new(channel_count: usize, bitrate_per_substream_bps: i32) -> Result<Self, String> {
        if channel_count == 0 || channel_count > 64 {
            return Err(format!("unsupported Opus channel count: {channel_count}"));
        }

        let mut encoders = Vec::new();
        let mut remaining = channel_count;
        while remaining > 0 {
            let channels = remaining.min(2);
            encoders.push(OpusChannelEncoder::new(
                channels,
                bitrate_per_substream_bps,
            )?);
            remaining -= channels;
        }

        Ok(Self {
            encoders,
            channel_count,
        })
    }

    /// Encodes one 10 ms interleaved frame. Sequence numbers are consecutive
    /// across the individual mono/stereo substream datagrams in the frame.
    pub fn encode(
        &mut self,
        interleaved_pcm: &[f32],
        starting_sequence: u32,
    ) -> Result<Vec<(u16, u32, Vec<u8>)>, String> {
        let expected_samples = OPUS_FRAME_SAMPLES * self.channel_count;
        if interleaved_pcm.len() != expected_samples {
            return Err(format!(
                "Opus multichannel encoder requires {expected_samples} PCM samples, got {}",
                interleaved_pcm.len()
            ));
        }

        let mut packets = Vec::with_capacity(self.encoders.len());
        let mut source_channel = 0usize;
        let mut sequence = starting_sequence;

        for (substream_index, encoder) in self.encoders.iter_mut().enumerate() {
            let channels = encoder.channels;
            packets.push((
                substream_index as u16,
                sequence,
                encoder.encode_multichannel(interleaved_pcm, self.channel_count, source_channel)?,
            ));
            sequence = sequence.wrapping_add(1);
            source_channel += channels;
        }

        Ok(packets)
    }
}

pub struct OpusChannelDecoder {
    decoder: OpusDecoder,
    channels: usize,
}

impl OpusChannelDecoder {
    pub fn new(channels: usize) -> Result<Self, String> {
        if !(1..=2).contains(&channels) {
            return Err(format!(
                "Opus decoder supports only 1 or 2 channels, got {channels}"
            ));
        }

        let decoder = OpusDecoder::new(OPUS_SAMPLE_RATE, channels)
            .map_err(|error| format!("failed to create Opus decoder: {error}"))?;
        Ok(Self { decoder, channels })
    }

    pub fn decode(&mut self, packet: &[u8]) -> Result<Vec<f32>, String> {
        self.decode_frame(packet)
    }

    pub fn conceal_loss(&mut self) -> Result<Vec<f32>, String> {
        self.decode_frame(&[])
    }

    fn decode_frame(&mut self, packet: &[u8]) -> Result<Vec<f32>, String> {
        let mut output = vec![0.0_f32; OPUS_FRAME_SAMPLES * self.channels];
        let decoded_frames = self
            .decoder
            .decode(packet, OPUS_FRAME_SAMPLES, &mut output)
            .map_err(|error| format!("Opus decode failed: {error}"))?;
        if decoded_frames != OPUS_FRAME_SAMPLES {
            return Err(format!(
                "Opus decoder returned {decoded_frames} frames; expected {OPUS_FRAME_SAMPLES}"
            ));
        }
        Ok(output)
    }
}

pub struct MultichannelOpusDecoder {
    decoders: Vec<OpusChannelDecoder>,
    channel_count: usize,
}

impl MultichannelOpusDecoder {
    pub fn new(channel_count: usize) -> Result<Self, String> {
        if channel_count == 0 || channel_count > 64 {
            return Err(format!("unsupported Opus channel count: {channel_count}"));
        }

        let mut decoders = Vec::new();
        let mut remaining = channel_count;
        while remaining > 0 {
            let channels = remaining.min(2);
            decoders.push(OpusChannelDecoder::new(channels)?);
            remaining -= channels;
        }

        Ok(Self {
            decoders,
            channel_count,
        })
    }

    /// Decodes the supplied substreams and uses Opus packet-loss concealment
    /// for each missing substream. Invalid, duplicate, or out-of-range
    /// substream indexes are rejected rather than silently misrouted.
    pub fn decode(&mut self, packets: &[(u16, &[u8])]) -> Result<Vec<f32>, String> {
        let mut seen = HashSet::with_capacity(packets.len());
        for (index, _) in packets {
            if *index as usize >= self.decoders.len() {
                return Err(format!("Opus substream index {index} is out of range"));
            }
            if !seen.insert(*index) {
                return Err(format!("duplicate Opus substream index {index}"));
            }
        }

        let mut output = vec![0.0_f32; OPUS_FRAME_SAMPLES * self.channel_count];
        let mut destination_channel = 0usize;

        for (expected_index, decoder) in self.decoders.iter_mut().enumerate() {
            let decoded = match packets
                .iter()
                .find(|(index, _)| *index == expected_index as u16)
            {
                Some((_, payload)) => decoder.decode(payload)?,
                None => decoder.conceal_loss()?,
            };
            let channels = decoder.channels;

            for frame in 0..OPUS_FRAME_SAMPLES {
                for channel in 0..channels {
                    output[frame * self.channel_count + destination_channel + channel] =
                        decoded[frame * channels + channel];
                }
            }
            destination_channel += channels;
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine_frame(channels: usize) -> Vec<f32> {
        (0..OPUS_FRAME_SAMPLES)
            .flat_map(|frame| {
                (0..channels).map(move |channel| {
                    let phase = frame as f32 / 480.0 + channel as f32 * 0.17;
                    phase.sin() * 0.25
                })
            })
            .collect()
    }

    #[test]
    fn mono_opus_round_trip() {
        let mut encoder = OpusChannelEncoder::new(1, 64_000).unwrap();
        let mut decoder = OpusChannelDecoder::new(1).unwrap();
        let encoded = encoder.encode(&sine_frame(1)).unwrap();
        let decoded = decoder.decode(&encoded).unwrap();

        assert_eq!(decoded.len(), OPUS_FRAME_SAMPLES);
        assert!(!encoded.is_empty() && encoded.len() < 1276);
        assert!(decoded.iter().all(|sample| sample.is_finite()));
    }

    #[test]
    fn multichannel_round_trip_assigns_consecutive_packet_sequences() {
        let mut encoder = MultichannelOpusEncoder::new(5, 128_000).unwrap();
        let mut decoder = MultichannelOpusDecoder::new(5).unwrap();
        let packets = encoder.encode(&sine_frame(5), u32::MAX - 1).unwrap();

        assert_eq!(
            packets
                .iter()
                .map(|(_, sequence, _)| *sequence)
                .collect::<Vec<_>>(),
            [u32::MAX - 1, u32::MAX, 0]
        );
        let packet_refs = packets
            .iter()
            .map(|(index, _, payload)| (*index, payload.as_slice()))
            .collect::<Vec<_>>();
        let decoded = decoder.decode(&packet_refs).unwrap();

        assert_eq!(decoded.len(), OPUS_FRAME_SAMPLES * 5);
        assert!(decoded.iter().all(|sample| sample.is_finite()));
    }

    #[test]
    fn missing_substream_is_concealed_and_invalid_indexes_are_rejected() {
        let mut encoder = MultichannelOpusEncoder::new(4, 128_000).unwrap();
        let mut decoder = MultichannelOpusDecoder::new(4).unwrap();
        let packets = encoder.encode(&sine_frame(4), 10).unwrap();
        let available = [(packets[0].0, packets[0].2.as_slice())];
        let decoded = decoder.decode(&available).unwrap();
        assert_eq!(decoded.len(), OPUS_FRAME_SAMPLES * 4);
        assert!(decoded.iter().all(|sample| sample.is_finite()));
        assert!(decoder.decode(&[(2, &[]), (2, &[])]).is_err());
        assert!(decoder.decode(&[(4, &[])]).is_err());
    }
}
