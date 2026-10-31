//! Shared decoding of incoming OpenAudio audio packets.
//!
//! Multichannel Opus is carried as consecutive mono/stereo substreams that
//! share one presentation timestamp. This module assembles those substreams,
//! decodes the resulting frame, and also decodes plain Float32 PCM packets so
//! individual receivers do not need format-specific packet handling.
//!
//! The assembler tolerates the two failure modes that matter to a receiver:
//! a substream that never arrives (decoded with Opus packet-loss concealment
//! once the frame is older than one Opus frame interval) and a stalled or hostile
//! sender (bounded pending-frame count).

use crate::opus::{MultichannelOpusDecoder, OPUS_FRAME_MS, OPUS_FRAME_SAMPLES};
use crate::protocol::{decode_float32, opus_payload, ParsedPacket, SAMPLE_FORMAT_OPUS};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Pending frames beyond this count are decoded or dropped oldest-first so a
/// misbehaving sender cannot grow receiver memory without bound.
const MAX_PENDING_FRAMES: usize = 32;

/// How long to wait for the remaining substreams of a frame before decoding
/// what arrived and concealing the rest.
const SUBSTREAM_WAIT: Duration = Duration::from_millis(OPUS_FRAME_MS as u64);

/// One decoded frame of interleaved audio, with the packet it came from.
pub struct DecodedAudioFrame {
    pub packet: ParsedPacket,
    pub pcm: Vec<f32>,
}

struct DecoderState {
    channels: u8,
    sample_rate: u32,
    decoder: MultichannelOpusDecoder,
}

struct PendingFrame {
    started: Instant,
    representative: ParsedPacket,
    expected_substreams: usize,
    substreams: BTreeMap<u16, Vec<u8>>,
}

/// Counters describing why Opus packets did not become audio.
///
/// Receivers surface these so a silent or glitchy stream can be told apart
/// from a stream that is being rejected packet by packet.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeStats {
    /// Packets rejected as a second copy of a substream already received.
    pub duplicate_substreams: u64,
    /// Packets rejected as malformed, truncated, or unsupported framing.
    pub malformed_packets: u64,
    /// Substreams that never arrived and were concealed by packet-loss
    /// concealment instead of being played.
    pub concealed_substreams: u64,
}

/// Decodes Float32 and Opus OpenAudio packets into interleaved f32 samples.
#[derive(Default)]
pub struct AudioPacketDecoder {
    decoders: HashMap<u32, DecoderState>,
    pending: BTreeMap<(u32, u64), PendingFrame>,
    stats: DecodeStats,
}

impl AudioPacketDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Forgets all per-stream state, for example when a stream restarts.
    pub fn clear(&mut self) {
        self.decoders.clear();
        self.pending.clear();
    }

    /// Decodes one network packet.
    ///
    /// A Float32 packet yields exactly one frame. An Opus substream yields
    /// nothing until its frame is complete, then one frame. Frames that have
    /// been waiting longer than one Opus frame interval are decoded first, with
    /// concealment for missing substreams, so audio does not stall behind a
    /// lost packet.
    pub fn decode(
        &mut self,
        packet: &[u8],
        parsed: &ParsedPacket,
    ) -> Result<Vec<DecodedAudioFrame>, String> {
        if parsed.sample_format != SAMPLE_FORMAT_OPUS {
            self.pending.clear();

            return decode_float32(packet, parsed)
                .map(|pcm| {
                    vec![DecodedAudioFrame {
                        packet: parsed.clone(),
                        pcm,
                    }]
                })
                .ok_or_else(|| "invalid or truncated Float32 PCM payload".to_string());
        }

        let expected_substreams = match validate_opus_packet(packet, parsed) {
            Ok(expected) => expected,
            Err(error) => {
                self.stats.malformed_packets = self.stats.malformed_packets.saturating_add(1);
                return Err(error);
            }
        };

        self.ensure_decoder(parsed)?;

        let key = (parsed.stream_id, parsed.presentation_timestamp_ns);
        let payload = opus_payload(packet, parsed)
            .ok_or_else(|| "invalid or truncated Opus payload".to_string())?;

        if let Some(frame) = self.pending.get(&key) {
            if frame.substreams.contains_key(&parsed.sub_stream_index) {
                self.stats.duplicate_substreams = self.stats.duplicate_substreams.saturating_add(1);

                return Err(format!(
                    "duplicate Opus substream index {}",
                    parsed.sub_stream_index
                ));
            }
        }

        self.pending
            .entry(key)
            .or_insert_with(|| PendingFrame {
                started: Instant::now(),
                representative: parsed.clone(),
                expected_substreams,
                substreams: BTreeMap::new(),
            })
            .substreams
            .insert(parsed.sub_stream_index, payload.to_vec());

        // Emit any frame that is complete or has waited long enough, oldest
        // first, including the frame this packet just completed.
        self.drain_ready(true, Some(key))
    }

    /// Decodes frames that have been waiting longer than one Opus frame interval
    /// and returns them oldest-first.
    ///
    /// Call this when the socket goes quiet. While packets keep arriving the
    /// packet that completes a frame triggers the same drain, but at the tail
    /// of a stream — or through a burst of loss — an incomplete frame would
    /// otherwise sit in `pending` with nothing to flush it.
    pub fn flush_expired(&mut self) -> Result<Vec<DecodedAudioFrame>, String> {
        self.drain_ready(false, None)
    }

    /// Counters for duplicate, malformed, and concealed Opus traffic.
    pub fn stats(&self) -> DecodeStats {
        self.stats
    }

    fn drain_ready(
        &mut self,
        decode_complete: bool,
        current_key: Option<(u32, u64)>,
    ) -> Result<Vec<DecodedAudioFrame>, String> {
        let mut decoded = Vec::new();

        while let Some(oldest_key) = self.pending.keys().next().copied() {
            let over_budget = self.pending.len() > MAX_PENDING_FRAMES;
            let ready = self
                .pending
                .get(&oldest_key)
                .map(|frame| {
                    (decode_complete && frame.substreams.len() >= frame.expected_substreams)
                        || (current_key.is_none_or(|key| oldest_key < key)
                            && frame.started.elapsed() >= SUBSTREAM_WAIT)
                })
                .unwrap_or(false);

            if !ready && !over_budget {
                break;
            }

            let Some(frame) = self.pending.remove(&oldest_key) else {
                break;
            };

            decoded.push(self.decode_frame(oldest_key.0, frame)?);
        }

        Ok(decoded)
    }

    fn ensure_decoder(&mut self, parsed: &ParsedPacket) -> Result<(), String> {
        let needs_decoder = self
            .decoders
            .get(&parsed.stream_id)
            .map(|state| {
                state.channels != parsed.channel_count || state.sample_rate != parsed.sample_rate
            })
            .unwrap_or(true);

        if needs_decoder {
            self.decoders.insert(
                parsed.stream_id,
                DecoderState {
                    channels: parsed.channel_count,
                    sample_rate: parsed.sample_rate,
                    decoder: MultichannelOpusDecoder::new(parsed.channel_count as usize)?,
                },
            );

            // A format change invalidates every partially assembled frame.
            self.pending
                .retain(|(stream_id, _), _| *stream_id != parsed.stream_id);
        }

        Ok(())
    }

    fn decode_frame(
        &mut self,
        stream_id: u32,
        frame: PendingFrame,
    ) -> Result<DecodedAudioFrame, String> {
        let concealed = frame
            .expected_substreams
            .saturating_sub(frame.substreams.len());
        self.stats.concealed_substreams = self
            .stats
            .concealed_substreams
            .saturating_add(concealed as u64);

        let substreams = frame
            .substreams
            .iter()
            .map(|(index, payload)| (*index, payload.as_slice()))
            .collect::<Vec<_>>();

        let decoder = self
            .decoders
            .get_mut(&stream_id)
            .ok_or_else(|| format!("Opus decoder for stream {stream_id} was not initialized"))?;

        Ok(DecodedAudioFrame {
            packet: frame.representative,
            pcm: decoder.decoder.decode(&substreams)?,
        })
    }
}

/// Validates Opus framing shared by every receiver and returns the number of
/// substreams the frame needs.
fn validate_opus_packet(packet: &[u8], parsed: &ParsedPacket) -> Result<usize, String> {
    if parsed.sample_rate != 48_000 {
        return Err(format!(
            "unsupported Opus sample rate {} Hz",
            parsed.sample_rate
        ));
    }

    if !(1..=64).contains(&parsed.channel_count) {
        return Err(format!(
            "unsupported Opus channel count {}",
            parsed.channel_count
        ));
    }

    if parsed.samples_per_channel as usize != OPUS_FRAME_SAMPLES {
        return Err("invalid Opus frame duration".to_string());
    }

    let expected_substreams = (parsed.channel_count as usize).div_ceil(2);

    if parsed.sub_stream_index as usize >= expected_substreams {
        return Err(format!(
            "Opus substream index {} is out of range",
            parsed.sub_stream_index
        ));
    }

    if opus_payload(packet, parsed).is_none() {
        return Err("invalid or truncated Opus payload".to_string());
    }

    Ok(expected_substreams)
}

#[cfg(test)]
mod tests {
    use super::AudioPacketDecoder;
    use crate::opus::{MultichannelOpusEncoder, OPUS_FRAME_SAMPLES};
    use crate::protocol::{encode_opus_packet, parse_packet, PacketHeader};

    fn encode_frame(channels: usize, timestamp_ns: u64) -> Vec<Vec<u8>> {
        let mut encoder = MultichannelOpusEncoder::new(channels, 64_000).unwrap();
        let pcm = (0..OPUS_FRAME_SAMPLES * channels)
            .map(|index| ((index as f32) * 0.01).sin() * 0.25)
            .collect::<Vec<f32>>();

        encoder
            .encode(&pcm, 0)
            .unwrap()
            .into_iter()
            .map(|(sub_stream_index, sequence_number, payload)| {
                encode_opus_packet(
                    &PacketHeader {
                        sub_stream_index,
                        stream_id: 3,
                        sequence_number,
                        presentation_timestamp_ns: timestamp_ns,
                    },
                    channels as u8,
                    48_000,
                    OPUS_FRAME_SAMPLES as u16,
                    &payload,
                )
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn multichannel_frame_is_decoded_only_when_its_substreams_are_complete() {
        let packets = encode_frame(6, 1_000);
        assert_eq!(packets.len(), 3);

        let mut decoder = AudioPacketDecoder::new();

        for (index, packet) in packets.iter().enumerate() {
            let parsed = parse_packet(packet).unwrap();
            let decoded = decoder.decode(packet, &parsed).unwrap();

            if index + 1 < packets.len() {
                assert!(decoded.is_empty(), "frame decoded before it completed");
            } else {
                assert_eq!(decoded.len(), 1);
                assert_eq!(decoded[0].pcm.len(), OPUS_FRAME_SAMPLES * 6);
            }
        }
    }

    #[test]
    fn duplicate_substream_is_rejected_instead_of_overwriting() {
        let packets = encode_frame(4, 2_000);
        let mut decoder = AudioPacketDecoder::new();

        let first = parse_packet(&packets[0]).unwrap();
        decoder.decode(&packets[0], &first).unwrap();

        assert!(decoder.decode(&packets[0], &first).is_err());
    }

    #[test]
    fn float32_packets_decode_without_opus_state() {
        let pcm = vec![0.1_f32, -0.25, 0.5, -0.75];
        let packet = crate::protocol::encode_float32_packet(
            &PacketHeader {
                sub_stream_index: 0,
                stream_id: 9,
                sequence_number: 1,
                presentation_timestamp_ns: 0,
            },
            2,
            48_000,
            &pcm,
        )
        .unwrap();

        let parsed = parse_packet(&packet).unwrap();
        let decoded = AudioPacketDecoder::new().decode(&packet, &parsed).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].pcm.len(), 4);
    }

    #[test]
    fn quiet_socket_flushes_a_concealed_frame() {
        let packets = encode_frame(6, 3_000);
        let mut decoder = AudioPacketDecoder::new();

        let first = parse_packet(&packets[0]).unwrap();
        decoder.decode(&packets[0], &first).unwrap();

        // Nothing is overdue yet, so a flush must not force the frame out.
        assert!(decoder.flush_expired().unwrap().is_empty());

        std::thread::sleep(std::time::Duration::from_millis(30));

        let flushed = decoder.flush_expired().unwrap();
        assert_eq!(flushed.len(), 1, "overdue frame was stranded");
        assert_eq!(flushed[0].pcm.len(), OPUS_FRAME_SAMPLES * 6);
        assert_eq!(decoder.stats().concealed_substreams, 2);

        // The frame is gone afterwards; a second flush must not repeat it.
        assert!(decoder.flush_expired().unwrap().is_empty());
    }

    #[test]
    fn rejected_packets_are_counted_by_reason() {
        let packets = encode_frame(4, 4_000);
        let mut decoder = AudioPacketDecoder::new();
        let first = parse_packet(&packets[0]).unwrap();
        decoder.decode(&packets[0], &first).unwrap();

        assert!(decoder.decode(&packets[0], &first).is_err());
        assert_eq!(decoder.stats().duplicate_substreams, 1);

        let mut unsupported = first.clone();
        unsupported.sample_rate = 44_100;
        assert!(decoder.decode(&packets[0], &unsupported).is_err());
        assert_eq!(decoder.stats().malformed_packets, 1);
        assert_eq!(decoder.stats().duplicate_substreams, 1);
    }
}
