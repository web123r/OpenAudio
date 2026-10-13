/// Fixed 24-byte OpenAudio packet header, per protocol spec section 3.1.
pub struct PacketHeader {
    pub sub_stream_index: u16,
    pub stream_id: u32,
    pub sequence_number: u32,
    pub presentation_timestamp_ns: u64,
}

const MAGIC: [u8; 4] = *b"OAv1";
const VERSION: u8 = 1;
const PACKET_TYPE_AUDIO: u8 = 0x01;
pub const MAX_SAFE_UDP_PAYLOAD_BYTES: usize = 1_200;
pub const OPENAUDIO_HEADER_BYTES: usize = 32;

/// Calculates the largest Float32 frame count that keeps the complete
/// OpenAudio packet within the conservative UDP payload budget.
pub fn max_float32_frames_per_packet(channel_count: usize) -> Result<usize, String> {
    if channel_count == 0 {
        return Err("cannot calculate packet size for zero channels".to_string());
    }

    let bytes_per_frame = channel_count
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| "Float32 packet frame size overflow".to_string())?;
    let audio_budget = MAX_SAFE_UDP_PAYLOAD_BYTES
        .checked_sub(OPENAUDIO_HEADER_BYTES)
        .ok_or_else(|| "UDP payload budget is smaller than OpenAudio headers".to_string())?;
    let frames = (audio_budget / bytes_per_frame).min(u16::MAX as usize);

    if frames == 0 {
        return Err(format!(
            "{channel_count} Float32 channel(s) cannot fit one frame within the \
             {MAX_SAFE_UDP_PAYLOAD_BYTES}-byte UDP payload budget"
        ));
    }

    Ok(frames)
}

impl PacketHeader {
    pub fn to_bytes(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = VERSION;
        buf[5] = PACKET_TYPE_AUDIO;
        buf[6..8].copy_from_slice(&self.sub_stream_index.to_be_bytes());
        buf[8..12].copy_from_slice(&self.stream_id.to_be_bytes());
        buf[12..16].copy_from_slice(&self.sequence_number.to_be_bytes());
        buf[16..24].copy_from_slice(&self.presentation_timestamp_ns.to_be_bytes());
        buf
    }
}

/// 8-byte audio payload header, per protocol spec section 3.2.
pub struct AudioPayloadHeader {
    pub channel_count: u8,
    pub sample_format: u8,
    pub sample_rate_code: u16,
    pub samples_per_channel: u16,
}

pub const SAMPLE_FORMAT_FLOAT32: u8 = 0x03;
/// Reserved wire-format identifier; no OpenAudio Opus encoder/decoder is implemented.
pub const SAMPLE_FORMAT_OPUS: u8 = 0x10;
/// Reserved wire-format identifier; no OpenAudio FLAC encoder/decoder is implemented.
pub const SAMPLE_FORMAT_FLAC: u8 = 0x11;

pub const SAMPLE_RATE_48000: u16 = 0x01;
pub const SAMPLE_RATE_44100: u16 = 0x02;
pub const SAMPLE_RATE_88200: u16 = 0x03;
pub const SAMPLE_RATE_96000: u16 = 0x04;
pub const SAMPLE_RATE_176400: u16 = 0x05;
pub const SAMPLE_RATE_192000: u16 = 0x06;
pub const SAMPLE_RATE_8000: u16 = 0x07;
pub const SAMPLE_RATE_16000: u16 = 0x08;
pub const SAMPLE_RATE_24000: u16 = 0x09;
pub const SAMPLE_RATE_32000: u16 = 0x0A;

impl AudioPayloadHeader {
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0] = self.channel_count;
        buf[1] = self.sample_format;
        buf[2..4].copy_from_slice(&self.sample_rate_code.to_be_bytes());
        buf[4..6].copy_from_slice(&self.samples_per_channel.to_be_bytes());
        buf
    }
}

pub fn sample_rate_to_code(rate: u32) -> u16 {
    match rate {
        48000 => SAMPLE_RATE_48000,
        44100 => SAMPLE_RATE_44100,
        88200 => SAMPLE_RATE_88200,
        96000 => SAMPLE_RATE_96000,
        176400 => SAMPLE_RATE_176400,
        192000 => SAMPLE_RATE_192000,
        8000 => SAMPLE_RATE_8000,
        16000 => SAMPLE_RATE_16000,
        24000 => SAMPLE_RATE_24000,
        32000 => SAMPLE_RATE_32000,
        _ => 0,
    }
}

#[derive(Debug)]
pub struct ParsedPacket {
    pub stream_id: u32,
    pub sequence_number: u32,
    pub presentation_timestamp_ns: u64,
    pub channel_count: u8,
    pub sample_format: u8,
    pub sample_rate: u32,
    pub samples_per_channel: u16,
    pub payload_offset: usize,
}

pub fn code_to_sample_rate(code: u16) -> u32 {
    match code {
        SAMPLE_RATE_48000 => 48000,
        SAMPLE_RATE_44100 => 44100,
        SAMPLE_RATE_88200 => 88200,
        SAMPLE_RATE_96000 => 96000,
        SAMPLE_RATE_176400 => 176400,
        SAMPLE_RATE_192000 => 192000,
        SAMPLE_RATE_8000 => 8000,
        SAMPLE_RATE_16000 => 16000,
        SAMPLE_RATE_24000 => 24000,
        SAMPLE_RATE_32000 => 32000,
        _ => 0,
    }
}

pub fn sample_format_name(sample_format: u8) -> Option<&'static str> {
    match sample_format {
        SAMPLE_FORMAT_FLOAT32 => Some("Float32 PCM"),
        SAMPLE_FORMAT_OPUS => Some("Opus"),
        SAMPLE_FORMAT_FLAC => Some("FLAC"),
        _ => None,
    }
}

pub fn encode_float32_packet(
    header: &PacketHeader,
    channel_count: u8,
    sample_rate: u32,
    interleaved_samples: &[f32],
) -> Result<Vec<u8>, String> {
    if !(1..=64).contains(&channel_count) {
        return Err(format!("unsupported channel count: {channel_count}"));
    }

    let sample_rate_code = sample_rate_to_code(sample_rate);
    if sample_rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let channels = channel_count as usize;
    if interleaved_samples.is_empty() || interleaved_samples.len() % channels != 0 {
        return Err("Float32 packet samples must contain complete interleaved frames".to_string());
    }

    let frames = interleaved_samples.len() / channels;
    let max_frames = max_float32_frames_per_packet(channels)?;
    if frames > max_frames || frames > u16::MAX as usize {
        return Err(format!(
            "{frames} audio frames exceed the safe packet limit of {max_frames}"
        ));
    }

    let payload_bytes = interleaved_samples
        .len()
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| "Float32 packet payload size overflow".to_string())?;
    let packet_bytes = OPENAUDIO_HEADER_BYTES
        .checked_add(payload_bytes)
        .ok_or_else(|| "Float32 packet size overflow".to_string())?;

    let mut packet = Vec::with_capacity(packet_bytes);
    packet.extend_from_slice(&header.to_bytes());
    packet.extend_from_slice(
        &AudioPayloadHeader {
            channel_count,
            sample_format: SAMPLE_FORMAT_FLOAT32,
            sample_rate_code,
            samples_per_channel: frames as u16,
        }
        .to_bytes(),
    );
    for sample in interleaved_samples {
        packet.extend_from_slice(&sample.to_le_bytes());
    }

    Ok(packet)
}

pub fn parse_packet(buf: &[u8]) -> Option<ParsedPacket> {
    if buf.len() < 32 || &buf[0..4] != b"OAv1" || buf[4] != 1 || buf[5] != 0x01 {
        return None;
    }

    let stream_id = u32::from_be_bytes(buf[8..12].try_into().ok()?);
    let sequence_number = u32::from_be_bytes(buf[12..16].try_into().ok()?);
    let presentation_timestamp_ns = u64::from_be_bytes(buf[16..24].try_into().ok()?);
    let channel_count = buf[24];
    let sample_format = buf[25];
    if sample_format != SAMPLE_FORMAT_FLOAT32
        && sample_format != SAMPLE_FORMAT_OPUS
        && sample_format != SAMPLE_FORMAT_FLAC
    {
        return None;
    }
    let sample_rate = code_to_sample_rate(u16::from_be_bytes(buf[26..28].try_into().ok()?));
    let samples_per_channel = u16::from_be_bytes(buf[28..30].try_into().ok()?);
    if channel_count == 0 || sample_rate == 0 || samples_per_channel == 0 {
        return None;
    }

    Some(ParsedPacket {
        stream_id,
        sequence_number,
        presentation_timestamp_ns,
        channel_count,
        sample_format,
        sample_rate,
        samples_per_channel,
        payload_offset: 32,
    })
}

/// Returns the interleaved little-endian Float32 payload when it is complete.
///
/// Compressed payloads have format-specific sizes and must be decoded by a
/// format-aware consumer instead of being treated as PCM.
pub fn float32_payload<'a>(buf: &'a [u8], packet: &ParsedPacket) -> Option<&'a [u8]> {
    if packet.sample_format != SAMPLE_FORMAT_FLOAT32 || packet.payload_offset > buf.len() {
        return None;
    }

    let samples =
        (packet.samples_per_channel as usize).checked_mul(packet.channel_count as usize)?;
    let payload_len = samples.checked_mul(std::mem::size_of::<f32>())?;
    let payload_end = packet.payload_offset.checked_add(payload_len)?;
    buf.get(packet.payload_offset..payload_end)
}

/// Decodes a complete interleaved little-endian Float32 audio packet.
pub fn decode_float32(buf: &[u8], packet: &ParsedPacket) -> Option<Vec<f32>> {
    let payload = float32_payload(buf, packet)?;
    Some(
        payload
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|sample| {
                let value = f32::from_le_bytes(sample.try_into().ok()?);
                value.is_finite().then_some(value)
            })
            .collect::<Option<Vec<_>>>()?,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        decode_float32, float32_payload, max_float32_frames_per_packet, parse_packet,
        AudioPayloadHeader, PacketHeader, MAX_SAFE_UDP_PAYLOAD_BYTES, OPENAUDIO_HEADER_BYTES,
        SAMPLE_FORMAT_FLOAT32,
    };

    #[test]
    fn packet_header_and_payload_round_trip() {
        let mut packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id: 7,
                sequence_number: 3,
                presentation_timestamp_ns: 99,
            }
            .to_bytes(),
        );
        packet.extend(
            AudioPayloadHeader {
                channel_count: 2,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 4,
            }
            .to_bytes(),
        );
        packet.extend([0u8; 16]);

        let parsed = parse_packet(&packet).expect("valid packet");
        assert_eq!(parsed.stream_id, 7);
        assert_eq!(parsed.channel_count, 2);
        assert_eq!(parsed.sample_rate, 48_000);
    }

    #[test]
    fn float32_packet_encoder_preserves_interleaved_samples_and_limits() {
        let header = PacketHeader {
            sub_stream_index: 0,
            stream_id: 7,
            sequence_number: 3,
            presentation_timestamp_ns: 99,
        };
        let samples = [0.25, -0.5, 0.75, -1.0];
        let packet = super::encode_float32_packet(&header, 2, 48_000, &samples).unwrap();
        let parsed = parse_packet(&packet).expect("encoded packet should parse");

        assert_eq!(parsed.samples_per_channel, 2);
        assert_eq!(decode_float32(&packet, &parsed).unwrap(), samples);
        assert!(super::encode_float32_packet(&header, 2, 48_000, &[0.25]).is_err());
        assert!(super::encode_float32_packet(&header, 2, 48_000, &[0.0; 600]).is_err());
    }

    #[test]
    fn safe_packet_frame_budget_respects_udp_limit() {
        for channels in [1, 2, 8, 16, 32, 64] {
            let frames = max_float32_frames_per_packet(channels).unwrap();
            let bytes = OPENAUDIO_HEADER_BYTES + frames * channels * 4;
            assert!(bytes <= MAX_SAFE_UDP_PAYLOAD_BYTES);

            if frames < u16::MAX as usize {
                assert!(
                    OPENAUDIO_HEADER_BYTES + (frames + 1) * channels * 4
                        > MAX_SAFE_UDP_PAYLOAD_BYTES
                );
            }
        }

        assert_eq!(max_float32_frames_per_packet(64).unwrap(), 4);
        assert!(max_float32_frames_per_packet(0).is_err());
    }

    #[test]
    fn test_multichannel_payload_limits() {
        // Validate 2, 8, 16, 32, 64 channels
        let channel_counts = [2, 8, 16, 32, 64];
        for &channels in &channel_counts {
            let mut packet = Vec::from(
                PacketHeader {
                    sub_stream_index: 0,
                    stream_id: 1,
                    sequence_number: 1,
                    presentation_timestamp_ns: 0,
                }
                .to_bytes(),
            );
            packet.extend(
                AudioPayloadHeader {
                    channel_count: channels as u8,
                    sample_format: SAMPLE_FORMAT_FLOAT32,
                    sample_rate_code: 1,
                    samples_per_channel: 64, // smaller block for large channels
                }
                .to_bytes(),
            );

            let payload_size = channels as usize * 64 * 4;
            packet.extend(vec![0u8; payload_size]);

            let parsed = parse_packet(&packet).unwrap();
            assert_eq!(parsed.channel_count, channels as u8);
            assert_eq!(packet.len(), 32 + payload_size);
        }
    }

    #[test]
    fn test_protocol_compatibility_labels() {
        // The current packet structure doesn't inline channel labels into the PCM stream
        // (they are sent via Discovery NodeAdvertisement). This test ensures the PCM packet
        // remains exactly 32 bytes of headers regardless of logical labels.
        let packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id: 1,
                sequence_number: 1,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );
        assert_eq!(packet.len(), 24);

        let mut full_packet = packet.clone();
        full_packet.extend(
            AudioPayloadHeader {
                channel_count: 2,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 1,
            }
            .to_bytes(),
        );
        full_packet.extend([0u8; 8]); // 2 channels * 1 sample * 4 bytes

        let parsed = parse_packet(&full_packet).unwrap();
        assert_eq!(parsed.payload_offset, 32);
    }

    #[test]
    fn parser_recognizes_reserved_compressed_format_identifiers_without_decoding() {
        use super::{SAMPLE_FORMAT_FLAC, SAMPLE_FORMAT_OPUS};

        for &fmt in &[SAMPLE_FORMAT_OPUS, SAMPLE_FORMAT_FLAC] {
            let mut packet = Vec::from(
                PacketHeader {
                    sub_stream_index: 0,
                    stream_id: 42,
                    sequence_number: 100,
                    presentation_timestamp_ns: 12345,
                }
                .to_bytes(),
            );
            packet.extend(
                AudioPayloadHeader {
                    channel_count: 2,
                    sample_format: fmt,
                    sample_rate_code: 1,
                    samples_per_channel: 480,
                }
                .to_bytes(),
            );
            packet.extend([0u8; 64]);

            let parsed = parse_packet(&packet).expect("valid opus/flac header");
            assert_eq!(parsed.sample_format, fmt);
            assert_eq!(parsed.stream_id, 42);
        }
    }

    #[test]
    fn float32_helpers_validate_format_and_payload_length() {
        let mut packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id: 1,
                sequence_number: 1,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );
        packet.extend(
            AudioPayloadHeader {
                channel_count: 2,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 2,
            }
            .to_bytes(),
        );
        for sample in [0.25_f32, -0.5, 0.75, -1.0] {
            packet.extend_from_slice(&sample.to_le_bytes());
        }

        let parsed = parse_packet(&packet).unwrap();
        assert_eq!(
            decode_float32(&packet, &parsed).unwrap(),
            [0.25, -0.5, 0.75, -1.0]
        );
        assert_eq!(float32_payload(&packet[..packet.len() - 1], &parsed), None);
    }

    #[test]
    fn parser_rejects_invalid_stream_format_fields() {
        let mut packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id: 1,
                sequence_number: 1,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );
        packet.extend(
            AudioPayloadHeader {
                channel_count: 0,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 1,
            }
            .to_bytes(),
        );
        packet.extend([0; 4]);
        assert!(parse_packet(&packet).is_none());

        packet[24] = 1;
        packet[26..28].copy_from_slice(&0_u16.to_be_bytes());
        assert!(parse_packet(&packet).is_none());
    }
}
