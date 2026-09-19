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

pub fn parse_packet(buf: &[u8]) -> Option<ParsedPacket> {
    if buf.len() < 32 || &buf[0..4] != b"OAv1" || buf[4] != 1 || buf[5] != 0x01 {
        return None;
    }

    let stream_id = u32::from_be_bytes(buf[8..12].try_into().ok()?);
    let sequence_number = u32::from_be_bytes(buf[12..16].try_into().ok()?);
    let presentation_timestamp_ns = u64::from_be_bytes(buf[16..24].try_into().ok()?);
    let channel_count = buf[24];
    if buf[25] != SAMPLE_FORMAT_FLOAT32 {
        return None;
    }

    Some(ParsedPacket {
        stream_id,
        sequence_number,
        presentation_timestamp_ns,
        channel_count,
        sample_rate: code_to_sample_rate(u16::from_be_bytes(buf[26..28].try_into().ok()?)),
        samples_per_channel: u16::from_be_bytes(buf[28..30].try_into().ok()?),
        payload_offset: 32,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_packet, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};

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
        packet.extend(AudioPayloadHeader {
            channel_count: 2,
            sample_format: SAMPLE_FORMAT_FLOAT32,
            sample_rate_code: 1,
            samples_per_channel: 4,
        }
        .to_bytes());
        packet.extend([0u8; 16]);

        let parsed = parse_packet(&packet).expect("valid packet");
        assert_eq!(parsed.stream_id, 7);
        assert_eq!(parsed.channel_count, 2);
        assert_eq!(parsed.sample_rate, 48_000);
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
            packet.extend(AudioPayloadHeader {
                channel_count: channels as u8,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 64, // smaller block for large channels
            }
            .to_bytes());
            
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
        full_packet.extend(AudioPayloadHeader {
            channel_count: 2,
            sample_format: SAMPLE_FORMAT_FLOAT32,
            sample_rate_code: 1,
            samples_per_channel: 1,
        }.to_bytes());
        full_packet.extend([0u8; 8]); // 2 channels * 1 sample * 4 bytes
        
        let parsed = parse_packet(&full_packet).unwrap();
        assert_eq!(parsed.payload_offset, 32);
    }
}
