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
    /// Serializes the fixed header into a 24-byte buffer.
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
    pub sample_format: u8, // 0x03 = Float32, matching our capture format
    pub sample_rate_code: u16,
    pub samples_per_channel: u16,
}

pub const SAMPLE_FORMAT_FLOAT32: u8 = 0x03;
pub const SAMPLE_RATE_48000: u16 = 0x01;
pub const SAMPLE_RATE_44100: u16 = 0x02;

impl AudioPayloadHeader {
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0] = self.channel_count;
        buf[1] = self.sample_format;
        buf[2..4].copy_from_slice(&self.sample_rate_code.to_be_bytes());
        buf[4..6].copy_from_slice(&self.samples_per_channel.to_be_bytes());
        // buf[6..8] reserved, left as zero
        buf
    }
}

pub fn sample_rate_to_code(rate: u32) -> u16 {
    match rate {
        48000 => SAMPLE_RATE_48000,
        44100 => SAMPLE_RATE_44100,
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
    pub payload_offset: usize, // where PCM data starts in the original buffer
}

pub fn code_to_sample_rate(code: u16) -> u32 {
    match code {
        0x01 => 48000,
        0x02 => 44100,
        _ => 0,
    }
}

/// Parses the 24-byte header + 8-byte audio payload header from a raw
/// received packet. Returns None if it's not a valid/recognized
/// OpenAudio audio packet.
pub fn parse_packet(buf: &[u8]) -> Option<ParsedPacket> {
    if buf.len() < 32 {
        return None; // shorter than fixed header + payload header
    }

    if &buf[0..4] != b"OAv1" {
        return None; // bad magic
    }
    let version = buf[4];
    if version != 1 {
        return None; // unsupported version, per spec section 9
    }
    let packet_type = buf[5];
    if packet_type != 0x01 {
        return None; // not an audio data packet
    }

    let stream_id = u32::from_be_bytes(buf[8..12].try_into().ok()?);
    let sequence_number = u32::from_be_bytes(buf[12..16].try_into().ok()?);
    let presentation_timestamp_ns = u64::from_be_bytes(buf[16..24].try_into().ok()?);

    let channel_count = buf[24];
    let sample_format = buf[25];
    if sample_format != SAMPLE_FORMAT_FLOAT32 {
        return None; // Milestone 3 only handles Float32 for now
    }
    let rate_code = u16::from_be_bytes(buf[26..28].try_into().ok()?);
    let sample_rate = code_to_sample_rate(rate_code);
    let samples_per_channel = u16::from_be_bytes(buf[28..30].try_into().ok()?);

    Some(ParsedPacket {
        stream_id,
        sequence_number,
        presentation_timestamp_ns,
        channel_count,
        sample_rate,
        samples_per_channel,
        payload_offset: 32,
    })
}