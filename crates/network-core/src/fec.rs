use crate::protocol::{
    encode_fec_packet, ParsedFecPacket, ParsedPacket, SAMPLE_FORMAT_FLOAT32,
};
use std::collections::BTreeMap;

pub const DEFAULT_FEC_GROUP_SIZE: usize = 5;

/// Accumulates outgoing audio packet payloads into an XOR parity FEC packet.
pub struct FecEncoder {
    group_size: usize,
    parity_buffer: Vec<u8>,
    count: usize,
    base_sequence_number: u32,
    base_timestamp_ns: u64,
    fec_sequence_number: u32,
    channel_count: u8,
    sample_rate: u32,
    samples_per_channel: u16,
}

impl FecEncoder {
    pub fn new(group_size: usize) -> Self {
        Self {
            group_size: group_size.max(2),
            parity_buffer: Vec::new(),
            count: 0,
            base_sequence_number: 0,
            base_timestamp_ns: 0,
            fec_sequence_number: 0,
            channel_count: 0,
            sample_rate: 0,
            samples_per_channel: 0,
        }
    }

    /// Feeds an outgoing raw audio packet's little-endian float32 payload bytes
    /// into the running parity accumulator. When `group_size` packets have been fed,
    /// returns the encoded FEC packet.
    pub fn push_audio_packet(
        &mut self,
        stream_id: u32,
        sequence_number: u32,
        timestamp_ns: u64,
        channel_count: u8,
        sample_rate: u32,
        samples_per_channel: u16,
        raw_float32_payload: &[u8],
    ) -> Option<Vec<u8>> {
        if self.count == 0 {
            self.base_sequence_number = sequence_number;
            self.base_timestamp_ns = timestamp_ns;
            self.channel_count = channel_count;
            self.sample_rate = sample_rate;
            self.samples_per_channel = samples_per_channel;
            self.parity_buffer.clear();
            self.parity_buffer.extend_from_slice(raw_float32_payload);
        } else {
            let len = self.parity_buffer.len().min(raw_float32_payload.len());
            for i in 0..len {
                self.parity_buffer[i] ^= raw_float32_payload[i];
            }
            if raw_float32_payload.len() > self.parity_buffer.len() {
                self.parity_buffer
                    .extend_from_slice(&raw_float32_payload[self.parity_buffer.len()..]);
            }
        }

        self.count += 1;

        if self.count >= self.group_size {
            let fec_seq = self.fec_sequence_number;
            self.fec_sequence_number = self.fec_sequence_number.wrapping_add(1);
            let packet = encode_fec_packet(
                stream_id,
                fec_seq,
                self.base_timestamp_ns,
                self.base_sequence_number,
                self.count as u8,
                self.channel_count,
                self.sample_rate,
                self.samples_per_channel,
                &self.parity_buffer,
            )
            .ok();

            self.count = 0;
            self.parity_buffer.clear();
            packet
        } else {
            None
        }
    }
}

/// Stores recently received audio packets and uses incoming FEC parity packets
/// to mathematically reconstruct missing packets with zero round-trip latency.
pub struct FecDecoder {
    history: BTreeMap<u32, Vec<u8>>,
    history_capacity: usize,
    recovered_count: u64,
}

impl FecDecoder {
    pub fn new(capacity: usize) -> Self {
        Self {
            history: BTreeMap::new(),
            history_capacity: capacity.max(32),
            recovered_count: 0,
        }
    }

    pub fn recovered_count(&self) -> u64 {
        self.recovered_count
    }

    /// Records an audio packet's payload so it can be used for parity reconstruction.
    pub fn record_received(&mut self, sequence_number: u32, payload: &[u8]) {
        self.history.insert(sequence_number, payload.to_vec());
        while self.history.len() > self.history_capacity {
            if let Some(oldest) = self.history.keys().next().copied() {
                self.history.remove(&oldest);
            }
        }
    }

    /// Checks if a received FEC packet can recover a single missing audio packet.
    /// If exactly 1 packet in the FEC group is missing, reconstructs and returns it.
    pub fn decode_fec(
        &mut self,
        fec: &ParsedFecPacket,
        fec_parity_payload: &[u8],
    ) -> Option<(ParsedPacket, Vec<f32>)> {
        let base = fec.base_sequence_number;
        let count = fec.packet_count as u32;

        let mut missing = Vec::new();
        let mut present = Vec::new();

        for i in 0..count {
            let seq = base.wrapping_add(i);
            if let Some(payload) = self.history.get(&seq) {
                present.push((seq, payload.as_slice()));
            } else {
                missing.push(seq);
            }
        }

        if missing.len() != 1 {
            return None;
        }

        let missing_seq = missing[0];
        let mut reconstructed = fec_parity_payload.to_vec();

        for (_, payload) in &present {
            let len = reconstructed.len().min(payload.len());
            for i in 0..len {
                reconstructed[i] ^= payload[i];
            }
        }

        // Reconstruct float32 samples from byte payload
        let expected_samples = (fec.channel_count as usize)
            .checked_mul(fec.samples_per_channel as usize)?;
        let expected_bytes = expected_samples.checked_mul(std::mem::size_of::<f32>())?;

        if reconstructed.len() < expected_bytes {
            return None;
        }

        let pcm: Vec<f32> = reconstructed[..expected_bytes]
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap_or([0; 4])))
            .collect();

        // Save reconstructed packet into history so subsequent overlapping groups work
        self.record_received(missing_seq, &reconstructed[..expected_bytes]);
        self.recovered_count = self.recovered_count.saturating_add(1);

        let parsed = ParsedPacket {
            sub_stream_index: 0,
            stream_id: fec.stream_id,
            sequence_number: missing_seq,
            presentation_timestamp_ns: fec.presentation_timestamp_ns,
            channel_count: fec.channel_count,
            sample_format: SAMPLE_FORMAT_FLOAT32,
            sample_rate: fec.sample_rate,
            samples_per_channel: fec.samples_per_channel,
            payload_offset: 0,
        };

        Some((parsed, pcm))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::parse_fec_packet;

    #[test]
    fn test_fec_encoder_decoder_recovers_missing_packet() {
        let mut encoder = FecEncoder::new(4);
        let mut decoder = FecDecoder::new(16);

        let stream_id = 99;
        let channel_count = 2;
        let sample_rate = 48_000;
        let samples_per_channel = 4;

        // Create 4 test packets
        let p0: Vec<f32> = vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
        let p1: Vec<f32> = vec![1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8];
        let p2: Vec<f32> = vec![2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8]; // Will be lost!
        let p3: Vec<f32> = vec![3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8];

        let p0_bytes: Vec<u8> = p0.iter().flat_map(|f| f.to_le_bytes()).collect();
        let p1_bytes: Vec<u8> = p1.iter().flat_map(|f| f.to_le_bytes()).collect();
        let p2_bytes: Vec<u8> = p2.iter().flat_map(|f| f.to_le_bytes()).collect();
        let p3_bytes: Vec<u8> = p3.iter().flat_map(|f| f.to_le_bytes()).collect();

        assert!(encoder
            .push_audio_packet(stream_id, 10, 0, channel_count, sample_rate, samples_per_channel, &p0_bytes)
            .is_none());
        assert!(encoder
            .push_audio_packet(stream_id, 11, 0, channel_count, sample_rate, samples_per_channel, &p1_bytes)
            .is_none());
        assert!(encoder
            .push_audio_packet(stream_id, 12, 0, channel_count, sample_rate, samples_per_channel, &p2_bytes)
            .is_none());
        let fec_packet = encoder
            .push_audio_packet(stream_id, 13, 0, channel_count, sample_rate, samples_per_channel, &p3_bytes)
            .expect("emits FEC packet on 4th packet");

        // Simulate receiver getting p0, p1, and p3 (p2 sequence 12 is lost!)
        decoder.record_received(10, &p0_bytes);
        decoder.record_received(11, &p1_bytes);
        decoder.record_received(13, &p3_bytes);

        // Parse FEC packet
        let parsed_fec = parse_fec_packet(&fec_packet).expect("valid fec");
        let fec_payload = &fec_packet[parsed_fec.payload_offset..parsed_fec.payload_offset + parsed_fec.payload_len];

        // Attempt recovery
        let (recovered_pkt, recovered_pcm) = decoder
            .decode_fec(&parsed_fec, fec_payload)
            .expect("should reconstruct lost packet 12");

        assert_eq!(recovered_pkt.sequence_number, 12);
        assert_eq!(recovered_pcm.len(), p2.len());
        for i in 0..p2.len() {
            assert!((recovered_pcm[i] - p2[i]).abs() < 1e-6);
        }
        assert_eq!(decoder.recovered_count(), 1);
    }
}
