use crate::discovery::send_subscribe_request;
use crate::protocol::{
    decode_float32, parse_packet, sample_format_name, ParsedPacket, SAMPLE_FORMAT_FLOAT32,
};
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, UdpSocket};
use std::time::{Duration, Instant};

const REORDER_GAP_TIMEOUT: Duration = Duration::from_millis(20);

#[derive(Debug)]
pub enum ReceiveEvent {
    Packet(ParsedPacket, Vec<f32>),
    FormatChanged { channels: u8, sample_rate: u32 },
    Dropped(u32),
    Disconnected,
    Reconnected,
}

#[derive(Debug)]
pub struct PacketReorderWindow {
    capacity: usize,
    next_expected_seq: Option<u32>,
    buffer: BTreeMap<u32, (ParsedPacket, Vec<f32>)>,
    gap_started: Option<Instant>,
}

impl PacketReorderWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_expected_seq: None,
            buffer: BTreeMap::new(),
            gap_started: None,
        }
    }

    fn flush_expired_gap(&mut self) -> Vec<ReceiveEvent> {
        let Some(gap_started) = self.gap_started else {
            return Vec::new();
        };
        if gap_started.elapsed() < REORDER_GAP_TIMEOUT {
            return Vec::new();
        }

        let Some(expected) = self.next_expected_seq else {
            self.buffer.clear();
            self.gap_started = None;
            return Vec::new();
        };

        let next_packet = self
            .buffer
            .keys()
            .copied()
            .filter_map(|sequence| {
                let distance = sequence.wrapping_sub(expected);
                (distance < 0x8000_0000).then_some((distance, sequence))
            })
            .min_by_key(|(distance, _)| *distance);

        let Some((missing, sequence)) = next_packet else {
            self.buffer.clear();
            self.gap_started = None;
            return Vec::new();
        };

        let mut events = Vec::new();
        if missing > 0 {
            events.push(ReceiveEvent::Dropped(missing));
        }
        if let Some((packet, pcm)) = self.buffer.remove(&sequence) {
            self.next_expected_seq = Some(sequence.wrapping_add(1));
            events.push(ReceiveEvent::Packet(packet, pcm));
        }

        while let Some(next_expected) = self.next_expected_seq {
            if let Some((packet, pcm)) = self.buffer.remove(&next_expected) {
                self.next_expected_seq = Some(next_expected.wrapping_add(1));
                events.push(ReceiveEvent::Packet(packet, pcm));
            } else {
                break;
            }
        }

        self.gap_started = if self.buffer.is_empty() {
            None
        } else {
            Some(Instant::now())
        };
        events
    }

    pub fn push(&mut self, packet: ParsedPacket, pcm: Vec<f32>) -> Vec<ReceiveEvent> {
        let mut events = Vec::new();
        let seq = packet.sequence_number;

        let expected = match self.next_expected_seq {
            Some(e) => e,
            None => {
                self.next_expected_seq = Some(seq.wrapping_add(1));
                events.push(ReceiveEvent::Packet(packet, pcm));
                return events;
            }
        };

        if seq == expected {
            self.next_expected_seq = Some(seq.wrapping_add(1));
            events.push(ReceiveEvent::Packet(packet, pcm));

            while let Some(next_exp) = self.next_expected_seq {
                if let Some((next_pkt, next_pcm)) = self.buffer.remove(&next_exp) {
                    self.next_expected_seq = Some(next_exp.wrapping_add(1));
                    events.push(ReceiveEvent::Packet(next_pkt, next_pcm));
                } else {
                    break;
                }
            }
            self.gap_started = if self.buffer.is_empty() {
                None
            } else {
                Some(Instant::now())
            };
        } else {
            let diff = seq.wrapping_sub(expected);
            if diff > 0x8000_0000 {
                return events;
            }

            if diff > 1_000_000 {
                self.buffer.clear();
                self.next_expected_seq = Some(seq.wrapping_add(1));
                events.push(ReceiveEvent::Packet(packet, pcm));
                return events;
            }

            if diff > 2 {
                events.push(ReceiveEvent::Dropped(diff));
                self.next_expected_seq = Some(seq.wrapping_add(1));
                events.push(ReceiveEvent::Packet(packet, pcm));
                return events;
            }

            self.buffer.insert(seq, (packet, pcm));
            self.gap_started.get_or_insert_with(Instant::now);

            while self.buffer.len() > self.capacity {
                if let Some(&oldest_seq) = self.buffer.keys().next() {
                    if let Some(curr_exp) = self.next_expected_seq {
                        if oldest_seq > curr_exp {
                            let dropped_count = oldest_seq.wrapping_sub(curr_exp);
                            events.push(ReceiveEvent::Dropped(dropped_count));
                        }
                    }
                    if let Some((pkt, pcm)) = self.buffer.remove(&oldest_seq) {
                        self.next_expected_seq = Some(oldest_seq.wrapping_add(1));
                        events.push(ReceiveEvent::Packet(pkt, pcm));
                    }
                }
            }
            if self.buffer.is_empty() {
                self.gap_started = None;
            }
        }

        events
    }
}

pub struct StreamReceiver {
    socket: UdpSocket,
    publisher_ip: String,
    control_port: u16,
    stream_id: u32,
    my_receive_port: u16,

    reorder_window: PacketReorderWindow,
    last_receive: Instant,
    current_format: Option<(u8, u32)>,
    is_connected: bool,

    event_queue: VecDeque<ReceiveEvent>,
}

impl StreamReceiver {
    pub fn new(
        publisher_ip: &str,
        control_port: u16,
        stream_id: u32,
        my_receive_port: u16,
    ) -> Result<Self, String> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, my_receive_port))
            .map_err(|e| format!("failed to bind receive port {my_receive_port}: {e}"))?;
        socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .map_err(|e| format!("failed to set read timeout: {e}"))?;

        let mut receiver = Self {
            socket,
            publisher_ip: publisher_ip.to_string(),
            control_port,
            stream_id,
            my_receive_port,
            reorder_window: PacketReorderWindow::new(32),
            last_receive: Instant::now() - Duration::from_secs(10),
            current_format: None,
            is_connected: false,
            event_queue: VecDeque::new(),
        };

        receiver.resubscribe();
        Ok(receiver)
    }

    fn resubscribe(&mut self) {
        let _ = send_subscribe_request(
            &self.publisher_ip,
            self.control_port,
            self.stream_id,
            self.my_receive_port,
        );
        self.last_receive = Instant::now();
    }

    /// Blocks until an event occurs or timeout, returning the next event.
    pub fn next_event(&mut self) -> Result<Option<ReceiveEvent>, String> {
        if let Some(event) = self.event_queue.pop_front() {
            return Ok(Some(event));
        }

        self.event_queue
            .extend(self.reorder_window.flush_expired_gap());
        if let Some(event) = self.event_queue.pop_front() {
            return Ok(Some(event));
        }

        if self.last_receive.elapsed() > Duration::from_secs(1) {
            let was_connected = self.is_connected;
            self.is_connected = false;
            self.resubscribe();
            if was_connected {
                return Ok(Some(ReceiveEvent::Disconnected));
            }
        }

        let mut buf = [0u8; 65536];
        let (len, _src) = match self.socket.recv_from(&mut buf) {
            Ok(value) => value,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Ok(None);
            }
            Err(e) => return Err(format!("receive error: {e}")),
        };

        let Some(packet) = parse_packet(&buf[..len]) else {
            return Ok(None);
        };

        if packet.stream_id != self.stream_id {
            return Ok(None);
        }

        if packet.sample_format != SAMPLE_FORMAT_FLOAT32 {
            let format = sample_format_name(packet.sample_format).unwrap_or("unknown");
            return Err(format!(
                "unsupported audio format {format} (0x{:02x}); this receiver currently supports Float32 PCM only",
                packet.sample_format
            ));
        }

        let Some(pcm) = decode_float32(&buf[..len], &packet) else {
            return Err("invalid or truncated Float32 PCM payload".to_string());
        };

        if !self.is_connected {
            self.is_connected = true;
            self.event_queue.push_back(ReceiveEvent::Reconnected);
        }

        self.last_receive = Instant::now();

        let format = (packet.channel_count, packet.sample_rate);
        if self.current_format != Some(format) {
            self.current_format = Some(format);
            self.event_queue.push_back(ReceiveEvent::FormatChanged {
                channels: packet.channel_count,
                sample_rate: packet.sample_rate,
            });
        }

        let events = self.reorder_window.push(packet, pcm);
        for ev in events {
            self.event_queue.push_back(ev);
        }

        Ok(self.event_queue.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
    use std::thread;

    fn make_packet(stream_id: u32, seq: u32) -> Vec<u8> {
        let mut packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number: seq,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );

        packet.extend(
            AudioPayloadHeader {
                sample_format: SAMPLE_FORMAT_FLOAT32,
                channel_count: 2,
                sample_rate_code: 1, // 48000
                samples_per_channel: 256,
            }
            .to_bytes(),
        );

        let payload_size = 2 * 256 * 4;
        packet.extend(vec![0u8; payload_size]);
        packet
    }

    #[test]
    fn test_packet_loss_reordering() {
        let receive_port = 17051;
        let mut receiver = StreamReceiver::new("127.0.0.1", 17052, 1, receive_port).unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .connect(format!("127.0.0.1:{}", receive_port))
            .unwrap();

        // Send seq 1
        sender.send(&make_packet(1, 1)).unwrap();

        let mut found_packet = false;
        for _ in 0..10 {
            if let Ok(Some(ReceiveEvent::Packet(_, _))) = receiver.next_event() {
                found_packet = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(found_packet);

        // Send seq 5 (skipping 2, 3, 4)
        sender.send(&make_packet(1, 5)).unwrap();

        let mut found_dropped = false;
        let mut found_next_packet = false;
        for _ in 0..10 {
            match receiver.next_event() {
                Ok(Some(ReceiveEvent::Dropped(count))) => {
                    assert_eq!(count, 3);
                    found_dropped = true;
                }
                Ok(Some(ReceiveEvent::Packet(_, _))) => {
                    found_next_packet = true;
                    break;
                }
                _ => {}
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(found_dropped);
        assert!(found_next_packet);
    }

    #[test]
    fn test_reconnect_stream_restart() {
        let receive_port = 17053;
        let mut receiver = StreamReceiver::new("127.0.0.1", 17054, 2, receive_port).unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .connect(format!("127.0.0.1:{}", receive_port))
            .unwrap();

        sender.send(&make_packet(2, 100)).unwrap();
        while !matches!(receiver.next_event(), Ok(Some(ReceiveEvent::Packet(_, _)))) {}

        // Stream stops for 1.5 seconds, then restarts at seq 1
        thread::sleep(Duration::from_millis(1500));

        let mut saw_disconnect = false;
        while let Ok(Some(event)) = receiver.next_event() {
            if matches!(event, ReceiveEvent::Disconnected) {
                saw_disconnect = true;
                break;
            }
        }
        assert!(saw_disconnect);

        sender.send(&make_packet(2, 1)).unwrap();

        let mut saw_reconnect = false;
        for _ in 0..20 {
            if let Ok(Some(ReceiveEvent::Reconnected)) = receiver.next_event() {
                saw_reconnect = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(saw_reconnect);
    }

    #[test]
    fn test_packet_reorder_window_out_of_order() {
        use crate::protocol::ParsedPacket;

        let mut window = PacketReorderWindow::new(8);
        let dummy_pkt = |seq: u32| ParsedPacket {
            stream_id: 1,
            sequence_number: seq,
            presentation_timestamp_ns: 0,
            channel_count: 2,
            sample_format: crate::protocol::SAMPLE_FORMAT_FLOAT32,
            sample_rate: 48000,
            samples_per_channel: 64,
            payload_offset: 32,
        };

        // Push seq 10
        let evs = window.push(dummy_pkt(10), vec![0.0; 128]);
        assert_eq!(evs.len(), 1);
        if let ReceiveEvent::Packet(p, _) = &evs[0] {
            assert_eq!(p.sequence_number, 10);
        }

        // Push seq 12 out-of-order (buffered)
        let evs = window.push(dummy_pkt(12), vec![0.0; 128]);
        assert_eq!(evs.len(), 0);

        // Push seq 11 (completes gap 11, then drains 12)
        let evs = window.push(dummy_pkt(11), vec![0.0; 128]);
        assert_eq!(evs.len(), 2);
        if let (ReceiveEvent::Packet(p1, _), ReceiveEvent::Packet(p2, _)) = (&evs[0], &evs[1]) {
            assert_eq!(p1.sequence_number, 11);
            assert_eq!(p2.sequence_number, 12);
        }
    }

    #[test]
    fn reorder_window_releases_packet_after_gap_timeout() {
        use crate::protocol::ParsedPacket;

        let mut window = PacketReorderWindow::new(8);
        let packet = |sequence_number| ParsedPacket {
            stream_id: 1,
            sequence_number,
            presentation_timestamp_ns: 0,
            channel_count: 2,
            sample_format: crate::protocol::SAMPLE_FORMAT_FLOAT32,
            sample_rate: 48_000,
            samples_per_channel: 1,
            payload_offset: 32,
        };

        assert!(matches!(
            window.push(packet(10), vec![0.0; 2]).as_slice(),
            [ReceiveEvent::Packet(_, _)]
        ));
        assert!(window.push(packet(12), vec![0.0; 2]).is_empty());
        window.gap_started = Some(Instant::now() - REORDER_GAP_TIMEOUT);

        let events = window.flush_expired_gap();
        assert!(matches!(
            events.as_slice(),
            [ReceiveEvent::Dropped(1), ReceiveEvent::Packet(packet, _)]
                if packet.sequence_number == 12
        ));
    }

    #[test]
    fn receiver_reports_unsupported_compressed_and_truncated_pcm_errors() {
        use crate::protocol::{AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_OPUS};

        let receive_port = 0;
        let mut receiver = StreamReceiver::new("127.0.0.1", 17054, 3, receive_port).unwrap();
        let receive_port = receiver.socket.local_addr().unwrap().port();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

        let mut compressed = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id: 3,
                sequence_number: 1,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );
        compressed.extend(
            AudioPayloadHeader {
                channel_count: 2,
                sample_format: SAMPLE_FORMAT_OPUS,
                sample_rate_code: 1,
                samples_per_channel: 1,
            }
            .to_bytes(),
        );
        compressed.extend([0; 4]);
        sender
            .send_to(&compressed, ("127.0.0.1", receive_port))
            .unwrap();

        let mut truncated = make_packet(3, 2);
        truncated.pop();
        sender
            .send_to(&truncated, ("127.0.0.1", receive_port))
            .unwrap();

        let error = receiver.next_event().unwrap_err();
        assert!(error.contains("unsupported audio format Opus"));
        let error = receiver.next_event().unwrap_err();
        assert!(error.contains("invalid or truncated Float32 PCM payload"));
        assert!(!receiver.is_connected);
    }
}
