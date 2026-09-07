use crate::discovery::send_subscribe_request;
use crate::protocol::{parse_packet, ParsedPacket};
use std::collections::VecDeque;
use std::net::{Ipv4Addr, UdpSocket};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum ReceiveEvent {
    Packet(ParsedPacket, Vec<f32>),
    FormatChanged { channels: u8, sample_rate: u32 },
    Dropped(u32),
    Disconnected,
    Reconnected,
}

pub struct StreamReceiver {
    socket: UdpSocket,
    publisher_ip: String,
    control_port: u16,
    stream_id: u32,
    my_receive_port: u16,

    last_sequence: Option<u32>,
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
            .set_read_timeout(Some(Duration::from_millis(100)))
            .map_err(|e| format!("failed to set read timeout: {e}"))?;

        let mut receiver = Self {
            socket,
            publisher_ip: publisher_ip.to_string(),
            control_port,
            stream_id,
            my_receive_port,
            last_sequence: None,
            last_receive: Instant::now() - Duration::from_secs(10), // force immediate subscribe
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
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
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

        // Add packet reordering policy and gap detection
        if let Some(prev) = self.last_sequence {
            let expected = prev.wrapping_add(1);
            if packet.sequence_number != expected {
                let gap = packet.sequence_number.wrapping_sub(expected);
                // If the gap is massive (e.g. > 2^31), it's likely a late packet or sender restart.
                // To prevent locking up forever on restart, we will unconditionally accept the packet
                // but only report 'Dropped' if the gap is reasonably positive.
                if gap > 0 && gap < 1_000_000 {
                    self.event_queue.push_back(ReceiveEvent::Dropped(gap));
                }
            }
        }
        
        self.last_sequence = Some(packet.sequence_number);

        let sample_count = packet.samples_per_channel as usize * packet.channel_count as usize;
        let mut pcm = Vec::with_capacity(sample_count);
        let payload = &buf[packet.payload_offset..packet.payload_offset + sample_count * 4];
        for chunk in payload.chunks_exact(4) {
            pcm.push(f32::from_le_bytes(chunk.try_into().unwrap()));
        }

        self.event_queue.push_back(ReceiveEvent::Packet(packet, pcm));

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
        sender.connect(format!("127.0.0.1:{}", receive_port)).unwrap();

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
        sender.connect(format!("127.0.0.1:{}", receive_port)).unwrap();

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
}
