use crate::protocol::parse_packet;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

/// Listens for OpenAudio UDP packets on `bind_addr` (e.g.
/// "0.0.0.0:6970") for `duration_secs`, reconstructs the PCM audio,
/// and writes it to a WAV file at `output_path`. This is Milestone 3
/// -- proving depacketization + reassembly works before we build
/// live playback in Milestone 4.
pub fn receive_to_wav(duration_secs: u64, bind_addr: &str, output_path: &str) -> Result<(), String> {
    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Listening on {bind_addr} for {duration_secs}s...");

    let mut writer: Option<hound::WavWriter<std::io::BufWriter<std::fs::File>>> = None;
    let mut buf = [0u8; 1500];
    let mut packets_received = 0u32;
    let mut packets_dropped = 0u32;
    let mut last_sequence: Option<u32> = None;

    let start = Instant::now();
    while start.elapsed().as_secs() < duration_secs {
        let (len, _src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else {
            continue; // not a recognized OpenAudio audio packet, skip
        };

        // Detect gaps for visibility (spec section 7: log, don't crash).
        if let Some(prev) = last_sequence {
            let expected = prev.wrapping_add(1);
            if parsed.sequence_number != expected {
                packets_dropped += parsed.sequence_number.wrapping_sub(expected);
            }
        }
        last_sequence = Some(parsed.sequence_number);
        packets_received += 1;

        // Lazily create the WAV writer once we know the real format
        // (channels/sample rate come from the first packet).
        if writer.is_none() {
            let spec = hound::WavSpec {
                channels: parsed.channel_count as u16,
                sample_rate: parsed.sample_rate,
                bits_per_sample: 32,
                sample_format: hound::SampleFormat::Float,
            };
            writer = Some(
                hound::WavWriter::create(output_path, spec)
                    .map_err(|e| format!("failed to create wav: {e}"))?,
            );
        }

        let w = writer.as_mut().unwrap();
        let sample_count =
            parsed.samples_per_channel as usize * parsed.channel_count as usize;
        let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];

        for chunk in payload.chunks_exact(4) {
            let sample = f32::from_le_bytes(chunk.try_into().unwrap());
            let _ = w.write_sample(sample);
        }
    }

    match writer {
        Some(w) => {
            w.finalize().map_err(|e| format!("failed to finalize wav: {e}"))?;
            println!(
                "Wrote {output_path} ({packets_received} packets received, ~{packets_dropped} dropped)"
            );
            Ok(())
        }
        None => Err("no packets received -- is the sender running and pointed at this address?".to_string()),
    }
}