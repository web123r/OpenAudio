use crate::protocol::{parse_packet, ParsedPacket};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Listens on `bind_addr` for packets from ANY stream_id and mixes
/// them together into a single output Bus -- Milestone 6: routing
/// multiple Publisher streams into one Subscriber-side Bus, per the
/// protocol spec's publish/subscribe model (section 1).
///
/// v1 restriction: every stream feeding this bus must share the same
/// channel count and sample rate (detected from the first packet).
/// Mismatched streams are logged and dropped rather than crashing the
/// bus -- real per-stream resampling is future work.
pub fn receive_and_play_bus(bind_addr: &str, duration_secs: u64) -> Result<(), String> {
    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Bus listening on {bind_addr}, waiting for first packet to detect format...");

    // One jitter buffer per stream_id feeding this bus.
    let buffers: Arc<Mutex<HashMap<u32, VecDeque<f32>>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut buf = [0u8; 1500];

    let (channel_count, sample_rate) = loop {
        let (len, _src) = socket
            .recv_from(&mut buf)
            .map_err(|e| format!("recv error while waiting for first packet: {e}"))?;
        if let Some(parsed) = parse_packet(&buf[..len]) {
            push_samples(&buffers, &buf, &parsed);
            break (parsed.channel_count as u16, parsed.sample_rate);
        }
    };

    println!("Bus detected {channel_count}ch @ {sample_rate}Hz. Setting up playback...");

    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| "no default output device found".to_string())?;
    let output_config = device
        .default_output_config()
        .map_err(|e| format!("failed to get default output config: {e}"))?;

    if output_config.channels() != channel_count || output_config.sample_rate().0 != sample_rate {
        return Err(format!(
            "output device default format ({}ch @ {}Hz) doesn't match incoming bus format ({}ch @ {}Hz). Resampling isn't implemented yet.",
            output_config.channels(), output_config.sample_rate().0, channel_count, sample_rate
        ));
    }

    let stream_config: cpal::StreamConfig = output_config.into();
    let buffers_for_callback = buffers.clone();
    let err_fn = |err| eprintln!("audio-core: bus playback stream error: {err}");

    let stream = device
        .build_output_stream(
            &stream_config,
            move |data: &mut [f32], _| {
                let mut buffers = buffers_for_callback.lock().unwrap();
                for sample in data.iter_mut() {
                    let mut mixed = 0.0f32;
                    for buf in buffers.values_mut() {
                        mixed += buf.pop_front().unwrap_or(0.0);
                    }
                    *sample = mixed.clamp(-1.0, 1.0);
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build output stream: {e}"))?;

    stream.play().map_err(|e| format!("failed to start playback stream: {e}"))?;
    println!("Bus playing back live for {duration_secs}s...");

    let start = Instant::now();
    let mut packets_received: u32 = 0;
    let mut streams_seen: HashSet<u32> = HashSet::new();

    while start.elapsed().as_secs() < duration_secs {
        let (len, _src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else { continue };

        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            eprintln!(
                "audio-core: bus dropping packet from stream {} -- format mismatch",
                parsed.stream_id
            );
            continue;
        }

        streams_seen.insert(parsed.stream_id);
        packets_received += 1;
        push_samples(&buffers, &buf, &parsed);

        // Cap each stream's buffer at ~200ms so a fast stream can't
        // grow unbounded if another feeding stream stalls.
        let max_samples = ((sample_rate as f64 * 0.2) as usize) * channel_count as usize;
        let mut guard = buffers.lock().unwrap();
        if let Some(b) = guard.get_mut(&parsed.stream_id) {
            while b.len() > max_samples {
                b.pop_front();
            }
        }
    }

    drop(stream);
    println!(
        "Done. {packets_received} packets received from {} distinct stream(s): {:?}",
        streams_seen.len(),
        streams_seen
    );
    Ok(())
}

fn push_samples(buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>, buf: &[u8], parsed: &ParsedPacket) {
    let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
    let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];
    let mut guard = buffers.lock().unwrap();
    let entry = guard.entry(parsed.stream_id).or_insert_with(VecDeque::new);
    for chunk in payload.chunks_exact(4) {
        entry.push_back(f32::from_le_bytes(chunk.try_into().unwrap()));
    }
}