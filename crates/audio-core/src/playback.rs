use crate::protocol::parse_packet;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Receives OpenAudio UDP packets on `bind_addr` and plays them live
/// through the default output device for `duration_secs`. This is
/// Milestone 4: real-time playback via a small jitter buffer, in
/// place of the WAV file from Milestone 3.
///
/// Note: this assumes the output device's default channel count and
/// sample rate match what the sender is transmitting. Resampling for
/// mismatched devices is a later milestone.
pub fn receive_and_play(bind_addr: &str, duration_secs: u64) -> Result<(), String> {
    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Listening on {bind_addr}, waiting for first packet to detect format...");

    let buffer: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut buf = [0u8; 1500];
    let mut last_sequence: Option<u32> = None;
    let mut packets_received = 0u32;
    let mut packets_dropped = 0u32;

    // Block until the first valid packet arrives, so we know channel
    // count and sample rate before opening an output stream (cpal
    // needs an explicit config up front, unlike the lazy WAV writer).
    let (channel_count, sample_rate) = loop {
        let (len, _src) = socket
            .recv_from(&mut buf)
            .map_err(|e| format!("recv error while waiting for first packet: {e}"))?;
        if let Some(parsed) = parse_packet(&buf[..len]) {
            let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
            let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];
            let mut jitter_buf = buffer.lock().unwrap();
            for chunk in payload.chunks_exact(4) {
                jitter_buf.push_back(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            last_sequence = Some(parsed.sequence_number);
            packets_received += 1;
            break (parsed.channel_count as u16, parsed.sample_rate);
        }
    };

    println!("Detected {channel_count}ch @ {sample_rate}Hz. Setting up playback...");

    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| "no default output device found".to_string())?;
    let output_config = device
        .default_output_config()
        .map_err(|e| format!("failed to get default output config: {e}"))?;

    if output_config.channels() != channel_count || output_config.sample_rate().0 != sample_rate {
        return Err(format!(
            "output device default format ({}ch @ {}Hz) doesn't match incoming stream ({}ch @ {}Hz). \
             Resampling isn't implemented yet -- that's a later milestone.",
            output_config.channels(),
            output_config.sample_rate().0,
            channel_count,
            sample_rate
        ));
    }

    let stream_config: cpal::StreamConfig = output_config.into();
    let buffer_for_callback = buffer.clone();
    let err_fn = |err| eprintln!("audio-core: playback stream error: {err}");

    let stream = device
        .build_output_stream(
            &stream_config,
            move |data: &mut [f32], _| {
                let mut buf = buffer_for_callback.lock().unwrap();
                for sample in data.iter_mut() {
                    *sample = buf.pop_front().unwrap_or(0.0); // underrun -> silence
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build output stream: {e}"))?;

    // Prime the jitter buffer toward the spec's 6ms default target
    // before starting playback, so the callback isn't starved
    // immediately (protocol spec section 6.1).
    let target_samples = ((sample_rate as f64 * 0.006) as usize) * channel_count as usize;
    let prime_deadline = Instant::now() + Duration::from_millis(500);
    while buffer.lock().unwrap().len() < target_samples && Instant::now() < prime_deadline {
        if let Ok((len, _src)) = socket.recv_from(&mut buf) {
            if let Some(parsed) = parse_packet(&buf[..len]) {
                if let Some(prev) = last_sequence {
                    let expected = prev.wrapping_add(1);
                    if parsed.sequence_number != expected {
                        packets_dropped += parsed.sequence_number.wrapping_sub(expected);
                    }
                }
                last_sequence = Some(parsed.sequence_number);
                packets_received += 1;
                let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
                let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];
                let mut jitter_buf = buffer.lock().unwrap();
                for chunk in payload.chunks_exact(4) {
                    jitter_buf.push_back(f32::from_le_bytes(chunk.try_into().unwrap()));
                }
            }
        }
    }

    stream
        .play()
        .map_err(|e| format!("failed to start playback stream: {e}"))?;
    println!("Playing back live for {duration_secs}s...");

    let start = Instant::now();
    while start.elapsed().as_secs() < duration_secs {
       let (len, _src) = match socket.recv_from(&mut buf) {
    Ok(r) => r,
    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
    Err(e) => return Err(format!("recv error: {e}")),
};

        let Some(parsed) = parse_packet(&buf[..len]) else {
            continue;
        };

        if let Some(prev) = last_sequence {
            let expected = prev.wrapping_add(1);
            if parsed.sequence_number != expected {
                packets_dropped += parsed.sequence_number.wrapping_sub(expected);
            }
        }
        last_sequence = Some(parsed.sequence_number);
        packets_received += 1;

        let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
        let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];

        let mut jitter_buf = buffer.lock().unwrap();
        for chunk in payload.chunks_exact(4) {
            jitter_buf.push_back(f32::from_le_bytes(chunk.try_into().unwrap()));
        }

        // Cap buffer growth at ~200ms in case playback ever falls behind.
        let max_samples = ((sample_rate as f64 * 0.2) as usize) * channel_count as usize;
        while jitter_buf.len() > max_samples {
            jitter_buf.pop_front();
        }
    }

    drop(stream);
    println!("Done. {packets_received} packets received, ~{packets_dropped} dropped.");
    Ok(())
}