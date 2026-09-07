use crate::backend::get_backend;
use crate::ensure_realtime_audio_thread;
use crate::protocol::parse_packet;
use crate::{resample_interleaved_linear, JITTER_BUFFER_TARGET_SECS};
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
    ensure_realtime_audio_thread();
    let socket =
        UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Listening on {bind_addr}, waiting for first packet to detect format...");

    let buffer: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));
    // Sized for worst case: protocol allows up to 255 channels
    // (see asio.rs validation) at MAX_FRAMES_PER_PACKET=58 frames,
    // 4 bytes/sample: 255 * 58 * 4 + headers ≈ 59KB. A 1500-byte
    // buffer (old MTU-sized default) causes WSAEMSGSIZE the moment
    // more than ~9 channels are streamed in one packet.
    let mut buf = [0u8; 65536];

    // No initial value here on purpose -- the loop below always
    // assigns this (via `Some(...)`) on the only path that exits
    // it, so the compiler can prove it's initialized before the
    // first real read, without us writing a throwaway `None` that
    // gets silently discarded (that's what caused the warning).
    let mut last_sequence: Option<u32>;
    let mut packets_received = 0u32;
    let mut packets_dropped = 0u32;

    // Block until the first valid packet arrives, so we know channel
    // count and sample rate before opening an output stream.
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

    let backend = get_backend();
    let (_device_label, output_config) = backend.get_output_config(None)?;

    if output_config.channels != channel_count {
        return Err(format!(
            "output device channel count ({}ch) doesn't match incoming stream ({}ch).",
            output_config.channels,
            channel_count
        ));
    }

    let output_sample_rate = output_config.sample_rate;
    if output_sample_rate != sample_rate {
        let initial = buffer.lock().unwrap().drain(..).collect::<Vec<_>>();
        let converted = resample_interleaved_linear(
            &initial,
            channel_count as usize,
            sample_rate,
            output_sample_rate,
        );
        buffer.lock().unwrap().extend(converted);
    }

    let buffer_for_callback = buffer.clone();

    let stream = backend.build_output_stream(
        None,
        Box::new(move |data: &mut [f32]| {
            ensure_realtime_audio_thread();
            let mut buf = buffer_for_callback.lock().unwrap();
            for sample in data.iter_mut() {
                *sample = buf.pop_front().unwrap_or(0.0); // underrun -> silence
            }
        }),
    )?;

    // Prime the jitter buffer toward the spec's 6ms default target
    // before starting playback, so the callback isn't starved
    // immediately (protocol spec section 6.1).
    let target_samples =
        ((output_sample_rate as f64 * JITTER_BUFFER_TARGET_SECS) as usize)
            * channel_count as usize;
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
                let sample_count =
                    parsed.samples_per_channel as usize * parsed.channel_count as usize;
                let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];
                let input = payload
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
                    .collect::<Vec<_>>();
                let converted = resample_interleaved_linear(
                    &input,
                    channel_count as usize,
                    sample_rate,
                    output_sample_rate,
                );
                buffer.lock().unwrap().extend(converted);
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
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
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

        let input = payload
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let converted = resample_interleaved_linear(
            &input,
            channel_count as usize,
            sample_rate,
            output_sample_rate,
        );
        let mut jitter_buf = buffer.lock().unwrap();
        jitter_buf.extend(converted);

        // Cap buffer growth at ~200ms in case playback ever falls behind.
        let max_samples = ((output_sample_rate as f64 * 0.2) as usize)
            * channel_count as usize;
        while jitter_buf.len() > max_samples {
            jitter_buf.pop_front();
        }
    }

    drop(stream);
    println!("Done. {packets_received} packets received, ~{packets_dropped} dropped.");
    Ok(())
}
