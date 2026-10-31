use crate::backend::get_backend;
use crate::ensure_realtime_audio_thread;
use crate::opus_receive::AudioPacketDecoder;
use crate::protocol::parse_packet;
use crate::util::trim_interleaved_queue;
use crate::{StreamingLinearResampler, JITTER_BUFFER_TARGET_SECS};
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Receives OpenAudio UDP packets on `bind_addr` and plays them live
/// through the default output device for `duration_secs`. This is
/// Milestone 4: real-time playback via a small jitter buffer, in
/// place of the WAV file from Milestone 3.
///
/// Both Float32 PCM and Opus streams are accepted; the wire format is
/// detected from the packets themselves.
///
/// Note: this assumes the output device's default channel count matches
/// what the sender is transmitting. Sample-rate conversion is handled.
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
    let mut decoder = AudioPacketDecoder::new();

    // Block until the first decodable audio arrives, so we know channel
    // count and sample rate before opening an output stream.
    let (channel_count, sample_rate) = loop {
        let len = socket
            .recv_from(&mut buf)
            .map(|(len, _src)| len)
            .map_err(|error| format!("recv error while waiting for first packet: {error}"))?;
        let Some(parsed) = parse_packet(&buf[..len]) else {
            continue;
        };
        let Ok(frames) = decoder.decode(&buf[..len], &parsed) else {
            continue;
        };

        if frames.is_empty() {
            continue;
        }

        let mut jitter_buf = buffer.lock().unwrap();
        for frame in &frames {
            jitter_buf.extend(frame.pcm.iter().copied());
        }

        break (parsed.channel_count as u16, parsed.sample_rate);
    };

    println!("Detected {channel_count}ch @ {sample_rate}Hz. Setting up playback...");

    let backend = get_backend();
    let (_device_label, output_config) = backend.get_output_config(None)?;

    if output_config.channels != channel_count {
        return Err(format!(
            "output device channel count ({}ch) doesn't match incoming stream ({}ch).",
            output_config.channels, channel_count
        ));
    }

    let output_sample_rate = output_config.sample_rate;
    let mut resampler =
        StreamingLinearResampler::new(channel_count as usize, sample_rate, output_sample_rate)?;
    let initial = buffer.lock().unwrap().drain(..).collect::<Vec<_>>();
    let converted = resampler.process(&initial)?;
    buffer.lock().unwrap().extend(converted);

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

    // Prime toward the configured jitter target before starting
    // playback to reduce immediate callback underruns.
    let target_samples =
        ((output_sample_rate as f64 * JITTER_BUFFER_TARGET_SECS) as usize) * channel_count as usize;
    let prime_deadline = Instant::now() + Duration::from_millis(500);

    let mut packets_received = 1u32;
    let mut packets_dropped = 0u32;
    let mut last_sequence = None;

    while buffer.lock().unwrap().len() < target_samples && Instant::now() < prime_deadline {
        let Ok(len) = socket.recv_from(&mut buf).map(|(len, _)| len) else {
            continue;
        };
        let Some(parsed) = parse_packet(&buf[..len]) else {
            continue;
        };
        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            continue;
        }
        let Ok(frames) = decoder.decode(&buf[..len], &parsed) else {
            continue;
        };
        if frames.is_empty() {
            continue;
        }

        track_sequence(
            parsed.sequence_number,
            &mut last_sequence,
            &mut packets_dropped,
        );
        packets_received += 1;

        for frame in frames {
            let converted = resampler.process(&frame.pcm)?;
            buffer.lock().unwrap().extend(converted);
        }
    }

    stream
        .play()
        .map_err(|e| format!("failed to start playback stream: {e}"))?;
    println!("Playing back live for {duration_secs}s...");

    let start = Instant::now();
    while start.elapsed().as_secs() < duration_secs {
        let len = match socket.recv_from(&mut buf) {
            Ok((len, _src)) => len,
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // The socket went quiet: emit any Opus frame that has been
                // waiting for substreams, concealed, instead of stranding it.
                for frame in decoder.flush_expired()? {
                    let converted = resampler.process(&frame.pcm)?;
                    let mut jitter_buf = buffer.lock().unwrap();
                    jitter_buf.extend(converted);

                    let max_frames = (output_sample_rate as f64 * 0.2) as usize;
                    trim_interleaved_queue(&mut jitter_buf, max_frames, channel_count as usize);
                }

                continue;
            }
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else {
            continue;
        };

        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            continue;
        }
        let Ok(frames) = decoder.decode(&buf[..len], &parsed) else {
            continue;
        };
        if frames.is_empty() {
            continue;
        }

        track_sequence(
            parsed.sequence_number,
            &mut last_sequence,
            &mut packets_dropped,
        );
        packets_received += 1;

        for frame in frames {
            let converted = resampler.process(&frame.pcm)?;
            let mut jitter_buf = buffer.lock().unwrap();
            jitter_buf.extend(converted);

            // Cap buffer growth at ~200ms in case playback ever falls behind.
            let max_frames = (output_sample_rate as f64 * 0.2) as usize;
            trim_interleaved_queue(&mut jitter_buf, max_frames, channel_count as usize);
        }
    }

    drop(stream);
    println!("Done. {packets_received} packets received, ~{packets_dropped} dropped.");
    Ok(())
}

fn track_sequence(
    sequence_number: u32,
    last_sequence: &mut Option<u32>,
    packets_dropped: &mut u32,
) {
    if let Some(previous) = *last_sequence {
        let expected = previous.wrapping_add(1);
        if sequence_number != expected {
            *packets_dropped =
                packets_dropped.saturating_add(sequence_number.wrapping_sub(expected));
        }
    }

    *last_sequence = Some(sequence_number);
}
