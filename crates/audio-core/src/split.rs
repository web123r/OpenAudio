use crate::devices::get_output_device;
use crate::ensure_realtime_audio_thread;
use crate::protocol::parse_packet;
use crate::recording::{create_wav_writer, finalize as finalize_recording, generate_record_path, write_samples, SharedWavWriter};
use crate::util::safe_lock;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn receive_and_split_to_devices(
    bind_addr: &str,
    device_targets: Vec<Option<String>>,
    record_each_channel: bool,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Split-receive listening on {bind_addr}, waiting for first packet...");

    // Sized for worst case: protocol allows up to 255 channels at
    // MAX_FRAMES_PER_PACKET=58 frames, 4 bytes/sample:
    // 255 * 58 * 4 + headers ≈ 59KB. The old 1500-byte (MTU-sized)
    // buffer caused WSAEMSGSIZE the moment a packet carried more
    // than ~9 channels in one datagram (e.g. 32ch ASIO input).
    let mut buf = [0u8; 65536];

    let (channel_count, sample_rate, first_payload_offset, first_len) = loop {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        match socket.recv_from(&mut buf) {
            Ok((len, _src)) => {
                if let Some(parsed) = parse_packet(&buf[..len]) {
                    break (
                        parsed.channel_count as usize,
                        parsed.sample_rate,
                        parsed.payload_offset,
                        len,
                    );
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error while waiting for first packet: {e}")),
        }
    };

    println!("Detected {channel_count}ch @ {sample_rate}Hz.");

    if device_targets.len() != channel_count {
        return Err(format!(
            "got {} device target(s) but the incoming stream has {channel_count} channel(s) -- these must match exactly",
            device_targets.len()
        ));
    }

    let writers: Vec<Option<SharedWavWriter>> = if record_each_channel {
        (0..channel_count)
            .map(|i| {
                let path = generate_record_path(&format!("split_ch{i}"));
                create_wav_writer(&path, 1, sample_rate).ok()
            })
            .collect()
    } else {
        vec![None; channel_count]
    };

    let per_channel_buffers: Vec<Arc<Mutex<VecDeque<f32>>>> =
        (0..channel_count).map(|_| Arc::new(Mutex::new(VecDeque::new()))).collect();

    demux_into_buffers(&per_channel_buffers, &writers, &buf[..first_len], first_payload_offset, channel_count);

    println!("Opening {channel_count} output device(s)...");

    let mut streams = Vec::with_capacity(channel_count);

    for (i, target) in device_targets.iter().enumerate() {
        let label = target.clone().unwrap_or_else(|| "System Default".to_string());
        let device = get_output_device(target.as_deref())
            .map_err(|e| format!("channel {i} ('{label}'): {e}"))?;
        let output_config = device
            .default_output_config()
            .map_err(|e| format!("channel {i} ('{label}'): failed to get output config: {e}"))?;

        if output_config.sample_rate().0 != sample_rate {
            return Err(format!(
                "channel {i} ('{label}'): device sample rate {}Hz doesn't match incoming stream's {sample_rate}Hz -- resampling isn't implemented yet",
                output_config.sample_rate().0
            ));
        }

        let out_channels = output_config.channels() as usize;
        let stream_config: cpal::StreamConfig = output_config.into();
        let buffer_for_callback = per_channel_buffers[i].clone();
        let channel_label = label.clone();
        let err_fn = move |err| eprintln!("audio-core: split output stream error ({channel_label}): {err}");

        let stream = device
            .build_output_stream(
                &stream_config,
                move |data: &mut [f32], _| {
                    ensure_realtime_audio_thread();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut buf = safe_lock(&buffer_for_callback);
                        let frame_count = data.len() / out_channels;
                        for f in 0..frame_count {
                            let sample = buf.pop_front().unwrap_or(0.0);
                            for c in 0..out_channels {
                                data[f * out_channels + c] = sample.clamp(-1.0, 1.0);
                            }
                        }
                    }));
                    if result.is_err() {
                        eprintln!("audio-core: panic caught in split output callback -- outputting silence this cycle");
                        for sample in data.iter_mut() {
                            *sample = 0.0;
                        }
                    }
                },
                err_fn,
                None,
            )
            .map_err(|e| format!("channel {i} ('{label}'): failed to build output stream: {e}"))?;

        stream.play().map_err(|e| format!("channel {i} ('{label}'): failed to start stream: {e}"))?;
        streams.push(stream);
    }

    println!("All {channel_count} channel(s) playing. Streaming...");

    let mut packets_received: u32 = 1;

    while keep_running.load(Ordering::Relaxed) {
        let (len, _src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else { continue };

        if parsed.channel_count as usize != channel_count || parsed.sample_rate != sample_rate {
            eprintln!("audio-core: split dropping packet -- format mismatch");
            continue;
        }

        packets_received += 1;
        demux_into_buffers(&per_channel_buffers, &writers, &buf[..len], parsed.payload_offset, channel_count);

        let max_samples = ((sample_rate as f64 * 0.2) as usize).max(1);
        for buf in &per_channel_buffers {
            let mut guard = safe_lock(buf);
            while guard.len() > max_samples {
                guard.pop_front();
            }
        }
    }

    for w in writers.iter().flatten() {
        finalize_recording(w);
    }

    drop(streams);
    println!(
        "Done. {packets_received} packets received across {channel_count} channel(s)."
    );
    Ok(())
}

fn demux_into_buffers(
    per_channel_buffers: &[Arc<Mutex<VecDeque<f32>>>],
    writers: &[Option<SharedWavWriter>],
    buf: &[u8],
    payload_offset: usize,
    channel_count: usize,
) {
    let payload = &buf[payload_offset..];
    let sample_count = payload.len() / 4;
    let frame_count = sample_count / channel_count;

    for f in 0..frame_count {
        for c in 0..channel_count {
            let idx = (f * channel_count + c) * 4;
            let sample = f32::from_le_bytes(payload[idx..idx + 4].try_into().unwrap());
            safe_lock(&per_channel_buffers[c]).push_back(sample);
            if let Some(w) = &writers[c] {
                write_samples(w, &[sample]);
            }
        }
    }
}