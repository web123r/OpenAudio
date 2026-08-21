use crate::devices::{get_input_device, get_output_device, is_skip};
use crate::discovery::{start_advertising, SubscriberRegistry};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
use crate::recording::{create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter};
use crate::util::safe_lock;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58;

/// Fixed format used when a source is set to "None" and there's no
/// real device to read a channel count / sample rate from. 2ch/48kHz
/// is the most universally compatible choice for downstream
/// subscribers (matches typical stereo bus expectations).
const SILENT_CHANNELS: u16 = 2;
const SILENT_SAMPLE_RATE: u32 = 48000;

pub fn transmit(duration_secs: u64, dest_addr: &str, stream_id: u32) -> Result<(), String> {
    transmit_multi(duration_secs, &[dest_addr], stream_id)
}

pub fn transmit_multi(duration_secs: u64, dest_addrs: &[&str], stream_id: u32) -> Result<(), String> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .ok_or_else(|| "no default input device found".to_string())?;

    let config = device
        .default_input_config()
        .map_err(|e| format!("failed to get default input config: {e}"))?;

    let channels = config.channels();
    let sample_rate = config.sample_rate().0;
    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let resolved_addrs: Vec<std::net::SocketAddr> = dest_addrs
        .iter()
        .map(|a| {
            a.to_socket_addrs()
                .map_err(|e| format!("invalid address {a}: {e}"))?
                .next()
                .ok_or_else(|| format!("could not resolve address: {a}"))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let subscriber_count = resolved_addrs.len();

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;

    let sequence = Arc::new(AtomicU32::new(0));
    let sequence_clone = sequence.clone();
    let start = Instant::now();

    let stream_config: cpal::StreamConfig = config.into();
    let err_fn = |err| eprintln!("audio-core: stream error: {err}");

    let stream = device
        .build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                let frame_count = data.len() / channels as usize;
                let mut frame_offset = 0usize;

                while frame_offset < frame_count {
                    let frames_this_packet =
                        (frame_count - frame_offset).min(MAX_FRAMES_PER_PACKET);

                    let seq = sequence_clone.fetch_add(1, Ordering::Relaxed);
                    let ts_ns = start.elapsed().as_nanos() as u64;

                    let header = PacketHeader {
                        sub_stream_index: 0,
                        stream_id,
                        sequence_number: seq,
                        presentation_timestamp_ns: ts_ns,
                    };
                    let payload_header = AudioPayloadHeader {
                        channel_count: channels as u8,
                        sample_format: SAMPLE_FORMAT_FLOAT32,
                        sample_rate_code: rate_code,
                        samples_per_channel: frames_this_packet as u16,
                    };

                    let mut packet = Vec::with_capacity(
                        24 + 8 + frames_this_packet * channels as usize * 4,
                    );
                    packet.extend_from_slice(&header.to_bytes());
                    packet.extend_from_slice(&payload_header.to_bytes());

                    let sample_start = frame_offset * channels as usize;
                    let sample_end = (frame_offset + frames_this_packet) * channels as usize;
                    for &sample in &data[sample_start..sample_end] {
                        packet.extend_from_slice(&sample.to_le_bytes());
                    }

                    for addr in &resolved_addrs {
                        if let Err(e) = socket.send_to(&packet, addr) {
                            eprintln!("audio-core: send to {addr} failed: {e}");
                        }
                    }

                    frame_offset += frames_this_packet;
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build input stream: {e}"))?;

    stream
        .play()
        .map_err(|e| format!("failed to start stream: {e}"))?;

    println!(
        "Transmitting {duration_secs}s of audio ({channels}ch @ {sample_rate}Hz) to {subscriber_count} subscriber(s)..."
    );
    std::thread::sleep(Duration::from_secs(duration_secs));
    drop(stream);

    println!("Done. Sent {} packets per subscriber.", sequence.load(Ordering::Relaxed));
    Ok(())
}

pub fn transmit_with_control(
    dest_addr: &str,
    stream_id: u32,
    device_name: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let device = get_input_device(device_name.as_deref())?;

    let config = device
        .default_input_config()
        .map_err(|e| format!("failed to get default input config: {e}"))?;

    let channels = config.channels();
    let sample_rate = config.sample_rate().0;
    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let resolved_addr: std::net::SocketAddr = dest_addr
        .to_socket_addrs()
        .map_err(|e| format!("invalid address {dest_addr}: {e}"))?
        .next()
        .ok_or_else(|| format!("could not resolve address: {dest_addr}"))?;

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;
    socket
        .connect(resolved_addr)
        .map_err(|e| format!("failed to connect to {dest_addr}: {e}"))?;

    let sequence = Arc::new(AtomicU32::new(0));
    let sequence_clone = sequence.clone();
    let start = Instant::now();

    let stream_config: cpal::StreamConfig = config.into();
    let err_fn = |err| eprintln!("audio-core: stream error: {err}");

    let stream = device
        .build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                let frame_count = data.len() / channels as usize;
                let mut frame_offset = 0usize;

                while frame_offset < frame_count {
                    let frames_this_packet =
                        (frame_count - frame_offset).min(MAX_FRAMES_PER_PACKET);

                    let seq = sequence_clone.fetch_add(1, Ordering::Relaxed);
                    let ts_ns = start.elapsed().as_nanos() as u64;

                    let header = PacketHeader {
                        sub_stream_index: 0,
                        stream_id,
                        sequence_number: seq,
                        presentation_timestamp_ns: ts_ns,
                    };
                    let payload_header = AudioPayloadHeader {
                        channel_count: channels as u8,
                        sample_format: SAMPLE_FORMAT_FLOAT32,
                        sample_rate_code: rate_code,
                        samples_per_channel: frames_this_packet as u16,
                    };

                    let mut packet = Vec::with_capacity(
                        24 + 8 + frames_this_packet * channels as usize * 4,
                    );
                    packet.extend_from_slice(&header.to_bytes());
                    packet.extend_from_slice(&payload_header.to_bytes());

                    let sample_start = frame_offset * channels as usize;
                    let sample_end = (frame_offset + frames_this_packet) * channels as usize;
                    for &sample in &data[sample_start..sample_end] {
                        packet.extend_from_slice(&sample.to_le_bytes());
                    }

                    if let Err(e) = socket.send(&packet) {
                        eprintln!("audio-core: send failed: {e}");
                    }

                    frame_offset += frames_this_packet;
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build input stream: {e}"))?;

    stream
        .play()
        .map_err(|e| format!("failed to start stream: {e}"))?;

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(stream);

    Ok(())
}

pub fn transmit_with_discovery(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    // ── "None" selected -> advertise + transmit silence, no mic opened ──
    if is_skip(&device_name) {
        println!("Publish: input set to None -- transmitting silence, no device opened");
        return transmit_silence(node_name, stream_name, stream_id, subscribers_by_stream, record_path, keep_running);
    }

    let device = get_input_device(device_name.as_deref())?;

    let config = device
        .default_input_config()
        .map_err(|e| format!("failed to get default input config: {e}"))?;

    let channels = config.channels();
    let sample_rate = config.sample_rate().0;
    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;
    std::thread::spawn(move || {
        if let Err(e) = start_advertising(
            node_name,
            stream_id,
            stream_name,
            channels_u8,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {e}");
        }
    });

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;

    let sequence = Arc::new(AtomicU32::new(0));
    let sequence_clone = sequence.clone();
    let start = Instant::now();

    let stream_config: cpal::StreamConfig = config.into();
    let err_fn = |err| eprintln!("audio-core: stream error: {err}");
    let writer_for_callback = writer.clone();

    let stream = device
        .build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                ensure_realtime_audio_thread();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if let Some(w) = &writer_for_callback {
                        write_samples(w, data);
                    }

                    let dests = {
                        let map = safe_lock(&subscribers_by_stream);
                        match map.get(&stream_id) {
                            Some(list) if !list.is_empty() => list.clone(),
                            _ => return,
                        }
                    };

                    let frame_count = data.len() / channels as usize;
                    let mut frame_offset = 0usize;

                    while frame_offset < frame_count {
                        let frames_this_packet =
                            (frame_count - frame_offset).min(MAX_FRAMES_PER_PACKET);

                        let seq = sequence_clone.fetch_add(1, Ordering::Relaxed);
                        let ts_ns = start.elapsed().as_nanos() as u64;

                        let header = PacketHeader {
                            sub_stream_index: 0,
                            stream_id,
                            sequence_number: seq,
                            presentation_timestamp_ns: ts_ns,
                        };
                        let payload_header = AudioPayloadHeader {
                            channel_count: channels as u8,
                            sample_format: SAMPLE_FORMAT_FLOAT32,
                            sample_rate_code: rate_code,
                            samples_per_channel: frames_this_packet as u16,
                        };

                        let mut packet = Vec::with_capacity(
                            24 + 8 + frames_this_packet * channels as usize * 4,
                        );
                        packet.extend_from_slice(&header.to_bytes());
                        packet.extend_from_slice(&payload_header.to_bytes());

                        let sample_start = frame_offset * channels as usize;
                        let sample_end = (frame_offset + frames_this_packet) * channels as usize;
                        for &sample in &data[sample_start..sample_end] {
                            packet.extend_from_slice(&sample.to_le_bytes());
                        }

                        for addr in &dests {
                            if let Err(e) = socket.send_to(&packet, addr) {
                                eprintln!("audio-core: send to {addr} failed: {e}");
                            }
                        }

                        frame_offset += frames_this_packet;
                    }
                }));
                if result.is_err() {
                    eprintln!("audio-core: panic caught in transmit callback -- dropping this cycle's audio");
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build input stream: {e}"))?;

    stream
        .play()
        .map_err(|e| format!("failed to start stream: {e}"))?;

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(stream);

    if let Some(w) = &writer {
        finalize_recording(w);
    }

    Ok(())
}

pub fn transmit_loopback_with_discovery(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    // ── "None" selected -> advertise + transmit silence, no device opened ──
    if is_skip(&device_name) {
        println!("Publish (loopback): output set to None -- transmitting silence, no device opened");
        return transmit_silence(node_name, stream_name, stream_id, subscribers_by_stream, record_path, keep_running);
    }

    let device = get_output_device(device_name.as_deref())?;

    let config = device
        .default_output_config()
        .map_err(|e| format!("failed to get default output config for loopback device: {e}"))?;

    let channels = config.channels();
    let sample_rate = config.sample_rate().0;
    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;
    std::thread::spawn(move || {
        if let Err(e) = start_advertising(
            node_name,
            stream_id,
            stream_name,
            channels_u8,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {e}");
        }
    });

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;

    let sequence = Arc::new(AtomicU32::new(0));
    let sequence_clone = sequence.clone();
    let start = Instant::now();

    let stream_config: cpal::StreamConfig = config.into();
    let err_fn = |err| eprintln!("audio-core: loopback stream error: {err}");
    let writer_for_callback = writer.clone();

    let stream = device
        .build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                ensure_realtime_audio_thread();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if let Some(w) = &writer_for_callback {
                        write_samples(w, data);
                    }

                    let dests = {
                        let map = safe_lock(&subscribers_by_stream);
                        match map.get(&stream_id) {
                            Some(list) if !list.is_empty() => list.clone(),
                            _ => return,
                        }
                    };

                    let frame_count = data.len() / channels as usize;
                    let mut frame_offset = 0usize;

                    while frame_offset < frame_count {
                        let frames_this_packet =
                            (frame_count - frame_offset).min(MAX_FRAMES_PER_PACKET);

                        let seq = sequence_clone.fetch_add(1, Ordering::Relaxed);
                        let ts_ns = start.elapsed().as_nanos() as u64;

                        let header = PacketHeader {
                            sub_stream_index: 0,
                            stream_id,
                            sequence_number: seq,
                            presentation_timestamp_ns: ts_ns,
                        };
                        let payload_header = AudioPayloadHeader {
                            channel_count: channels as u8,
                            sample_format: SAMPLE_FORMAT_FLOAT32,
                            sample_rate_code: rate_code,
                            samples_per_channel: frames_this_packet as u16,
                        };

                        let mut packet = Vec::with_capacity(
                            24 + 8 + frames_this_packet * channels as usize * 4,
                        );
                        packet.extend_from_slice(&header.to_bytes());
                        packet.extend_from_slice(&payload_header.to_bytes());

                        let sample_start = frame_offset * channels as usize;
                        let sample_end = (frame_offset + frames_this_packet) * channels as usize;
                        for &sample in &data[sample_start..sample_end] {
                            packet.extend_from_slice(&sample.to_le_bytes());
                        }

                        for addr in &dests {
                            if let Err(e) = socket.send_to(&packet, addr) {
                                eprintln!("audio-core: send to {addr} failed: {e}");
                            }
                        }

                        frame_offset += frames_this_packet;
                    }
                }));
                if result.is_err() {
                    eprintln!("audio-core: panic caught in loopback transmit callback -- dropping this cycle's audio");
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build loopback input stream (device may not support WASAPI loopback): {e}"))?;

    stream
        .play()
        .map_err(|e| format!("failed to start loopback stream: {e}"))?;

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(stream);

    if let Some(w) = &writer {
        finalize_recording(w);
    }

    Ok(())
}

/// Shared "None" path for both mic and loopback single-source publish.
/// No physical device is opened at all -- instead a timer thread wakes
/// up roughly every MAX_FRAMES_PER_PACKET worth of time at
/// SILENT_SAMPLE_RATE and sends an all-zero packet to every current
/// subscriber, so the stream keeps advertising/existing and any
/// downstream combine/bus channel gets clean silence instead of an
/// error. Recording (if enabled) writes silence too, so file length
/// still matches wall-clock session duration.
fn transmit_silence(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    let channels = SILENT_CHANNELS;
    let sample_rate = SILENT_SAMPLE_RATE;
    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;
    std::thread::spawn(move || {
        if let Err(e) = start_advertising(
            node_name,
            stream_id,
            stream_name,
            channels_u8,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {e}");
        }
    });

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;
    let sequence = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    let frames_per_packet = MAX_FRAMES_PER_PACKET;
    let packet_interval = Duration::from_secs_f64(frames_per_packet as f64 / sample_rate as f64);
    let silent_samples = vec![0.0f32; frames_per_packet * channels as usize];

    println!("Publish: sending silent keep-alive stream ({channels}ch @ {sample_rate}Hz)...");

    while keep_running.load(Ordering::Relaxed) {
        if let Some(w) = &writer {
            write_samples(w, &silent_samples);
        }

        let dests = {
            let map = safe_lock(&subscribers_by_stream);
            match map.get(&stream_id) {
                Some(list) if !list.is_empty() => Some(list.clone()),
                _ => None,
            }
        };

        if let Some(dests) = dests {
            let seq = sequence.fetch_add(1, Ordering::Relaxed);
            let ts_ns = start.elapsed().as_nanos() as u64;

            let header = PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number: seq,
                presentation_timestamp_ns: ts_ns,
            };
            let payload_header = AudioPayloadHeader {
                channel_count: channels as u8,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: rate_code,
                samples_per_channel: frames_per_packet as u16,
            };

            let mut packet = Vec::with_capacity(24 + 8 + silent_samples.len() * 4);
            packet.extend_from_slice(&header.to_bytes());
            packet.extend_from_slice(&payload_header.to_bytes());
            for sample in &silent_samples {
                packet.extend_from_slice(&sample.to_le_bytes());
            }

            for addr in &dests {
                if let Err(e) = socket.send_to(&packet, addr) {
                    eprintln!("audio-core: send to {addr} failed: {e}");
                }
            }
        }

        std::thread::sleep(packet_interval);
    }

    if let Some(w) = &writer {
        finalize_recording(w);
    }

    println!("Publish: silent stream stopped.");
    Ok(())
}