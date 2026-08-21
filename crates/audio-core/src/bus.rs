use crate::devices::{get_output_device, is_skip};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{parse_packet, ParsedPacket};
use crate::recording::{create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter};
use crate::util::safe_lock;
use crate::JITTER_BUFFER_TARGET_SECS;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type VolumeControl = Arc<AtomicU32>;

pub fn new_volume_control(initial: f32) -> VolumeControl {
    Arc::new(AtomicU32::new(initial.to_bits()))
}

pub fn set_volume(control: &VolumeControl, value: f32) {
    control.store(value.to_bits(), Ordering::Relaxed);
}

pub fn get_volume(control: &VolumeControl) -> f32 {
    f32::from_bits(control.load(Ordering::Relaxed))
}

pub fn receive_and_play_bus(bind_addr: &str, duration_secs: u64) -> Result<(), String> {
    let keep_running = Arc::new(AtomicBool::new(true));
    let keep_running_timer = keep_running.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(duration_secs));
        keep_running_timer.store(false, Ordering::Relaxed);
    });
    let result = receive_and_play_bus_with_control(bind_addr, None, keep_running);
    let _ = handle.join();
    result
}

pub fn receive_and_play_bus_with_control(
    bind_addr: &str,
    device_name: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    receive_and_play_bus_with_volume(bind_addr, device_name, new_volume_control(1.0), None, keep_running)
}

pub fn receive_and_play_bus_with_volume(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Bus listening on {bind_addr}, waiting for first packet to detect format...");

    let buffers: Arc<Mutex<HashMap<u32, VecDeque<f32>>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut buf = [0u8; 65536];

    let (channel_count, sample_rate) = loop {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        match socket.recv_from(&mut buf) {
            Ok((len, _src)) => {
                if let Some(parsed) = parse_packet(&buf[..len]) {
                    push_samples(&buffers, &buf, &parsed);
                    break (parsed.channel_count as u16, parsed.sample_rate);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error while waiting for first packet: {e}")),
        }
    };

    println!("Bus detected {channel_count}ch @ {sample_rate}Hz. Setting up playback...");

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channel_count, sample_rate)?),
        None => None,
    };

    // ── "None" selected -> headless mode, no physical device opened ──
    if is_skip(&device_name) {
        println!("Bus: output set to None -- running headless (no audio hardware opened)");

        let headless_buffers = buffers.clone();
        let headless_volume = volume.clone();
        let headless_writer = writer.clone();
        let headless_keep_running = keep_running.clone();
        std::thread::spawn(move || {
            ensure_realtime_audio_thread();
            run_headless_bus(
                headless_buffers,
                channel_count as usize,
                sample_rate,
                headless_volume,
                headless_writer,
                headless_keep_running,
            );
        });

        return run_receive_loop(&socket, &mut buf, &buffers, channel_count, sample_rate, keep_running);
    }

    // ── Normal device path ──
    let device = get_output_device(device_name.as_deref())?;
    let device_label = device.name().unwrap_or_else(|_| "unknown device".to_string());

    // Pick the best available output config for this sample rate.
    // Prefer an exact channel-count match (no remap needed), but
    // fall back to whatever the device supports -- we no longer
    // hard-fail on channel mismatch, since remap_frame() below
    // handles arbitrary in/out channel combinations.
    let output_config = pick_output_config(&device, sample_rate, channel_count)
        .map_err(|e| format!("device '{device_label}': {e}"))?;

    let out_channels = output_config.channels() as usize;
    let in_channels = channel_count as usize;

    if out_channels != in_channels {
        println!(
            "Bus: remapping {in_channels}ch bus -> {out_channels}ch device '{device_label}' (downmix/upmix active)"
        );
    }

    let stream_config: cpal::StreamConfig = output_config.into();
    let buffers_for_callback = buffers.clone();
    let volume_for_callback = volume.clone();
    let writer_for_callback = writer.clone();
    let err_fn = |err| eprintln!("audio-core: bus playback stream error: {err}");

    let stream = device
        .build_output_stream(
            &stream_config,
            move |data: &mut [f32], _| {
                ensure_realtime_audio_thread();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let gain = f32::from_bits(volume_for_callback.load(Ordering::Relaxed));
                    let mut buffers = safe_lock(&buffers_for_callback);
                    let frame_count = data.len() / out_channels;

                    for f in 0..frame_count {
                        let mut mixed = vec![0.0f32; out_channels];

                        // Each buffer is one incoming STREAM (a source),
                        // interleaved at in_channels. Pop one full input
                        // frame per stream, remap it to out_channels,
                        // then sum across streams (multi-source mixing).
                        for buf in buffers.values_mut() {
                            let mut in_frame = vec![0.0f32; in_channels];
                            for s in in_frame.iter_mut() {
                                *s = buf.pop_front().unwrap_or(0.0);
                            }
                            let out_frame = remap_frame(&in_frame, out_channels);
                            for (m, o) in mixed.iter_mut().zip(out_frame.iter()) {
                                *m += o;
                            }
                        }

                        for c in 0..out_channels {
                            data[f * out_channels + c] = (mixed[c] * gain).clamp(-1.0, 1.0);
                        }
                    }

                    if let Some(w) = &writer_for_callback {
                        write_samples(w, data);
                    }
                }));
                if result.is_err() {
                    eprintln!("audio-core: panic caught in playback callback -- outputting silence this cycle");
                    for sample in data.iter_mut() {
                        *sample = 0.0;
                    }
                }
            },
            err_fn,
            None,
        )
        .map_err(|e| format!("failed to build output stream: {e}"))?;

    prime_bus_buffers(
        &socket,
        &mut buf,
        &buffers,
        channel_count,
        sample_rate,
        keep_running.clone(),
    )?;

    stream.play().map_err(|e| format!("failed to start playback stream: {e}"))?;
    println!("Bus playing back...");

    let result = run_receive_loop(&socket, &mut buf, &buffers, channel_count, sample_rate, keep_running);

    drop(stream);

    if let Some(w) = &writer {
        finalize_recording(w);
    }

    result
}

fn total_buffered_samples(buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>) -> usize {
    safe_lock(buffers).values().map(|b| b.len()).sum()
}

fn prime_bus_buffers(
    socket: &UdpSocket,
    buf: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: u16,
    sample_rate: u32,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let target_samples =
        ((sample_rate as f64 * JITTER_BUFFER_TARGET_SECS) as usize) * channel_count as usize;
    let prime_deadline = Instant::now() + Duration::from_millis(500);

    while total_buffered_samples(buffers) < target_samples && Instant::now() < prime_deadline {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        match socket.recv_from(buf) {
            Ok((len, _src)) => {
                if let Some(parsed) = parse_packet(&buf[..len]) {
                    if parsed.channel_count as u16 == channel_count && parsed.sample_rate == sample_rate {
                        push_samples(buffers, buf, &parsed);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error while priming bus buffer: {e}")),
        }
    }

    Ok(())
}

/// Shared receive loop used by both the headless path (device_name ==
/// None sentinel "None") and the normal device path. Extracted so the
/// two paths can never silently drift apart in packet-handling logic.
fn run_receive_loop(
    socket: &UdpSocket,
    buf: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: u16,
    sample_rate: u32,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    let mut packets_received: u32 = 0;
    let mut streams_seen: HashSet<u32> = HashSet::new();

    while keep_running.load(Ordering::Relaxed) {
        let (len, _src) = match socket.recv_from(buf) {
            Ok(r) => r,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else { continue };

        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            eprintln!("audio-core: bus dropping packet from stream {} -- format mismatch", parsed.stream_id);
            continue;
        }

        streams_seen.insert(parsed.stream_id);
        packets_received += 1;
        push_samples(buffers, buf, &parsed);

        let max_samples = ((sample_rate as f64 * 0.2) as usize) * channel_count as usize;
        let mut guard = safe_lock(buffers);
        if let Some(b) = guard.get_mut(&parsed.stream_id) {
            while b.len() > max_samples {
                b.pop_front();
            }
        }
    }

    println!(
        "Done. {packets_received} packets received from {} distinct stream(s): {:?}",
        streams_seen.len(),
        streams_seen
    );
    Ok(())
}

/// Drains and mixes the incoming buffers at roughly real-time pace
/// with no physical output device opened -- used when the user picks
/// "None" for the output. Keeps WAV recording timing correct and
/// prevents buffers from growing unbounded while headless.
fn run_headless_bus(
    buffers: Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: usize,
    sample_rate: u32,
    volume: VolumeControl,
    writer: Option<SharedWavWriter>,
    keep_running: Arc<AtomicBool>,
) {
    ensure_realtime_audio_thread();
    let chunk_ms = 10u64;
    let frames_per_chunk = ((sample_rate as u64 * chunk_ms) / 1000).max(1) as usize;
    let sleep_dur = Duration::from_millis(chunk_ms);

    while keep_running.load(Ordering::Relaxed) {
        let gain = get_volume(&volume);
        let mut mixed = vec![0.0f32; frames_per_chunk * channel_count];

        {
            let mut guard = safe_lock(&buffers);
            for buf in guard.values_mut() {
                for slot in mixed.iter_mut() {
                    if let Some(s) = buf.pop_front() {
                        *slot += s;
                    }
                }
            }
        }

        for s in mixed.iter_mut() {
            *s = (*s * gain).clamp(-1.0, 1.0);
        }

        if let Some(w) = &writer {
            write_samples(w, &mixed);
        }

        std::thread::sleep(sleep_dur);
    }

    if let Some(w) = &writer {
        finalize_recording(w);
    }
    println!("Bus: headless playback stopped.");
}

/// Finds the best cpal output config for `sample_rate`, preferring an
/// exact match on `preferred_channels`. Falls back to whatever config
/// supports the sample rate, choosing the highest channel count
/// available (so multichannel ASIO/interface outputs get used at
/// full capacity instead of being ignored). Falls back to the
/// device's plain default config as a last resort.
fn pick_output_config(
    device: &cpal::Device,
    sample_rate: u32,
    preferred_channels: u16,
) -> Result<cpal::SupportedStreamConfig, String> {
    let supported: Vec<_> = device
        .supported_output_configs()
        .map_err(|e| format!("failed to query supported output configs: {e}"))?
        .collect();

    let in_range = |c: &cpal::SupportedStreamConfigRange| {
        sample_rate >= c.min_sample_rate().0 && sample_rate <= c.max_sample_rate().0
    };

    if let Some(c) = supported.iter().find(|c| c.channels() == preferred_channels && in_range(c)) {
        return Ok(c.clone().with_sample_rate(cpal::SampleRate(sample_rate)));
    }

    if let Some(c) = supported.iter().filter(|c| in_range(c)).max_by_key(|c| c.channels()) {
        return Ok(c.clone().with_sample_rate(cpal::SampleRate(sample_rate)));
    }

    device
        .default_output_config()
        .map_err(|e| format!("no config matches {sample_rate}Hz and default config unavailable: {e}"))
}

/// Remaps one interleaved input frame (in_channels samples) to an
/// output frame (out_channels samples). Pragmatic rules, not a full
/// mixing console:
/// - equal counts: passthrough
/// - anything -> mono: average all input channels
/// - mono -> anything: duplicate to every output channel
/// - N -> stereo (N>2): even channels average into L, odd into R
/// - N -> M (general downmix, N>M): round-robin sum into M outputs,
///   scaled down so it doesn't clip
/// - N -> M (upmix, N<M): copy input channels into the first N
///   outputs, silence on the rest
fn remap_frame(input: &[f32], out_channels: usize) -> Vec<f32> {
    let in_channels = input.len();

    if in_channels == out_channels {
        return input.to_vec();
    }

    if out_channels == 1 {
        let sum: f32 = input.iter().sum();
        return vec![sum / in_channels.max(1) as f32];
    }

    if in_channels == 1 {
        return vec![input[0]; out_channels];
    }

    if out_channels == 2 {
        let mut l = 0.0f32;
        let mut r = 0.0f32;
        let mut lc = 0usize;
        let mut rc = 0usize;
        for (i, &s) in input.iter().enumerate() {
            if i % 2 == 0 {
                l += s;
                lc += 1;
            } else {
                r += s;
                rc += 1;
            }
        }
        return vec![l / lc.max(1) as f32, r / rc.max(1) as f32];
    }

    if in_channels < out_channels {
        let mut v = input.to_vec();
        v.resize(out_channels, 0.0);
        return v;
    }

    let mut v = vec![0.0f32; out_channels];
    let mut counts = vec![0usize; out_channels];
    for (i, &s) in input.iter().enumerate() {
        let bucket = i % out_channels;
        v[bucket] += s;
        counts[bucket] += 1;
    }
    for (val, count) in v.iter_mut().zip(counts.iter()) {
        *val /= (*count).max(1) as f32;
    }
    v
}

fn push_samples(buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>, buf: &[u8], parsed: &ParsedPacket) {
    let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
    let payload = &buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4];
    let mut guard = safe_lock(buffers);
    let entry = guard.entry(parsed.stream_id).or_insert_with(VecDeque::new);
    for chunk in payload.chunks_exact(4) {
        entry.push_back(f32::from_le_bytes(chunk.try_into().unwrap()));
    }
}