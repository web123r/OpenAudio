use crate::devices::{get_output_device, is_skip};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{parse_packet, ParsedPacket};
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter,
};
use crate::util::safe_lock;
use crate::{
    register_scoped_signal_meter, SignalDirection, SignalMeter, JITTER_BUFFER_TARGET_SECS,
};
use cpal::traits::{DeviceTrait, StreamTrait};
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
    receive_and_play_bus_with_volume(
        bind_addr,
        device_name,
        new_volume_control(1.0),
        None,
        keep_running,
    )
}

pub fn receive_and_play_bus_with_volume(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    let socket = UdpSocket::bind(bind_addr)
        .map_err(|error| format!("failed to bind {bind_addr}: {error}"))?;

    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|error| format!("failed to set read timeout: {error}"))?;

    println!(
        "Bus listening on {bind_addr}, waiting for first packet \
         to detect format..."
    );

    let buffers: Arc<Mutex<HashMap<u32, VecDeque<f32>>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut packet_buffer = [0u8; 65_536];

    let (channel_count, sample_rate) = loop {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }

        match socket.recv_from(&mut packet_buffer) {
            Ok((length, _source)) => {
                if let Some(parsed) = parse_packet(&packet_buffer[..length]) {
                    push_samples(&buffers, &packet_buffer[..length], &parsed);

                    break (parsed.channel_count as u16, parsed.sample_rate);
                }
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "recv error while waiting for first packet: \
                     {error}"
                ));
            }
        }
    };

    println!(
        "Bus detected {channel_count}ch @ {sample_rate}Hz. \
         Setting up playback..."
    );

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channel_count, sample_rate)?),
        None => None,
    };

    // Headless path: drain, mix, record, and meter the bus without
    // opening physical output hardware.
    if is_skip(&device_name) {
        println!(
            "Bus: output set to None — running headless \
             (no audio hardware opened)"
        );

        let (signal_meter, signal_meter_guard) = register_scoped_signal_meter(
            format!("bus-output:{bind_addr}"),
            format!("Mixed Subscribe — {bind_addr} (headless)"),
            SignalDirection::Output,
            channel_count as usize,
        );

        let headless_buffers = buffers.clone();
        let headless_volume = volume.clone();
        let headless_writer = writer.clone();
        let headless_keep_running = keep_running.clone();

        std::thread::spawn(move || {
            ensure_realtime_audio_thread();

            // Keep the meter registered for the lifetime of this
            // headless playback worker.
            let _signal_meter_guard = signal_meter_guard;

            run_headless_bus(
                headless_buffers,
                channel_count as usize,
                sample_rate,
                headless_volume,
                headless_writer,
                headless_keep_running,
                signal_meter,
            );
        });

        return run_receive_loop(
            &socket,
            &mut packet_buffer,
            &buffers,
            channel_count,
            sample_rate,
            keep_running,
        );
    }

    // Physical output path.
    let device = get_output_device(device_name.as_deref())?;

    let device_label = device
        .name()
        .unwrap_or_else(|_| "unknown device".to_string());

    let output_config = pick_output_config(&device, sample_rate, channel_count)
        .map_err(|error| format!("device '{device_label}': {error}"))?;

    let output_channels = output_config.channels() as usize;
    let input_channels = channel_count as usize;

    if output_channels != input_channels {
        println!(
            "Bus: remapping {input_channels}ch bus -> \
             {output_channels}ch device '{device_label}' \
             (downmix/upmix active)"
        );
    }

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("bus-output:{bind_addr}"),
        format!("Mixed Subscribe — {bind_addr} → {device_label}"),
        SignalDirection::Output,
        output_channels,
    );

    let stream_config: cpal::StreamConfig = output_config.into();

    let buffers_for_callback = buffers.clone();
    let volume_for_callback = volume.clone();
    let writer_for_callback = writer.clone();
    let signal_meter_for_callback = signal_meter.clone();

    let error_callback = |error| {
        eprintln!("audio-core: bus playback stream error: {error}");
    };

    let stream = device
        .build_output_stream(
            &stream_config,
            move |data: &mut [f32], _| {
                ensure_realtime_audio_thread();

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    render_bus_output(
                        data,
                        output_channels,
                        input_channels,
                        &buffers_for_callback,
                        &volume_for_callback,
                    );

                    // Observe the final samples after mixing,
                    // channel remapping, gain, and clipping.
                    signal_meter_for_callback.observe_interleaved(data, output_channels);

                    if let Some(writer) = &writer_for_callback {
                        write_samples(writer, data);
                    }
                }));

                if result.is_err() {
                    eprintln!(
                        "audio-core: panic caught in playback \
                         callback — outputting silence this cycle"
                    );

                    data.fill(0.0);

                    signal_meter_for_callback.observe_interleaved(data, output_channels);
                }
            },
            error_callback,
            None,
        )
        .map_err(|error| format!("failed to build output stream: {error}"))?;

    prime_bus_buffers(
        &socket,
        &mut packet_buffer,
        &buffers,
        channel_count,
        sample_rate,
        keep_running.clone(),
    )?;

    stream
        .play()
        .map_err(|error| format!("failed to start playback stream: {error}"))?;

    println!("Bus playing back...");

    let result = run_receive_loop(
        &socket,
        &mut packet_buffer,
        &buffers,
        channel_count,
        sample_rate,
        keep_running,
    );

    drop(stream);

    if let Some(writer) = &writer {
        finalize_recording(writer);
    }

    result
}

/// Renders one physical output callback.
///
/// The mixed-frame and input-frame buffers are allocated once for each
/// callback invocation rather than once for every rendered frame.
fn render_bus_output(
    data: &mut [f32],
    output_channels: usize,
    input_channels: usize,
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    volume: &VolumeControl,
) {
    if output_channels == 0 || input_channels == 0 {
        data.fill(0.0);
        return;
    }

    let gain = get_volume(volume);
    let frame_count = data.len() / output_channels;
    let mut buffers_guard = safe_lock(buffers);

    let mut mixed_frame = vec![0.0f32; output_channels];
    let mut input_frame = vec![0.0f32; input_channels];
    let mut remapped_frame = vec![0.0f32; output_channels];

    for frame_index in 0..frame_count {
        mixed_frame.fill(0.0);

        for source_buffer in buffers_guard.values_mut() {
            for sample in input_frame.iter_mut() {
                *sample = source_buffer.pop_front().unwrap_or(0.0);
            }

            remap_frame_into(&input_frame, &mut remapped_frame);

            for (mixed_sample, remapped_sample) in mixed_frame.iter_mut().zip(remapped_frame.iter())
            {
                *mixed_sample += *remapped_sample;
            }
        }

        let output_offset = frame_index * output_channels;

        for channel in 0..output_channels {
            data[output_offset + channel] = (mixed_frame[channel] * gain).clamp(-1.0, 1.0);
        }
    }
}

fn total_buffered_samples(buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>) -> usize {
    safe_lock(buffers).values().map(VecDeque::len).sum()
}

fn prime_bus_buffers(
    socket: &UdpSocket,
    packet_buffer: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: u16,
    sample_rate: u32,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let target_samples = ((sample_rate as f64 * JITTER_BUFFER_TARGET_SECS) as usize)
        .saturating_mul(channel_count as usize);

    let prime_deadline = Instant::now() + Duration::from_millis(500);

    while total_buffered_samples(buffers) < target_samples && Instant::now() < prime_deadline {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }

        match socket.recv_from(packet_buffer) {
            Ok((length, _source)) => {
                let packet = &packet_buffer[..length];

                if let Some(parsed) = parse_packet(packet) {
                    if parsed.channel_count as u16 == channel_count
                        && parsed.sample_rate == sample_rate
                    {
                        push_samples(buffers, packet, &parsed);
                    }
                }
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "recv error while priming bus buffer: \
                     {error}"
                ));
            }
        }
    }

    Ok(())
}

/// Shared packet receive loop for physical and headless playback.
fn run_receive_loop(
    socket: &UdpSocket,
    packet_buffer: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: u16,
    sample_rate: u32,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    let mut packets_received = 0u32;
    let mut streams_seen = HashSet::<u32>::new();

    while keep_running.load(Ordering::Relaxed) {
        let (length, _source) = match socket.recv_from(packet_buffer) {
            Ok(result) => result,
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                continue;
            }
            Err(error) => {
                return Err(format!("recv error: {error}"));
            }
        };

        let packet = &packet_buffer[..length];

        let Some(parsed) = parse_packet(packet) else {
            continue;
        };

        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            eprintln!(
                "audio-core: bus dropping packet from stream \
                 {} — format mismatch",
                parsed.stream_id
            );
            continue;
        }

        streams_seen.insert(parsed.stream_id);
        packets_received = packets_received.saturating_add(1);

        push_samples(buffers, packet, &parsed);

        let maximum_samples =
            ((sample_rate as f64 * 0.2) as usize).saturating_mul(channel_count as usize);

        let mut guard = safe_lock(buffers);

        if let Some(stream_buffer) = guard.get_mut(&parsed.stream_id) {
            while stream_buffer.len() > maximum_samples {
                stream_buffer.pop_front();
            }
        }
    }

    println!(
        "Done. {packets_received} packets received from {} \
         distinct stream(s): {:?}",
        streams_seen.len(),
        streams_seen
    );

    Ok(())
}

/// Drains and mixes incoming streams at approximately real-time pace
/// without opening physical output hardware.
fn run_headless_bus(
    buffers: Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    channel_count: usize,
    sample_rate: u32,
    volume: VolumeControl,
    writer: Option<SharedWavWriter>,
    keep_running: Arc<AtomicBool>,
    signal_meter: Arc<SignalMeter>,
) {
    ensure_realtime_audio_thread();

    let chunk_milliseconds = 10u64;

    let frames_per_chunk = ((sample_rate as u64 * chunk_milliseconds) / 1_000).max(1) as usize;

    let sleep_duration = Duration::from_millis(chunk_milliseconds);

    let mut mixed = vec![0.0f32; frames_per_chunk.saturating_mul(channel_count)];

    while keep_running.load(Ordering::Relaxed) {
        let gain = get_volume(&volume);
        mixed.fill(0.0);

        {
            let mut guard = safe_lock(&buffers);

            for stream_buffer in guard.values_mut() {
                for sample in mixed.iter_mut() {
                    if let Some(source_sample) = stream_buffer.pop_front() {
                        *sample += source_sample;
                    }
                }
            }
        }

        for sample in mixed.iter_mut() {
            *sample = (*sample * gain).clamp(-1.0, 1.0);
        }

        // Headless output is metered after mixing and volume.
        signal_meter.observe_interleaved(&mixed, channel_count);

        if let Some(writer) = &writer {
            write_samples(writer, &mixed);
        }

        std::thread::sleep(sleep_duration);
    }

    if let Some(writer) = &writer {
        finalize_recording(writer);
    }

    println!("Bus: headless playback stopped.");
}

/// Finds the best output config for the incoming sample rate.
///
/// An exact channel-count match is preferred. Otherwise, the highest
/// available channel count supporting the sample rate is selected.
fn pick_output_config(
    device: &cpal::Device,
    sample_rate: u32,
    preferred_channels: u16,
) -> Result<cpal::SupportedStreamConfig, String> {
    let supported: Vec<_> = device
        .supported_output_configs()
        .map_err(|error| {
            format!(
                "failed to query supported output configs: \
                 {error}"
            )
        })?
        .collect();

    let supports_sample_rate = |config: &cpal::SupportedStreamConfigRange| {
        sample_rate >= config.min_sample_rate().0 && sample_rate <= config.max_sample_rate().0
    };

    if let Some(config) = supported
        .iter()
        .find(|config| config.channels() == preferred_channels && supports_sample_rate(config))
    {
        return Ok(config
            .clone()
            .with_sample_rate(cpal::SampleRate(sample_rate)));
    }

    if let Some(config) = supported
        .iter()
        .filter(|config| supports_sample_rate(config))
        .max_by_key(|config| config.channels())
    {
        return Ok(config
            .clone()
            .with_sample_rate(cpal::SampleRate(sample_rate)));
    }

    device.default_output_config().map_err(|error| {
        format!(
            "no config matches {sample_rate}Hz and default \
             config unavailable: {error}"
        )
    })
}

/// Remaps one input frame into a preallocated output frame.
fn remap_frame_into(input: &[f32], output: &mut [f32]) {
    output.fill(0.0);

    let input_channels = input.len();
    let output_channels = output.len();

    if input_channels == 0 || output_channels == 0 {
        return;
    }

    if input_channels == output_channels {
        output.copy_from_slice(input);
        return;
    }

    if output_channels == 1 {
        let sum: f32 = input.iter().sum();
        output[0] = sum / input_channels as f32;
        return;
    }

    if input_channels == 1 {
        output.fill(input[0]);
        return;
    }

    if output_channels == 2 {
        let mut left = 0.0f32;
        let mut right = 0.0f32;
        let mut left_count = 0usize;
        let mut right_count = 0usize;

        for (index, &sample) in input.iter().enumerate() {
            if index % 2 == 0 {
                left += sample;
                left_count += 1;
            } else {
                right += sample;
                right_count += 1;
            }
        }

        output[0] = left / left_count.max(1) as f32;
        output[1] = right / right_count.max(1) as f32;
        return;
    }

    if input_channels < output_channels {
        output[..input_channels].copy_from_slice(input);
        return;
    }

    let mut counts = vec![0usize; output_channels];

    for (index, &sample) in input.iter().enumerate() {
        let output_channel = index % output_channels;
        output[output_channel] += sample;
        counts[output_channel] += 1;
    }

    for (sample, count) in output.iter_mut().zip(counts.iter()) {
        *sample /= (*count).max(1) as f32;
    }
}

fn push_samples(
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    packet: &[u8],
    parsed: &ParsedPacket,
) {
    let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;

    let payload_bytes = match sample_count.checked_mul(4) {
        Some(value) => value,
        None => return,
    };

    let payload_end = match parsed.payload_offset.checked_add(payload_bytes) {
        Some(value) => value,
        None => return,
    };

    let Some(payload) = packet.get(parsed.payload_offset..payload_end) else {
        eprintln!(
            "audio-core: bus dropping truncated payload from \
             stream {}",
            parsed.stream_id
        );
        return;
    };

    let mut guard = safe_lock(buffers);

    let stream_buffer = guard.entry(parsed.stream_id).or_insert_with(VecDeque::new);

    for bytes in payload.chunks_exact(4) {
        stream_buffer.push_back(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
}
