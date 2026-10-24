use crate::crypto::StreamCipher;
use crate::devices::is_skip;
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter,
};
use crate::util::{safe_lock, trim_interleaved_queue};
use crate::{
    ensure_realtime_audio_thread, register_scoped_signal_meter, SignalDirection, SignalMeter,
    StreamingLinearResampler,
};
use network_core::fec::FecDecoder;
use network_core::plc::PacketLossConcealer;
use network_core::protocol::{
    decode_float32, float32_payload, parse_fec_packet, parse_packet, peek_packet_type,
    ParsedPacket, PACKET_TYPE_FEC,
};
use network_core::receiver::{PacketReorderWindow, ReceiveEvent};
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
    receive_and_play_bus_filtered(
        bind_addr,
        device_name,
        volume,
        record_path,
        keep_running,
        None,
    )
}

pub fn receive_and_play_bus_filtered(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
) -> Result<(), String> {
    receive_and_play_bus_filtered_with_status(
        bind_addr,
        device_name,
        volume,
        record_path,
        keep_running,
        target_stream_id,
        None,
    )
}

pub fn receive_and_play_bus_filtered_with_status(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
    status_text: Option<Arc<Mutex<String>>>,
) -> Result<(), String> {
    receive_and_play_bus_filtered_with_status_and_jitter(
        bind_addr,
        device_name,
        volume,
        record_path,
        keep_running,
        target_stream_id,
        status_text,
        None,
    )
}

pub fn receive_and_play_bus_filtered_with_status_and_jitter(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
    status_text: Option<Arc<Mutex<String>>>,
    jitter_target_ms: Option<Arc<AtomicU32>>,
) -> Result<(), String> {
    receive_and_play_bus_filtered_with_status_jitter_and_cipher(
        bind_addr,
        device_name,
        volume,
        record_path,
        keep_running,
        target_stream_id,
        status_text,
        jitter_target_ms,
        None,
    )
}

pub fn receive_and_play_bus_filtered_with_status_jitter_and_cipher(
    bind_addr: &str,
    device_name: Option<String>,
    volume: VolumeControl,
    record_path: Option<String>,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
    status_text: Option<Arc<Mutex<String>>>,
    jitter_target_ms: Option<Arc<AtomicU32>>,
    cipher: Option<Arc<StreamCipher>>,
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

    let (channel_count, sample_rate, first_packet) = loop {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }

        match socket.recv_from(&mut packet_buffer) {
            Ok((length, _source)) => {
                let raw_packet = &packet_buffer[..length];
                let decrypted_buf: Vec<u8>;
                let packet: &[u8] = if let Some(ref c) = cipher {
                    if raw_packet.len() >= 6 && raw_packet[5] == network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED {
                        match crate::crypto::decrypt_audio_packet(c, raw_packet) {
                            Ok(dec) => {
                                decrypted_buf = dec;
                                &decrypted_buf
                            }
                            Err(_) => continue,
                        }
                    } else {
                        continue;
                    }
                } else {
                    raw_packet
                };

                if let Some(parsed) = parse_packet(packet) {
                    if let Some(target) = target_stream_id {
                        if parsed.stream_id != target {
                            continue;
                        }
                    }
                    let is_supported = float32_payload(packet, &parsed).is_some()
                        || (parsed.sample_format == network_core::protocol::SAMPLE_FORMAT_OPUS
                            && network_core::protocol::opus_payload(packet, &parsed).is_some());
                    if !is_supported {
                        continue;
                    }
                    break (
                        parsed.channel_count as u16,
                        parsed.sample_rate,
                        packet.to_vec(),
                    );
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
        let mut resamplers = HashMap::new();
        let first_parsed = parse_packet(&first_packet)
            .ok_or_else(|| "first bus packet became invalid".to_string())?;
        push_samples(
            &buffers,
            &mut resamplers,
            &first_packet,
            &first_parsed,
            sample_rate,
        )?;

        let writer: Option<SharedWavWriter> = match &record_path {
            Some(path) => Some(create_wav_writer(path, channel_count, sample_rate)?),
            None => None,
        };
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
            &mut resamplers,
            channel_count,
            sample_rate,
            keep_running,
            target_stream_id,
            status_text,
            jitter_target_ms,
            cipher,
        );
    }

    // Physical output path.
    let backend = crate::backend::get_backend();
    let (device_label, output_config) = backend.get_output_config(device_name.as_deref())?;

    let output_channels = output_config.channels as usize;
    let output_sample_rate = output_config.sample_rate;
    let input_channels = channel_count as usize;

    if output_channels == 0 {
        return Err(format!(
            "output device '{device_label}' exposes zero channels"
        ));
    }

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(
            path,
            output_channels as u16,
            output_sample_rate,
        )?),
        None => None,
    };

    let mut resamplers = HashMap::new();
    let first_parsed =
        parse_packet(&first_packet).ok_or_else(|| "first bus packet became invalid".to_string())?;
    push_samples(
        &buffers,
        &mut resamplers,
        &first_packet,
        &first_parsed,
        output_sample_rate,
    )?;

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

    let buffers_for_callback = buffers.clone();
    let volume_for_callback = volume.clone();
    let writer_for_callback = writer.clone();
    let signal_meter_for_callback = signal_meter.clone();
    let mut mixed_frame = vec![0.0f32; output_channels];
    let mut input_frame = vec![0.0f32; input_channels];
    let mut remapped_frame = vec![0.0f32; output_channels];

    let stream = backend.build_output_stream(
        device_name.as_deref(),
        Box::new(move |data: &mut [f32]| {
            ensure_realtime_audio_thread();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                render_bus_output(
                    data,
                    output_channels,
                    input_channels,
                    &buffers_for_callback,
                    &volume_for_callback,
                    &mut mixed_frame,
                    &mut input_frame,
                    &mut remapped_frame,
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
            }
        }),
    )?;

    let initial_target_ms = jitter_target_ms
        .as_ref()
        .map(|a| a.load(Ordering::Relaxed))
        .unwrap_or(20)
        .clamp(3, 200) as f64;

    prime_bus_buffers(
        &socket,
        &mut packet_buffer,
        &buffers,
        &mut resamplers,
        channel_count,
        sample_rate,
        output_sample_rate,
        keep_running.clone(),
        target_stream_id,
        initial_target_ms / 1000.0,
        cipher.as_deref(),
    )?;

    stream
        .play()
        .map_err(|error| format!("failed to start playback stream: {error}"))?;

    println!("Bus playing back...");

    let result = run_receive_loop(
        &socket,
        &mut packet_buffer,
        &buffers,
        &mut resamplers,
        channel_count,
        output_sample_rate,
        keep_running,
        target_stream_id,
        status_text,
        jitter_target_ms,
        cipher,
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
    mixed_frame: &mut [f32],
    input_frame: &mut [f32],
    remapped_frame: &mut [f32],
) {
    if output_channels == 0 || input_channels == 0 {
        data.fill(0.0);
        return;
    }

    let gain = get_volume(volume);
    let frame_count = data.len() / output_channels;
    let mut buffers_guard = safe_lock(buffers);

    for frame_index in 0..frame_count {
        mixed_frame.fill(0.0);

        for source_buffer in buffers_guard.values_mut() {
            for sample in input_frame.iter_mut() {
                // Smooth exponential decay if buffer momentarily runs dry, avoiding sharp Dirac step clicks
                *sample = source_buffer.pop_front().unwrap_or_else(|| *sample * 0.85);
            }

            remap_frame_into(input_frame, remapped_frame);

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

    data[frame_count * output_channels..].fill(0.0);
}

fn total_buffered_samples(buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>) -> usize {
    safe_lock(buffers).values().map(VecDeque::len).sum()
}

fn prime_bus_buffers(
    socket: &UdpSocket,
    packet_buffer: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    resamplers: &mut HashMap<u32, (u32, StreamingLinearResampler)>,
    channel_count: u16,
    sample_rate: u32,
    output_sample_rate: u32,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
    jitter_target_secs: f64,
    cipher: Option<&StreamCipher>,
) -> Result<(), String> {
    let target_samples = ((output_sample_rate as f64 * jitter_target_secs) as usize)
        .saturating_mul(channel_count as usize);

    let prime_deadline = Instant::now() + Duration::from_millis((jitter_target_secs * 1000.0 * 2.5).clamp(25.0, 500.0) as u64);

    while total_buffered_samples(buffers) < target_samples && Instant::now() < prime_deadline {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }

        match socket.recv_from(packet_buffer) {
            Ok((length, _source)) => {
                let raw_packet = &packet_buffer[..length];
                let decrypted_buf: Vec<u8>;
                let packet: &[u8] = if let Some(c) = cipher {
                    if raw_packet.len() >= 6 && raw_packet[5] == network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED {
                        match crate::crypto::decrypt_audio_packet(c, raw_packet) {
                            Ok(dec) => {
                                decrypted_buf = dec;
                                &decrypted_buf
                            }
                            Err(_) => continue,
                        }
                    } else {
                        continue;
                    }
                } else {
                    raw_packet
                };

                if let Some(parsed) = parse_packet(packet) {
                    if let Some(target) = target_stream_id {
                        if parsed.stream_id != target {
                            continue;
                        }
                    }
                    if parsed.channel_count as u16 == channel_count
                        && parsed.sample_rate == sample_rate
                    {
                        if parsed.sample_format == network_core::protocol::SAMPLE_FORMAT_FLOAT32
                            && float32_payload(packet, &parsed).is_some()
                        {
                            push_samples(buffers, resamplers, packet, &parsed, output_sample_rate)?;
                        } else if parsed.sample_format == network_core::protocol::SAMPLE_FORMAT_OPUS {
                            if let Some(opus_data) = network_core::protocol::opus_payload(packet, &parsed) {
                                if let Ok(mut dec) = crate::opus::OpusChannelDecoder::new(parsed.sample_rate, parsed.channel_count as usize) {
                                    if let Ok(pcm) = dec.decode_packet(opus_data) {
                                        push_pcm_samples(buffers, resamplers, parsed.stream_id, parsed.channel_count, parsed.sample_rate, output_sample_rate, &pcm)?;
                                    }
                                }
                            }
                        }
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

fn handle_receive_event(
    event: ReceiveEvent,
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    resamplers: &mut HashMap<u32, (u32, StreamingLinearResampler)>,
    plc: &mut PacketLossConcealer,
    last_frames_per_packet: &mut usize,
    channel_count: u16,
    output_sample_rate: u32,
    packets_lost: &mut u64,
) -> Result<(), String> {
    match event {
        ReceiveEvent::Packet(pkt, mut pcm) => {
            plc.process_incoming(&mut pcm);
            let ch = (pkt.channel_count as usize).max(1);
            *last_frames_per_packet = pcm.len() / ch;
            push_pcm_samples(
                buffers,
                resamplers,
                pkt.stream_id,
                pkt.channel_count,
                pkt.sample_rate,
                output_sample_rate,
                &pcm,
            )?;
        }
        ReceiveEvent::Dropped(count) => {
            *packets_lost = packets_lost.saturating_add(count as u64);
            let frames_per_packet = if *last_frames_per_packet > 0 {
                *last_frames_per_packet
            } else {
                network_core::protocol::max_float32_frames_per_packet(channel_count as usize).unwrap_or(58)
            };
            let total_frames = (count as usize) * frames_per_packet;
            let concealed = plc.synthesize_concealment(total_frames);
            let stream_ids: Vec<u32> = {
                let guard = safe_lock(buffers);
                guard.keys().copied().collect()
            };
            for stream_id in stream_ids {
                let _ = push_pcm_samples(
                    buffers,
                    resamplers,
                    stream_id,
                    channel_count as u8,
                    output_sample_rate,
                    output_sample_rate,
                    &concealed,
                );
            }
        }
        _ => {}
    }
    Ok(())
}

/// Shared packet receive loop for physical and headless playback.
fn run_receive_loop(
    socket: &UdpSocket,
    packet_buffer: &mut [u8],
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    resamplers: &mut HashMap<u32, (u32, StreamingLinearResampler)>,
    channel_count: u16,
    output_sample_rate: u32,
    keep_running: Arc<AtomicBool>,
    target_stream_id: Option<u32>,
    status_text: Option<Arc<Mutex<String>>>,
    jitter_target_ms: Option<Arc<AtomicU32>>,
    cipher: Option<Arc<StreamCipher>>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    let mut packets_received = 0u64;
    let mut packets_lost = 0u64;
    let mut fec_recovered = 0u64;
    let mut interval_packets = 0u32;
    let mut last_status_update = Instant::now();
    let mut streams_seen = HashSet::<u32>::new();
    let mut jitter_ms = 0.0f64;
    let mut last_packet_transit: Option<(Instant, u64)> = None;

    let mut fec_decoder = FecDecoder::new(32);
    let mut reorder_window = PacketReorderWindow::new(16);
    let mut plc = PacketLossConcealer::new(channel_count as usize);
    let mut last_frames_per_packet = 0usize;
    let mut opus_decoders: HashMap<u32, crate::opus::OpusChannelDecoder> = HashMap::new();

    socket.set_read_timeout(Some(Duration::from_millis(5))).map_err(|e| e.to_string())?;

    while keep_running.load(Ordering::Relaxed) {
        let current_target_ms = jitter_target_ms
            .as_ref()
            .map(|a| a.load(Ordering::Relaxed))
            .unwrap_or(20)
            .clamp(3, 200);

        reorder_window.set_gap_timeout(Duration::from_millis(
            (current_target_ms / 2).clamp(4, 40) as u64,
        ));

        for event in reorder_window.flush_expired_gap() {
            handle_receive_event(
                event,
                buffers,
                resamplers,
                &mut plc,
                &mut last_frames_per_packet,
                channel_count,
                output_sample_rate,
                &mut packets_lost,
            )?;
        }

        let length = match socket.recv_from(packet_buffer) {
            Ok((len, _source)) => len,
            Err(ref error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                continue;
            }
            Err(error) => {
                return Err(format!("recv error: {error}"));
            }
        };

        let raw_packet = &packet_buffer[..length];

        let decrypted_store: Vec<u8>;
        let packet: &[u8] = if let Some(ref c) = cipher {
            if raw_packet.len() >= 6 && raw_packet[5] == network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED {
                match crate::crypto::decrypt_audio_packet(c, raw_packet) {
                    Ok(dec) => {
                        decrypted_store = dec;
                        &decrypted_store
                    }
                    Err(_) => continue,
                }
            } else {
                continue;
            }
        } else {
            if let Some(packet_type) = peek_packet_type(raw_packet) {
                if packet_type == PACKET_TYPE_FEC {
                    if let Some(fec) = parse_fec_packet(raw_packet) {
                        if target_stream_id.is_none() || target_stream_id == Some(fec.stream_id) {
                            let payload = &raw_packet[fec.payload_offset..fec.payload_offset + fec.payload_len];
                            if let Some((rec_pkt, rec_pcm)) = fec_decoder.decode_fec(&fec, payload) {
                                fec_recovered = fec_recovered.saturating_add(1);
                                for event in reorder_window.push_recovered(rec_pkt, rec_pcm) {
                                    handle_receive_event(
                                        event,
                                        buffers,
                                        resamplers,
                                        &mut plc,
                                        &mut last_frames_per_packet,
                                        channel_count,
                                        output_sample_rate,
                                        &mut packets_lost,
                                    )?;
                                }
                            }
                        }
                    }
                    continue;
                }
            }
            raw_packet
        };

        let Some(parsed) = parse_packet(packet) else {
            continue;
        };

        if let Some(target) = target_stream_id {
            if parsed.stream_id != target {
                continue;
            }
        }

        if parsed.channel_count as u16 != channel_count {
            eprintln!(
                "audio-core: bus dropping packet from stream \
                 {} — format mismatch",
                parsed.stream_id
            );
            continue;
        }

        let pcm = if parsed.sample_format == network_core::protocol::SAMPLE_FORMAT_FLOAT32 {
            let Some(payload) = float32_payload(packet, &parsed) else {
                continue;
            };
            fec_decoder.record_received(parsed.sequence_number, payload);
            let Some(pcm) = decode_float32(packet, &parsed) else {
                continue;
            };
            pcm
        } else if parsed.sample_format == network_core::protocol::SAMPLE_FORMAT_OPUS {
            let Some(opus_data) = network_core::protocol::opus_payload(packet, &parsed) else {
                continue;
            };
            let decoder = match opus_decoders.entry(parsed.stream_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    match crate::opus::OpusChannelDecoder::new(parsed.sample_rate, parsed.channel_count as usize) {
                        Ok(dec) => entry.insert(dec),
                        Err(e) => {
                            eprintln!("audio-core: failed to create Opus decoder for stream {}: {e}", parsed.stream_id);
                            continue;
                        }
                    }
                }
            };
            match decoder.decode_packet(opus_data) {
                Ok(pcm) => pcm,
                Err(e) => {
                    eprintln!("audio-core: Opus decode error for stream {}: {e}", parsed.stream_id);
                    continue;
                }
            }
        } else {
            continue;
        };

        streams_seen.insert(parsed.stream_id);
        packets_received = packets_received.saturating_add(1);
        interval_packets = interval_packets.saturating_add(1);

        let now = Instant::now();
        if let Some((prev_arr, prev_ts)) = last_packet_transit {
            let arr_delta = (now - prev_arr).as_secs_f64() * 1000.0;
            let ts_delta = (parsed.presentation_timestamp_ns.saturating_sub(prev_ts)) as f64 / 1_000_000.0;
            let d = (arr_delta - ts_delta).abs();
            jitter_ms += (d - jitter_ms) / 16.0;
        }
        last_packet_transit = Some((now, parsed.presentation_timestamp_ns));

        for event in reorder_window.push(parsed.clone(), pcm) {
            handle_receive_event(
                event,
                buffers,
                resamplers,
                &mut plc,
                &mut last_frames_per_packet,
                channel_count,
                output_sample_rate,
                &mut packets_lost,
            )?;
        }

        if let Some(status) = &status_text {
            let elapsed = last_status_update.elapsed();
            if elapsed >= Duration::from_secs(1) {
                let total = packets_received.saturating_add(packets_lost);
                let delivery_pct = if total > 0 {
                    (packets_received as f64 / total as f64) * 100.0
                } else {
                    100.0
                };
                let rate = interval_packets as f64 / elapsed.as_secs_f64();
                let msg = if fec_recovered > 0 {
                    format!("Receiving • {delivery_pct:.1}% delivered ({fec_recovered} recovered via FEC, {packets_lost} lost) • Jitter: {jitter_ms:.1}ms • {rate:.0} pkts/s • {current_target_ms}ms buffer")
                } else if packets_lost == 0 {
                    format!("Receiving • {delivery_pct:.1}% delivered (0 lost) • Jitter: {jitter_ms:.1}ms • {rate:.0} pkts/s • {current_target_ms}ms buffer")
                } else {
                    format!("Receiving • {delivery_pct:.1}% delivered ({packets_lost} lost) • Jitter: {jitter_ms:.1}ms • {rate:.0} pkts/s • {current_target_ms}ms buffer")
                };
                if let Ok(mut guard) = status.lock() {
                    *guard = msg;
                }
                interval_packets = 0;
                last_status_update = Instant::now();
            }
        }

        let mut guard = safe_lock(buffers);

        if let Some(stream_buffer) = guard.get_mut(&parsed.stream_id) {
            let max_queue_secs = ((current_target_ms as f64) * 3.5 / 1000.0).max(0.025);
            let maximum_frames = (output_sample_rate as f64 * max_queue_secs) as usize;
            trim_interleaved_queue(stream_buffer, maximum_frames, channel_count as usize);
        }
    }

    if let Some(status) = &status_text {
        let total = packets_received.saturating_add(packets_lost);
        let delivery_pct = if total > 0 {
            (packets_received as f64 / total as f64) * 100.0
        } else {
            100.0
        };
        let msg = if fec_recovered > 0 {
            format!("Stopped. {delivery_pct:.1}% delivered ({fec_recovered} recovered via FEC, {packets_lost} lost)")
        } else {
            format!("Stopped. {delivery_pct:.1}% delivered ({packets_lost} lost)")
        };
        if let Ok(mut guard) = status.lock() {
            *guard = msg;
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

/// Remaps one input frame into a preallocated output frame.
pub fn remap_frame_into(input: &[f32], output: &mut [f32]) {
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

    for (index, &sample) in input.iter().enumerate() {
        let output_channel = index % output_channels;
        output[output_channel] += sample;
    }

    let base_count = input_channels / output_channels;
    let extra_channels = input_channels % output_channels;
    for (index, sample) in output.iter_mut().enumerate() {
        let count = base_count + usize::from(index < extra_channels);
        *sample /= count.max(1) as f32;
    }
}

fn push_pcm_samples(
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    resamplers: &mut HashMap<u32, (u32, StreamingLinearResampler)>,
    stream_id: u32,
    channel_count: u8,
    sample_rate: u32,
    output_sample_rate: u32,
    input: &[f32],
) -> Result<(), String> {
    let needs_resampler = resamplers
        .get(&stream_id)
        .map(|(input_rate, _)| *input_rate != sample_rate)
        .unwrap_or(true);

    if needs_resampler {
        resamplers.insert(
            stream_id,
            (
                sample_rate,
                StreamingLinearResampler::new(
                    channel_count as usize,
                    sample_rate,
                    output_sample_rate,
                )?,
            ),
        );
    }

    let (_, resampler) = resamplers
        .get_mut(&stream_id)
        .ok_or_else(|| "bus resampler was not initialized".to_string())?;
    let converted = resampler.process(input)?;

    let mut guard = safe_lock(buffers);
    let stream_buffer = guard.entry(stream_id).or_insert_with(VecDeque::new);
    stream_buffer.extend(converted);
    Ok(())
}

fn push_samples(
    buffers: &Arc<Mutex<HashMap<u32, VecDeque<f32>>>>,
    resamplers: &mut HashMap<u32, (u32, StreamingLinearResampler)>,
    packet: &[u8],
    parsed: &ParsedPacket,
    output_sample_rate: u32,
) -> Result<(), String> {
    let Some(payload) = float32_payload(packet, parsed) else {
        return Err(format!(
            "bus received unsupported or truncated payload from stream {}",
            parsed.stream_id
        ));
    };

    let input = payload
        .chunks_exact(4)
        .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect::<Vec<_>>();

    push_pcm_samples(
        buffers,
        resamplers,
        parsed.stream_id,
        parsed.channel_count,
        parsed.sample_rate,
        output_sample_rate,
        &input,
    )
}

#[cfg(test)]
mod tests {
    use super::remap_frame_into;

    #[test]
    fn remapping_groups_channels_without_allocating_per_frame_scratch() {
        let mut output = [0.0; 3];

        remap_frame_into(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], &mut output);

        assert_eq!(output, [2.5, 3.5, 4.5]);
    }
}
