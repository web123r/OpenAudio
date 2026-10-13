use crate::backend::get_backend;
use crate::devices::is_skip;
use crate::discovery::{start_advertising, SubscriberRegistry};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter,
};
use crate::util::BoundedAudioQueue;
use crate::{register_scoped_signal_meter, SignalDirection};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58;
const MAX_CAPTURE_QUEUE_MILLISECONDS: usize = 100;

/// Fixed format used when a source is set to "None" and there is no
/// physical device from which to determine channel count or sample rate.
const SILENT_CHANNELS: u16 = 2;
const SILENT_SAMPLE_RATE: u32 = 48_000;

enum PublisherDestinations {
    Discovery(SubscriberRegistry),
    Fixed(Vec<SocketAddr>),
}

fn capture_queue_capacity_frames(sample_rate: u32, minimum_frames: usize) -> Result<usize, String> {
    let queued_frames = (sample_rate as usize)
        .checked_mul(MAX_CAPTURE_QUEUE_MILLISECONDS)
        .ok_or_else(|| "capture queue frame capacity overflow".to_string())?
        / 1_000;
    Ok(queued_frames.max(minimum_frames))
}

impl PublisherDestinations {
    fn snapshot(&self, stream_id: u32) -> Option<Vec<SocketAddr>> {
        match self {
            Self::Discovery(registry) => crate::discovery::active_subscribers(registry, stream_id)
                .filter(|addresses| !addresses.is_empty()),
            Self::Fixed(addresses) if !addresses.is_empty() => Some(addresses.clone()),
            Self::Fixed(_) => None,
        }
    }
}

pub fn transmit(duration_secs: u64, dest_addr: &str, stream_id: u32) -> Result<(), String> {
    transmit_multi(duration_secs, &[dest_addr], stream_id)
}

pub fn transmit_multi(
    duration_secs: u64,
    dest_addrs: &[&str],
    stream_id: u32,
) -> Result<(), String> {
    let backend = get_backend();
    let (device_label, config) = backend.get_input_config(None)?;

    let channels = config.channels;
    let max_frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET);
    let sample_rate = config.sample_rate;
    let rate_code = sample_rate_to_code(sample_rate);

    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let resolved_addrs: Vec<std::net::SocketAddr> = dest_addrs
        .iter()
        .map(|address| {
            address
                .to_socket_addrs()
                .map_err(|error| format!("invalid address {address}: {error}"))?
                .next()
                .ok_or_else(|| format!("could not resolve address: {address}"))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let subscriber_count = resolved_addrs.len();

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("direct-publish:{stream_id}"),
        format!("Direct Publish — {device_label} — Stream {stream_id}"),
        SignalDirection::Input,
        channels as usize,
    );

    let maximum_queued_frames = capture_queue_capacity_frames(sample_rate, max_frames_per_packet)?;
    let audio_queue = Arc::new(BoundedAudioQueue::new(
        channels as usize,
        maximum_queued_frames,
    )?);
    let packets_sent = Arc::new(AtomicU32::new(0));
    let start = Instant::now();
    let queue_for_callback = audio_queue.clone();
    let signal_meter_for_callback = signal_meter.clone();

    let stream = backend.build_input_stream(
        None,
        Box::new(move |data: &[f32]| {
            signal_meter_for_callback.observe_interleaved(data, channels as usize);
            let timestamp_ns = start.elapsed().as_nanos() as u64;
            if let Err(error) = queue_for_callback.try_push(data, timestamp_ns) {
                eprintln!("audio-core: dropping malformed capture block: {error}");
            }
        }),
    )?;

    stream
        .play()
        .map_err(|error| format!("failed to start stream: {error}"))?;

    let publisher_worker = spawn_capture_publisher(
        audio_queue.clone(),
        PublisherDestinations::Fixed(resolved_addrs),
        stream_id,
        socket,
        channels,
        rate_code,
        max_frames_per_packet,
        None,
        packets_sent.clone(),
    );

    println!(
        "Transmitting {duration_secs}s of audio \
         ({channels}ch @ {sample_rate}Hz) to \
         {subscriber_count} subscriber(s)..."
    );

    std::thread::sleep(Duration::from_secs(duration_secs));

    drop(stream);
    audio_queue.close();
    publisher_worker
        .join()
        .map_err(|_| "direct publisher worker panicked".to_string())?;

    println!(
        "Done. Sent {} packets per subscriber.",
        packets_sent.load(Ordering::Relaxed)
    );

    Ok(())
}

pub fn transmit_with_control(
    dest_addr: &str,
    stream_id: u32,
    device_name: Option<String>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let backend = get_backend();
    let (device_label, config) = backend.get_input_config(device_name.as_deref())?;

    let channels = config.channels;
    let max_frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET);
    let sample_rate = config.sample_rate;
    let rate_code = sample_rate_to_code(sample_rate);

    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let resolved_addr: std::net::SocketAddr = dest_addr
        .to_socket_addrs()
        .map_err(|error| format!("invalid address {dest_addr}: {error}"))?
        .next()
        .ok_or_else(|| format!("could not resolve address: {dest_addr}"))?;

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("controlled-publish:{stream_id}"),
        format!("Publish — {device_label} — Stream {stream_id}"),
        SignalDirection::Input,
        channels as usize,
    );

    let maximum_queued_frames = capture_queue_capacity_frames(sample_rate, max_frames_per_packet)?;
    let audio_queue = Arc::new(BoundedAudioQueue::new(
        channels as usize,
        maximum_queued_frames,
    )?);
    let packets_sent = Arc::new(AtomicU32::new(0));
    let queue_for_callback = audio_queue.clone();
    let signal_meter_for_callback = signal_meter.clone();
    let start = Instant::now();

    let stream = backend.build_input_stream(
        device_name.as_deref(),
        Box::new(move |data: &[f32]| {
            signal_meter_for_callback.observe_interleaved(data, channels as usize);
            let timestamp_ns = start.elapsed().as_nanos() as u64;
            if let Err(error) = queue_for_callback.try_push(data, timestamp_ns) {
                eprintln!("audio-core: dropping malformed capture block: {error}");
            }
        }),
    )?;

    stream
        .play()
        .map_err(|error| format!("failed to start stream: {error}"))?;

    let publisher_worker = spawn_capture_publisher(
        audio_queue.clone(),
        PublisherDestinations::Fixed(vec![resolved_addr]),
        stream_id,
        socket,
        channels,
        rate_code,
        max_frames_per_packet,
        None,
        packets_sent,
    );

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(stream);
    audio_queue.close();
    publisher_worker
        .join()
        .map_err(|_| "controlled publisher worker panicked".to_string())?;
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
    transmit_with_discovery_labeled(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        None,
        keep_running,
    )
}

pub fn transmit_with_discovery_labeled(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    if is_skip(&device_name) {
        println!(
            "Publish: input set to None — transmitting \
             silence, no device opened"
        );

        return transmit_silence(
            node_name,
            stream_name,
            stream_id,
            subscribers_by_stream,
            record_path,
            keep_running,
        );
    }

    let backend = get_backend();
    let (device_label, config) = backend.get_input_config(device_name.as_deref())?;

    let channels = config.channels;
    let max_frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET);
    let sample_rate = config.sample_rate;
    let rate_code = sample_rate_to_code(sample_rate);

    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let meter_label = format!("{node_name} — {stream_name} — {device_label}");

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("wasapi-publish:{stream_id}"),
        meter_label,
        SignalDirection::Input,
        channels as usize,
    );

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };
    let maximum_queued_frames = capture_queue_capacity_frames(sample_rate, max_frames_per_packet)?;
    let audio_queue = Arc::new(BoundedAudioQueue::new(
        channels as usize,
        maximum_queued_frames,
    )?);

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;

    let labels = channel_labels.unwrap_or_else(|| {
        (0..channels)
            .map(|index| format!("Channel {}", index + 1))
            .collect()
    });

    std::thread::spawn(move || {
        if let Err(error) = crate::discovery::start_advertising_with_labels(
            node_name,
            stream_id,
            stream_name,
            channels_u8,
            labels,
            crate::discovery::CONTROL_PORT,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let start = Instant::now();
    let queue_for_callback = audio_queue.clone();
    let signal_meter_for_callback = signal_meter.clone();
    let stream = backend.build_input_stream(
        device_name.as_deref(),
        Box::new(move |data: &[f32]| {
            ensure_realtime_audio_thread();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                signal_meter_for_callback.observe_interleaved(data, channels as usize);
                let timestamp_ns = start.elapsed().as_nanos() as u64;
                if let Err(error) = queue_for_callback.try_push(data, timestamp_ns) {
                    eprintln!("audio-core: dropping malformed capture block: {error}");
                }
            }));

            if result.is_err() {
                eprintln!(
                    "audio-core: panic caught in transmit \
                     callback — dropping this cycle's audio"
                );
            }
        }),
    )?;

    stream
        .play()
        .map_err(|error| format!("failed to start stream: {error}"))?;

    let publisher_worker = spawn_capture_publisher(
        audio_queue.clone(),
        PublisherDestinations::Discovery(subscribers_by_stream),
        stream_id,
        socket,
        channels,
        rate_code,
        max_frames_per_packet,
        writer.clone(),
        Arc::new(AtomicU32::new(0)),
    );

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(stream);
    audio_queue.close();
    publisher_worker
        .join()
        .map_err(|_| "capture publisher worker panicked".to_string())?;

    if let Some(writer) = &writer {
        finalize_recording(writer);
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
    transmit_loopback_with_discovery_labeled(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        None,
        keep_running,
    )
}

pub fn transmit_loopback_with_discovery_labeled(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    if is_skip(&device_name) {
        println!(
            "Publish (loopback): output set to None — \
             transmitting silence, no device opened"
        );

        return transmit_silence(
            node_name,
            stream_name,
            stream_id,
            subscribers_by_stream,
            record_path,
            keep_running,
        );
    }

    let backend = get_backend();
    let (device_label, config) = backend.get_loopback_config(device_name.as_deref())?;

    let channels = config.channels;
    let max_frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET);
    let sample_rate = config.sample_rate;
    let rate_code = sample_rate_to_code(sample_rate);

    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let meter_label = format!(
        "{node_name} — {stream_name} — \
         {device_label} (loopback)"
    );

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("wasapi-loopback-publish:{stream_id}"),
        meter_label,
        SignalDirection::Input,
        channels as usize,
    );

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };
    let maximum_queued_frames = capture_queue_capacity_frames(sample_rate, max_frames_per_packet)?;
    let audio_queue = Arc::new(BoundedAudioQueue::new(
        channels as usize,
        maximum_queued_frames,
    )?);

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;
    let labels = channel_labels.unwrap_or_else(|| {
        (0..channels)
            .map(|index| format!("Channel {}", index + 1))
            .collect()
    });

    std::thread::spawn(move || {
        if let Err(error) = crate::discovery::start_advertising_with_labels(
            node_name,
            stream_id,
            stream_name,
            channels_u8,
            labels,
            crate::discovery::CONTROL_PORT,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let start = Instant::now();
    let queue_for_callback = audio_queue.clone();
    let signal_meter_for_callback = signal_meter.clone();

    let stream = backend
        .build_loopback_stream(
            device_name.as_deref(),
            Box::new(move |data: &[f32]| {
                ensure_realtime_audio_thread();

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    signal_meter_for_callback.observe_interleaved(data, channels as usize);
                    let timestamp_ns = start.elapsed().as_nanos() as u64;
                    if let Err(error) = queue_for_callback.try_push(data, timestamp_ns) {
                        eprintln!("audio-core: dropping malformed loopback block: {error}");
                    }
                }));

                if result.is_err() {
                    eprintln!(
                        "audio-core: panic caught in loopback \
                     callback — dropping this cycle's audio"
                    );
                }
            }),
        )
        .map_err(|error| {
            format!(
                "failed to build loopback input stream \
             (device may not support WASAPI loopback): \
             {error}"
            )
        })?;

    stream
        .play()
        .map_err(|error| format!("failed to start loopback stream: {error}"))?;

    let publisher_worker = spawn_capture_publisher(
        audio_queue.clone(),
        PublisherDestinations::Discovery(subscribers_by_stream),
        stream_id,
        socket,
        channels,
        rate_code,
        max_frames_per_packet,
        writer.clone(),
        Arc::new(AtomicU32::new(0)),
    );

    while keep_running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    drop(stream);
    audio_queue.close();
    publisher_worker
        .join()
        .map_err(|_| "loopback publisher worker panicked".to_string())?;

    if let Some(writer) = &writer {
        finalize_recording(writer);
    }

    Ok(())
}

fn spawn_capture_publisher(
    audio_queue: Arc<BoundedAudioQueue>,
    destinations_source: PublisherDestinations,
    stream_id: u32,
    socket: UdpSocket,
    channels: u16,
    rate_code: u16,
    max_frames_per_packet: usize,
    writer: Option<SharedWavWriter>,
    packets_sent: Arc<AtomicU32>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut last_drop_report = Instant::now();

        loop {
            if last_drop_report.elapsed() >= Duration::from_secs(1) {
                let dropped_frames = audio_queue.take_dropped_frames();
                if dropped_frames > 0 {
                    eprintln!("audio-core: publisher queue dropped {dropped_frames} frame(s)");
                }
                last_drop_report = Instant::now();
            }

            if let Some(block) = audio_queue.try_pop() {
                let data = block.samples;
                if let Some(writer) = &writer {
                    write_samples(writer, &data);
                }

                let destinations = destinations_source.snapshot(stream_id);

                if let Some(destinations) = destinations {
                    let frame_count = data.len() / channels as usize;
                    let mut frame_offset = 0usize;

                    while frame_offset < frame_count {
                        let frames_this_packet =
                            (frame_count - frame_offset).min(max_frames_per_packet);
                        let sequence_number = packets_sent.fetch_add(1, Ordering::Relaxed);
                        let header = PacketHeader {
                            sub_stream_index: 0,
                            stream_id,
                            sequence_number,
                            presentation_timestamp_ns: block.presentation_timestamp_ns,
                        };

                        let sample_start = frame_offset * channels as usize;
                        let sample_end = (frame_offset + frames_this_packet) * channels as usize;
                        let packet = match network_core::protocol::encode_float32_packet(
                            &header,
                            channels as u8,
                            network_core::protocol::code_to_sample_rate(rate_code),
                            &data[sample_start..sample_end],
                        ) {
                            Ok(packet) => packet,
                            Err(error) => {
                                eprintln!("audio-core: failed to encode audio packet: {error}");
                                frame_offset += frames_this_packet;
                                continue;
                            }
                        };

                        for address in &destinations {
                            if let Err(error) = socket.send_to(&packet, address) {
                                eprintln!("audio-core: send to {address} failed: {error}");
                            }
                        }

                        frame_offset += frames_this_packet;
                    }
                }
                continue;
            }

            if audio_queue.is_closed_and_empty() {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        let dropped_frames = audio_queue.take_dropped_frames();
        if dropped_frames > 0 {
            eprintln!("audio-core: publisher queue dropped {dropped_frames} frame(s)");
        }
    })
}

/// Shared `None` source for microphone and loopback publish paths.
///
/// No physical device is opened. Silent packets preserve discovery,
/// stream format, recording length, and downstream timing.
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

    let meter_label = format!("{node_name} — {stream_name} (silent source)");

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("silent-publish:{stream_id}"),
        meter_label,
        SignalDirection::Input,
        channels as usize,
    );

    let writer: Option<SharedWavWriter> = match &record_path {
        Some(path) => Some(create_wav_writer(path, channels, sample_rate)?),
        None => None,
    };

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channels as u8;

    std::thread::spawn(move || {
        if let Err(error) = start_advertising(
            node_name,
            stream_id,
            stream_name.clone(),
            channels_u8,
            crate::discovery::CONTROL_PORT,
            advertise_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let sequence = AtomicU32::new(0);
    let start = Instant::now();

    let frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET);

    let packet_interval = Duration::from_secs_f64(frames_per_packet as f64 / sample_rate as f64);

    let silent_samples = vec![0.0f32; frames_per_packet * channels as usize];

    println!(
        "Publish: sending silent keep-alive stream \
         ({channels}ch @ {sample_rate}Hz)..."
    );

    while keep_running.load(Ordering::Relaxed) {
        // Keep the meter active, but correctly report silence.
        signal_meter.observe_interleaved(&silent_samples, channels as usize);

        if let Some(writer) = &writer {
            write_samples(writer, &silent_samples);
        }

        let destinations = crate::discovery::active_subscribers(&subscribers_by_stream, stream_id)
            .filter(|addresses| !addresses.is_empty());

        if let Some(destinations) = destinations {
            let sequence_number = sequence.fetch_add(1, Ordering::Relaxed);

            let timestamp_ns = start.elapsed().as_nanos() as u64;

            let header = PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number,
                presentation_timestamp_ns: timestamp_ns,
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

            for address in &destinations {
                if let Err(error) = socket.send_to(&packet, address) {
                    eprintln!(
                        "audio-core: send to {address} failed: \
                         {error}"
                    );
                }
            }
        }

        std::thread::sleep(packet_interval);
    }

    if let Some(writer) = &writer {
        finalize_recording(writer);
    }

    println!("Publish: silent stream stopped.");
    Ok(())
}
