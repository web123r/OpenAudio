use crate::backend::get_backend;
use crate::devices::is_skip;
use crate::discovery::{
    active_subscribers_accept_opus, start_advertising, DestinationSet, SubscriberRegistry,
};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter,
};
use crate::util::BoundedAudioQueue;
use crate::{register_scoped_signal_meter, SignalDirection, StreamingLinearResampler};
use serde::{Deserialize, Serialize};
use socket2::SockRef;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58;
const MAX_CAPTURE_QUEUE_MILLISECONDS: usize = 100;
const UDP_SEND_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// Fixed format used when a source is set to "None" and there is no
/// physical device from which to determine channel count or sample rate.
const SILENT_CHANNELS: u16 = 2;
const SILENT_SAMPLE_RATE: u32 = 48_000;

enum PublisherDestinations {
    Discovery(SubscriberRegistry),
    Fixed(Vec<SocketAddr>),
}

fn configure_audio_send_socket(socket: &UdpSocket) {
    if let Err(error) = SockRef::from(socket).set_send_buffer_size(UDP_SEND_BUFFER_BYTES) {
        eprintln!("audio-core: could not increase UDP send buffer: {error}");
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishFormat {
    #[default]
    Float32,
    Opus,
    Auto,
}

impl PublishFormat {
    /// Resolve the opt-in automatic policy once the source channel count is known.
    /// Multichannel streams (8+ channels) use Opus; smaller streams retain PCM.
    pub fn resolve_for_channels(self, channels: usize) -> Self {
        self.resolve(channels, None)
    }

    /// Resolve `Auto` using the channel count together with what the subscribers
    /// already waiting on this stream declared they can decode.
    ///
    /// Multichannel streams prefer Opus, but if every subscriber that is already
    /// waiting declared PCM only, Opus would deliver audio to nobody, so the
    /// publisher stays on PCM instead. `None` means nobody has subscribed yet and
    /// the channel-count policy alone decides; subscribers that arrive later with
    /// a narrower format are reported by [`report_incompatible_subscribers`].
    pub fn resolve(self, channels: usize, subscribers_accept_opus: Option<bool>) -> Self {
        match self {
            Self::Auto if channels >= 8 => match subscribers_accept_opus {
                Some(false) => Self::Float32,
                Some(true) | None => Self::Opus,
            },
            Self::Auto => Self::Float32,
            explicit => explicit,
        }
    }

    /// Wire format identifier advertised to subscribers for this policy.
    /// Callers must resolve `Auto` first.
    pub fn wire_sample_format(self) -> u8 {
        match self {
            Self::Opus => network_core::protocol::SAMPLE_FORMAT_OPUS,
            Self::Float32 | Self::Auto => network_core::protocol::SAMPLE_FORMAT_FLOAT32,
        }
    }
}

#[cfg(test)]
mod publish_format_tests {
    use super::PublishFormat;

    #[test]
    fn automatic_format_uses_opus_for_eight_or_more_channels() {
        assert_eq!(
            PublishFormat::Auto.resolve_for_channels(7),
            PublishFormat::Float32
        );
        assert_eq!(
            PublishFormat::Auto.resolve_for_channels(8),
            PublishFormat::Opus
        );
        assert_eq!(
            PublishFormat::Auto.resolve_for_channels(64),
            PublishFormat::Opus
        );
    }

    #[test]
    fn automatic_format_respects_what_waiting_subscribers_can_decode() {
        // Multichannel with nobody subscribed yet keeps the channel-count policy.
        assert_eq!(PublishFormat::Auto.resolve(16, None), PublishFormat::Opus);
        assert_eq!(
            PublishFormat::Auto.resolve(16, Some(true)),
            PublishFormat::Opus
        );
        // Every waiting subscriber is PCM-only, so Opus would reach nobody.
        assert_eq!(
            PublishFormat::Auto.resolve(16, Some(false)),
            PublishFormat::Float32
        );
        // Small streams stay PCM regardless of capability.
        assert_eq!(
            PublishFormat::Auto.resolve(2, Some(true)),
            PublishFormat::Float32
        );
    }

    #[test]
    fn explicit_format_is_never_overridden_by_auto_policy() {
        assert_eq!(
            PublishFormat::Float32.resolve_for_channels(64),
            PublishFormat::Float32
        );
        assert_eq!(
            PublishFormat::Opus.resolve_for_channels(2),
            PublishFormat::Opus
        );
    }
}

fn capture_queue_capacity_frames(sample_rate: u32, minimum_frames: usize) -> Result<usize, String> {
    let queued_frames = (sample_rate as usize)
        .checked_mul(MAX_CAPTURE_QUEUE_MILLISECONDS)
        .ok_or_else(|| "capture queue frame capacity overflow".to_string())?
        / 1_000;
    Ok(queued_frames.max(minimum_frames))
}

impl PublisherDestinations {
    fn snapshot(&self, stream_id: u32, sample_format: u8) -> DestinationSet {
        match self {
            Self::Discovery(registry) => {
                crate::discovery::active_subscribers_for_format(registry, stream_id, sample_format)
            }
            Self::Fixed(addresses) => DestinationSet {
                addresses: addresses.clone(),
                skipped_incompatible: 0,
            },
        }
    }
}

pub(crate) fn report_incompatible_subscribers(
    stream_id: u32,
    format: PublishFormat,
    skipped: usize,
    last_reported: &mut usize,
) {
    if skipped == *last_reported {
        return;
    }

    if skipped > 0 {
        eprintln!(
            "audio-core: {skipped} subscriber(s) of stream {stream_id} did not declare \
             support for {format:?}; they will not receive audio"
        );
    }

    *last_reported = skipped;
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
    configure_audio_send_socket(&socket);

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
            let _ = queue_for_callback.try_push(data, timestamp_ns);
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
        sample_rate,
        PublishFormat::Float32,
        None,
        packets_sent.clone(),
        None,
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
    configure_audio_send_socket(&socket);

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
            let _ = queue_for_callback.try_push(data, timestamp_ns);
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
        sample_rate,
        PublishFormat::Float32,
        None,
        packets_sent,
        None,
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
    transmit_with_discovery_labeled_encrypted(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        channel_labels,
        keep_running,
        None,
    )
}

pub fn transmit_with_discovery_labeled_encrypted(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
    cipher: Option<Arc<crate::crypto::StreamCipher>>,
) -> Result<(), String> {
    transmit_with_discovery_labeled_encrypted_format(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        channel_labels,
        keep_running,
        cipher,
        PublishFormat::Float32,
    )
}

pub fn transmit_with_discovery_labeled_encrypted_format(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
    cipher: Option<Arc<crate::crypto::StreamCipher>>,
    format: PublishFormat,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    if format == PublishFormat::Opus && is_skip(&device_name) {
        return Err("Opus publishing requires a selected input device; silent-source publishing remains Float32".to_string());
    }

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
    let format = format.resolve(
        channels as usize,
        active_subscribers_accept_opus(&subscribers_by_stream, stream_id),
    );
    let sample_rate = config.sample_rate;
    let max_frames_per_packet = if format == PublishFormat::Opus {
        network_core::opus::OPUS_FRAME_SAMPLES
    } else {
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET)
    };
    if format == PublishFormat::Opus && !(1..=64).contains(&channels) {
        return Err(format!("Opus supports 1 to 64 channels, got {channels}"));
    }
    let wire_sample_rate = if format == PublishFormat::Opus {
        48_000
    } else {
        sample_rate
    };
    let rate_code = sample_rate_to_code(wire_sample_rate);

    if format == PublishFormat::Opus && rate_code == 0 {
        return Err(format!(
            "unsupported Opus wire sample rate: {wire_sample_rate}"
        ));
    }
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {wire_sample_rate}"));
    }

    let meter_label = format!("{node_name} — {stream_name} — {device_label}");

    if format == PublishFormat::Opus {
        println!(
            "Publish: Opus selected; capture audio is resampled to 48 kHz in the publisher worker."
        );
    }
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

    let is_encrypted = cipher.is_some();
    let advertised_format = format.wire_sample_format();
    std::thread::spawn(move || {
        if let Err(error) =
            crate::discovery::start_advertising_with_labels_and_active_encrypted_format(
                node_name,
                stream_id,
                stream_name,
                channels_u8,
                labels,
                crate::discovery::CONTROL_PORT,
                is_encrypted,
                advertised_format,
                advertise_keep_running.clone(),
                advertise_keep_running,
            )
        {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;
    configure_audio_send_socket(&socket);

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
                let _ = queue_for_callback.try_push(data, timestamp_ns);
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
        sample_rate,
        format,
        writer.clone(),
        Arc::new(AtomicU32::new(0)),
        cipher,
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
    transmit_loopback_with_discovery_labeled_encrypted(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        channel_labels,
        keep_running,
        None,
    )
}

pub fn transmit_loopback_with_discovery_labeled_encrypted(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
    cipher: Option<Arc<crate::crypto::StreamCipher>>,
) -> Result<(), String> {
    transmit_loopback_with_discovery_labeled_encrypted_format(
        node_name,
        stream_name,
        stream_id,
        device_name,
        subscribers_by_stream,
        record_path,
        channel_labels,
        keep_running,
        cipher,
        PublishFormat::Float32,
    )
}

pub fn transmit_loopback_with_discovery_labeled_encrypted_format(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    device_name: Option<String>,
    subscribers_by_stream: SubscriberRegistry,
    record_path: Option<String>,
    channel_labels: Option<Vec<String>>,
    keep_running: Arc<AtomicBool>,
    cipher: Option<Arc<crate::crypto::StreamCipher>>,
    format: PublishFormat,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    if format == PublishFormat::Opus && is_skip(&device_name) {
        return Err("Opus publishing requires a selected output device; silent-source publishing remains Float32".to_string());
    }

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
    let format = format.resolve(
        channels as usize,
        active_subscribers_accept_opus(&subscribers_by_stream, stream_id),
    );
    let sample_rate = config.sample_rate;
    let max_frames_per_packet = if format == PublishFormat::Opus {
        network_core::opus::OPUS_FRAME_SAMPLES
    } else {
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET)
    };
    if format == PublishFormat::Opus && !(1..=64).contains(&channels) {
        return Err(format!("Opus supports 1 to 64 channels, got {channels}"));
    }
    let wire_sample_rate = if format == PublishFormat::Opus {
        48_000
    } else {
        sample_rate
    };
    let rate_code = sample_rate_to_code(wire_sample_rate);

    if format == PublishFormat::Opus && rate_code == 0 {
        return Err(format!(
            "unsupported Opus wire sample rate: {wire_sample_rate}"
        ));
    }
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {wire_sample_rate}"));
    }

    let meter_label = format!(
        "{node_name} — {stream_name} — \
         {device_label} (loopback)"
    );

    if format == PublishFormat::Opus {
        println!("Publish (loopback): Opus selected; capture audio is resampled to 48 kHz in the publisher worker.");
    }
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

    let is_encrypted = cipher.is_some();
    let advertised_format = format.wire_sample_format();
    std::thread::spawn(move || {
        if let Err(error) =
            crate::discovery::start_advertising_with_labels_and_active_encrypted_format(
                node_name,
                stream_id,
                stream_name,
                channels_u8,
                labels,
                crate::discovery::CONTROL_PORT,
                is_encrypted,
                advertised_format,
                advertise_keep_running.clone(),
                advertise_keep_running,
            )
        {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;
    configure_audio_send_socket(&socket);

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
                    let _ = queue_for_callback.try_push(data, timestamp_ns);
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
        sample_rate,
        format,
        writer.clone(),
        Arc::new(AtomicU32::new(0)),
        cipher,
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
    source_sample_rate: u32,
    format: PublishFormat,
    writer: Option<SharedWavWriter>,
    packets_sent: Arc<AtomicU32>,
    cipher: Option<Arc<crate::crypto::StreamCipher>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut last_drop_report = Instant::now();
        let mut fec_encoder =
            network_core::fec::FecEncoder::new(network_core::fec::DEFAULT_FEC_GROUP_SIZE);
        let mut opus_encoder = if format == PublishFormat::Opus {
            match network_core::opus::MultichannelOpusEncoder::new(channels as usize, 64_000) {
                Ok(encoder) => Some(encoder),
                Err(error) => {
                    eprintln!("audio-core: failed to initialize Opus publisher: {error}");
                    return;
                }
            }
        } else {
            None
        };
        let mut opus_resampler = if format == PublishFormat::Opus && source_sample_rate != 48_000 {
            match StreamingLinearResampler::new(channels as usize, source_sample_rate, 48_000) {
                Ok(resampler) => Some(resampler),
                Err(error) => {
                    eprintln!("audio-core: failed to initialize Opus resampler: {error}");
                    return;
                }
            }
        } else {
            None
        };
        let mut opus_pending = Vec::<f32>::new();
        let mut opus_pending_timestamp_ns = None;
        let mut opus_next_timestamp_ns = None;
        let mut opus_sequence = 0u32;
        let mut last_incompatible_report = 0usize;

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

                let destinations =
                    destinations_source.snapshot(stream_id, format.wire_sample_format());
                report_incompatible_subscribers(
                    stream_id,
                    format,
                    destinations.skipped_incompatible,
                    &mut last_incompatible_report,
                );

                if format == PublishFormat::Opus {
                    let converted = match opus_resampler.as_mut() {
                        Some(resampler) => match resampler.process(&data) {
                            Ok(converted) => converted,
                            Err(error) => {
                                eprintln!("audio-core: Opus resampling failed: {error}");
                                continue;
                            }
                        },
                        None => data.clone(),
                    };
                    if opus_pending.is_empty() {
                        opus_pending_timestamp_ns =
                            Some(opus_next_timestamp_ns.unwrap_or(block.presentation_timestamp_ns));
                    }
                    opus_pending.extend(converted);

                    let frame_samples = network_core::opus::OPUS_FRAME_SAMPLES * channels as usize;
                    while opus_pending.len() >= frame_samples {
                        let frame: Vec<f32> = opus_pending.drain(..frame_samples).collect();
                        let timestamp_ns =
                            opus_pending_timestamp_ns.unwrap_or(block.presentation_timestamp_ns);
                        let Some(encoder) = opus_encoder.as_mut() else {
                            eprintln!("audio-core: Opus encoder became unavailable");
                            break;
                        };
                        let substream_count = channels.div_ceil(2) as u32;
                        let sequence_start = opus_sequence;
                        opus_sequence = opus_sequence.wrapping_add(substream_count);
                        let encoded = match encoder.encode(&frame, sequence_start) {
                            Ok(encoded) => encoded,
                            Err(error) => {
                                eprintln!("audio-core: Opus encoding failed: {error}");
                                opus_pending_timestamp_ns = Some(
                                    timestamp_ns
                                        .saturating_add(network_core::opus::OPUS_FRAME_DURATION_NS),
                                );
                                continue;
                            }
                        };

                        if !destinations.addresses.is_empty() {
                            for (substream_index, sequence_number, payload) in encoded {
                                let header = PacketHeader {
                                    sub_stream_index: substream_index,
                                    stream_id,
                                    sequence_number,
                                    presentation_timestamp_ns: timestamp_ns,
                                };
                                let packet = match network_core::protocol::encode_opus_packet(
                                    &header,
                                    channels as u8,
                                    48_000,
                                    network_core::opus::OPUS_FRAME_SAMPLES as u16,
                                    &payload,
                                ) {
                                    Ok(packet) => packet,
                                    Err(error) => {
                                        eprintln!(
                                            "audio-core: failed to frame Opus packet: {error}"
                                        );
                                        continue;
                                    }
                                };
                                let packet_to_send = if let Some(ref cipher) = cipher {
                                    match crate::crypto::encrypt_audio_packet(cipher, &packet) {
                                        Ok(encrypted) => encrypted,
                                        Err(error) => {
                                            eprintln!("audio-core: failed to encrypt Opus packet: {error}");
                                            continue;
                                        }
                                    }
                                } else {
                                    packet
                                };
                                for address in &destinations.addresses {
                                    if let Err(error) = socket.send_to(&packet_to_send, address) {
                                        eprintln!(
                                            "audio-core: Opus send to {address} failed: {error}"
                                        );
                                    }
                                }
                            }
                        }

                        let next_timestamp =
                            timestamp_ns.saturating_add(network_core::opus::OPUS_FRAME_DURATION_NS);
                        opus_pending_timestamp_ns = if opus_pending.len() >= frame_samples {
                            Some(next_timestamp)
                        } else {
                            None
                        };
                        opus_next_timestamp_ns = Some(next_timestamp);
                        packets_sent.fetch_add(substream_count, Ordering::Relaxed);
                    }
                    continue;
                }

                if destinations.addresses.is_empty() {
                    continue;
                }
                let destinations = destinations.addresses;
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
                    let sample_rate = network_core::protocol::code_to_sample_rate(rate_code);
                    let packet = match network_core::protocol::encode_float32_packet(
                        &header,
                        channels as u8,
                        sample_rate,
                        &data[sample_start..sample_end],
                    ) {
                        Ok(packet) => packet,
                        Err(error) => {
                            eprintln!("audio-core: failed to encode audio packet: {error}");
                            frame_offset += frames_this_packet;
                            continue;
                        }
                    };

                    let packet_to_send = if let Some(ref cipher) = cipher {
                        match crate::crypto::encrypt_audio_packet(cipher, &packet) {
                            Ok(enc) => enc,
                            Err(error) => {
                                eprintln!("audio-core: failed to encrypt audio packet: {error}");
                                frame_offset += frames_this_packet;
                                continue;
                            }
                        }
                    } else {
                        packet.clone()
                    };

                    for address in &destinations {
                        if let Err(error) = socket.send_to(&packet_to_send, address) {
                            eprintln!("audio-core: send to {address} failed: {error}");
                        }
                    }

                    if cipher.is_none() && packet.len() >= 32 {
                        let maybe_fec = fec_encoder.push_audio_packet(
                            stream_id,
                            sequence_number,
                            header.presentation_timestamp_ns,
                            channels as u8,
                            sample_rate,
                            frames_this_packet as u16,
                            &packet[32..],
                        );
                        if let Some(fec_packet) = maybe_fec {
                            for address in &destinations {
                                let _ = socket.send_to(&fec_packet, address);
                            }
                        }
                    }

                    frame_offset += frames_this_packet;
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
    configure_audio_send_socket(&socket);

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

        let destinations = crate::discovery::active_subscribers_for_format(
            &subscribers_by_stream,
            stream_id,
            network_core::protocol::SAMPLE_FORMAT_FLOAT32,
        );

        if !destinations.addresses.is_empty() {
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

            for address in &destinations.addresses {
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
