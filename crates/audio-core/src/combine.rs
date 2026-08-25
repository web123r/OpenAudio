use crate::devices::{get_input_device, get_output_device, is_skip};
use crate::discovery::{start_advertising, SubscriberRegistry};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, generate_record_path, write_samples,
    SharedWavWriter,
};
use crate::util::safe_lock;
use crate::{register_scoped_signal_meter, SignalDirection};
use cpal::traits::{DeviceTrait, StreamTrait};
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58;

#[derive(Clone)]
pub struct ChannelSource {
    pub device_name: Option<String>,
    pub is_loopback: bool,
}

struct OpenedChannel {
    buffer: Arc<Mutex<VecDeque<f32>>>,
    _stream: cpal::Stream,
    sample_rate: u32,
    label: String,
    writer: Option<SharedWavWriter>,
}

/// One slot exists for every requested output channel.
///
/// A skipped source contributes silence at its original position, so
/// channel count and channel ordering never change.
enum ChannelSlot {
    Active(OpenedChannel),
    Skipped,
}

pub fn capture_and_combine_with_discovery(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    sources: Vec<ChannelSource>,
    subscribers_by_stream: SubscriberRegistry,
    record_each_channel: bool,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    let channel_count = sources.len();

    if channel_count == 0 {
        return Err("no sources provided".to_string());
    }

    if channel_count > u8::MAX as usize {
        return Err("channel count exceeds protocol limit of 255".to_string());
    }

    let mut slots = Vec::<ChannelSlot>::with_capacity(channel_count);

    for (channel_index, source) in sources.iter().enumerate() {
        if is_skip(&source.device_name) {
            println!(
                "Combine channel {channel_index}: set to None — \
                 sending silence, no device opened"
            );

            slots.push(ChannelSlot::Skipped);
            continue;
        }

        let label_base = source
            .device_name
            .clone()
            .unwrap_or_else(|| "System Default".to_string());

        let label = if source.is_loopback {
            format!("{label_base} (loopback)")
        } else {
            label_base
        };

        let (device, input_channels, sample_rate, stream_config): (
            cpal::Device,
            usize,
            u32,
            cpal::StreamConfig,
        ) = if source.is_loopback {
            let device = get_output_device(source.device_name.as_deref()).map_err(|error| {
                format!(
                    "channel {channel_index} ('{label}'): \
                     {error}"
                )
            })?;

            let config = device.default_output_config().map_err(|error| {
                format!(
                    "channel {channel_index} \
                         ('{label}'): failed to get output \
                         config: {error}"
                )
            })?;

            let input_channels = config.channels() as usize;
            let sample_rate = config.sample_rate().0;

            (device, input_channels, sample_rate, config.into())
        } else {
            let device = get_input_device(source.device_name.as_deref()).map_err(|error| {
                format!(
                    "channel {channel_index} ('{label}'): \
                     {error}"
                )
            })?;

            let config = device.default_input_config().map_err(|error| {
                format!(
                    "channel {channel_index} \
                         ('{label}'): failed to get input \
                         config: {error}"
                )
            })?;

            let input_channels = config.channels() as usize;
            let sample_rate = config.sample_rate().0;

            (device, input_channels, sample_rate, config.into())
        };

        if input_channels == 0 {
            return Err(format!(
                "channel {channel_index} ('{label}') \
                 exposes zero input channels"
            ));
        }

        let writer: Option<SharedWavWriter> = if record_each_channel {
            let path = generate_record_path(&format!("combine{stream_id}_ch{channel_index}"));

            Some(create_wav_writer(&path, 1, sample_rate)?)
        } else {
            None
        };

        let buffer = Arc::new(Mutex::new(VecDeque::<f32>::new()));

        let buffer_for_callback = buffer.clone();
        let writer_for_callback = writer.clone();
        let error_channel_label = label.clone();

        let error_callback = move |error| {
            eprintln!(
                "audio-core: combine capture error \
                 ({error_channel_label}): {error}"
            );
        };

        let stream = device
            .build_input_stream(
                &stream_config,
                move |data: &[f32], _| {
                    ensure_realtime_audio_thread();

                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        capture_first_channel(
                            data,
                            input_channels,
                            &buffer_for_callback,
                            writer_for_callback.as_ref(),
                        );
                    }));

                    if result.is_err() {
                        eprintln!(
                            "audio-core: panic caught in \
                             combine capture callback — \
                             dropping this cycle"
                        );
                    }
                },
                error_callback,
                None,
            )
            .map_err(|error| {
                format!(
                    "channel {channel_index} ('{label}'): \
                     failed to build input stream \
                     (loopback devices must support WASAPI \
                     loopback): {error}"
                )
            })?;

        stream.play().map_err(|error| {
            format!(
                "channel {channel_index} ('{label}'): \
                 failed to start stream: {error}"
            )
        })?;

        slots.push(ChannelSlot::Active(OpenedChannel {
            buffer,
            _stream: stream,
            sample_rate,
            label,
            writer,
        }));
    }

    // The pacing clock comes from the first active device.
    let sample_rate = slots
        .iter()
        .find_map(|slot| match slot {
            ChannelSlot::Active(channel) => Some(channel.sample_rate),
            ChannelSlot::Skipped => None,
        })
        .ok_or_else(|| {
            "combine requires at least one channel with a \
             real device — all channels are set to None"
                .to_string()
        })?;

    for slot in &slots {
        if let ChannelSlot::Active(channel) = slot {
            if channel.sample_rate != sample_rate {
                return Err(format!(
                    "channel '{}' runs at {}Hz but another \
                     active channel runs at {}Hz — all \
                     active channels must share one sample \
                     rate",
                    channel.label, channel.sample_rate, sample_rate
                ));
            }
        }
    }

    let sample_rate_code = sample_rate_to_code(sample_rate);

    if sample_rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    // Register before moving node_name and stream_name into the
    // discovery worker.
    let meter_label = format!("{node_name} — {stream_name} (combined)");

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("combine-publish:{stream_id}"),
        meter_label,
        SignalDirection::Input,
        channel_count,
    );

    let advertising_keep_running = keep_running.clone();
    let advertised_channel_count = channel_count as u8;

    std::thread::spawn(move || {
        if let Err(error) = start_advertising(
            node_name,
            stream_id,
            stream_name,
            advertised_channel_count,
            advertising_keep_running,
        ) {
            eprintln!("audio-core: advertising stopped: {error}");
        }
    });

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|error| format!("failed to bind socket: {error}"))?;

    let sequence = AtomicU32::new(0);
    let clock_start = Instant::now();

    println!(
        "Combining {channel_count} channel(s) @ \
         {sample_rate}Hz into stream {stream_id}..."
    );

    let poll_interval = Duration::from_millis(2);

    while keep_running.load(Ordering::Relaxed) {
        // Only active channels determine readiness. A skipped slot
        // never receives samples and must not block the combined stream.
        let available_frames = slots
            .iter()
            .filter_map(|slot| match slot {
                ChannelSlot::Active(channel) => Some(safe_lock(&channel.buffer).len()),
                ChannelSlot::Skipped => None,
            })
            .min()
            .unwrap_or(0);

        if available_frames == 0 {
            std::thread::sleep(poll_interval);
            continue;
        }

        let frames_to_send = available_frames.min(MAX_FRAMES_PER_PACKET);

        let destinations = {
            let subscribers = safe_lock(&subscribers_by_stream);

            subscribers
                .get(&stream_id)
                .filter(|addresses| !addresses.is_empty())
                .cloned()
        };

        // Always drain active capture buffers, including when no
        // subscriber is connected, to keep latency and memory bounded.
        let mut per_channel_samples = Vec::<Vec<f32>>::with_capacity(channel_count);

        for slot in &slots {
            match slot {
                ChannelSlot::Active(channel) => {
                    let mut buffer = safe_lock(&channel.buffer);

                    let samples = buffer.drain(..frames_to_send).collect::<Vec<_>>();

                    per_channel_samples.push(samples);
                }
                ChannelSlot::Skipped => {
                    per_channel_samples.push(vec![0.0f32; frames_to_send]);
                }
            }
        }

        let mut interleaved =
            Vec::<f32>::with_capacity(frames_to_send.saturating_mul(channel_count));

        for frame_index in 0..frames_to_send {
            for channel_index in 0..channel_count {
                interleaved.push(per_channel_samples[channel_index][frame_index]);
            }
        }

        // Observe the final combined stream before subscriber lookup
        // affects packet transmission. This makes the meter useful even
        // when no receiver is currently subscribed.
        signal_meter.observe_interleaved(&interleaved, channel_count);

        let Some(destinations) = destinations else {
            continue;
        };

        let sequence_number = sequence.fetch_add(1, Ordering::Relaxed);

        let presentation_timestamp_ns = clock_start.elapsed().as_nanos() as u64;

        let packet_header = PacketHeader {
            sub_stream_index: 0,
            stream_id,
            sequence_number,
            presentation_timestamp_ns,
        };

        let payload_header = AudioPayloadHeader {
            channel_count: channel_count as u8,
            sample_format: SAMPLE_FORMAT_FLOAT32,
            sample_rate_code,
            samples_per_channel: frames_to_send as u16,
        };

        let mut packet = Vec::<u8>::with_capacity(24 + 8 + interleaved.len() * 4);

        packet.extend_from_slice(&packet_header.to_bytes());

        packet.extend_from_slice(&payload_header.to_bytes());

        for sample in &interleaved {
            packet.extend_from_slice(&sample.to_le_bytes());
        }

        for destination in &destinations {
            if let Err(error) = socket.send_to(&packet, destination) {
                eprintln!(
                    "audio-core: send to {destination} \
                     failed: {error}"
                );
            }
        }
    }

    for slot in &slots {
        if let ChannelSlot::Active(channel) = slot {
            if let Some(writer) = &channel.writer {
                finalize_recording(writer);
            }
        }
    }

    drop(slots);

    println!("Done combining.");
    Ok(())
}

/// Extracts channel zero from an interleaved input stream.
///
/// Each configured source contributes exactly one logical channel to the
/// combined publisher. Recording receives the same mono samples that are
/// placed into the combine queue.
fn capture_first_channel(
    data: &[f32],
    input_channels: usize,
    buffer: &Arc<Mutex<VecDeque<f32>>>,
    writer: Option<&SharedWavWriter>,
) {
    if input_channels == 0 {
        return;
    }

    let frame_count = data.len() / input_channels;

    if frame_count == 0 {
        return;
    }

    let mut mono = Vec::<f32>::with_capacity(frame_count);

    for frame_index in 0..frame_count {
        let sample_index = frame_index * input_channels;

        mono.push(data[sample_index]);
    }

    if let Some(writer) = writer {
        write_samples(writer, &mono);
    }

    let mut queue = safe_lock(buffer);
    queue.extend(mono);
}
