use crate::backend::get_backend;
use crate::devices::is_skip;
use crate::discovery::SubscriberRegistry;
use crate::ensure_realtime_audio_thread;
use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use crate::recording::{
    create_wav_writer, finalize as finalize_recording, generate_record_path, write_samples,
    SharedWavWriter,
};
use crate::util::BoundedAudioQueue;
use crate::{register_scoped_signal_meter, SignalDirection};
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58;

#[derive(Clone)]
pub struct ChannelSource {
    pub device_name: Option<String>,
    pub is_loopback: bool,
}

struct OpenedChannel {
    buffer: Arc<BoundedAudioQueue>,
    pending: VecDeque<f32>,
    _stream: Box<dyn crate::backend::AudioStream>,
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
    capture_and_combine_with_labels(
        node_name,
        stream_name,
        stream_id,
        sources,
        subscribers_by_stream,
        record_each_channel,
        None,
        keep_running,
    )
}

pub fn capture_and_combine_with_labels(
    node_name: String,
    stream_name: String,
    stream_id: u32,
    sources: Vec<ChannelSource>,
    subscribers_by_stream: SubscriberRegistry,
    record_each_channel: bool,
    channel_labels: Option<Vec<String>>,
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

    let max_frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channel_count)?
            .min(MAX_FRAMES_PER_PACKET);

    let mut slots = Vec::<ChannelSlot>::with_capacity(channel_count);
    let backend = get_backend();

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

        let (_device_label, input_channels, sample_rate) = if source.is_loopback {
            let (lbl, config) = backend
                .get_loopback_config(source.device_name.as_deref())
                .map_err(|error| format!("channel {channel_index} ('{label}'): {error}"))?;
            (lbl, config.channels as usize, config.sample_rate)
        } else {
            let (lbl, config) = backend
                .get_input_config(source.device_name.as_deref())
                .map_err(|error| format!("channel {channel_index} ('{label}'): {error}"))?;
            (lbl, config.channels as usize, config.sample_rate)
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

        let maximum_buffered_frames = (sample_rate as usize)
            .checked_mul(100)
            .ok_or_else(|| "combine capture queue capacity overflow".to_string())?
            / 1_000;
        let buffer = Arc::new(BoundedAudioQueue::new(1, maximum_buffered_frames.max(1))?);

        let buffer_for_callback = buffer.clone();
        let capture_clock = Instant::now();

        let stream = if source.is_loopback {
            backend.build_loopback_stream(
                source.device_name.as_deref(),
                Box::new(move |data: &[f32]| {
                    ensure_realtime_audio_thread();

                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        capture_first_channel(
                            data,
                            input_channels,
                            &buffer_for_callback,
                            capture_clock.elapsed().as_nanos() as u64,
                        );
                    }));

                    if result.is_err() {
                        eprintln!(
                            "audio-core: panic caught in combine \
                             loopback callback"
                        );
                    }
                }),
            )
        } else {
            backend.build_input_stream(
                source.device_name.as_deref(),
                Box::new(move |data: &[f32]| {
                    ensure_realtime_audio_thread();

                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        capture_first_channel(
                            data,
                            input_channels,
                            &buffer_for_callback,
                            capture_clock.elapsed().as_nanos() as u64,
                        );
                    }));

                    if result.is_err() {
                        eprintln!(
                            "audio-core: panic caught in combine \
                             input callback"
                        );
                    }
                }),
            )
        }
        .map_err(|error| {
            format!("channel {channel_index} ('{label}'): failed to build stream: {error}")
        })?;

        stream.play().map_err(|error| {
            format!(
                "channel {channel_index} ('{label}'): \
                 failed to start stream: {error}"
            )
        })?;

        slots.push(ChannelSlot::Active(OpenedChannel {
            buffer,
            pending: VecDeque::new(),
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
    let labels = channel_labels.unwrap_or_else(|| {
        (0..channel_count)
            .map(|index| format!("Channel {}", index + 1))
            .collect()
    });

    std::thread::spawn(move || {
        if let Err(error) = crate::discovery::start_advertising_with_labels(
            node_name,
            stream_id,
            stream_name,
            advertised_channel_count,
            labels,
            crate::discovery::CONTROL_PORT,
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
    let mut last_drop_report = Instant::now();

    while keep_running.load(Ordering::Relaxed) {
        for slot in &mut slots {
            let ChannelSlot::Active(channel) = slot else {
                continue;
            };

            while let Some(block) = channel.buffer.try_pop() {
                if let Some(writer) = &channel.writer {
                    write_samples(writer, &block.samples);
                }
                channel.pending.extend(block.samples);
            }
        }

        if last_drop_report.elapsed() >= Duration::from_secs(1) {
            for slot in &slots {
                if let ChannelSlot::Active(channel) = slot {
                    let dropped_frames = channel.buffer.take_dropped_frames();
                    if dropped_frames > 0 {
                        eprintln!(
                            "audio-core: combine channel '{}' dropped \
                             {dropped_frames} capture frame(s)",
                            channel.label
                        );
                    }
                }
            }
            last_drop_report = Instant::now();
        }

        // Only active channels determine readiness. A skipped slot
        // never receives samples and must not block the combined stream.
        let available_frames = slots
            .iter()
            .filter_map(|slot| match slot {
                ChannelSlot::Active(channel) => Some(channel.pending.len()),
                ChannelSlot::Skipped => None,
            })
            .min()
            .unwrap_or(0);

        if available_frames == 0 {
            std::thread::sleep(poll_interval);
            continue;
        }

        let frames_to_send = available_frames.min(max_frames_per_packet);

        let destinations = crate::discovery::active_subscribers(&subscribers_by_stream, stream_id)
            .filter(|addresses| !addresses.is_empty());

        // Always drain active capture buffers, including when no
        // subscriber is connected, to keep latency and memory bounded.
        let mut per_channel_samples = Vec::<Vec<f32>>::with_capacity(channel_count);

        for slot in &mut slots {
            match slot {
                ChannelSlot::Active(channel) => {
                    let samples = channel.pending.drain(..frames_to_send).collect::<Vec<_>>();

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
    buffer: &Arc<BoundedAudioQueue>,
    presentation_timestamp_ns: u64,
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

    if let Err(error) = buffer.try_push_owned(mono, presentation_timestamp_ns) {
        eprintln!("audio-core: dropping malformed combine capture block: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::capture_first_channel;
    use crate::util::BoundedAudioQueue;
    use std::sync::Arc;

    #[test]
    fn combine_capture_extracts_first_channel_into_bounded_queue() {
        let queue = Arc::new(BoundedAudioQueue::new(1, 4).unwrap());
        capture_first_channel(&[0.25, -0.25, 0.5, -0.5, 0.75, -0.75], 2, &queue, 12_345);

        let block = queue.try_pop().expect("captured block");
        assert_eq!(block.samples, vec![0.25, 0.5, 0.75]);
        assert_eq!(block.presentation_timestamp_ns, 12_345);
        assert!(queue.try_pop().is_none());
    }
}
