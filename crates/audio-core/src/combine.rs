use crate::devices::{get_input_device, get_output_device, is_skip};
use crate::discovery::{start_advertising, SubscriberRegistry};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
use crate::recording::{create_wav_writer, finalize as finalize_recording, generate_record_path, write_samples, SharedWavWriter};
use crate::util::safe_lock;
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

/// One slot per requested channel. A channel set to "None" in the GUI
/// becomes Skipped: no device is opened, no capture happens, and the
/// slot contributes silence at its fixed position in the interleaved
/// stream -- channel_count and indexing never change based on which
/// channels are skipped.
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
    if channel_count > 255 {
        return Err("channel count exceeds protocol limit of 255".to_string());
    }

    let mut slots: Vec<ChannelSlot> = Vec::with_capacity(channel_count);

    for (i, source) in sources.iter().enumerate() {
        if is_skip(&source.device_name) {
            println!("Combine channel {i}: set to None -- sending silence, no device opened");
            slots.push(ChannelSlot::Skipped);
            continue;
        }

        let label_base = source.device_name.clone().unwrap_or_else(|| "System Default".to_string());
        let label = if source.is_loopback {
            format!("{label_base} (loopback)")
        } else {
            label_base.clone()
        };

        let (device, in_channels, sample_rate, stream_config): (cpal::Device, usize, u32, cpal::StreamConfig) =
            if source.is_loopback {
                let device = get_output_device(source.device_name.as_deref())
                    .map_err(|e| format!("channel {i} ('{label}'): {e}"))?;
                let config = device
                    .default_output_config()
                    .map_err(|e| format!("channel {i} ('{label}'): failed to get output config: {e}"))?;
                let channels = config.channels() as usize;
                let rate = config.sample_rate().0;
                (device, channels, rate, config.into())
            } else {
                let device = get_input_device(source.device_name.as_deref())
                    .map_err(|e| format!("channel {i} ('{label}'): {e}"))?;
                let config = device
                    .default_input_config()
                    .map_err(|e| format!("channel {i} ('{label}'): failed to get input config: {e}"))?;
                let channels = config.channels() as usize;
                let rate = config.sample_rate().0;
                (device, channels, rate, config.into())
            };

        let writer: Option<SharedWavWriter> = if record_each_channel {
            let path = generate_record_path(&format!("combine{stream_id}_ch{i}"));
            Some(create_wav_writer(&path, 1, sample_rate)?)
        } else {
            None
        };

        let buffer: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));
        let buffer_for_callback = buffer.clone();
        let writer_for_callback = writer.clone();
        let channel_label = label.clone();
        let err_fn = move |err| eprintln!("audio-core: combine capture error ({channel_label}): {err}");

        let stream = device
            .build_input_stream(
                &stream_config,
                move |data: &[f32], _| {
                    ensure_realtime_audio_thread();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut buf = safe_lock(&buffer_for_callback);
                        let frame_count = data.len() / in_channels;
                        if let Some(w) = &writer_for_callback {
                            // Collect just the one channel we're using, so
                            // the recording matches exactly what's sent.
                            let mut mono: Vec<f32> = Vec::with_capacity(frame_count);
                            for f in 0..frame_count {
                                mono.push(data[f * in_channels]);
                            }
                            write_samples(w, &mono);
                            for &s in &mono {
                                buf.push_back(s);
                            }
                        } else {
                            for f in 0..frame_count {
                                buf.push_back(data[f * in_channels]);
                            }
                        }
                    }));
                    if result.is_err() {
                        eprintln!("audio-core: panic caught in combine capture callback -- dropping this cycle");
                    }
                },
                err_fn,
                None,
            )
            .map_err(|e| format!("channel {i} ('{label}'): failed to build input stream (loopback devices must support WASAPI loopback): {e}"))?;

        stream.play().map_err(|e| format!("channel {i} ('{label}'): failed to start stream: {e}"))?;

        slots.push(ChannelSlot::Active(OpenedChannel {
            buffer,
            _stream: stream,
            sample_rate,
            label,
            writer,
        }));
    }

    // Sample rate and pacing clock come from the first ACTIVE channel.
    // If every channel is set to None there's no real audio clock to
    // synchronize against, so we require at least one active device.
    let sample_rate = slots
        .iter()
        .find_map(|s| match s {
            ChannelSlot::Active(ch) => Some(ch.sample_rate),
            ChannelSlot::Skipped => None,
        })
        .ok_or_else(|| "combine requires at least one channel with a real device -- all channels are set to None".to_string())?;

    for s in &slots {
        if let ChannelSlot::Active(ch) = s {
            if ch.sample_rate != sample_rate {
                return Err(format!(
                    "channel '{}' runs at {}Hz but another active channel runs at {}Hz -- all active channels must share one sample rate",
                    ch.label, ch.sample_rate, sample_rate
                ));
            }
        }
    }

    let rate_code = sample_rate_to_code(sample_rate);
    if rate_code == 0 {
        return Err(format!("unsupported sample rate: {sample_rate}"));
    }

    let advertise_keep_running = keep_running.clone();
    let channels_u8 = channel_count as u8;
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

    println!("Combining {channel_count} channel(s) @ {sample_rate}Hz into stream {stream_id}...");

    let poll_interval = Duration::from_millis(2);

    while keep_running.load(Ordering::Relaxed) {
        // Only ACTIVE channels gate how many frames are ready -- a
        // Skipped slot never fills a buffer, so it must not hold up
        // the whole combine waiting for data that will never arrive.
        let available = slots
            .iter()
            .filter_map(|s| match s {
                ChannelSlot::Active(ch) => Some(safe_lock(&ch.buffer).len()),
                ChannelSlot::Skipped => None,
            })
            .min()
            .unwrap_or(0);

        if available == 0 {
            std::thread::sleep(poll_interval);
            continue;
        }

        let frames_to_send = available.min(MAX_FRAMES_PER_PACKET);

        let dests = {
            let map = safe_lock(&subscribers_by_stream);
            match map.get(&stream_id) {
                Some(list) if !list.is_empty() => Some(list.clone()),
                _ => None,
            }
        };

        // Drain active buffers (always, even with no subscribers, to
        // prevent overflow) and synthesize silence for skipped slots.
        let mut per_channel_samples: Vec<Vec<f32>> = Vec::with_capacity(channel_count);
        for s in &slots {
            match s {
                ChannelSlot::Active(ch) => {
                    let mut guard = safe_lock(&ch.buffer);
                    let samples: Vec<f32> = guard.drain(..frames_to_send).collect();
                    per_channel_samples.push(samples);
                }
                ChannelSlot::Skipped => {
                    per_channel_samples.push(vec![0.0f32; frames_to_send]);
                }
            }
        }

        if let Some(dests) = dests {
            let mut interleaved = Vec::with_capacity(frames_to_send * channel_count);
            for f in 0..frames_to_send {
                for c in 0..channel_count {
                    interleaved.push(per_channel_samples[c][f]);
                }
            }

            let seq = sequence.fetch_add(1, Ordering::Relaxed);
            let ts_ns = start.elapsed().as_nanos() as u64;

            let header = PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number: seq,
                presentation_timestamp_ns: ts_ns,
            };
            let payload_header = AudioPayloadHeader {
                channel_count: channel_count as u8,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: rate_code,
                samples_per_channel: frames_to_send as u16,
            };

            let mut packet = Vec::with_capacity(24 + 8 + interleaved.len() * 4);
            packet.extend_from_slice(&header.to_bytes());
            packet.extend_from_slice(&payload_header.to_bytes());
            for sample in &interleaved {
                packet.extend_from_slice(&sample.to_le_bytes());
            }

            for addr in &dests {
                if let Err(e) = socket.send_to(&packet, addr) {
                    eprintln!("audio-core: send to {addr} failed: {e}");
                }
            }
        }
    }

    for s in &slots {
        if let ChannelSlot::Active(ch) = s {
            if let Some(w) = &ch.writer {
                finalize_recording(w);
            }
        }
    }

    drop(slots);
    println!("Done combining.");
    Ok(())
}