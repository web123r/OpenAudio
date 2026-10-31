use crate::protocol::{sample_rate_to_code, SAMPLE_FORMAT_FLOAT32};
use crate::transmit::{report_incompatible_subscribers, PublishFormat};
use crate::StreamingLinearResampler;
use network_core::discovery::{
    active_subscribers_for_format, bind_control_listener_on, run_control_listener,
    start_advertising_with_format, SubscriberRegistry,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tungstenite::{Message, WebSocket};

#[derive(Deserialize)]
struct PublishHandshake {
    #[serde(rename = "type")]
    msg_type: String, // "publish"
    #[serde(rename = "streamName")]
    stream_name: String,
    channels: u8,
    #[serde(rename = "sampleRate")]
    sample_rate: u32,
    /// Wire format requested by the page. Absent means Float32, which keeps
    /// older publisher pages working.
    #[serde(default)]
    format: PublishFormat,
}

const MAX_FRAMES_PER_PACKET: usize = 58;
const MAX_CHANNELS: u8 = 64;
/// Opus always transmits at 48 kHz; other capture rates are resampled first.
const OPUS_WIRE_SAMPLE_RATE: u32 = 48_000;

fn validate_channels(channels: u8) -> Result<u8, String> {
    if channels == 0 || channels > MAX_CHANNELS {
        return Err(format!(
            "Unsupported channel count: {channels} (expected 1-{MAX_CHANNELS})"
        ));
    }

    Ok(channels)
}

/// Resolves the page's requested format.
///
/// No subscriber has declared anything by handshake time, so `Auto` falls back
/// to the channel-count policy alone.
fn negotiated_format(handshake: &PublishHandshake) -> PublishFormat {
    handshake.format.resolve(handshake.channels as usize, None)
}

pub fn handle_publish_ws(
    mut ws: WebSocket<TcpStream>,
    global_keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    // 1. Wait for handshake
    let msg = match ws.read() {
        Ok(m) => m,
        Err(e) => return Err(format!("failed to read handshake: {e}")),
    };

    let handshake = if let Message::Text(text) = msg {
        match serde_json::from_str::<PublishHandshake>(&text) {
            Ok(h) if h.msg_type == "publish" => h,
            _ => return Err("Invalid handshake JSON or type".into()),
        }
    } else {
        return Err("Expected text message for handshake".into());
    };

    let channels = validate_channels(handshake.channels)?;
    let format = negotiated_format(&handshake);

    let wire_sample_rate = if format == PublishFormat::Opus {
        OPUS_WIRE_SAMPLE_RATE
    } else {
        handshake.sample_rate
    };
    let rate_code = sample_rate_to_code(wire_sample_rate);
    if rate_code == 0 {
        return Err(format!(
            "Unsupported sample rate: {}",
            handshake.sample_rate
        ));
    }

    let stream_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u32;

    let max_frames_per_packet = if format == PublishFormat::Opus {
        network_core::opus::OPUS_FRAME_SAMPLES
    } else {
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?
            .min(MAX_FRAMES_PER_PACKET)
    };

    let mut opus_encoder = if format == PublishFormat::Opus {
        Some(network_core::opus::MultichannelOpusEncoder::new(
            channels as usize,
            64_000,
        )?)
    } else {
        None
    };
    let mut opus_resampler =
        if format == PublishFormat::Opus && handshake.sample_rate != OPUS_WIRE_SAMPLE_RATE {
            Some(StreamingLinearResampler::new(
                channels as usize,
                handshake.sample_rate,
                OPUS_WIRE_SAMPLE_RATE,
            )?)
        } else {
            None
        };

    // We need a UdpSocket for sending audio packets
    let audio_socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;

    // The control listener registers subscribers together with the formats they
    // declared, so this publisher skips receivers that cannot decode it.
    let control_socket = bind_control_listener_on(0)?;
    let control_port = control_socket.local_addr().unwrap().port();
    let subscribers: SubscriberRegistry = Arc::new(Mutex::new(HashMap::new()));

    let keep_running = Arc::new(AtomicBool::new(true));

    // Start discovery advertising with the concrete wire format
    let adv_keep_running = keep_running.clone();
    let mut channel_labels = vec![];
    for i in 0..channels {
        channel_labels.push(format!("Channel {}", i + 1));
    }

    let stream_name = handshake.stream_name.clone();
    let advertised_format = format.wire_sample_format();
    std::thread::spawn(move || {
        let _ = start_advertising_with_format(
            "Web Publisher".into(),
            stream_id,
            stream_name,
            channels,
            channel_labels,
            control_port,
            advertised_format,
            adv_keep_running,
        );
    });

    // Start control listener
    let ctrl_keep_running = keep_running.clone();
    let ctrl_registry = subscribers.clone();
    std::thread::spawn(move || {
        let _ = run_control_listener(control_socket, ctrl_registry, ctrl_keep_running);
    });

    println!(
        "Web Publisher started: id={} port={} format={:?}",
        stream_id, control_port, format
    );

    // Audio sending loop
    let mut sequence: u32 = 0;
    let mut wire_frames_sent: u64 = 0;
    let mut opus_pending: Vec<f32> = Vec::new();
    let mut last_incompatible_report = 0usize;

    loop {
        if !global_keep_running.load(Ordering::Relaxed) {
            break;
        }

        match ws.read() {
            Ok(Message::Binary(data)) => {
                // `data` is a flat little-endian float32 array
                let mut samples = Vec::with_capacity(data.len() / 4);
                for bytes in data.chunks_exact(4) {
                    let mut b = [0u8; 4];
                    b.copy_from_slice(bytes);
                    samples.push(f32::from_le_bytes(b));
                }
                if samples.is_empty() {
                    continue;
                }

                let destinations = active_subscribers_for_format(
                    &subscribers,
                    stream_id,
                    format.wire_sample_format(),
                );
                report_incompatible_subscribers(
                    stream_id,
                    format,
                    destinations.skipped_incompatible,
                    &mut last_incompatible_report,
                );

                if destinations.addresses.is_empty() {
                    continue; // drop if no compatible subs
                }

                let timestamp_ns =
                    |frames: u64| frames.saturating_mul(1_000_000_000) / wire_sample_rate as u64;

                if format == PublishFormat::Opus {
                    let converted = match opus_resampler.as_mut() {
                        Some(resampler) => match resampler.process(&samples) {
                            Ok(converted) => converted,
                            Err(error) => {
                                eprintln!("audio-core: web publisher resampling failed: {error}");
                                continue;
                            }
                        },
                        None => samples,
                    };
                    opus_pending.extend(converted);

                    let frame_samples = network_core::opus::OPUS_FRAME_SAMPLES * channels as usize;
                    let substream_count = (channels as usize).div_ceil(2) as u32;

                    while opus_pending.len() >= frame_samples {
                        let frame: Vec<f32> = opus_pending.drain(..frame_samples).collect();
                        let frame_timestamp_ns = timestamp_ns(wire_frames_sent);
                        wire_frames_sent += network_core::opus::OPUS_FRAME_SAMPLES as u64;

                        let Some(encoder) = opus_encoder.as_mut() else {
                            eprintln!("audio-core: web publisher Opus encoder unavailable");
                            break;
                        };
                        let sequence_start = sequence;
                        sequence = sequence.wrapping_add(substream_count);
                        let encoded = match encoder.encode(&frame, sequence_start) {
                            Ok(encoded) => encoded,
                            Err(error) => {
                                eprintln!(
                                    "audio-core: web publisher Opus encoding failed: {error}"
                                );
                                continue;
                            }
                        };

                        for (substream_index, sequence_number, payload) in encoded {
                            let header = crate::protocol::PacketHeader {
                                sub_stream_index: substream_index,
                                stream_id,
                                sequence_number,
                                presentation_timestamp_ns: frame_timestamp_ns,
                            };
                            let packet = match network_core::protocol::encode_opus_packet(
                                &header,
                                channels,
                                OPUS_WIRE_SAMPLE_RATE,
                                network_core::opus::OPUS_FRAME_SAMPLES as u16,
                                &payload,
                            ) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    eprintln!(
                                        "audio-core: web publisher failed to frame Opus packet: {error}"
                                    );
                                    continue;
                                }
                            };

                            for dest in &destinations.addresses {
                                let _ = audio_socket.send_to(&packet, dest);
                            }
                        }
                    }

                    continue;
                }

                let frame_count = samples.len() / channels as usize;
                let mut frame_offset = 0;

                while frame_offset < frame_count {
                    let frames_this_packet =
                        (frame_count - frame_offset).min(max_frames_per_packet);
                    let sequence_number = sequence;
                    sequence = sequence.wrapping_add(1);
                    let packet_timestamp_ns = timestamp_ns(wire_frames_sent);
                    wire_frames_sent += frames_this_packet as u64;

                    let header = crate::protocol::PacketHeader {
                        sub_stream_index: 0,
                        stream_id,
                        sequence_number,
                        presentation_timestamp_ns: packet_timestamp_ns,
                    };

                    let payload_header = crate::protocol::AudioPayloadHeader {
                        channel_count: channels,
                        sample_format: SAMPLE_FORMAT_FLOAT32,
                        sample_rate_code: rate_code,
                        samples_per_channel: frames_this_packet as u16,
                    };

                    let mut packet =
                        Vec::with_capacity(24 + 8 + frames_this_packet * channels as usize * 4);
                    packet.extend_from_slice(&header.to_bytes());
                    packet.extend_from_slice(&payload_header.to_bytes());

                    let sample_start = frame_offset * channels as usize;
                    let sample_end = (frame_offset + frames_this_packet) * channels as usize;

                    for &sample in &samples[sample_start..sample_end] {
                        packet.extend_from_slice(&sample.to_le_bytes());
                    }

                    for dest in &destinations.addresses {
                        let _ = audio_socket.send_to(&packet, dest);
                    }

                    frame_offset += frames_this_packet;
                }
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Ok(_) => {} // Ignore ping/pong/text
            Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => {
                eprintln!("Websocket error: {e}");
                break;
            }
        }
    }

    keep_running.store(false, Ordering::Relaxed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{negotiated_format, validate_channels, PublishFormat, PublishHandshake};

    fn handshake(json: &str) -> PublishHandshake {
        serde_json::from_str(json).expect("handshake must parse")
    }

    #[test]
    fn handshake_without_format_stays_on_float32() {
        let parsed = handshake(
            r#"{"type":"publish","streamName":"Mobile Mic","channels":2,"sampleRate":48000}"#,
        );
        assert_eq!(negotiated_format(&parsed), PublishFormat::Float32);
    }

    #[test]
    fn handshake_format_is_honoured_and_auto_still_follows_channel_count() {
        let opus = handshake(
            r#"{"type":"publish","streamName":"Mic","channels":2,"sampleRate":48000,"format":"opus"}"#,
        );
        assert_eq!(negotiated_format(&opus), PublishFormat::Opus);

        let auto_stereo = handshake(
            r#"{"type":"publish","streamName":"Mic","channels":2,"sampleRate":48000,"format":"auto"}"#,
        );
        assert_eq!(negotiated_format(&auto_stereo), PublishFormat::Float32);

        let auto_multichannel = handshake(
            r#"{"type":"publish","streamName":"Mic","channels":16,"sampleRate":48000,"format":"auto"}"#,
        );
        assert_eq!(negotiated_format(&auto_multichannel), PublishFormat::Opus);
    }

    #[test]
    fn channel_count_is_validated_before_any_audio_is_split() {
        assert_eq!(validate_channels(1).unwrap(), 1);
        assert_eq!(validate_channels(64).unwrap(), 64);
        assert!(validate_channels(0).is_err());
        assert!(validate_channels(65).is_err());
    }
}
