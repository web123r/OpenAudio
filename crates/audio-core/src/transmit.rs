use crate::protocol::{sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_FRAMES_PER_PACKET: usize = 58; // matches spec section 3.4 reference math

/// Captures from the default input device and streams it as OpenAudio
/// UDP packets to a single `dest_addr` (e.g. "127.0.0.1:6970") for
/// `duration_secs`. Milestone 2.
pub fn transmit(duration_secs: u64, dest_addr: &str, stream_id: u32) -> Result<(), String> {
    transmit_multi(duration_secs, &[dest_addr], stream_id)
}

/// Captures from the default input device and fans each packet out to
/// multiple destinations -- proving the pub/sub model (one Publisher,
/// N Subscribers). Milestone 5.
///
/// Real multicast (one send, many receivers) is a later optimization;
/// this proves the model via simple unicast fan-out first.
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

    // Grab the count now, before resolved_addrs gets moved into the
    // closure below -- we only need the number afterward, not the vec.
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