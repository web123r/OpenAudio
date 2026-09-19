use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use network_core::discovery::{
    start_advertising_with_labels, SubscribeRequest,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
}

const MAX_FRAMES_PER_PACKET: usize = 58;

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

    let rate_code = sample_rate_to_code(handshake.sample_rate);
    if rate_code == 0 {
        return Err(format!("Unsupported sample rate: {}", handshake.sample_rate));
    }

    let stream_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as u32;
    let channels = handshake.channels;
    
    // We need a UdpSocket for sending audio packets
    let audio_socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    
    // We need a UdpSocket for the control port (to receive SubscribeRequest)
    let control_socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    control_socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let control_port = control_socket.local_addr().unwrap().port();

    let subscribers = Arc::new(Mutex::new(HashSet::<SocketAddr>::new()));
    
    // Start discovery advertising
    let adv_keep_running = Arc::new(AtomicBool::new(true));
    let adv_keep_running_clone = adv_keep_running.clone();
    
    let mut channel_labels = vec![];
    for i in 0..channels {
        channel_labels.push(format!("Channel {}", i + 1));
    }
    
    let stream_name = handshake.stream_name.clone();
    std::thread::spawn(move || {
        let _ = start_advertising_with_labels(
            "Web Publisher".into(),
            stream_id,
            stream_name,
            channels,
            channel_labels,
            control_port,
            adv_keep_running_clone,
        );
    });

    // Start control listener
    let ctrl_keep_running = adv_keep_running.clone();
    let subs_clone = subscribers.clone();
    std::thread::spawn(move || {
        let mut buf = [0u8; 256];
        while ctrl_keep_running.load(Ordering::Relaxed) {
            if let Ok((len, src)) = control_socket.recv_from(&mut buf) {
                if let Ok(req) = serde_json::from_slice::<SubscribeRequest>(&buf[..len]) {
                    if req.stream_id == stream_id {
                        let dest = SocketAddr::new(src.ip(), req.receiver_port);
                        subs_clone.lock().unwrap().insert(dest);
                    }
                }
            }
        }
    });

    println!("Web Publisher started: id={} port={}", stream_id, control_port);
    
    // Audio sending loop
    let sequence = Arc::new(AtomicU32::new(0));
    let start_time = Instant::now();
    
    loop {
        if !global_keep_running.load(Ordering::Relaxed) {
            break;
        }

        match ws.read() {
            Ok(Message::Binary(data)) => {
                // `data` is a flat float32 array
                let floats_count = data.len() / 4;
                if floats_count == 0 { continue; }
                
                // parse as f32
                let mut samples = Vec::with_capacity(floats_count);
                for i in 0..floats_count {
                    let mut b = [0u8; 4];
                    b.copy_from_slice(&data[i*4..(i+1)*4]);
                    samples.push(f32::from_le_bytes(b));
                }

                let frame_count = floats_count / channels as usize;
                let mut frame_offset = 0;
                
                let subs = {
                    let s = subscribers.lock().unwrap();
                    s.iter().cloned().collect::<Vec<_>>()
                };

                if subs.is_empty() {
                    continue; // drop if no subs
                }

                while frame_offset < frame_count {
                    let frames_this_packet = (frame_count - frame_offset).min(MAX_FRAMES_PER_PACKET);
                    let sequence_number = sequence.fetch_add(1, Ordering::Relaxed);
                    let timestamp_ns = start_time.elapsed().as_nanos() as u64;

                    let header = PacketHeader {
                        sub_stream_index: 0,
                        stream_id,
                        sequence_number,
                        presentation_timestamp_ns: timestamp_ns,
                    };

                    let payload_header = AudioPayloadHeader {
                        channel_count: channels,
                        sample_format: SAMPLE_FORMAT_FLOAT32,
                        sample_rate_code: rate_code,
                        samples_per_channel: frames_this_packet as u16,
                    };

                    let mut packet = Vec::with_capacity(24 + 8 + frames_this_packet * channels as usize * 4);
                    packet.extend_from_slice(&header.to_bytes());
                    packet.extend_from_slice(&payload_header.to_bytes());

                    let sample_start = frame_offset * channels as usize;
                    let sample_end = (frame_offset + frames_this_packet) * channels as usize;

                    for &sample in &samples[sample_start..sample_end] {
                        packet.extend_from_slice(&sample.to_le_bytes());
                    }

                    for dest in &subs {
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

    adv_keep_running.store(false, Ordering::Relaxed);
    Ok(())
}
