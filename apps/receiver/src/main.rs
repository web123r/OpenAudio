use network_core::discovery::{NodeAdvertisement, CONTROL_PORT, DISCOVERY_MULTICAST_ADDR, DISCOVERY_PORT};
use std::collections::HashMap;
use std::env;
use std::net::{Ipv4Addr, UdpSocket};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Options {
    list: bool,
    inspect: bool,
    publisher: Option<String>,
    stream_id: Option<u32>,
    receive_port: u16,
    seconds: u64,
}

fn main() -> Result<(), String> {
    let options = parse_options()?;

    if options.list || (options.publisher.is_none() && options.stream_id.is_none()) {
        let streams = discover_streams(Duration::from_secs(3))?;
        if streams.is_empty() {
            println!("No OpenAudio streams discovered.");
        } else {
            for (ip, stream) in streams.values() {
                println!(
                    "{} / {} — {}ch @ labels {:?} — {}:{} — stream {}",
                    stream.node_name,
                    stream.stream_name,
                    stream.channel_count,
                    stream.channel_labels,
                    ip,
                    stream.control_port,
                    stream.stream_id
                );
            }
        }

        if options.publisher.is_none() && options.stream_id.is_none() {
            return Ok(());
        }
    }

    let publisher = options
        .publisher
        .ok_or_else(|| "--publisher IP is required when subscribing".to_string())?;
    let stream_id = options
        .stream_id
        .ok_or_else(|| "--stream-id is required when subscribing".to_string())?;

    if options.inspect {
        inspect_stream(&publisher, CONTROL_PORT, stream_id, options.receive_port, options.seconds)?;
    } else {
        play_stream(&publisher, CONTROL_PORT, stream_id, options.receive_port, options.seconds)?;
    }
    Ok(())
}

fn discover_streams(timeout: Duration) -> Result<HashMap<String, (String, NodeAdvertisement)>, String> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT))
        .map_err(|error| format!("failed to bind discovery socket: {error}"))?;
    socket
        .join_multicast_v4(
            &DISCOVERY_MULTICAST_ADDR
                .parse()
                .map_err(|error| format!("invalid multicast address: {error}"))?,
            &Ipv4Addr::UNSPECIFIED,
        )
        .map_err(|error| format!("failed to join discovery multicast group: {error}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(250)))
        .map_err(|error| format!("failed to set discovery timeout: {error}"))?;

    let deadline = Instant::now() + timeout;
    let mut buffer = [0u8; 4096];
    let mut streams = HashMap::new();

    while Instant::now() < deadline {
        match socket.recv_from(&mut buffer) {
            Ok((length, source)) => {
                if let Ok(advertisement) = serde_json::from_slice::<NodeAdvertisement>(&buffer[..length]) {
                    streams.insert(
                        advertisement.node_id.clone(),
                        (source.ip().to_string(), advertisement),
                    );
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Err(error) => return Err(format!("discovery receive failed: {error}")),
        }
    }

    Ok(streams)
}

fn inspect_stream(
    publisher: &str,
    control_port: u16,
    stream_id: u32,
    receive_port: u16,
    seconds: u64,
) -> Result<(u64, u64), String> {
    use network_core::receiver::{ReceiveEvent, StreamReceiver};

    println!(
        "Subscribed to {publisher}, stream {stream_id}; listening on UDP port {receive_port} for {seconds}s."
    );

    let mut receiver = StreamReceiver::new(publisher, control_port, stream_id, receive_port)?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    
    let mut packets = 0u64;
    let mut dropped = 0u64;
    let mut format = None;

    while Instant::now() < deadline {
        match receiver.next_event()? {
            Some(ReceiveEvent::Packet(_packet, _pcm)) => {
                packets += 1;
                if packets % 100 == 0 {
                    println!(
                        "source={publisher} packets={packets} dropped={dropped} format={format:?}"
                    );
                }
            }
            Some(ReceiveEvent::Dropped(count)) => dropped += count as u64,
            Some(ReceiveEvent::FormatChanged { channels, sample_rate }) => {
                format = Some((channels, sample_rate));
            }
            Some(ReceiveEvent::Disconnected) => {}
            Some(ReceiveEvent::Reconnected) => {}
            None => {}
        }
    }

    println!(
        "Finished: packets={packets}, dropped={dropped}, format={format:?}"
    );
    Ok((packets, dropped))
}

fn play_stream(
    publisher: &str,
    control_port: u16,
    stream_id: u32,
    receive_port: u16,
    seconds: u64,
) -> Result<(), String> {
    use network_core::receiver::{ReceiveEvent, StreamReceiver};
    use audio_core::backend::get_backend;
    use audio_core::{resample_interleaved_linear, ensure_realtime_audio_thread};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    println!(
        "Subscribed to {publisher}, stream {stream_id}; listening on UDP port {receive_port} for {seconds}s."
    );

    let mut receiver = StreamReceiver::new(publisher, control_port, stream_id, receive_port)?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    
    let backend = get_backend();
    let (_device_label, output_config) = backend.get_output_config(None)?;
    let output_sample_rate = output_config.sample_rate;
    
    let mut format = None;
    let mut _stream = None;
    let buffer: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));

    let mut packets = 0u64;
    let mut dropped = 0u64;

    while Instant::now() < deadline {
        match receiver.next_event()? {
            Some(ReceiveEvent::Packet(_packet, pcm)) => {
                packets += 1;
                if packets % 100 == 0 {
                    println!(
                        "source={publisher} packets={packets} dropped={dropped} format={format:?}"
                    );
                }

                if let Some((channels, sample_rate)) = format {
                    let to_push = if output_sample_rate != sample_rate {
                        resample_interleaved_linear(
                            &pcm,
                            channels as usize,
                            sample_rate,
                            output_sample_rate,
                        )
                    } else {
                        pcm
                    };
                    
                    let mut jitter_buf = buffer.lock().unwrap();
                    jitter_buf.extend(to_push);
                    
                    // Cap buffer to ~200ms
                    let max_samples = ((output_sample_rate as f64 * 0.2) as usize) * channels as usize;
                    while jitter_buf.len() > max_samples {
                        jitter_buf.pop_front();
                    }
                }
            }
            Some(ReceiveEvent::Dropped(count)) => {
                dropped += count as u64;
            }
            Some(ReceiveEvent::FormatChanged { channels, sample_rate }) => {
                format = Some((channels, sample_rate));
                buffer.lock().unwrap().clear();

                let buffer_for_callback = buffer.clone();
                let s = backend.build_output_stream(None, Box::new(move |data: &mut [f32]| {
                    ensure_realtime_audio_thread();
                    let mut buf = buffer_for_callback.lock().unwrap();
                    for sample in data.iter_mut() {
                        *sample = buf.pop_front().unwrap_or(0.0);
                    }
                })).map_err(|e| format!("failed to build output stream: {e}"))?;
                
                s.play().map_err(|e| format!("failed to play stream: {e}"))?;
                _stream = Some(s);
            }
            Some(ReceiveEvent::Disconnected) => {
                _stream = None;
            }
            Some(ReceiveEvent::Reconnected) => {}
            None => {}
        }
    }

    println!("Finished: packets={packets}, dropped={dropped}, format={format:?}");
    Ok(())
}

fn parse_options() -> Result<Options, String> {
    let mut options = Options {
        list: false,
        inspect: false,
        publisher: None,
        stream_id: None,
        receive_port: 7050,
        seconds: 10,
    };
    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--list" => options.list = true,
            "--inspect" => options.inspect = true,
            "--publisher" => options.publisher = Some(next_value(&mut args, "--publisher")?),
            "--stream-id" => {
                options.stream_id = Some(
                    next_value(&mut args, "--stream-id")?
                        .parse()
                        .map_err(|_| "--stream-id must be an integer".to_string())?,
                )
            }
            "--port" => {
                options.receive_port = next_value(&mut args, "--port")?
                    .parse()
                    .map_err(|_| "--port must be a valid UDP port".to_string())?;
            }
            "--seconds" => {
                options.seconds = next_value(&mut args, "--seconds")?
                    .parse()
                    .map_err(|_| "--seconds must be an integer".to_string())?;
            }
            "--help" | "-h" => {
                println!("OpenAudio receiver\n\n  --list\n  --publisher <IP> --stream-id <ID> [--port <UDP>] [--seconds <N>]");
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown argument '{unknown}'; use --help")),
        }
    }

    Ok(options)
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} requires a value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use network_core::protocol::{AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    fn make_packet(stream_id: u32, seq: u32) -> Vec<u8> {
        let mut packet = Vec::from(
            PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number: seq,
                presentation_timestamp_ns: 0,
            }
            .to_bytes(),
        );
        packet.extend(
            AudioPayloadHeader {
                channel_count: 2,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code: 1,
                samples_per_channel: 64,
            }
            .to_bytes(),
        );
        // padding for payload
        packet.extend([0u8; 64 * 2 * 4]);
        packet
    }

    #[test]
    fn test_inspect_stream_integration() {
        let control_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let control_port = control_socket.local_addr().unwrap().port();
        // Use port 0 to get an ephemeral port for the receiver
        let receive_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receive_port = receive_socket.local_addr().unwrap().port();
        drop(receive_socket); // Free it so inspect_stream can use it
        
        let stream_id = 999;
        
        let received_packets = Arc::new(AtomicU64::new(0));
        let rx_count = received_packets.clone();

        thread::spawn(move || {
            let mut buf = [0u8; 1024];
            
            // First subscribe
            let (len, _src) = control_socket.recv_from(&mut buf).unwrap();
            let req: serde_json::Value = serde_json::from_slice(&buf[..len]).unwrap();
            assert_eq!(req["stream_id"], stream_id);
            let target_port = req["receiver_port"].as_u64().unwrap() as u16;

            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let target = format!("127.0.0.1:{}", target_port);
            
            // Send seq 0
            socket.send_to(&make_packet(stream_id, 0), &target).unwrap();
            thread::sleep(Duration::from_millis(10));
            // Send seq 1
            socket.send_to(&make_packet(stream_id, 1), &target).unwrap();
            thread::sleep(Duration::from_millis(10));
            // Drop seq 2, send seq 3
            socket.send_to(&make_packet(stream_id, 3), &target).unwrap();
            rx_count.store(3, Ordering::SeqCst);
            
            // Wait for reconnect behavior timeout (>1s)
            // Second subscribe due to missing packets for 1 sec
            let (len2, _src2) = control_socket.recv_from(&mut buf).unwrap();
            let req2: serde_json::Value = serde_json::from_slice(&buf[..len2]).unwrap();
            assert_eq!(req2["stream_id"], stream_id);
            
            // Send seq 4 after reconnect
            socket.send_to(&make_packet(stream_id, 4), &target).unwrap();
        });

        // Run inspect_stream for 2 seconds (long enough for the 1 sec reconnect timeout)
        let (packets, dropped) = inspect_stream("127.0.0.1", control_port, stream_id, receive_port, 2).unwrap();
        
        assert_eq!(packets, 4, "Should have received exactly 4 packets");
        assert_eq!(dropped, 1, "Should have detected exactly 1 dropped packet (seq 2)");
    }
}

