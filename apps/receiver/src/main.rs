use network_core::discovery::{
    NodeAdvertisement, CONTROL_PORT, DISCOVERY_MULTICAST_ADDR, DISCOVERY_PORT,
};
use std::collections::HashMap;
use std::env;
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
enum CliCommand {
    Subscribe(Options),
    Publish(PublishOptions),
}

#[derive(Debug)]
struct PublishOptions {
    list_devices: bool,
    device: Option<String>,
    stream_name: String,
    node_name: String,
    stream_id: u32,
    control_port: u16,
    codec: String,
    bitrate_kbps: u32,
    record: Option<String>,
    seconds: u64,
    passphrase: Option<String>,
}

#[derive(Debug)]
struct Options {
    list: bool,
    inspect: bool,
    publisher: Option<String>,
    stream_id: Option<u32>,
    receive_port: u16,
    seconds: u64,
    channels: Option<Vec<usize>>,
    downmix: bool,
    record: Option<String>,
}

fn main() -> Result<(), String> {
    match parse_options()? {
        CliCommand::Publish(publish_options) => run_publish(publish_options),
        CliCommand::Subscribe(options) => {
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
                inspect_stream(
                    &publisher,
                    CONTROL_PORT,
                    stream_id,
                    options.receive_port,
                    options.seconds,
                )?;
            } else {
                play_stream(
                    &publisher,
                    CONTROL_PORT,
                    stream_id,
                    options.receive_port,
                    options.seconds,
                    options.channels.as_deref(),
                    options.record.as_deref(),
                )?;
            }
            Ok(())
        }
    }
}

fn discover_streams(
    timeout: Duration,
) -> Result<HashMap<String, (String, NodeAdvertisement)>, String> {
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
                if let Ok(advertisement) =
                    serde_json::from_slice::<NodeAdvertisement>(&buffer[..length])
                {
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

fn nominal_packet_rate(channels: u8, sample_rate: u32) -> Result<f64, String> {
    if sample_rate == 0 {
        return Err("cannot calculate packet rate for a zero sample rate".to_string());
    }

    let frames_per_packet =
        network_core::protocol::max_float32_frames_per_packet(channels as usize)?;
    Ok(sample_rate as f64 / frames_per_packet as f64)
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
    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(seconds);

    let mut packets = 0u64;
    let mut dropped = 0u64;
    let mut received_frames = 0u64;
    let mut format = None;

    while Instant::now() < deadline {
        match receiver.next_event()? {
            Some(ReceiveEvent::Packet(packet, _pcm)) => {
                packets += 1;
                received_frames = received_frames.saturating_add(packet.samples_per_channel as u64);
                format = Some((packet.channel_count, packet.sample_rate));
                if packets % 100 == 0 {
                    println!(
                        "source={publisher} packets={packets} dropped={dropped} \
                         frames={received_frames} format={format:?}"
                    );
                }
            }
            Some(ReceiveEvent::Dropped(count)) => dropped += count as u64,
            Some(ReceiveEvent::FormatChanged {
                channels,
                sample_rate,
            }) => {
                format = Some((channels, sample_rate));
            }
            Some(ReceiveEvent::Disconnected) => {}
            Some(ReceiveEvent::Reconnected) => {}
            None => {}
        }
    }

    let elapsed = started_at.elapsed();
    let elapsed_seconds = elapsed.as_secs_f64();
    let observed_packets_per_second = if elapsed_seconds > 0.0 {
        packets as f64 / elapsed_seconds
    } else {
        0.0
    };
    let received_audio_seconds = format
        .filter(|(_, sample_rate)| *sample_rate > 0)
        .map(|(_, sample_rate)| received_frames as f64 / sample_rate as f64)
        .unwrap_or(0.0);
    let nominal_packets_per_second = match format {
        Some((channels, sample_rate)) => nominal_packet_rate(channels, sample_rate)?,
        _ => 0.0,
    };
    let nominal_expected_packets = nominal_packets_per_second * elapsed_seconds;
    let nominal_receive_percentage = if nominal_expected_packets > 0.0 {
        packets as f64 / nominal_expected_packets * 100.0
    } else {
        0.0
    };

    println!(
        "Finished: packets={packets}, dropped={dropped}, format={format:?}, \
         elapsed={elapsed_seconds:.1}s, received_audio={received_audio_seconds:.2}s, \
         observed_rate={observed_packets_per_second:.0} packets/s, \
         nominal_rate={nominal_packets_per_second:.0} packets/s, \
         nominal_receive={nominal_receive_percentage:.1}%"
    );
    Ok((packets, dropped))
}

fn map_or_downmix_pcm(
    pcm: &[f32],
    input_channels: usize,
    output_channels: usize,
    channel_selection: Option<&[usize]>,
) -> Vec<f32> {
    if input_channels == 0 || output_channels == 0 {
        return Vec::new();
    }
    let frames = pcm.len() / input_channels;
    if frames == 0 {
        return Vec::new();
    }

    if let Some(sel) = channel_selection {
        let sel_count = sel.len();
        let mut mapped = Vec::with_capacity(frames * output_channels);
        let mut frame_buf = vec![0.0f32; sel_count];
        let mut out_frame = vec![0.0f32; output_channels];

        for f in 0..frames {
            let offset = f * input_channels;
            for (out_idx, &in_ch) in sel.iter().enumerate() {
                frame_buf[out_idx] = if in_ch < input_channels {
                    pcm[offset + in_ch]
                } else {
                    0.0
                };
            }
            audio_core::remap_frame_into(&frame_buf, &mut out_frame);
            mapped.extend_from_slice(&out_frame);
        }
        mapped
    } else if input_channels == output_channels {
        pcm.to_vec()
    } else {
        let mut mapped = Vec::with_capacity(frames * output_channels);
        let mut out_frame = vec![0.0f32; output_channels];
        for f in 0..frames {
            let in_frame = &pcm[f * input_channels..(f + 1) * input_channels];
            audio_core::remap_frame_into(in_frame, &mut out_frame);
            mapped.extend_from_slice(&out_frame);
        }
        mapped
    }
}

fn play_stream(
    publisher: &str,
    control_port: u16,
    stream_id: u32,
    receive_port: u16,
    seconds: u64,
    channel_selection: Option<&[usize]>,
    record_path: Option<&str>,
) -> Result<(), String> {
    use audio_core::backend::get_backend;
    use audio_core::{ensure_realtime_audio_thread, StreamingLinearResampler};
    use network_core::receiver::{ReceiveEvent, StreamReceiver};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    println!(
        "Subscribed to {publisher}, stream {stream_id}; listening on UDP port {receive_port} for {}s.",
        if seconds == 0 { "∞".to_string() } else { seconds.to_string() }
    );

    let mut receiver = StreamReceiver::new(publisher, control_port, stream_id, receive_port)?;

    let mut opus_decoder: Option<audio_core::OpusChannelDecoder> = None;
    receiver.set_custom_decoder(move |packet, payload| {
        if packet.sample_format == network_core::protocol::SAMPLE_FORMAT_OPUS {
            if opus_decoder.is_none() {
                opus_decoder = audio_core::OpusChannelDecoder::new(packet.sample_rate, packet.channel_count as usize).ok();
            }
            if let Some(ref mut dec) = opus_decoder {
                dec.decode_packet(payload)
            } else {
                Err("failed to initialize Opus decoder".to_string())
            }
        } else {
            Err(format!("unsupported audio format 0x{:02x}", packet.sample_format))
        }
    });

    let deadline = if seconds == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(seconds))
    };

    let backend = get_backend();
    let (device_label, output_config) = backend.get_output_config(None)?;
    let output_sample_rate = output_config.sample_rate;
    let output_channels = output_config.channels as usize;
    if output_channels == 0 {
        return Err("default output device exposes zero channels".to_string());
    }

    let writer = match record_path {
        Some(path) => {
            let w = audio_core::create_audio_writer(path, output_channels as u16, output_sample_rate)?;
            println!("Per-stream recording active -> {path}");
            Some(w)
        }
        None => None,
    };

    let mut format = None;
    let mut _stream = None;
    let mut resampler: Option<StreamingLinearResampler> = None;
    let mut plc: Option<network_core::plc::PacketLossConcealer> = None;
    let buffer: Arc<Mutex<VecDeque<f32>>> = Arc::new(Mutex::new(VecDeque::new()));

    let mut packets = 0u64;
    let mut dropped = 0u64;
    let mut last_packet_frames = 0usize;
    let mut last_quality_report = Instant::now();

    while deadline.map(|d| Instant::now() < d).unwrap_or(true) {
        match receiver.next_event()? {
            Some(ReceiveEvent::Packet(packet, mut pcm)) => {
                packets += 1;
                let ch = (packet.channel_count as usize).max(1);
                last_packet_frames = pcm.len() / ch;

                if let Some(plc_inst) = &mut plc {
                    plc_inst.process_incoming(&mut pcm);
                }

                if let Some((channels, _)) = format {
                    let remapped = map_or_downmix_pcm(
                        &pcm,
                        channels as usize,
                        output_channels,
                        channel_selection,
                    );
                    let to_push = resampler
                        .as_mut()
                        .ok_or_else(|| "stream resampler was not initialized".to_string())?
                        .process(&remapped)?;

                    if let Some(ref w) = writer {
                        audio_core::write_samples(w, &to_push);
                    }

                    let mut jitter_buf = buffer.lock().unwrap();
                    jitter_buf.extend(to_push);

                    // Keep the queue bounded without dropping only part of
                    // an interleaved audio frame.
                    let max_frames = (output_sample_rate as f64 * 0.2) as usize;
                    trim_interleaved_queue(&mut jitter_buf, max_frames, output_channels);
                }
            }
            Some(ReceiveEvent::Dropped(count)) => {
                dropped += count as u64;
                if let Some(plc_inst) = &mut plc {
                    let frames_per_packet = if last_packet_frames > 0 {
                        last_packet_frames
                    } else if let Some((channels, _)) = format {
                        network_core::protocol::max_float32_frames_per_packet(channels as usize).unwrap_or(58)
                    } else {
                        58
                    };
                    let frames = (count as usize) * frames_per_packet;
                    let concealed = plc_inst.synthesize_concealment(frames);
                    let in_channels = format.map(|f| f.0 as usize).unwrap_or(output_channels);
                    let remapped = map_or_downmix_pcm(
                        &concealed,
                        in_channels,
                        output_channels,
                        channel_selection,
                    );
                    if let Some(resamp) = resampler.as_mut() {
                        if let Ok(to_push) = resamp.process(&remapped) {
                            if let Some(ref w) = writer {
                                audio_core::write_samples(w, &to_push);
                            }
                            let mut jitter_buf = buffer.lock().unwrap();
                            jitter_buf.extend(to_push);
                            let max_frames = (output_sample_rate as f64 * 0.2) as usize;
                            trim_interleaved_queue(&mut jitter_buf, max_frames, output_channels);
                        }
                    }
                }
            }
            Some(ReceiveEvent::FormatChanged {
                channels,
                sample_rate,
            }) => {
                if channels as usize != output_channels {
                    println!(
                        "Receiver: incoming {channels}ch stream -> {output_channels}ch device '{device_label}' (active channel remapping/downmix)"
                    );
                }

                format = Some((channels, sample_rate));
                plc = Some(network_core::plc::PacketLossConcealer::new(channels as usize));
                resampler = Some(StreamingLinearResampler::new(
                    output_channels,
                    sample_rate,
                    output_sample_rate,
                )?);
                buffer.lock().unwrap().clear();

                let buffer_for_callback = buffer.clone();
                let s = backend
                    .build_output_stream(
                        None,
                        Box::new(move |data: &mut [f32]| {
                            ensure_realtime_audio_thread();
                            let mut buf = buffer_for_callback
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());

                            let complete_len = data.len() / output_channels * output_channels;
                            data[complete_len..].fill(0.0);
                            for frame in data[..complete_len].chunks_mut(output_channels) {
                                for sample in frame {
                                    *sample = buf.pop_front().unwrap_or(0.0);
                                }
                            }
                        }),
                    )
                    .map_err(|e| format!("failed to build output stream: {e}"))?;

                s.play()
                    .map_err(|e| format!("failed to play stream: {e}"))?;
                _stream = Some(s);
            }
            Some(ReceiveEvent::Disconnected) => {
                println!("[WARN] Stream disconnected (no packets received). Auto-reconnecting to {publisher}:{control_port}...");
                _stream = None;
                receiver.resubscribe();
            }
            Some(ReceiveEvent::Reconnected) => {
                println!("[OK] Stream reconnected successfully! Resuming playback...");
            }
            None => {}
        }

        if last_quality_report.elapsed() >= Duration::from_secs(1) {
            let loss_pct = receiver.packet_loss_pct();
            let jitter = receiver.jitter_ms();
            let q_buf = buffer.lock().unwrap().len() / output_channels;
            let q_buf_ms = q_buf as f64 / output_sample_rate as f64 * 1000.0;
            let rec_count = receiver.total_packets_received();
            let drop_count = receiver.total_packets_dropped();
            let fec_rec = receiver.fec_recovered_count();
            println!(
                "[Network Quality] Delivery: {:.1}% | Loss: {:.1}% (dropped: {}, fec_rec: {}) | Jitter: {:.1}ms | Buffer: {} frames ({:.1}ms) | Pkts: {}",
                100.0 - loss_pct, loss_pct, drop_count, fec_rec, jitter, q_buf, q_buf_ms, rec_count
            );
            last_quality_report = Instant::now();
        }
    }

    if let Some(ref w) = writer {
        audio_core::finalize_recording(w);
        if let Some(p) = record_path {
            println!("Recorded audio finalized and saved to '{p}'.");
        }
    }

    println!("Finished: packets={packets}, dropped={dropped}, format={format:?}");
    Ok(())
}

fn run_publish(options: PublishOptions) -> Result<(), String> {
    if options.list_devices {
        println!("Available audio input devices:");
        let devices = audio_core::list_input_devices();
        if devices.is_empty() {
            println!("  (No input devices found)");
        } else {
            for dev in devices {
                println!(
                    "  • {} (channels: {}, default: {})",
                    dev.name, dev.max_input_channels, dev.is_default
                );
            }
        }
        return Ok(());
    }

    let keep_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let subscribers_by_stream: audio_core::SubscriberRegistry =
        Arc::new(Mutex::new(HashMap::new()));
    let registry_for_ctrl = subscribers_by_stream.clone();
    let keep_ctrl = keep_running.clone();

    std::thread::Builder::new()
        .name("headless-control-listener".to_string())
        .spawn(move || {
            let _ = audio_core::start_control_listener(registry_for_ctrl, keep_ctrl);
        })
        .map_err(|e| format!("failed to spawn control listener: {e}"))?;

    let cipher = options.passphrase.filter(|p| !p.trim().is_empty()).map(|p| {
        Arc::new(audio_core::StreamCipher::from_passphrase(&p))
    });

    let codec = match options.codec.to_lowercase().as_str() {
        "opus" => audio_core::PayloadCodec::Opus {
            bitrate_kbps: options.bitrate_kbps as i32,
        },
        _ => audio_core::PayloadCodec::Float32,
    };

    println!(
        "OpenAudio Headless Publisher active:\n\
         • Node name:    {}\n\
         • Stream name:  {}\n\
         • Stream ID:    {}\n\
         • Input device: {}\n\
         • Codec:        {:?}\n\
         • Control port: {}\n\
         • Recording:    {:?}",
        options.node_name,
        options.stream_name,
        options.stream_id,
        options.device.as_deref().unwrap_or("Default System Input"),
        codec,
        options.control_port,
        options.record.as_deref().unwrap_or("None")
    );
    println!("Listening for subscribers... Running. (Press Ctrl+C to terminate)");

    let transmit_running = keep_running.clone();
    let handle = std::thread::spawn(move || {
        audio_core::transmit_with_discovery_labeled_encrypted_codec(
            options.node_name,
            options.stream_name,
            options.stream_id,
            options.device,
            subscribers_by_stream,
            options.record,
            None,
            transmit_running,
            cipher,
            codec,
        )
    });

    if options.seconds > 0 {
        let deadline = Instant::now() + Duration::from_secs(options.seconds);
        while Instant::now() < deadline && keep_running.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(200));
        }
        keep_running.store(false, std::sync::atomic::Ordering::Release);
    } else {
        while keep_running.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    match handle.join() {
        Ok(res) => res,
        Err(_) => Err("publisher thread panicked".to_string()),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_output_channel_count(input_channels: u8, output_channels: usize) -> Result<(), String> {
    if input_channels as usize != output_channels {
        return Err(format!(
            "incoming stream has {input_channels} channel(s), but the default output device has \
             {output_channels}; change the system default output device or use a stream with a \
             matching channel count"
        ));
    }

    Ok(())
}

fn trim_interleaved_queue(
    queue: &mut std::collections::VecDeque<f32>,
    maximum_frames: usize,
    channels: usize,
) {
    assert!(
        channels > 0,
        "interleaved audio must have at least one channel"
    );

    let maximum_samples = maximum_frames.saturating_mul(channels);
    if queue.len() > maximum_samples {
        let excess_samples = queue.len() - maximum_samples;
        let frames_to_drop = excess_samples.div_ceil(channels);
        let samples_to_drop = frames_to_drop.saturating_mul(channels);
        queue.drain(..samples_to_drop);
    }
}

fn parse_options() -> Result<CliCommand, String> {
    parse_args(env::args().skip(1).collect())
}

fn parse_args(mut raw_args: Vec<String>) -> Result<CliCommand, String> {
    if raw_args.is_empty() {
        return Ok(CliCommand::Subscribe(Options {
            list: false,
            inspect: false,
            publisher: None,
            stream_id: None,
            receive_port: 7050,
            seconds: 10,
            channels: None,
            downmix: false,
            record: None,
        }));
    }

    if raw_args[0] == "publish" {
        raw_args.remove(0);
        let mut publish_opts = PublishOptions {
            list_devices: false,
            device: None,
            stream_name: "Headless Stream".to_string(),
            node_name: "Headless Node".to_string(),
            stream_id: 1,
            control_port: CONTROL_PORT,
            codec: "pcm".to_string(),
            bitrate_kbps: 128,
            record: None,
            seconds: 0,
            passphrase: None,
        };

        let mut iter = raw_args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--list-devices" => publish_opts.list_devices = true,
                "--device" => publish_opts.device = Some(next_value(&mut iter, "--device")?),
                "--stream-name" => publish_opts.stream_name = next_value(&mut iter, "--stream-name")?,
                "--node-name" => publish_opts.node_name = next_value(&mut iter, "--node-name")?,
                "--stream-id" => {
                    publish_opts.stream_id = next_value(&mut iter, "--stream-id")?
                        .parse()
                        .map_err(|_| "--stream-id must be an integer".to_string())?;
                }
                "--control-port" => {
                    publish_opts.control_port = next_value(&mut iter, "--control-port")?
                        .parse()
                        .map_err(|_| "--control-port must be a valid UDP port".to_string())?;
                }
                "--codec" => publish_opts.codec = next_value(&mut iter, "--codec")?,
                "--bitrate" => {
                    publish_opts.bitrate_kbps = next_value(&mut iter, "--bitrate")?
                        .parse()
                        .map_err(|_| "--bitrate must be an integer (kbps)".to_string())?;
                }
                "--record" => {
                    publish_opts.record = Some(next_value(&mut iter, "--record")?);
                }
                "--passphrase" => {
                    publish_opts.passphrase = Some(next_value(&mut iter, "--passphrase")?);
                }
                "--seconds" => {
                    publish_opts.seconds = next_value(&mut iter, "--seconds")?
                        .parse()
                        .map_err(|_| "--seconds must be an integer".to_string())?;
                }
                "--help" | "-h" => {
                    println!(
                        "OpenAudio headless publisher\n\n\
                         Usage:\n\
                           receiver publish [--device <NAME>] [--stream-name <NAME>] [--stream-id <ID>] [--codec <pcm|opus>] [--record <PATH>]\n\
                           receiver publish --list-devices\n\n\
                         Options:\n\
                           --list-devices       List available input devices and exit\n\
                           --device <NAME>      Input audio device name\n\
                           --stream-name <NAME> Stream name to broadcast (default: 'Headless Stream')\n\
                           --node-name <NAME>   Node name for discovery (default: 'Headless Node')\n\
                           --stream-id <ID>     Stream ID (default: 1)\n\
                           --control-port <P>   Discovery control port (default: 7001)\n\
                           --codec <pcm|opus>   Audio codec (default: pcm)\n\
                           --bitrate <kbps>     Opus bitrate in kbps (default: 128)\n\
                           --record <PATH>      Record captured audio to WAV or FLAC (e.g. out.flac)\n\
                           --passphrase <PASS>  AES-256-GCM encryption passphrase\n\
                           --seconds <N>        Run duration in seconds (0 = indefinite)\n\
                           --help, -h           Show this help"
                    );
                    std::process::exit(0);
                }
                unknown => return Err(format!("unknown publish argument '{unknown}'; use --help")),
            }
        }
        return Ok(CliCommand::Publish(publish_opts));
    }

    let mut options = Options {
        list: false,
        inspect: false,
        publisher: None,
        stream_id: None,
        receive_port: 7050,
        seconds: 10,
        channels: None,
        downmix: false,
        record: None,
    };
    let mut args = raw_args.into_iter();

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--list" => options.list = true,
            "--inspect" => options.inspect = true,
            "--downmix" => options.downmix = true,
            "--record" => {
                options.record = Some(next_value(&mut args, "--record")?);
            }
            "--channels" => {
                let val = next_value(&mut args, "--channels")?;
                let indices = val
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<usize>()
                            .map_err(|_| "--channels must be comma-separated integers, e.g. 1,2".to_string())
                            .and_then(|idx| {
                                if idx > 0 {
                                    Ok(idx - 1)
                                } else {
                                    Err("channel numbers must be 1-based (>= 1)".to_string())
                                }
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                options.channels = Some(indices);
            }
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
                println!(
                    "OpenAudio receiver CLI\n\n\
                     Usage:\n\
                       receiver --list\n\
                       receiver --publisher <IP> --stream-id <ID> [--port <UDP>] [--seconds <N>] [--channels <CH1,CH2,...>] [--downmix] [--record <PATH>]\n\
                       receiver --publisher <IP> --stream-id <ID> --inspect [--port <UDP>] [--seconds <N>]\n\
                       receiver publish [--device <NAME>] [--codec <pcm|opus>] [--record <PATH>]\n\n\
                     Options:\n\
                       --list              Discover OpenAudio streams\n\
                       --publisher <IP>    Publisher IPv4 address\n\
                       --stream-id <ID>    Stream ID to receive\n\
                       --inspect           Count packets and sequence gaps without playback\n\
                       --port <UDP>        Local receive port (default: 7050)\n\
                       --seconds <N>       Run duration in seconds (0 = indefinite, default: 10)\n\
                       --channels <list>   Select 1-based channel subset (e.g. --channels 1,2)\n\
                       --downmix           Downmix multichannel stream to match output device\n\
                       --record <PATH>     Record received stream to WAV or FLAC (e.g. rec.flac)\n\
                       --help, -h          Show this help"
                );
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown argument '{unknown}'; use --help")),
        }
    }

    Ok(CliCommand::Subscribe(options))
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use network_core::protocol::{AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn nominal_packet_rate_reports_expected_64_channel_rate() {
        assert_eq!(nominal_packet_rate(64, 48_000).unwrap(), 12_000.0);
        assert!((nominal_packet_rate(2, 48_000).unwrap() - 48_000.0 / 146.0).abs() < 0.001);
        assert!(nominal_packet_rate(2, 0).is_err());
    }

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
    fn queue_trimming_preserves_interleaved_frame_alignment() {
        let mut queue = VecDeque::from([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);

        trim_interleaved_queue(&mut queue, 2, 2);

        assert_eq!(queue, VecDeque::from([3.0, 4.0, 5.0, 6.0]));
        assert_eq!(queue.len() % 2, 0);
    }

    #[test]
    fn playback_rejects_output_channel_count_mismatch() {
        assert!(validate_output_channel_count(2, 2).is_ok());
        assert!(validate_output_channel_count(6, 2).is_err());
    }

    #[test]
    fn test_map_or_downmix_pcm_multichannel_to_stereo() {
        // 4 frames of 4-channel audio
        let in_pcm = vec![
            1.0, 0.5, 0.2, 0.8, // frame 0
            0.4, 0.6, 0.1, 0.9, // frame 1
            0.0, 0.0, 1.0, 1.0, // frame 2
            0.5, 0.5, 0.5, 0.5, // frame 3
        ];
        let downmixed = map_or_downmix_pcm(&in_pcm, 4, 2, None);
        assert_eq!(downmixed.len(), 4 * 2);

        // Test channel selection: selecting channels 1 and 3 (0-based: 0, 2)
        let selected = map_or_downmix_pcm(&in_pcm, 4, 2, Some(&[0, 2]));
        assert_eq!(selected.len(), 4 * 2);
        // frame 0: ch0 = 1.0, ch2 = 0.2
        assert!((selected[0] - 1.0).abs() < 1e-4);
        assert!((selected[1] - 0.2).abs() < 1e-4);
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
            thread::sleep(Duration::from_millis(50));
        });

        // Leave room for the reconnect timeout and slower CI scheduling.
        let (packets, dropped) =
            inspect_stream("127.0.0.1", control_port, stream_id, receive_port, 7).unwrap();

        assert_eq!(packets, 4, "Should have received exactly 4 packets");
        assert_eq!(
            dropped, 1,
            "Should have detected exactly 1 dropped packet (seq 2)"
        );
    }

    #[test]
    fn test_parse_publish_and_subscribe_arguments() {
        // Test publish parsing
        let pub_cmd = parse_args(vec![
            "publish".into(),
            "--device".into(),
            "Desk Mic".into(),
            "--codec".into(),
            "opus".into(),
            "--record".into(),
            "rec.flac".into(),
        ])
        .unwrap();

        match pub_cmd {
            CliCommand::Publish(p) => {
                assert_eq!(p.device.as_deref(), Some("Desk Mic"));
                assert_eq!(p.codec, "opus");
                assert_eq!(p.record.as_deref(), Some("rec.flac"));
            }
            _ => panic!("Expected Publish command"),
        }

        // Test subscribe parsing with recording
        let sub_cmd = parse_args(vec![
            "--publisher".into(),
            "192.168.1.50".into(),
            "--stream-id".into(),
            "2".into(),
            "--record".into(),
            "received.flac".into(),
            "--downmix".into(),
        ])
        .unwrap();

        match sub_cmd {
            CliCommand::Subscribe(s) => {
                assert_eq!(s.publisher.as_deref(), Some("192.168.1.50"));
                assert_eq!(s.stream_id, Some(2));
                assert_eq!(s.record.as_deref(), Some("received.flac"));
                assert!(s.downmix);
            }
            _ => panic!("Expected Subscribe command"),
        }
    }
}
