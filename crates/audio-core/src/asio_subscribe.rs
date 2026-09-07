//! OpenAudio network stream playback through an ASIO output driver.
//!
//! A subscription receives interleaved Float32 PCM over UDP, buffers it,
//! and routes each incoming channel to an optional ASIO output channel.
//!
//! Routing uses zero-based indices:
//!
//! ```text
//! vec![
//!     Some(0), // incoming channel 1 -> ASIO output 1
//!     Some(1), // incoming channel 2 -> ASIO output 2
//!     None,    // incoming channel 3 -> disabled
//!     Some(7), // incoming channel 4 -> ASIO output 8
//! ]
//! ```
//!
//! Multiple incoming channels may target the same ASIO output. They are
//! averaged to reduce clipping risk.

#[cfg(feature = "asio")]
mod inner {
    use crate::asio::get_asio_device;
    use crate::ensure_realtime_audio_thread;
    use crate::protocol::{parse_packet, ParsedPacket};
    use crate::recording::{
        create_wav_writer, finalize as finalize_recording, write_samples, SharedWavWriter,
    };
    use crate::util::safe_lock;
    use crate::{
        register_scoped_signal_meter, AdaptiveJitterController, ClockSynchronizer,
        resample_interleaved_linear, SignalDirection, SignalMeter,
    };

    use cpal::traits::{DeviceTrait, StreamTrait};
    use cpal::{SampleFormat, SampleRate, Stream, StreamConfig, SupportedStreamConfig};

    use std::collections::VecDeque;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    const MAX_UDP_PACKET_BYTES: usize = 65_536;
    const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(200);
    const PRIME_TIMEOUT: Duration = Duration::from_millis(750);
    const MAX_BUFFER_SECS: f64 = 0.2;

    type SharedAudioBuffer = Arc<Mutex<VecDeque<f32>>>;

    /// Routes an OpenAudio network stream to an ASIO output driver.
    ///
    /// `output_routes` contains one entry for each incoming stream
    /// channel. Each entry is either:
    ///
    /// - `Some(output_index)` to route that incoming channel to the
    ///   selected zero-based ASIO output channel.
    /// - `None` to mute that incoming channel.
    ///
    /// The incoming sample rate must be supported directly by the
    /// selected ASIO driver. Resampling is deliberately not performed.
    pub fn receive_and_play_asio(
        bind_addr: &str,
        driver_name: String,
        output_routes: Vec<Option<usize>>,
        record_path: Option<String>,
        keep_running: Arc<AtomicBool>,
    ) -> Result<(), String> {
        ensure_realtime_audio_thread();

        if output_routes.is_empty() {
            return Err("ASIO output routing must contain at least one \
                 incoming channel"
                .to_string());
        }

        if output_routes.iter().all(Option::is_none) {
            return Err("ASIO output routing has no enabled channels. \
                 Route at least one incoming channel."
                .to_string());
        }

        let socket = UdpSocket::bind(bind_addr).map_err(|error| {
            format!(
                "failed to bind ASIO subscriber to \
                     {bind_addr}: {error}"
            )
        })?;

        socket
            .set_read_timeout(Some(SOCKET_READ_TIMEOUT))
            .map_err(|error| {
                format!(
                    "failed to set ASIO subscriber read \
                     timeout: {error}"
                )
            })?;

        println!(
            "ASIO Subscribe: listening on {bind_addr}; \
             waiting for the first audio packet..."
        );

        let audio_buffer: SharedAudioBuffer = Arc::new(Mutex::new(VecDeque::new()));

        let mut udp_buffer = vec![0u8; MAX_UDP_PACKET_BYTES];

        let first = wait_for_first_packet(
            &socket,
            &mut udp_buffer,
            &audio_buffer,
            keep_running.clone(),
        )?;

        let incoming_channels = first.channel_count as usize;
        let sample_rate = first.sample_rate;
        let subscribed_stream_id = first.stream_id;
        let mut last_sequence = Some(first.sequence_number);
        let mut packets_received = 1u64;
        let mut packets_dropped = 0u64;
        let mut jitter_controller = AdaptiveJitterController::new(sample_rate);
        let receive_clock_start = Instant::now();
        let mut clock_synchronizer = ClockSynchronizer::default();
        let mut last_clock_adjustment = Instant::now();

        if incoming_channels == 0 {
            return Err("incoming OpenAudio stream reports zero channels".to_string());
        }

        if sample_rate == 0 {
            return Err("incoming OpenAudio stream uses an unsupported \
                 or invalid sample rate"
                .to_string());
        }

        if output_routes.len() != incoming_channels {
            return Err(format!(
                "ASIO routing count mismatch: the incoming \
                 stream has {incoming_channels} channel(s), \
                 but {} route entries were provided",
                output_routes.len()
            ));
        }

        let device = get_asio_device(&driver_name)?;

        let output_config = pick_asio_output_config(&device, sample_rate)
            .map_err(|error| format!("ASIO driver '{driver_name}': {error}"))?;

        let output_channels = output_config.channels() as usize;
        let output_sample_rate = output_config.sample_rate().0;

        if output_sample_rate != sample_rate {
            resample_buffer(
                &audio_buffer,
                incoming_channels,
                sample_rate,
                output_sample_rate,
            );
        }

        validate_routes(
            &output_routes,
            incoming_channels,
            output_channels,
            &driver_name,
        )?;

        let sample_format = output_config.sample_format();

        let stream_config: StreamConfig = output_config.clone().into();

        println!(
            "ASIO Subscribe: stream {subscribed_stream_id}, \
             {incoming_channels}ch @ {sample_rate}Hz -> \
             '{}' ({} ASIO outputs, {:?})",
            driver_name, output_channels, sample_format
        );

        let writer: Option<SharedWavWriter> = match record_path.as_deref() {
            Some(path) => Some(create_wav_writer(
                path,
                output_channels as u16,
                output_sample_rate,
            )?),
            None => None,
        };

        let (output_meter, _output_meter_guard) = register_scoped_signal_meter(
            format!(
                "asio-subscribe:{}:{}:{}",
                subscribed_stream_id, bind_addr, driver_name,
            ),
            format!(
                "ASIO Subscribe — Stream {} → '{}'",
                subscribed_stream_id, driver_name,
            ),
            SignalDirection::Output,
            output_channels,
        );

        prime_buffer(
            &socket,
            &mut udp_buffer,
            &audio_buffer,
            subscribed_stream_id,
            incoming_channels,
            sample_rate,
            output_sample_rate,
            &jitter_controller,
            keep_running.clone(),
            &mut last_sequence,
            &mut packets_received,
            &mut packets_dropped,
        )?;

        if !keep_running.load(Ordering::Relaxed) {
            if let Some(writer) = &writer {
                finalize_recording(writer);
            }

            return Ok(());
        }

        let stream = build_output_stream(
            &device,
            &stream_config,
            sample_format,
            audio_buffer.clone(),
            incoming_channels,
            output_channels,
            output_routes,
            writer.clone(),
            output_meter,
            driver_name.clone(),
        )?;

        stream.play().map_err(|error| {
            format!(
                "failed to start ASIO output stream \
                 '{driver_name}': {error}"
            )
        })?;

        println!(
            "ASIO Subscribe: playback started through \
             '{driver_name}'."
        );

        let receive_result = run_receive_loop(
            &socket,
            &mut udp_buffer,
            &audio_buffer,
            subscribed_stream_id,
            incoming_channels,
            sample_rate,
            output_sample_rate,
            keep_running,
            &mut last_sequence,
            &mut packets_received,
            &mut packets_dropped,
            &mut jitter_controller,
            &mut clock_synchronizer,
            receive_clock_start,
            &mut last_clock_adjustment,
        );

        // Stop the callback before finalizing the recording writer.
        drop(stream);

        if let Some(writer) = &writer {
            finalize_recording(writer);
        }

        println!(
            "ASIO Subscribe: stopped. {packets_received} \
             packet(s) received, approximately \
             {packets_dropped} packet(s) dropped."
        );

        receive_result
    }

    fn wait_for_first_packet(
        socket: &UdpSocket,
        udp_buffer: &mut [u8],
        audio_buffer: &SharedAudioBuffer,
        keep_running: Arc<AtomicBool>,
    ) -> Result<ParsedPacket, String> {
        loop {
            if !keep_running.load(Ordering::Relaxed) {
                return Err("ASIO subscription cancelled before \
                     audio arrived"
                    .to_string());
            }

            let (length, _) = match socket.recv_from(udp_buffer) {
                Ok(value) => value,
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "receive error while waiting \
                             for the first ASIO subscription \
                             packet: {error}"
                    ));
                }
            };

            let packet = &udp_buffer[..length];

            let Some(parsed) = parse_packet(packet) else {
                continue;
            };

            validate_packet_payload(packet, &parsed)?;

            if parsed.channel_count == 0 {
                continue;
            }

            if parsed.sample_rate == 0 {
                continue;
            }

            append_packet_samples(audio_buffer, packet, &parsed, parsed.sample_rate)?;

            return Ok(parsed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prime_buffer(
        socket: &UdpSocket,
        udp_buffer: &mut [u8],
        audio_buffer: &SharedAudioBuffer,
        stream_id: u32,
        incoming_channels: usize,
        sample_rate: u32,
        output_sample_rate: u32,
        jitter_controller: &AdaptiveJitterController,
        keep_running: Arc<AtomicBool>,
        last_sequence: &mut Option<u32>,
        packets_received: &mut u64,
        packets_dropped: &mut u64,
    ) -> Result<(), String> {
        let target_frames = jitter_controller.target_frames();

        let target_samples = target_frames.saturating_mul(incoming_channels);

        let deadline = Instant::now() + PRIME_TIMEOUT;

        while buffered_samples(audio_buffer) < target_samples && Instant::now() < deadline {
            if !keep_running.load(Ordering::Relaxed) {
                return Ok(());
            }

            let (length, _) = match socket.recv_from(udp_buffer) {
                Ok(value) => value,
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "receive error while priming \
                             ASIO jitter buffer: {error}"
                    ));
                }
            };

            let packet = &udp_buffer[..length];

            let Some(parsed) = parse_packet(packet) else {
                continue;
            };

            if parsed.stream_id != stream_id {
                continue;
            }

            if parsed.channel_count as usize != incoming_channels
                || parsed.sample_rate != sample_rate
            {
                continue;
            }

            validate_packet_payload(packet, &parsed)?;

            update_sequence_statistics(parsed.sequence_number, last_sequence, packets_dropped);

            append_packet_samples(
                audio_buffer,
                packet,
                &parsed,
                output_sample_rate,
            )?;

            *packets_received = packets_received.saturating_add(1);
        }

        println!(
            "ASIO Subscribe: jitter buffer primed with \
             {} sample(s), target {}.",
            buffered_samples(audio_buffer),
            target_samples
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn run_receive_loop(
        socket: &UdpSocket,
        udp_buffer: &mut [u8],
        audio_buffer: &SharedAudioBuffer,
        stream_id: u32,
        incoming_channels: usize,
        sample_rate: u32,
        output_sample_rate: u32,
        keep_running: Arc<AtomicBool>,
        last_sequence: &mut Option<u32>,
        packets_received: &mut u64,
        packets_dropped: &mut u64,
        jitter_controller: &mut AdaptiveJitterController,
        clock_synchronizer: &mut ClockSynchronizer,
        receive_clock_start: Instant,
        last_clock_adjustment: &mut Instant,
    ) -> Result<(), String> {
        let max_buffered_frames = (sample_rate as f64 * MAX_BUFFER_SECS) as usize;

        while keep_running.load(Ordering::Relaxed) {
            let (length, _) = match socket.recv_from(udp_buffer) {
                Ok(value) => value,
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "ASIO subscriber receive error: \
                             {error}"
                    ));
                }
            };

            let packet = &udp_buffer[..length];

            let Some(parsed) = parse_packet(packet) else {
                continue;
            };

            if parsed.stream_id != stream_id {
                continue;
            }

            if parsed.channel_count as usize != incoming_channels
                || parsed.sample_rate != sample_rate
            {
                eprintln!(
                    "audio-core: ASIO subscriber dropped \
                     packet from stream {} because its \
                     format changed from {}ch @ {}Hz to \
                     {}ch @ {}Hz",
                    stream_id,
                    incoming_channels,
                    output_sample_rate,
                    parsed.channel_count,
                    parsed.sample_rate
                );

                continue;
            }

            validate_packet_payload(packet, &parsed)?;

            clock_synchronizer.observe(
                parsed.presentation_timestamp_ns,
                receive_clock_start.elapsed(),
            );

            update_sequence_statistics(parsed.sequence_number, last_sequence, packets_dropped);

            append_packet_samples(
                audio_buffer,
                packet,
                &parsed,
                output_sample_rate,
            )?;

            apply_clock_correction(
                audio_buffer,
                incoming_channels,
                clock_synchronizer.correction_ppm(),
                last_clock_adjustment,
            );

            *packets_received = packets_received.saturating_add(1);

            let buffered_frames = buffered_samples(audio_buffer) / incoming_channels.max(1);
            let target_frames = jitter_controller.target_frames();
            jitter_controller.observe(
                buffered_frames,
                buffered_frames < target_frames / 2,
                buffered_frames > target_frames.saturating_mul(2),
            );

            let max_buffered_samples = max_buffered_frames
                .min(jitter_controller.target_frames().saturating_mul(4))
                .saturating_mul(incoming_channels);

            if *packets_received % 500 == 0 {
                println!(
                    "ASIO Subscribe timing: target={:.1}ms, buffered={:.1}ms, clock correction={:.2}ppm",
                    jitter_controller.target_duration().as_secs_f64() * 1_000.0,
                    buffered_frames as f64 / sample_rate as f64 * 1_000.0,
                    clock_synchronizer.correction_ppm(),
                );
            }

            let mut guard = safe_lock(audio_buffer);

            while guard.len() > max_buffered_samples {
                // Drop complete frames so the interleaved
                // channel alignment remains intact.
                for _ in 0..incoming_channels {
                    if guard.pop_front().is_none() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn pick_asio_output_config(
        device: &cpal::Device,
        sample_rate: u32,
    ) -> Result<SupportedStreamConfig, String> {
        let supported: Vec<_> = device
            .supported_output_configs()
            .map_err(|error| {
                format!(
                    "failed to query ASIO output \
                     configurations: {error}"
                )
            })?
            .collect();

        let mut matching: Vec<_> = supported
            .into_iter()
            .filter(|config| {
                sample_rate >= config.min_sample_rate().0
                    && sample_rate <= config.max_sample_rate().0
            })
            .collect();

        matching.sort_by(|left, right| {
            let channel_order = right.channels().cmp(&left.channels());

            if channel_order != std::cmp::Ordering::Equal {
                return channel_order;
            }

            sample_format_priority(right.sample_format())
                .cmp(&sample_format_priority(left.sample_format()))
        });

        if let Some(config) = matching.into_iter().next() {
            return Ok(config.with_sample_rate(SampleRate(sample_rate)));
        }

        let default = device.default_output_config().map_err(|error| {
            format!(
                "no ASIO output configuration \
                         supports {sample_rate}Hz, and the \
                         default output configuration could \
                         not be read: {error}"
            )
        })?;

        Ok(default)
    }

    fn sample_format_priority(format: SampleFormat) -> u8 {
        match format {
            SampleFormat::F32 => 6,
            SampleFormat::F64 => 5,
            SampleFormat::I32 => 4,
            SampleFormat::I16 => 3,
            SampleFormat::U32 => 2,
            SampleFormat::U16 => 1,
            _ => 0,
        }
    }

    fn validate_routes(
        output_routes: &[Option<usize>],
        incoming_channels: usize,
        output_channels: usize,
        driver_name: &str,
    ) -> Result<(), String> {
        if output_routes.len() != incoming_channels {
            return Err(format!(
                "expected {incoming_channels} ASIO route \
                 entries, received {}",
                output_routes.len()
            ));
        }

        if output_channels == 0 {
            return Err(format!(
                "ASIO driver '{driver_name}' exposes no \
                 output channels"
            ));
        }

        for (incoming_index, output_index) in output_routes.iter().enumerate() {
            if let Some(output_index) = output_index {
                if *output_index >= output_channels {
                    return Err(format!(
                        "invalid ASIO route: incoming \
                         channel {} targets output {}, but \
                         driver '{}' exposes only {} output \
                         channel(s)",
                        incoming_index + 1,
                        output_index + 1,
                        driver_name,
                        output_channels
                    ));
                }
            }
        }

        if output_routes.iter().all(Option::is_none) {
            return Err("all incoming channels are disabled in \
                 the ASIO routing map"
                .to_string());
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_output_stream(
        device: &cpal::Device,
        config: &StreamConfig,
        sample_format: SampleFormat,
        audio_buffer: SharedAudioBuffer,
        incoming_channels: usize,
        output_channels: usize,
        output_routes: Vec<Option<usize>>,
        writer: Option<SharedWavWriter>,
        output_meter: Arc<SignalMeter>,
        driver_name: String,
    ) -> Result<Stream, String> {
        match sample_format {
            SampleFormat::F32 => build_typed_output_stream::<f32>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            SampleFormat::F64 => build_typed_output_stream::<f64>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            SampleFormat::I16 => build_typed_output_stream::<i16>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            SampleFormat::I32 => build_typed_output_stream::<i32>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            SampleFormat::U16 => build_typed_output_stream::<u16>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            SampleFormat::U32 => build_typed_output_stream::<u32>(
                device,
                config,
                audio_buffer,
                incoming_channels,
                output_channels,
                output_routes,
                writer,
                output_meter,
                driver_name,
            ),
            other => Err(format!(
                "ASIO output sample format {other:?} is \
                 not currently supported"
            )),
        }
    }

    trait AsioOutputSample: cpal::SizedSample + Send + 'static {
        fn from_normalized_f32(value: f32) -> Self;
    }

    impl AsioOutputSample for f32 {
        fn from_normalized_f32(value: f32) -> Self {
            value.clamp(-1.0, 1.0)
        }
    }

    impl AsioOutputSample for f64 {
        fn from_normalized_f32(value: f32) -> Self {
            value.clamp(-1.0, 1.0) as f64
        }
    }

    impl AsioOutputSample for i16 {
        fn from_normalized_f32(value: f32) -> Self {
            let value = value.clamp(-1.0, 1.0);

            if value <= -1.0 {
                i16::MIN
            } else {
                (value * i16::MAX as f32).round() as i16
            }
        }
    }

    impl AsioOutputSample for i32 {
        fn from_normalized_f32(value: f32) -> Self {
            let value = value.clamp(-1.0, 1.0);

            if value <= -1.0 {
                i32::MIN
            } else {
                (value as f64 * i32::MAX as f64).round() as i32
            }
        }
    }

    impl AsioOutputSample for u16 {
        fn from_normalized_f32(value: f32) -> Self {
            let normalized = value.clamp(-1.0, 1.0) * 0.5 + 0.5;

            (normalized * u16::MAX as f32).round() as u16
        }
    }

    impl AsioOutputSample for u32 {
        fn from_normalized_f32(value: f32) -> Self {
            let normalized = value.clamp(-1.0, 1.0) as f64 * 0.5 + 0.5;

            (normalized * u32::MAX as f64).round() as u32
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_typed_output_stream<T>(
        device: &cpal::Device,
        config: &StreamConfig,
        audio_buffer: SharedAudioBuffer,
        incoming_channels: usize,
        output_channels: usize,
        output_routes: Vec<Option<usize>>,
        writer: Option<SharedWavWriter>,
        output_meter: Arc<SignalMeter>,
        driver_name: String,
    ) -> Result<Stream, String>
    where
        T: AsioOutputSample,
    {
        let route_divisors = build_route_divisors(&output_routes, output_channels);

        // Reused across callbacks. It only reallocates if the ASIO
        // backend changes its callback buffer length.
        let mut routed_scratch = Vec::<f32>::new();

        let error_driver_name = driver_name.clone();

        let error_callback = move |error| {
            eprintln!(
                "audio-core: ASIO output stream error on \
                 '{}': {}",
                error_driver_name, error
            );
        };

        device
            .build_output_stream(
                config,
                move |output: &mut [T], _| {
                    ensure_realtime_audio_thread();

                    let callback_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            if routed_scratch.len() != output.len() {
                                routed_scratch.resize(output.len(), 0.0);
                            }

                            render_routed_audio(
                                &audio_buffer,
                                &mut routed_scratch,
                                incoming_channels,
                                output_channels,
                                &output_routes,
                                &route_divisors,
                            );

                            // Meter after routing, mixing,
                            // averaging, clipping, and underrun
                            // silence insertion.
                            output_meter.observe_interleaved(&routed_scratch, output_channels);

                            for (destination, source) in
                                output.iter_mut().zip(routed_scratch.iter().copied())
                            {
                                *destination = T::from_normalized_f32(source);
                            }

                            if let Some(writer) = &writer {
                                write_samples(writer, &routed_scratch);
                            }
                        }));

                    if callback_result.is_err() {
                        eprintln!(
                            "audio-core: panic caught in \
                             ASIO output callback; outputting \
                             silence for this cycle"
                        );

                        for sample in output.iter_mut() {
                            *sample = T::from_normalized_f32(0.0);
                        }

                        if routed_scratch.len() != output.len() {
                            routed_scratch.resize(output.len(), 0.0);
                        } else {
                            routed_scratch.fill(0.0);
                        }

                        // Clear stale peaks after a failed callback.
                        output_meter.observe_interleaved(&routed_scratch, output_channels);
                    }
                },
                error_callback,
                None,
            )
            .map_err(|error| {
                format!(
                    "failed to build ASIO output stream \
                     for '{}': {}",
                    driver_name, error
                )
            })
    }

    fn build_route_divisors(output_routes: &[Option<usize>], output_channels: usize) -> Vec<f32> {
        let mut route_counts = vec![0usize; output_channels];

        for output_index in output_routes.iter().flatten() {
            if *output_index < output_channels {
                route_counts[*output_index] += 1;
            }
        }

        route_counts
            .into_iter()
            .map(|count| if count > 1 { 1.0 / count as f32 } else { 1.0 })
            .collect()
    }

    fn render_routed_audio(
        audio_buffer: &SharedAudioBuffer,
        output: &mut [f32],
        incoming_channels: usize,
        output_channels: usize,
        output_routes: &[Option<usize>],
        route_divisors: &[f32],
    ) {
        output.fill(0.0);

        if incoming_channels == 0 || output_channels == 0 {
            return;
        }

        let output_frames = output.len() / output_channels;

        let mut guard = safe_lock(audio_buffer);

        for frame_index in 0..output_frames {
            if guard.len() < incoming_channels {
                // The output buffer was pre-cleared, so missing
                // network frames remain silence.
                continue;
            }

            let output_frame_offset = frame_index * output_channels;

            for (incoming_index, route) in output_routes.iter().enumerate() {
                let sample = guard.pop_front().unwrap_or(0.0);

                if incoming_index >= incoming_channels {
                    continue;
                }

                let Some(output_index) = route else {
                    continue;
                };

                if *output_index >= output_channels {
                    continue;
                }

                output[output_frame_offset + *output_index] += sample;
            }

            for output_index in 0..output_channels {
                let position = output_frame_offset + output_index;

                output[position] =
                    (output[position] * route_divisors[output_index]).clamp(-1.0, 1.0);
            }
        }

        // CPAL buffers should contain complete frames. Clear any
        // unexpected trailing sample positions explicitly.
        let rendered_samples = output_frames * output_channels;

        if rendered_samples < output.len() {
            output[rendered_samples..].fill(0.0);
        }
    }

    fn validate_packet_payload(packet: &[u8], parsed: &ParsedPacket) -> Result<(), String> {
        let sample_count = (parsed.samples_per_channel as usize)
            .checked_mul(parsed.channel_count as usize)
            .ok_or_else(|| "OpenAudio packet sample count overflow".to_string())?;

        let payload_bytes = sample_count
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| "OpenAudio packet payload-size overflow".to_string())?;

        let required_length = parsed
            .payload_offset
            .checked_add(payload_bytes)
            .ok_or_else(|| "OpenAudio packet length overflow".to_string())?;

        if packet.len() < required_length {
            return Err(format!(
                "truncated OpenAudio packet: expected at \
                 least {required_length} bytes, received {}",
                packet.len()
            ));
        }

        Ok(())
    }

    fn append_packet_samples(
        audio_buffer: &SharedAudioBuffer,
        packet: &[u8],
        parsed: &ParsedPacket,
        output_sample_rate: u32,
    ) -> Result<(), String> {
        validate_packet_payload(packet, parsed)?;

        let sample_count = (parsed.samples_per_channel as usize)
            .checked_mul(parsed.channel_count as usize)
            .ok_or_else(|| "OpenAudio packet sample count overflow".to_string())?;

        let payload_length = sample_count
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| "OpenAudio packet payload-size overflow".to_string())?;

        let payload_end = parsed
            .payload_offset
            .checked_add(payload_length)
            .ok_or_else(|| "OpenAudio packet payload-end overflow".to_string())?;

        let payload = packet
            .get(parsed.payload_offset..payload_end)
            .ok_or_else(|| "OpenAudio packet payload is truncated".to_string())?;

        let samples = payload
            .chunks_exact(4)
            .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect::<Vec<_>>();
        let samples = resample_interleaved_linear(
            &samples,
            parsed.channel_count as usize,
            parsed.sample_rate,
            output_sample_rate,
        );

        let mut guard = safe_lock(audio_buffer);
        guard.reserve(samples.len());
        guard.extend(samples);

        Ok(())
    }

    fn resample_buffer(
        audio_buffer: &SharedAudioBuffer,
        channels: usize,
        input_rate: u32,
        output_rate: u32,
    ) {
        let mut guard = safe_lock(audio_buffer);
        let input = guard.drain(..).collect::<Vec<_>>();
        let output = resample_interleaved_linear(&input, channels, input_rate, output_rate);
        guard.extend(output);
    }

    fn apply_clock_correction(
        audio_buffer: &SharedAudioBuffer,
        channels: usize,
        correction_ppm: f64,
        last_adjustment: &mut Instant,
    ) {
        const CORRECTION_THRESHOLD_PPM: f64 = 200.0;
        const ADJUSTMENT_INTERVAL: Duration = Duration::from_millis(250);

        if channels == 0
            || correction_ppm.abs() < CORRECTION_THRESHOLD_PPM
            || last_adjustment.elapsed() < ADJUSTMENT_INTERVAL
        {
            return;
        }

        let mut buffer = safe_lock(audio_buffer);
        if correction_ppm > 0.0 {
            for _ in 0..channels {
                buffer.pop_front();
            }
        } else if buffer.len() >= channels {
            let frame = buffer.iter().take(channels).copied().collect::<Vec<_>>();
            buffer.extend(frame);
        }

        *last_adjustment = Instant::now();
    }

    fn update_sequence_statistics(
        sequence_number: u32,
        last_sequence: &mut Option<u32>,
        packets_dropped: &mut u64,
    ) {
        if let Some(previous) = *last_sequence {
            let expected = previous.wrapping_add(1);

            if sequence_number != expected {
                let gap = sequence_number.wrapping_sub(expected);

                // A very large wrapped value usually means an old or
                // reordered packet, not billions of dropped packets.
                if gap < 1_000_000 {
                    *packets_dropped = packets_dropped.saturating_add(gap as u64);
                }
            }
        }

        *last_sequence = Some(sequence_number);
    }

    fn buffered_samples(audio_buffer: &SharedAudioBuffer) -> usize {
        safe_lock(audio_buffer).len()
    }
}

#[cfg(feature = "asio")]
pub use inner::receive_and_play_asio;

#[cfg(not(feature = "asio"))]
pub fn receive_and_play_asio(
    _bind_addr: &str,
    _driver_name: String,
    _output_routes: Vec<Option<usize>>,
    _record_path: Option<String>,
    _keep_running: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), String> {
    Err("ASIO support is not compiled in. Build with \
         --features asio after setting CPAL_ASIO_DIR."
        .to_string())
}
