//! ASIO driver enumeration and multichannel capture.
//!
//! This module opens an ASIO driver, captures selected channels,
//! converts them to interleaved Float32 audio, meters the final selected
//! channel stream, and publishes MTU-safe UDP packets.

#[cfg(feature = "asio")]
pub mod inner {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    use cpal::SampleFormat;
    use socket2::SockRef;
    use std::collections::VecDeque;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crate::discovery::{start_advertising, SubscriberRegistry};
    use crate::ensure_realtime_audio_thread;
    use crate::protocol::{
        sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
    };
    use crate::util::safe_lock;
    use crate::{register_scoped_signal_meter, SignalDirection, SignalMeter};

    /// Conservative payload limit that avoids IP fragmentation on most
    /// Ethernet, Wi-Fi, VPN, and tunneled network paths.
    const SAFE_UDP_PAYLOAD_BYTES: usize = 1_200;

    const PACKET_HEADER_BYTES: usize = 24;
    const AUDIO_PAYLOAD_HEADER_BYTES: usize = 8;
    const OPENAUDIO_HEADER_BYTES: usize = PACKET_HEADER_BYTES + AUDIO_PAYLOAD_HEADER_BYTES;
    const BYTES_PER_SAMPLE: usize = std::mem::size_of::<f32>();

    /// Maximum captured audio retained while the network worker is
    /// delayed. Old frames are discarded when this limit is exceeded.
    const MAX_CAPTURE_BUFFER_MS: usize = 100;

    const UDP_SEND_BUFFER_BYTES: usize = 4 * 1024 * 1024;

    const EMPTY_BUFFER_SLEEP: Duration = Duration::from_micros(250);

    type SharedCaptureBuffer = Arc<Mutex<VecDeque<f32>>>;

    #[derive(Debug, Clone)]
    pub struct AsioDriverInfo {
        pub name: String,
        pub max_input_channels: u16,
        pub max_output_channels: u16,
        pub default_sample_rate: Option<u32>,
    }

    #[derive(Default)]
    struct CaptureMetrics {
        callback_frames: AtomicU64,
        dropped_capture_frames: AtomicU64,
        packets_sent: AtomicU64,
        packet_send_errors: AtomicU64,
        datagrams_sent: AtomicU64,
    }

    #[cfg(windows)]
    fn init_com_for_asio() {
        use windows::Win32::System::Com::{CoInitializeEx, COINIT_APARTMENTTHREADED};

        unsafe {
            let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        }
    }

    #[cfg(not(windows))]
    fn init_com_for_asio() {}

    /// Lists installed ASIO drivers exposing at least one input or
    /// output channel.
    pub fn list_asio_drivers() -> Result<Vec<AsioDriverInfo>, String> {
        init_com_for_asio();

        let host = cpal::host_from_id(cpal::HostId::Asio)
            .map_err(|error| format!("ASIO host unavailable: {error}"))?;

        let devices = host.devices().map_err(|error| {
            format!(
                "failed to enumerate ASIO devices: \
                 {error}"
            )
        })?;

        let mut result = Vec::new();

        for device in devices {
            let name = match device.name() {
                Ok(name) => name,
                Err(error) => {
                    eprintln!(
                        "audio-core: skipping ASIO device \
                         whose name could not be read: \
                         {error}"
                    );
                    continue;
                }
            };

            let max_input_channels = device
                .supported_input_configs()
                .ok()
                .and_then(|configs| configs.map(|config| config.channels()).max())
                .unwrap_or(0);

            let max_output_channels = device
                .supported_output_configs()
                .ok()
                .and_then(|configs| configs.map(|config| config.channels()).max())
                .unwrap_or(0);

            if max_input_channels == 0 && max_output_channels == 0 {
                continue;
            }

            let default_sample_rate = device
                .default_output_config()
                .ok()
                .map(|config| config.sample_rate().0)
                .or_else(|| {
                    device
                        .default_input_config()
                        .ok()
                        .map(|config| config.sample_rate().0)
                });

            result.push(AsioDriverInfo {
                name,
                max_input_channels,
                max_output_channels,
                default_sample_rate,
            });
        }

        result.sort_by(|left, right| left.name.to_lowercase().cmp(&right.name.to_lowercase()));

        Ok(result)
    }

    /// Opens an installed ASIO driver using its exact registered name.
    pub fn get_asio_device(driver_name: &str) -> Result<cpal::Device, String> {
        init_com_for_asio();

        let host = cpal::host_from_id(cpal::HostId::Asio)
            .map_err(|error| format!("ASIO host unavailable: {error}"))?;

        host.devices()
            .map_err(|error| {
                format!(
                    "failed to enumerate ASIO devices: \
                     {error}"
                )
            })?
            .find(|device| {
                device
                    .name()
                    .map(|name| name == driver_name)
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                format!(
                    "ASIO driver '{driver_name}' not found. \
                     Is it installed? Check \
                     HKLM\\SOFTWARE\\ASIO\\"
                )
            })
    }

    /// Captures selected zero-based ASIO input channels and publishes
    /// them as one interleaved OpenAudio stream.
    pub fn capture_asio_with_discovery(
        node_name: String,
        stream_name: String,
        stream_id: u32,
        driver_name: String,
        channel_indices: Vec<usize>,
        subscribers_by_stream: SubscriberRegistry,
        keep_running: Arc<AtomicBool>,
    ) -> Result<(), String> {
        init_com_for_asio();
        ensure_realtime_audio_thread();

        validate_channel_selection(&channel_indices)?;

        let device = get_asio_device(&driver_name)?;

        let input_config = device.default_input_config().map_err(|error| {
            format!(
                "failed to get ASIO input config for \
                     '{driver_name}': {error}"
            )
        })?;

        let total_driver_channels = input_config.channels() as usize;
        let sample_rate = input_config.sample_rate().0;
        let sample_format = input_config.sample_format();

        validate_driver_channels(&driver_name, total_driver_channels, &channel_indices)?;

        let sample_rate_code = sample_rate_to_code(sample_rate);

        if sample_rate_code == 0 {
            return Err(format!(
                "ASIO driver sample rate {sample_rate}Hz \
                 is not supported by the OpenAudio \
                 protocol. Set the driver to 44100 or \
                 48000 Hz in its control panel."
            ));
        }

        let selected_channel_count = channel_indices.len();

        let meter_label = format!(
            "{node_name} — {stream_name} — ASIO \
             '{driver_name}'"
        );

        let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
            format!("asio-publish:{stream_id}"),
            meter_label,
            SignalDirection::Input,
            selected_channel_count,
        );

        let max_frames_per_packet = calculate_max_frames_per_packet(selected_channel_count)?;

        let maximum_packet_bytes = OPENAUDIO_HEADER_BYTES
            + max_frames_per_packet * selected_channel_count * BYTES_PER_SAMPLE;

        let maximum_buffered_frames =
            ((sample_rate as usize * MAX_CAPTURE_BUFFER_MS) / 1_000).max(max_frames_per_packet);

        let maximum_buffered_samples =
            maximum_buffered_frames.saturating_mul(selected_channel_count);

        let capture_buffer: SharedCaptureBuffer = Arc::new(Mutex::new(VecDeque::with_capacity(
            maximum_buffered_samples,
        )));

        let channel_indices = Arc::new(channel_indices);

        let metrics = Arc::new(CaptureMetrics::default());

        let stream_config: cpal::StreamConfig = input_config.clone().into();

        let stream = build_asio_input_stream(
            &device,
            &stream_config,
            sample_format,
            total_driver_channels,
            channel_indices,
            capture_buffer.clone(),
            maximum_buffered_samples,
            selected_channel_count,
            metrics.clone(),
            driver_name.clone(),
        )?;

        stream.play().map_err(|error| {
            format!(
                "failed to start ASIO input stream \
                 '{driver_name}': {error}"
            )
        })?;

        let advertising_flag = keep_running.clone();
        let advertising_node_name = node_name;
        let advertising_stream_name = stream_name;
        let advertised_channels = selected_channel_count as u8;

        std::thread::spawn(move || {
            if let Err(error) = start_advertising(
                advertising_node_name,
                stream_id,
                advertising_stream_name,
                advertised_channels,
                advertising_flag,
            ) {
                eprintln!(
                    "audio-core: ASIO advertising \
                     stopped: {error}"
                );
            }
        });

        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|error| {
            format!(
                "failed to bind ASIO publisher \
                         socket: {error}"
            )
        })?;

        configure_send_socket(&socket);

        let sequence = AtomicU32::new(0);
        let clock_start = Instant::now();

        println!("ASIO Publish: '{}' -> stream {}", driver_name, stream_id);

        println!(
            "ASIO Publish: {} selected channel(s) from \
             {} driver channel(s) at {}Hz using {:?}",
            selected_channel_count, total_driver_channels, sample_rate, sample_format
        );

        println!(
            "ASIO Publish: MTU-safe packetization = up \
             to {} frame(s), {} byte maximum UDP payload",
            max_frames_per_packet, maximum_packet_bytes
        );

        println!(
            "ASIO Publish: estimated raw PCM rate = \
             {:.2} Mbps",
            estimated_pcm_megabits_per_second(selected_channel_count, sample_rate,)
        );

        let result = run_transmit_loop(
            &socket,
            stream_id,
            sample_rate_code,
            selected_channel_count,
            max_frames_per_packet,
            &capture_buffer,
            &subscribers_by_stream,
            &sequence,
            clock_start,
            keep_running,
            metrics.clone(),
            signal_meter,
        );

        drop(stream);

        print_final_metrics(&driver_name, selected_channel_count, sample_rate, &metrics);

        result
    }

    fn validate_channel_selection(channel_indices: &[usize]) -> Result<(), String> {
        if channel_indices.is_empty() {
            return Err("channel_indices must not be empty".to_string());
        }

        if channel_indices.len() > u8::MAX as usize {
            return Err("channel count exceeds the OpenAudio \
                 protocol limit of 255"
                .to_string());
        }

        let mut sorted = channel_indices.to_vec();
        sorted.sort_unstable();

        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("channel_indices contains duplicate \
                 ASIO input channels"
                .to_string());
        }

        Ok(())
    }

    fn validate_driver_channels(
        driver_name: &str,
        total_driver_channels: usize,
        channel_indices: &[usize],
    ) -> Result<(), String> {
        if total_driver_channels == 0 {
            return Err(format!(
                "ASIO driver '{driver_name}' exposes \
                 no input channels"
            ));
        }

        for &channel_index in channel_indices {
            if channel_index >= total_driver_channels {
                return Err(format!(
                    "channel index {channel_index} is out \
                     of range: ASIO driver '{driver_name}' \
                     exposes only {total_driver_channels} \
                     input channel(s)"
                ));
            }
        }

        Ok(())
    }

    fn calculate_max_frames_per_packet(channel_count: usize) -> Result<usize, String> {
        if channel_count == 0 {
            return Err("cannot calculate packet size for zero \
                 channels"
                .to_string());
        }

        let audio_budget = SAFE_UDP_PAYLOAD_BYTES
            .checked_sub(OPENAUDIO_HEADER_BYTES)
            .ok_or_else(|| {
                "safe UDP payload is smaller than \
                     the OpenAudio headers"
                    .to_string()
            })?;

        let bytes_per_frame = channel_count
            .checked_mul(BYTES_PER_SAMPLE)
            .ok_or_else(|| "ASIO packet frame-size overflow".to_string())?;

        let frames = audio_budget / bytes_per_frame;

        if frames == 0 {
            return Err(format!(
                "{channel_count} Float32 channel(s) \
                 cannot fit one complete audio frame \
                 inside the configured \
                 {SAFE_UDP_PAYLOAD_BYTES}-byte UDP \
                 payload limit"
            ));
        }

        Ok(frames.min(u16::MAX as usize))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_asio_input_stream(
        device: &cpal::Device,
        stream_config: &cpal::StreamConfig,
        sample_format: SampleFormat,
        total_driver_channels: usize,
        channel_indices: Arc<Vec<usize>>,
        capture_buffer: SharedCaptureBuffer,
        maximum_buffered_samples: usize,
        selected_channel_count: usize,
        metrics: Arc<CaptureMetrics>,
        driver_name: String,
    ) -> Result<cpal::Stream, String> {
        match sample_format {
            SampleFormat::F32 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: f32| sample,
            ),
            SampleFormat::F64 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: f64| sample as f32,
            ),
            SampleFormat::I16 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: i16| sample as f32 / i16::MAX as f32,
            ),
            SampleFormat::I32 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: i32| sample as f32 / i32::MAX as f32,
            ),
            SampleFormat::U16 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: u16| (sample as f32 / u16::MAX as f32) * 2.0 - 1.0,
            ),
            SampleFormat::U32 => build_typed_input_stream(
                device,
                stream_config,
                total_driver_channels,
                channel_indices,
                capture_buffer,
                maximum_buffered_samples,
                selected_channel_count,
                metrics,
                driver_name,
                |sample: u32| (sample as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32,
            ),
            other => Err(format!(
                "ASIO driver '{}' uses unsupported \
                 input sample format {:?}",
                driver_name, other
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_typed_input_stream<T, Convert>(
        device: &cpal::Device,
        stream_config: &cpal::StreamConfig,
        total_driver_channels: usize,
        channel_indices: Arc<Vec<usize>>,
        capture_buffer: SharedCaptureBuffer,
        maximum_buffered_samples: usize,
        selected_channel_count: usize,
        metrics: Arc<CaptureMetrics>,
        driver_name: String,
        convert: Convert,
    ) -> Result<cpal::Stream, String>
    where
        T: cpal::SizedSample + Copy + Send + 'static,
        Convert: Fn(T) -> f32 + Send + Sync + 'static,
    {
        let callback_driver_name = driver_name.clone();
        let error_driver_name = driver_name.clone();

        device
            .build_input_stream(
                stream_config,
                move |data: &[T], _| {
                    let callback_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            capture_selected_channels(
                                data,
                                total_driver_channels,
                                &channel_indices,
                                &capture_buffer,
                                maximum_buffered_samples,
                                selected_channel_count,
                                &metrics,
                                &convert,
                            );
                        }));

                    if callback_result.is_err() {
                        eprintln!(
                            "audio-core: panic caught in \
                             ASIO capture callback for '{}'; \
                             dropping this cycle",
                            callback_driver_name
                        );
                    }
                },
                move |error| {
                    eprintln!(
                        "audio-core: ASIO input stream \
                         error on '{}': {}",
                        error_driver_name, error
                    );
                },
                None,
            )
            .map_err(|error| {
                format!(
                    "failed to build ASIO input stream \
                     for '{driver_name}': {error}"
                )
            })
    }

    /// Selects, converts, clips, and queues the chosen ASIO channels.
    ///
    /// The capture queue is locked once per callback rather than once
    /// per sample.
    #[allow(clippy::too_many_arguments)]
    fn capture_selected_channels<T, Convert>(
        data: &[T],
        total_driver_channels: usize,
        selected_indices: &[usize],
        capture_buffer: &SharedCaptureBuffer,
        maximum_buffered_samples: usize,
        selected_channel_count: usize,
        metrics: &CaptureMetrics,
        convert: &Convert,
    ) where
        T: Copy,
        Convert: Fn(T) -> f32,
    {
        if total_driver_channels == 0 || selected_channel_count == 0 {
            return;
        }

        let frame_count = data.len() / total_driver_channels;

        if frame_count == 0 {
            return;
        }

        metrics
            .callback_frames
            .fetch_add(frame_count as u64, Ordering::Relaxed);

        let incoming_selected_samples = frame_count.saturating_mul(selected_channel_count);

        let mut queue = safe_lock(capture_buffer);

        let required_capacity = queue.len().saturating_add(incoming_selected_samples);

        if required_capacity > maximum_buffered_samples {
            let excess_samples = required_capacity - maximum_buffered_samples;

            let frames_to_drop = excess_samples.div_ceil(selected_channel_count);

            let samples_to_drop = frames_to_drop
                .saturating_mul(selected_channel_count)
                .min(queue.len());

            for _ in 0..samples_to_drop {
                queue.pop_front();
            }

            metrics.dropped_capture_frames.fetch_add(
                samples_to_drop.div_ceil(selected_channel_count) as u64,
                Ordering::Relaxed,
            );
        }

        for frame_index in 0..frame_count {
            let frame_offset = frame_index * total_driver_channels;

            for &channel_index in selected_indices {
                let sample = data[frame_offset + channel_index];

                queue.push_back(convert(sample).clamp(-1.0, 1.0));
            }
        }
    }

    fn configure_send_socket(socket: &UdpSocket) {
        let socket_ref = SockRef::from(socket);

        match socket_ref.set_send_buffer_size(UDP_SEND_BUFFER_BYTES) {
            Ok(()) => {
                println!(
                    "ASIO Publish: requested {} MiB UDP \
                     send buffer",
                    UDP_SEND_BUFFER_BYTES / (1024 * 1024)
                );
            }
            Err(error) => {
                eprintln!(
                    "audio-core: could not increase UDP \
                     send buffer: {error}. Continuing \
                     with the operating-system default."
                );
            }
        }

        if let Err(error) = socket.set_nonblocking(false) {
            eprintln!(
                "audio-core: could not configure \
                 publisher socket as blocking: {error}"
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_transmit_loop(
        socket: &UdpSocket,
        stream_id: u32,
        sample_rate_code: u16,
        channel_count: usize,
        max_frames_per_packet: usize,
        capture_buffer: &SharedCaptureBuffer,
        subscribers_by_stream: &SubscriberRegistry,
        sequence: &AtomicU32,
        clock_start: Instant,
        keep_running: Arc<AtomicBool>,
        metrics: Arc<CaptureMetrics>,
        signal_meter: Arc<SignalMeter>,
    ) -> Result<(), String> {
        let maximum_packet_size =
            OPENAUDIO_HEADER_BYTES + max_frames_per_packet * channel_count * BYTES_PER_SAMPLE;

        let mut packet = Vec::with_capacity(maximum_packet_size);

        let mut samples = Vec::with_capacity(max_frames_per_packet * channel_count);

        let mut last_metrics_report = Instant::now();

        while keep_running.load(Ordering::Relaxed) {
            let available_frames = {
                let queue = safe_lock(capture_buffer);

                queue.len() / channel_count
            };

            if available_frames == 0 {
                if last_metrics_report.elapsed() >= Duration::from_secs(5) {
                    print_periodic_metrics(capture_buffer, channel_count, &metrics);

                    last_metrics_report = Instant::now();
                }

                std::thread::sleep(EMPTY_BUFFER_SLEEP);

                continue;
            }

            let frames_to_send = available_frames.min(max_frames_per_packet);

            let sample_count = frames_to_send * channel_count;

            samples.clear();

            {
                let mut queue = safe_lock(capture_buffer);

                if queue.len() < sample_count {
                    continue;
                }

                samples.extend(queue.drain(..sample_count));
            }

            // Meter the final selected and converted ASIO stream before
            // checking for subscribers. The meter therefore remains
            // useful while the publisher is waiting for a receiver.
            signal_meter.observe_interleaved(&samples, channel_count);

            let destinations = {
                let subscribers = safe_lock(subscribers_by_stream);

                subscribers
                    .get(&stream_id)
                    .filter(|addresses| !addresses.is_empty())
                    .cloned()
            };

            // Captured data is always drained to prevent unlimited
            // latency, even when no subscriber is connected.
            let Some(destinations) = destinations else {
                continue;
            };

            let sequence_number = sequence.fetch_add(1, Ordering::Relaxed);

            let packet_header = PacketHeader {
                sub_stream_index: 0,
                stream_id,
                sequence_number,
                presentation_timestamp_ns: clock_start.elapsed().as_nanos() as u64,
            };

            let payload_header = AudioPayloadHeader {
                channel_count: channel_count as u8,
                sample_format: SAMPLE_FORMAT_FLOAT32,
                sample_rate_code,
                samples_per_channel: frames_to_send as u16,
            };

            packet.clear();

            packet.extend_from_slice(&packet_header.to_bytes());

            packet.extend_from_slice(&payload_header.to_bytes());

            for &sample in &samples {
                packet.extend_from_slice(&sample.to_le_bytes());
            }

            debug_assert!(
                packet.len() <= SAFE_UDP_PAYLOAD_BYTES,
                "ASIO publisher generated a \
                 fragmented UDP payload: {} bytes",
                packet.len()
            );

            metrics.packets_sent.fetch_add(1, Ordering::Relaxed);

            for destination in destinations {
                match socket.send_to(&packet, destination) {
                    Ok(bytes_sent) if bytes_sent == packet.len() => {
                        metrics.datagrams_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(bytes_sent) => {
                        metrics.packet_send_errors.fetch_add(1, Ordering::Relaxed);

                        eprintln!(
                            "audio-core: partial ASIO \
                             UDP send to {destination}: \
                             sent {bytes_sent} of {} \
                             bytes",
                            packet.len()
                        );
                    }
                    Err(error) => {
                        metrics.packet_send_errors.fetch_add(1, Ordering::Relaxed);

                        eprintln!(
                            "audio-core: ASIO UDP send \
                             to {destination} failed: \
                             {error}"
                        );
                    }
                }
            }

            if last_metrics_report.elapsed() >= Duration::from_secs(5) {
                print_periodic_metrics(capture_buffer, channel_count, &metrics);

                last_metrics_report = Instant::now();
            }
        }

        Ok(())
    }

    fn print_periodic_metrics(
        capture_buffer: &SharedCaptureBuffer,
        channel_count: usize,
        metrics: &CaptureMetrics,
    ) {
        let queued_frames = {
            let queue = safe_lock(capture_buffer);

            queue.len() / channel_count.max(1)
        };

        println!(
            "ASIO Publish metrics: queued={} frames, \
             captured={} frames, capture-dropped={} \
             frames, packets={}, datagrams={}, \
             send-errors={}",
            queued_frames,
            metrics.callback_frames.load(Ordering::Relaxed),
            metrics.dropped_capture_frames.load(Ordering::Relaxed),
            metrics.packets_sent.load(Ordering::Relaxed),
            metrics.datagrams_sent.load(Ordering::Relaxed),
            metrics.packet_send_errors.load(Ordering::Relaxed),
        );
    }

    fn print_final_metrics(
        driver_name: &str,
        channel_count: usize,
        sample_rate: u32,
        metrics: &CaptureMetrics,
    ) {
        println!(
            "ASIO Publish stopped: '{}' ({}ch @ \
             {}Hz). Captured {} frame(s), dropped {} \
             capture frame(s), built {} packet(s), \
             sent {} datagram(s), {} send error(s).",
            driver_name,
            channel_count,
            sample_rate,
            metrics.callback_frames.load(Ordering::Relaxed),
            metrics.dropped_capture_frames.load(Ordering::Relaxed),
            metrics.packets_sent.load(Ordering::Relaxed),
            metrics.datagrams_sent.load(Ordering::Relaxed),
            metrics.packet_send_errors.load(Ordering::Relaxed),
        );
    }

    fn estimated_pcm_megabits_per_second(channel_count: usize, sample_rate: u32) -> f64 {
        channel_count as f64 * sample_rate as f64 * BYTES_PER_SAMPLE as f64 * 8.0 / 1_000_000.0
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn packet_size_is_safe_for_32_channels() {
            let frames = calculate_max_frames_per_packet(32).unwrap();

            let bytes = OPENAUDIO_HEADER_BYTES + frames * 32 * BYTES_PER_SAMPLE;

            assert!(frames > 0);
            assert!(bytes <= SAFE_UDP_PAYLOAD_BYTES);
        }

        #[test]
        fn packet_size_decreases_as_channels_increase() {
            let stereo = calculate_max_frames_per_packet(2).unwrap();

            let thirty_two = calculate_max_frames_per_packet(32).unwrap();

            assert!(stereo > thirty_two);
        }

        #[test]
        fn zero_channels_are_rejected() {
            assert!(calculate_max_frames_per_packet(0).is_err());
        }
    }
}

#[cfg(feature = "asio")]
pub use inner::{capture_asio_with_discovery, get_asio_device, list_asio_drivers, AsioDriverInfo};

// Non-ASIO build stubs.

#[cfg(not(feature = "asio"))]
#[derive(Debug, Clone)]
pub struct AsioDriverInfo {
    pub name: String,
    pub max_input_channels: u16,
    pub max_output_channels: u16,
    pub default_sample_rate: Option<u32>,
}

#[cfg(not(feature = "asio"))]
pub fn list_asio_drivers() -> Result<Vec<AsioDriverInfo>, String> {
    Err("ASIO support is not compiled in. Build with \
         --features asio and set CPAL_ASIO_DIR."
        .to_string())
}

#[cfg(not(feature = "asio"))]
pub fn get_asio_device(_driver_name: &str) -> Result<(), String> {
    Err("ASIO support is not compiled in.".to_string())
}

#[cfg(not(feature = "asio"))]
pub fn capture_asio_with_discovery(
    _node_name: String,
    _stream_name: String,
    _stream_id: u32,
    _driver_name: String,
    _channel_indices: Vec<usize>,
    _subscribers_by_stream: crate::discovery::SubscriberRegistry,
    _keep_running: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), String> {
    Err("ASIO support is not compiled in. Build with \
         --features asio and set CPAL_ASIO_DIR."
        .to_string())
}
