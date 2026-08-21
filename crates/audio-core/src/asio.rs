//! ASIO driver enumeration and capture.
//!
//! This module is compiled only when the `asio` feature is enabled.
//! It mirrors the public surface of `devices.rs` and `combine.rs` but
//! targets the ASIO host instead of the default WASAPI host.
//!
//! Key difference from WASAPI:
//!   WASAPI  → one stereo WDM device per channel pair  (need combine.rs)
//!   ASIO    → one driver device with ALL channels      (open once, read all)

#[cfg(feature = "asio")]
pub mod inner {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    use cpal::SampleFormat;
    use std::collections::VecDeque;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crate::discovery::{start_advertising, SubscriberRegistry};
    use crate::ensure_realtime_audio_thread;
    use crate::protocol::{
        sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
    };
    use crate::util::safe_lock;

    const MAX_FRAMES_PER_PACKET: usize = 58;

    /// Initializes COM on the calling thread using the apartment-threaded
    /// model. ASIO drivers are COM objects under the hood — any thread
    /// that calls into cpal's ASIO host (host.devices(), build_input_stream,
    /// etc.) MUST have COM initialized first, or driver lookups silently
    /// fail with "driver not found" regardless of which driver is installed.
    ///
    /// Safe to call multiple times on the same thread — subsequent calls
    /// are no-ops (COM tracks init count internally). We intentionally
    /// swallow the error: if COM is already initialized (e.g. by eframe's
    /// windowing on the main thread), CoInitializeEx returns S_FALSE, which
    /// the `windows` crate surfaces as an "error" we don't care about.
    #[cfg(windows)]
    fn init_com_for_asio() {
        use windows::Win32::System::Com::{CoInitializeEx, COINIT_APARTMENTTHREADED};
        unsafe {
            let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        }
    }

    #[cfg(not(windows))]
    fn init_com_for_asio() {}

    /// Basic information about a detected ASIO driver.
    #[derive(Debug, Clone)]
    pub struct AsioDriverInfo {
        /// The driver name as registered in HKLM\SOFTWARE\ASIO\
        /// e.g. "Midas M32 ASIO", "Yamaha Steinberg USB ASIO"
        pub name: String,
        pub max_input_channels: u16,
        pub max_output_channels: u16,
        pub default_sample_rate: Option<u32>,
    }

    /// Returns all ASIO drivers currently installed on this machine.
    /// Reads from the ASIO host, which in turn reads the Windows registry
    /// at HKLM\SOFTWARE\ASIO\ — exactly what every DAW does.
    pub fn list_asio_drivers() -> Result<Vec<AsioDriverInfo>, String> {
        init_com_for_asio();

        let host = cpal::host_from_id(cpal::HostId::Asio)
            .map_err(|e| format!("ASIO host unavailable: {e}"))?;

        let devices = host
            .devices()
            .map_err(|e| format!("failed to enumerate ASIO devices: {e}"))?;

        let mut result = Vec::new();

        for device in devices {
            let name = match device.name() {
                Ok(n) => n,
                Err(_) => continue,
            };

            let max_input_channels = device
                .supported_input_configs()
                .ok()
                .and_then(|cfgs| cfgs.map(|c| c.channels()).max())
                .unwrap_or(0);

            let max_output_channels = device
                .supported_output_configs()
                .ok()
                .and_then(|cfgs| cfgs.map(|c| c.channels()).max())
                .unwrap_or(0);

            // Skip devices with no inputs (output-only interfaces)
            if max_input_channels == 0 {
                continue;
            }

            let default_sample_rate = device
                .default_input_config()
                .ok()
                .map(|c| c.sample_rate().0);

            result.push(AsioDriverInfo {
                name,
                max_input_channels,
                max_output_channels,
                default_sample_rate,
            });
        }

        Ok(result)
    }

    /// Opens a specific ASIO driver by name.
    /// Returns the cpal Device ready for stream building.
    pub fn get_asio_device(driver_name: &str) -> Result<cpal::Device, String> {
        init_com_for_asio();

        let host = cpal::host_from_id(cpal::HostId::Asio)
            .map_err(|e| format!("ASIO host unavailable: {e}"))?;

        host.devices()
            .map_err(|e| format!("failed to enumerate ASIO devices: {e}"))?
            .find(|d| d.name().map(|n| n == driver_name).unwrap_or(false))
            .ok_or_else(|| {
                format!(
                    "ASIO driver '{}' not found. Is it installed? Check HKLM\\SOFTWARE\\ASIO\\",
                    driver_name
                )
            })
    }

    /// Captures all channels from a single ASIO driver and streams them
    /// as an interleaved multichannel UDP stream — identical wire format
    /// to combine.rs so the receiver side needs zero changes.
    ///
    /// `channel_indices`: which input channels to stream, 0-based.
    ///   Pass vec![0,1,2,...,N-1] to stream all channels.
    ///   Pass vec![0,1] to stream only the first stereo pair.
    pub fn capture_asio_with_discovery(
        node_name: String,
        stream_name: String,
        stream_id: u32,
        driver_name: String,
        channel_indices: Vec<usize>,
        subscribers_by_stream: SubscriberRegistry,
        keep_running: Arc<AtomicBool>,
    ) -> Result<(), String> {
        // MUST be the first call in this function — this runs on a thread
        // spawned via std::thread::spawn in main.rs, which has no COM
        // context by default. Without this, get_asio_device() below will
        // fail with "driver not found" for EVERY driver, regardless of
        // which one is installed.
        init_com_for_asio();

        if channel_indices.is_empty() {
            return Err("channel_indices must not be empty".to_string());
        }
        if channel_indices.len() > 255 {
            return Err("channel count exceeds protocol limit of 255".to_string());
        }

        let device = get_asio_device(&driver_name)?;

        let config = device
            .default_input_config()
            .map_err(|e| format!("failed to get ASIO input config for '{driver_name}': {e}"))?;

        let total_driver_channels = config.channels() as usize;
        let sample_rate = config.sample_rate().0;
        let sample_format = config.sample_format();

        // Validate requested channel indices against what the driver exposes
        for &idx in &channel_indices {
            if idx >= total_driver_channels {
                return Err(format!(
                    "channel index {idx} is out of range — driver '{}' only has {total_driver_channels} input channels",
                    driver_name
                ));
            }
        }

        let rate_code = sample_rate_to_code(sample_rate);
        if rate_code == 0 {
            return Err(format!(
                "ASIO driver sample rate {sample_rate}Hz is not supported by the OpenAudio protocol. \
                 Set the driver to 44100 or 48000 Hz in its control panel."
            ));
        }

        let channel_count = channel_indices.len();
        let channel_indices = Arc::new(channel_indices);

        // Shared ring buffer: one VecDeque per selected channel
        let buffers: Vec<Arc<Mutex<VecDeque<f32>>>> = (0..channel_count)
            .map(|_| Arc::new(Mutex::new(VecDeque::new())))
            .collect();

        let buffers_for_callback = buffers.clone();
        let indices_for_callback = channel_indices.clone();

        let err_fn = {
            let name = driver_name.clone();
            move |err| eprintln!("audio-core: ASIO stream error ({name}): {err}")
        };

        let stream_config: cpal::StreamConfig = config.clone().into();

        // Build the input stream — ASIO delivers ALL channels interleaved
        // in one callback, so we demux here into per-channel ring buffers.
        let stream = match sample_format {
            SampleFormat::F32 => device.build_input_stream(
                &stream_config,
                move |data: &[f32], _| {
                    asio_demux_f32(
                        data,
                        total_driver_channels,
                        &indices_for_callback,
                        &buffers_for_callback,
                    );
                },
                err_fn,
                None,
            ),
            SampleFormat::I16 => {
                let buffers_cb = buffers_for_callback.clone();
                let indices_cb = indices_for_callback.clone();
                device.build_input_stream(
                    &stream_config,
                    move |data: &[i16], _| {
                        let converted: Vec<f32> =
                            data.iter().map(|&s| s as f32 / i16::MAX as f32).collect();
                        asio_demux_f32(
                            &converted,
                            total_driver_channels,
                            &indices_cb,
                            &buffers_cb,
                        );
                    },
                    {
                        let name = driver_name.clone();
                        move |err| eprintln!("audio-core: ASIO stream error ({name}): {err}")
                    },
                    None,
                )
            }
            SampleFormat::I32 => {
                let buffers_cb = buffers_for_callback.clone();
                let indices_cb = indices_for_callback.clone();
                device.build_input_stream(
                    &stream_config,
                    move |data: &[i32], _| {
                        let converted: Vec<f32> =
                            data.iter().map(|&s| s as f32 / i32::MAX as f32).collect();
                        asio_demux_f32(
                            &converted,
                            total_driver_channels,
                            &indices_cb,
                            &buffers_cb,
                        );
                    },
                    {
                        let name = driver_name.clone();
                        move |err| eprintln!("audio-core: ASIO stream error ({name}): {err}")
                    },
                    None,
                )
            }
            SampleFormat::F64 => {
                let buffers_cb = buffers_for_callback.clone();
                let indices_cb = indices_for_callback.clone();
                device.build_input_stream(
                    &stream_config,
                    move |data: &[f64], _| {
                        let converted: Vec<f32> = data.iter().map(|&s| s as f32).collect();
                        asio_demux_f32(
                            &converted,
                            total_driver_channels,
                            &indices_cb,
                            &buffers_cb,
                        );
                    },
                    {
                        let name = driver_name.clone();
                        move |err| eprintln!("audio-core: ASIO stream error ({name}): {err}")
                    },
                    None,
                )
            }
            _ => {
                return Err(format!(
                    "ASIO driver '{}' uses an unsupported sample format: {:?}",
                    driver_name, sample_format
                ))
            }
        }
        .map_err(|e| format!("failed to build ASIO input stream for '{driver_name}': {e}"))?;

        stream
            .play()
            .map_err(|e| format!("failed to start ASIO stream for '{driver_name}': {e}"))?;

        // Start advertising this stream on the network
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
                eprintln!("audio-core: ASIO advertising stopped: {e}");
            }
        });

        let socket =
            UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;
        let sequence = Arc::new(AtomicU32::new(0));
        let start = Instant::now();
        let poll_interval = Duration::from_millis(2);

        println!(
            "ASIO: streaming {channel_count} channel(s) from '{}' @ {sample_rate}Hz → stream {stream_id}",
            driver_name
        );

        // Main transmit loop: drain ring buffers → build packets → send UDP
        while keep_running.load(Ordering::Relaxed) {
            let available = buffers
                .iter()
                .map(|b| safe_lock(b).len())
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

            // Always drain buffers even if no subscribers, to prevent overflow
            let mut per_channel_samples: Vec<Vec<f32>> = Vec::with_capacity(channel_count);
            for buf in &buffers {
                let mut guard = safe_lock(buf);
                let samples: Vec<f32> = guard.drain(..frames_to_send).collect();
                per_channel_samples.push(samples);
            }

            if let Some(dests) = dests {
                // Interleave: [Ch0_f0, Ch1_f0, ..., ChN_f0, Ch0_f1, Ch1_f1, ...]
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
                        eprintln!("audio-core: ASIO send to {addr} failed: {e}");
                    }
                }
            }
        }

        drop(stream);
        println!("ASIO: stream stopped.");
        Ok(())
    }

    /// Demux interleaved ASIO callback data into per-channel ring buffers.
    /// ASIO delivers: [Ch0_f0, Ch1_f0, ..., ChN_f0, Ch0_f1, Ch1_f1, ...]
    /// We extract only the channels in `selected_indices`.
    fn asio_demux_f32(
        data: &[f32],
        total_channels: usize,
        selected_indices: &[usize],
        buffers: &[Arc<Mutex<VecDeque<f32>>>],
    ) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if total_channels == 0 {
                return;
            }
            let frame_count = data.len() / total_channels;
            for f in 0..frame_count {
                for (buf_idx, &ch_idx) in selected_indices.iter().enumerate() {
                    let sample = data[f * total_channels + ch_idx];
                    safe_lock(&buffers[buf_idx]).push_back(sample);
                }
            }
        }));
        if result.is_err() {
            eprintln!("audio-core: panic caught in ASIO demux callback — dropping this cycle");
        }
    }
}

// Re-export inner when asio feature is active
#[cfg(feature = "asio")]
pub use inner::{capture_asio_with_discovery, get_asio_device, list_asio_drivers, AsioDriverInfo};

// Stub types when asio feature is NOT active — keeps the rest of the
// codebase compiling without #[cfg] scattered everywhere.
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
    Err("ASIO support is not compiled in. Build with --features asio and set CPAL_ASIO_DIR.".to_string())
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
    Err("ASIO support is not compiled in. Build with --features asio and set CPAL_ASIO_DIR.".to_string())
}