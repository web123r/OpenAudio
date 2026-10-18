use crossbeam::queue::ArrayQueue;
use network_core::discovery::{active_subscribers, SubscriberRegistry};
use network_core::protocol::{encode_float32_packet, max_float32_frames_per_packet, PacketHeader};
use nice_plug::prelude::*;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const AUDIO_BLOCK_COUNT: usize = 4;
const MAX_HOST_BLOCK_FRAMES: usize = 8_192;

static STREAM_ID_COUNTER: AtomicU32 = AtomicU32::new(1);
static CONTROL_SERVICE: OnceLock<Mutex<Weak<ControlService>>> = OnceLock::new();

struct AudioBlock {
    samples: Vec<f32>,
    presentation_timestamp_ns: u64,
}

fn acquire_audio_block(
    free_blocks: &ArrayQueue<AudioBlock>,
    ready_blocks: &ArrayQueue<AudioBlock>,
    channels: usize,
    dropped_frames: &AtomicU64,
) -> Option<AudioBlock> {
    let mut block = free_blocks.pop().or_else(|| ready_blocks.pop())?;
    if !block.samples.is_empty() {
        dropped_frames.fetch_add((block.samples.len() / channels) as u64, Ordering::Relaxed);
    }
    block.samples.clear();
    Some(block)
}

fn discard_queued_audio(
    free_blocks: &ArrayQueue<AudioBlock>,
    ready_blocks: &ArrayQueue<AudioBlock>,
) {
    while let Some(mut block) = ready_blocks.pop() {
        block.samples.clear();
        let _ = free_blocks.push(block);
    }
}

struct ControlService {
    subscribers: SubscriberRegistry,
    control_port: u16,
    running: Arc<AtomicBool>,
    listener: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ControlService {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        let listener = self
            .listener
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(listener) = listener {
            if listener.join().is_err() {
                eprintln!("OpenAudio VST3: subscription listener thread panicked");
            }
        }
    }
}

fn control_service() -> Result<Arc<ControlService>, String> {
    let service_slot = CONTROL_SERVICE.get_or_init(|| Mutex::new(Weak::<ControlService>::new()));
    let mut service_slot = service_slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(service) = service_slot.upgrade() {
        return Ok(service);
    }

    let socket = network_core::discovery::bind_control_listener_on(0)?;
    let control_port = socket
        .local_addr()
        .map_err(|error| format!("failed to inspect subscription listener: {error}"))?
        .port();
    let subscribers = Arc::new(Mutex::new(HashMap::new()));
    let running = Arc::new(AtomicBool::new(true));
    let listener_subscribers = subscribers.clone();
    let listener_running = running.clone();
    let listener = thread::Builder::new()
        .name("openaudio-vst3-control".to_string())
        .spawn(move || {
            if let Err(error) = network_core::discovery::run_control_listener(
                socket,
                listener_subscribers,
                listener_running,
            ) {
                eprintln!("OpenAudio VST3: subscription listener stopped: {error}");
            }
        })
        .map_err(|error| format!("failed to start subscription listener: {error}"))?;

    let service = Arc::new(ControlService {
        subscribers,
        control_port,
        running,
        listener: Mutex::new(Some(listener)),
    });
    *service_slot = Arc::downgrade(&service);
    Ok(service)
}

struct PublisherRuntime {
    channels: usize,
    maximum_block_frames: usize,
    started_at: Instant,
    stream_id: u32,
    /// Retained to identify the named stream during logging; not read after construction.
    #[allow(dead_code)]
    stream_sequence: u32,
    sample_rate: u32,
    free_blocks: Arc<ArrayQueue<AudioBlock>>,
    ready_blocks: Arc<ArrayQueue<AudioBlock>>,
    /// Frames dropped because the ready-block pool was full.
    dropped_frames: Arc<AtomicU64>,
    /// UDP send errors counted by the publisher worker.
    /// Arc is moved into the closure; the field is kept here for potential
    /// future introspection (e.g. a plugin editor or state export).
    #[allow(dead_code)]
    send_errors: Arc<AtomicU64>,
    /// Packets successfully sent since the last stats report.
    #[allow(dead_code)]
    packets_sent: Arc<AtomicU64>,
    #[allow(dead_code)]
    publish_enabled: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    publisher: Option<JoinHandle<()>>,
    advertiser: Option<JoinHandle<()>>,
    _control_service: Arc<ControlService>,
}

impl PublisherRuntime {
    fn start(
        channels: usize,
        sample_rate: f32,
        maximum_block_frames: usize,
        publish_enabled: Arc<AtomicBool>,
        slot: u32,
        encrypt_enabled: Arc<AtomicBool>,
        active_pin: Arc<AtomicU32>,
    ) -> Result<Self, String> {
        if channels == 0 || channels > 64 {
            return Err(format!(
                "the VST3 sender supports 1 to 64 channels, not {channels} channels"
            ));
        }
        if !sample_rate.is_finite() || maximum_block_frames == 0 {
            return Err("the DAW supplied an invalid audio configuration".to_string());
        }
        if maximum_block_frames > MAX_HOST_BLOCK_FRAMES {
            return Err(format!(
                "the DAW block size ({maximum_block_frames} frames) exceeds the VST3 sender limit of {MAX_HOST_BLOCK_FRAMES}"
            ));
        }

        let rounded_sample_rate = sample_rate.round();
        if (sample_rate - rounded_sample_rate).abs() > 0.01 {
            return Err(format!(
                "unsupported non-integer sample rate: {sample_rate}"
            ));
        }
        let sample_rate = rounded_sample_rate as u32;
        let sample_rate_code = network_core::protocol::sample_rate_to_code(sample_rate);
        if sample_rate_code == 0 {
            return Err(format!("unsupported sample rate: {sample_rate}"));
        }

        let stream_sequence = slot;
        let stream_id = std::process::id()
            .wrapping_mul(2_654_435_761)
            .wrapping_add(stream_sequence)
            .max(1);
        let stream_name = format!("DAW Slot {stream_sequence}");

        let control_service = control_service()?;
        let control_port = control_service.control_port;
        let socket = UdpSocket::bind(("0.0.0.0", 0))
            .map_err(|error| format!("failed to bind audio socket: {error}"))?;
        let maximum_packet_frames = max_float32_frames_per_packet(channels)?;
        let samples_per_block = channels
            .checked_mul(maximum_block_frames)
            .ok_or_else(|| "DAW audio block size overflow".to_string())?;

        let ready_blocks = Arc::new(ArrayQueue::<AudioBlock>::new(AUDIO_BLOCK_COUNT));
        let free_blocks = Arc::new(ArrayQueue::<AudioBlock>::new(AUDIO_BLOCK_COUNT * 2));
        for _ in 0..AUDIO_BLOCK_COUNT {
            free_blocks
                .push(AudioBlock {
                    samples: Vec::with_capacity(samples_per_block),
                    presentation_timestamp_ns: 0,
                })
                .map_err(|_| "failed to initialize VST3 audio block pool".to_string())?;
        }

        let running = Arc::new(AtomicBool::new(true));
        let dropped_frames = Arc::new(AtomicU64::new(0));
        let send_errors = Arc::new(AtomicU64::new(0));
        let packets_sent = Arc::new(AtomicU64::new(0));
        let started_at = Instant::now();
        let publisher_running = running.clone();
        let publisher_publish_enabled = publish_enabled.clone();
        let publisher_ready_blocks = ready_blocks.clone();
        let publisher_free_blocks = free_blocks.clone();
        let publisher_dropped_frames = dropped_frames.clone();
        let publisher_send_errors = send_errors.clone();
        let publisher_packets_sent = packets_sent.clone();
        let publisher_subscribers = control_service.subscribers.clone();

        let publisher_encrypt_enabled = encrypt_enabled.clone();
        let publisher_active_pin = active_pin.clone();

        eprintln!(
            "OpenAudio VST3 [stream {stream_id}]: activated — \
             {channels}ch @ {sample_rate} Hz, max block {maximum_block_frames} frames, \
             control port {control_port}, stream name \"{stream_name}\""
        );

        let publisher = thread::Builder::new()
            .name(format!("openaudio-vst3-publish-{stream_id}"))
            .spawn(move || {
                let mut sequence_number = 0u32;
                let mut last_stats_report = Instant::now();
                let mut fec_encoder = network_core::fec::FecEncoder::new(
                    network_core::fec::DEFAULT_FEC_GROUP_SIZE,
                );
                let mut current_cipher: Option<(u32, audio_core::crypto::StreamCipher)> = None;

                while publisher_running.load(Ordering::Acquire)
                    || !publisher_ready_blocks.is_empty()
                {
                    if !publisher_publish_enabled.load(Ordering::Acquire) {
                        while let Some(mut block) = publisher_ready_blocks.pop() {
                            block.samples.clear();
                            let _ = publisher_free_blocks.push(block);
                        }
                        if let Ok(mut registry) = publisher_subscribers.lock() {
                            registry.remove(&stream_id);
                        }
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }

                    if publisher_encrypt_enabled.load(Ordering::Relaxed) {
                        let pin = publisher_active_pin.load(Ordering::Relaxed);
                        if current_cipher.as_ref().map(|(p, _)| *p) != Some(pin) {
                            current_cipher = Some((pin, audio_core::crypto::StreamCipher::from_passphrase(&pin.to_string())));
                        }
                    } else {
                        current_cipher = None;
                    }

                    if let Some(block) = publisher_ready_blocks.pop() {
                        let maybe_destinations = active_subscribers(
                            &publisher_subscribers,
                            stream_id,
                        )
                        .filter(|destinations| !destinations.is_empty());

                        if let Some(destinations) = maybe_destinations {
                            for samples in block
                                .samples
                                .chunks(maximum_packet_frames * channels)
                            {
                                let header = PacketHeader {
                                    sub_stream_index: 0,
                                    stream_id,
                                    sequence_number,
                                    presentation_timestamp_ns: block.presentation_timestamp_ns,
                                };
                                sequence_number = sequence_number.wrapping_add(1);

                                match encode_float32_packet(
                                    &header,
                                    channels as u8,
                                    sample_rate,
                                    samples,
                                ) {
                                    Ok(mut packet) => {
                                        if let Some((_, ref cipher)) = current_cipher {
                                            if let Ok(enc) = audio_core::crypto::encrypt_audio_packet(cipher, &packet) {
                                                packet = enc;
                                            }
                                        }

                                        for destination in &destinations {
                                            if let Err(error) =
                                                socket.send_to(&packet, destination)
                                            {
                                                publisher_send_errors
                                                    .fetch_add(1, Ordering::Relaxed);
                                                eprintln!(
                                                    "OpenAudio VST3 [stream {stream_id}]: \
                                                     send to {destination} failed: {error}"
                                                );
                                            } else {
                                                publisher_packets_sent
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                        }

                                        if packet.len() >= 32 {
                                            let frames = samples.len() / channels;
                                            let maybe_fec = fec_encoder.push_audio_packet(
                                                stream_id,
                                                header.sequence_number,
                                                header.presentation_timestamp_ns,
                                                channels as u8,
                                                sample_rate,
                                                frames as u16,
                                                &packet[32..],
                                            );
                                            if let Some(fec_packet) = maybe_fec {
                                                for destination in &destinations {
                                                    let _ = socket.send_to(&fec_packet, destination);
                                                }
                                            }
                                        }
                                    }
                                    Err(error) => eprintln!(
                                        "OpenAudio VST3 [stream {stream_id}]: \
                                         packet encoding failed: {error}"
                                    ),
                                }
                            }
                        }

                        let mut block = block;
                        block.samples.clear();
                        if publisher_free_blocks.push(block).is_err() {
                            eprintln!(
                                "OpenAudio VST3 [stream {stream_id}]: \
                                 audio block pool invariant violated"
                            );
                        }
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }

                    // Emit a one-line stats summary every second.  All four counters
                    // are reset after printing so each line shows the interval delta.
                    // This is designed to be grep-able during a REAPER smoke test.
                    if last_stats_report.elapsed() >= Duration::from_secs(1) {
                        let dropped = publisher_dropped_frames.swap(0, Ordering::Relaxed);
                        let errors = publisher_send_errors.swap(0, Ordering::Relaxed);
                        let sent = publisher_packets_sent.swap(0, Ordering::Relaxed);
                        let subscriber_count = publisher_subscribers
                            .lock()
                            .ok()
                            .and_then(|registry| registry.get(&stream_id).map(|s| s.len()))
                            .unwrap_or(0);

                        if dropped > 0 || errors > 0 || sent > 0 || subscriber_count > 0 {
                            eprintln!(
                                "OpenAudio VST3 [stream {stream_id}] stats: \
                                 sent={sent} dropped_frames={dropped} \
                                 send_errors={errors} subscribers={subscriber_count}"
                            );
                        }
                        last_stats_report = Instant::now();
                    }
                }
            })
            .map_err(|error| format!("failed to start audio publisher: {error}"))?;

        let advertiser_running = running.clone();
        let advertiser_active = publish_enabled.clone();
        let advertiser_encrypted = encrypt_enabled.clone();
        let advertiser = match thread::Builder::new()
            .name(format!("openaudio-vst3-discovery-{stream_id}"))
            .spawn(move || {
                let is_enc = advertiser_encrypted.load(Ordering::Relaxed);
                if let Err(error) = audio_core::start_advertising_with_labels_and_active_encrypted(
                    "OpenAudio VST3".to_string(),
                    stream_id,
                    stream_name,
                    channels as u8,
                    network_core::discovery::default_channel_labels(channels as u8),
                    control_port,
                    is_enc,
                    advertiser_running,
                    advertiser_active,
                ) {
                    eprintln!(
                        "OpenAudio VST3 [stream {stream_id}]: \
                         discovery advertising stopped: {error}"
                    );
                }
            }) {
            Ok(advertiser) => advertiser,
            Err(error) => {
                running.store(false, Ordering::Release);
                if publisher.join().is_err() {
                    eprintln!(
                        "OpenAudio VST3 [stream {stream_id}]: audio publisher thread panicked"
                    );
                }
                return Err(format!("failed to start discovery advertiser: {error}"));
            }
        };

        Ok(Self {
            channels,
            maximum_block_frames,
            started_at,
            stream_id,
            stream_sequence,
            sample_rate,
            free_blocks,
            ready_blocks,
            dropped_frames,
            send_errors,
            packets_sent,
            publish_enabled,
            running,
            publisher: Some(publisher),
            advertiser: Some(advertiser),
            _control_service: control_service,
        })
    }

    fn stop(&mut self) {
        let uptime_secs = self.started_at.elapsed().as_secs();
        eprintln!(
            "OpenAudio VST3 [stream {}]: deactivating after {}s — \
             {} ch @ {} Hz",
            self.stream_id, uptime_secs, self.channels, self.sample_rate
        );
        self.running.store(false, Ordering::Release);
        for worker in [&mut self.advertiser, &mut self.publisher] {
            if let Some(worker) = worker.take() {
                if worker.join().is_err() {
                    eprintln!(
                        "OpenAudio VST3 [stream {}]: background worker thread panicked",
                        self.stream_id
                    );
                }
            }
        }
        eprintln!("OpenAudio VST3 [stream {}]: deactivated.", self.stream_id);
    }
}

impl Drop for PublisherRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}


pub struct OpenAudioDawSender {
    params: Arc<OpenAudioDawSenderParams>,
    runtime: Option<PublisherRuntime>,
    publish_enabled: Arc<AtomicBool>,
    active_slot: Arc<AtomicU32>,
    encrypt_enabled: Arc<AtomicBool>,
    active_pin: Arc<AtomicU32>,
}

#[derive(Params)]
pub struct OpenAudioDawSenderParams {
    #[id = "publish"]
    pub publish: BoolParam,

    #[id = "slot"]
    pub slot: IntParam,

    #[id = "encrypt"]
    pub encrypt: BoolParam,

    #[id = "pin"]
    pub pin: IntParam,
}

impl Default for OpenAudioDawSender {
    fn default() -> Self {
        let publish_enabled = Arc::new(AtomicBool::new(false));
        let cb_enabled = publish_enabled.clone();
        let default_slot = ((STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed) - 1) % 32 + 1) as i32;
        let active_slot = Arc::new(AtomicU32::new(default_slot as u32));
        let cb_slot = active_slot.clone();
        let encrypt_enabled = Arc::new(AtomicBool::new(false));
        let cb_encrypt = encrypt_enabled.clone();
        let active_pin = Arc::new(AtomicU32::new(1234));
        let cb_pin = active_pin.clone();

        Self {
            params: Arc::new(OpenAudioDawSenderParams {
                publish: BoolParam::new("Publish to OpenAudio", false)
                    .with_callback(Arc::new(move |value| {
                        cb_enabled.store(value, Ordering::Release);
                    })),
                slot: IntParam::new(
                    "Network Track Slot",
                    default_slot,
                    IntRange::Linear { min: 1, max: 32 },
                )
                .with_callback(Arc::new(move |value| {
                    cb_slot.store(value.clamp(1, 32) as u32, Ordering::Release);
                })),
                encrypt: BoolParam::new("AES-256-GCM Encryption", false)
                    .with_callback(Arc::new(move |value| {
                        cb_encrypt.store(value, Ordering::Release);
                    })),
                pin: IntParam::new(
                    "Encryption Key PIN",
                    1234,
                    IntRange::Linear { min: 0, max: 999_999 },
                )
                .with_callback(Arc::new(move |value| {
                    cb_pin.store(value.clamp(0, 999_999) as u32, Ordering::Release);
                })),
            }),
            runtime: None,
            publish_enabled,
            active_slot,
            encrypt_enabled,
            active_pin,
        }
    }
}

impl Plugin for OpenAudioDawSender {
    const NAME: &'static str = "OpenAudio DAW Sender";
    const VENDOR: &'static str = "OpenAudio";
    const URL: &'static str = "";
    const EMAIL: &'static str = "";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const AUDIO_IO_LAYOUTS: &'static [AudioIOLayout] = &[
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(2),
            main_output_channels: NonZeroU32::new(2),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(1),
            main_output_channels: NonZeroU32::new(1),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(4),
            main_output_channels: NonZeroU32::new(4),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(6),
            main_output_channels: NonZeroU32::new(6),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(8),
            main_output_channels: NonZeroU32::new(8),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(16),
            main_output_channels: NonZeroU32::new(16),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(32),
            main_output_channels: NonZeroU32::new(32),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(64),
            main_output_channels: NonZeroU32::new(64),
            ..AudioIOLayout::const_default()
        },
    ];

    type SysExMessage = ();
    type BackgroundTask = ();
    type Editor = ();

    fn params(&self) -> Arc<dyn Params> {
        self.params.clone()
    }

    fn activate(
        &mut self,
        audio_io_layout: &AudioIOLayout,
        buffer_config: &BufferConfig,
        _context: &mut impl ActivateContext<Self>,
    ) -> bool {
        let channels = audio_io_layout
            .main_input_channels
            .map(NonZeroU32::get)
            .unwrap_or_default() as usize;
        self.publish_enabled
            .store(self.params.publish.value(), Ordering::Release);
        let slot = self.params.slot.value().clamp(1, 32) as u32;
        self.active_slot.store(slot, Ordering::Release);
        let is_encrypted = self.params.encrypt.value();
        self.encrypt_enabled.store(is_encrypted, Ordering::Release);
        let pin = self.params.pin.value().clamp(0, 999_999) as u32;
        self.active_pin.store(pin, Ordering::Release);

        match PublisherRuntime::start(
            channels,
            buffer_config.sample_rate,
            buffer_config.max_buffer_size as usize,
            self.publish_enabled.clone(),
            slot,
            self.encrypt_enabled.clone(),
            self.active_pin.clone(),
        ) {
            Ok(runtime) => {
                self.runtime = Some(runtime);
                true
            }
            Err(error) => {
                eprintln!("OpenAudio VST3: activation failed: {error}");
                false
            }
        }
    }

    fn process(
        &mut self,
        buffer: &mut Buffer,
        _aux: &mut AuxiliaryBuffers,
        _context: &mut impl ProcessContext<Self>,
    ) -> ProcessStatus {
        let Some(runtime) = &self.runtime else {
            return ProcessStatus::Normal;
        };

        let is_publishing = self.params.publish.value();
        self.publish_enabled.store(is_publishing, Ordering::Release);
        if !is_publishing {
            discard_queued_audio(&runtime.free_blocks, &runtime.ready_blocks);
            return ProcessStatus::Normal;
        }
        if buffer.is_empty() {
            return ProcessStatus::Normal;
        }
        if buffer.channels() != runtime.channels || buffer.samples() > runtime.maximum_block_frames
        {
            runtime
                .dropped_frames
                .fetch_add(buffer.samples() as u64, Ordering::Relaxed);
            return ProcessStatus::Normal;
        }

        let Some(mut block) = acquire_audio_block(
            &runtime.free_blocks,
            &runtime.ready_blocks,
            runtime.channels,
            &runtime.dropped_frames,
        ) else {
            runtime
                .dropped_frames
                .fetch_add(buffer.samples() as u64, Ordering::Relaxed);
            return ProcessStatus::Normal;
        };
        for frame in buffer.iter_samples() {
            for sample in frame {
                block.samples.push(*sample);
            }
        }
        block.presentation_timestamp_ns = runtime.started_at.elapsed().as_nanos() as u64;

        if let Err(block) = runtime.ready_blocks.push(block) {
            runtime
                .dropped_frames
                .fetch_add(buffer.samples() as u64, Ordering::Relaxed);
            let _ = runtime.free_blocks.push(block);
        }

        ProcessStatus::Normal
    }

    fn deactivate(&mut self) {
        self.publish_enabled.store(false, Ordering::Release);
        if let Some(mut runtime) = self.runtime.take() {
            runtime.stop();
        }
    }
}

impl Vst3Plugin for OpenAudioDawSender {
    const VST3_CLASS_ID: [u8; 16] = *b"OpenAudioSend001";
    const VST3_SUBCATEGORIES: &'static [Vst3SubCategory] =
        &[Vst3SubCategory::Fx, Vst3SubCategory::Tools];
}

struct ReceivedAudioBlock {
    samples: Vec<f32>,
    channels: usize,
    #[allow(dead_code)]
    sample_rate: u32,
    #[allow(dead_code)]
    sequence: u32,
}

struct ReceiverRuntime {
    channels: usize,
    sample_rate: u32,
    maximum_block_frames: usize,
    slot: u32,
    port: u16,
    decrypt_enabled: bool,
    pin: u32,
    free_blocks: Arc<ArrayQueue<ReceivedAudioBlock>>,
    ready_blocks: Arc<ArrayQueue<ReceivedAudioBlock>>,
    running: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    packets_received: Arc<AtomicU64>,
}

impl ReceiverRuntime {
    fn start(
        channels: usize,
        sample_rate: f32,
        maximum_block_frames: usize,
        slot: u32,
        port: u16,
        decrypt_enabled: bool,
        pin: u32,
    ) -> Result<Self, String> {
        if channels == 0 || channels > 64 {
            return Err(format!(
                "the VST3 receiver supports 1 to 64 channels, not {channels} channels"
            ));
        }
        let rounded_sample_rate = sample_rate.round();
        let sample_rate = rounded_sample_rate as u32;

        let socket = UdpSocket::bind(("0.0.0.0", port))
            .map_err(|e| format!("failed to bind receiver UDP port {port}: {e}"))?;
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|e| format!("failed to set socket read timeout: {e}"))?;

        let running = Arc::new(AtomicBool::new(true));
        let worker_running = running.clone();
        let packets_received = Arc::new(AtomicU64::new(0));
        let worker_packets = packets_received.clone();

        let queue_cap = AUDIO_BLOCK_COUNT * 4;
        let ready_blocks = Arc::new(ArrayQueue::<ReceivedAudioBlock>::new(queue_cap));
        let free_blocks = Arc::new(ArrayQueue::<ReceivedAudioBlock>::new(queue_cap));
        let block_sample_cap = channels.max(64) * 2048;

        for _ in 0..queue_cap {
            let _ = free_blocks.push(ReceivedAudioBlock {
                samples: Vec::with_capacity(block_sample_cap),
                channels,
                sample_rate,
                sequence: 0,
            });
        }

        let worker_free = free_blocks.clone();
        let worker_ready = ready_blocks.clone();
        let worker_slot = slot;

        let maybe_cipher = if decrypt_enabled {
            Some(audio_core::crypto::StreamCipher::from_passphrase(&pin.to_string()))
        } else {
            None
        };

        let worker = thread::Builder::new()
            .name(format!("openaudio-vst3-rx-{port}"))
            .spawn(move || {
                let mut buf = vec![0u8; 65536];
                let mut last_sub_attempt = Instant::now() - Duration::from_secs(5);
                let target_stream_name = format!("DAW Slot {worker_slot}");

                let discovery_dir = Arc::new(Mutex::new(HashMap::new()));
                let discovery_running = worker_running.clone();
                let discovery_dir_clone = discovery_dir.clone();

                let _ = thread::Builder::new()
                    .name(format!("openaudio-rx-disc-{port}"))
                    .spawn(move || {
                        let _ = network_core::discovery::start_discovery_listener(
                            discovery_dir_clone,
                            discovery_running,
                        );
                    });

                while worker_running.load(Ordering::Relaxed) {
                    if last_sub_attempt.elapsed() >= Duration::from_secs(1) {
                        last_sub_attempt = Instant::now();
                        let dir = discovery_dir.lock().unwrap_or_else(|p| p.into_inner()).clone();
                        for (_id, node) in dir {
                            if node.stream_name == target_stream_name
                                || node.stream_id % 32 == worker_slot % 32
                            {
                                let _ = network_core::discovery::send_subscribe_request(
                                    &node.ip,
                                    node.control_port,
                                    node.stream_id,
                                    port,
                                );
                            }
                        }
                    }

                    let (len, _src) = match socket.recv_from(&mut buf) {
                        Ok(res) => res,
                        Err(ref e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            continue;
                        }
                        Err(_) => continue,
                    };

                    let raw_packet = &buf[..len];
                    if raw_packet.len() < 24 {
                        continue;
                    }

                    let decrypted_storage: Vec<u8>;
                    let packet: &[u8] = if raw_packet[5] == network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED {
                        if let Some(ref cipher) = maybe_cipher {
                            match audio_core::crypto::decrypt_audio_packet(cipher, raw_packet) {
                                Ok(dec) => {
                                    decrypted_storage = dec;
                                    &decrypted_storage[..]
                                }
                                Err(_) => continue,
                            }
                        } else {
                            continue;
                        }
                    } else {
                        raw_packet
                    };

                    let Some(parsed) = network_core::protocol::parse_packet(packet) else {
                        continue;
                    };

                    if parsed.channel_count == 0 || parsed.samples_per_channel == 0 {
                        continue;
                    }

                    let Some(decoded_samples) = network_core::protocol::decode_float32(packet, &parsed) else {
                        continue;
                    };
                    if decoded_samples.is_empty() {
                        continue;
                    }

                    let mut block = match worker_free.pop() {
                        Some(b) => b,
                        None => match worker_ready.pop() {
                            Some(b) => b,
                            None => continue,
                        },
                    };

                    block.samples.clear();
                    block.samples.extend(decoded_samples);
                    block.channels = parsed.channel_count as usize;
                    block.sample_rate = parsed.sample_rate;
                    block.sequence = parsed.sequence_number;

                    worker_packets.fetch_add(1, Ordering::Relaxed);
                    let _ = worker_ready.push(block);
                }
            })
            .map_err(|e| format!("failed to spawn receiver worker: {e}"))?;

        Ok(Self {
            channels,
            sample_rate,
            maximum_block_frames,
            slot,
            port,
            decrypt_enabled,
            pin,
            free_blocks,
            ready_blocks,
            running,
            worker: Some(worker),
            packets_received,
        })
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for ReceiverRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct OpenAudioDawReceiver {
    params: Arc<OpenAudioDawReceiverParams>,
    runtime: Option<ReceiverRuntime>,
    receive_enabled: Arc<AtomicBool>,
    active_slot: Arc<AtomicU32>,
    active_port: Arc<AtomicU32>,
    decrypt_enabled: Arc<AtomicBool>,
    active_pin: Arc<AtomicU32>,
    current_block: Option<ReceivedAudioBlock>,
    block_offset: usize,
    current_channels: usize,
}

#[derive(Params)]
pub struct OpenAudioDawReceiverParams {
    #[id = "receive"]
    pub receive: BoolParam,

    #[id = "slot"]
    pub slot: IntParam,

    #[id = "port"]
    pub port: IntParam,

    #[id = "decrypt"]
    pub decrypt: BoolParam,

    #[id = "pin"]
    pub pin: IntParam,
}

impl Default for OpenAudioDawReceiver {
    fn default() -> Self {
        let receive_enabled = Arc::new(AtomicBool::new(false));
        let cb_receive = receive_enabled.clone();
        let default_slot = 1;
        let active_slot = Arc::new(AtomicU32::new(default_slot));
        let cb_slot = active_slot.clone();
        let default_port = 7100 + default_slot;
        let active_port = Arc::new(AtomicU32::new(default_port));
        let cb_port = active_port.clone();
        let decrypt_enabled = Arc::new(AtomicBool::new(false));
        let cb_decrypt = decrypt_enabled.clone();
        let active_pin = Arc::new(AtomicU32::new(1234));
        let cb_pin = active_pin.clone();

        Self {
            params: Arc::new(OpenAudioDawReceiverParams {
                receive: BoolParam::new("Receive from OpenAudio", false)
                    .with_callback(Arc::new(move |val| {
                        cb_receive.store(val, Ordering::Release);
                    })),
                slot: IntParam::new(
                    "Network Track Slot",
                    default_slot as i32,
                    IntRange::Linear { min: 1, max: 32 },
                )
                .with_callback(Arc::new(move |val| {
                    let s = val.clamp(1, 32) as u32;
                    cb_slot.store(s, Ordering::Release);
                })),
                port: IntParam::new(
                    "UDP Listen Port",
                    default_port as i32,
                    IntRange::Linear { min: 1024, max: 65535 },
                )
                .with_callback(Arc::new(move |val| {
                    let p = val.clamp(1024, 65535) as u32;
                    cb_port.store(p, Ordering::Release);
                })),
                decrypt: BoolParam::new("AES-256-GCM Decryption", false)
                    .with_callback(Arc::new(move |val| {
                        cb_decrypt.store(val, Ordering::Release);
                    })),
                pin: IntParam::new(
                    "Decryption Key PIN",
                    1234,
                    IntRange::Linear { min: 0, max: 999_999 },
                )
                .with_callback(Arc::new(move |val| {
                    cb_pin.store(val.clamp(0, 999_999) as u32, Ordering::Release);
                })),
            }),
            runtime: None,
            receive_enabled,
            active_slot,
            active_port,
            decrypt_enabled,
            active_pin,
            current_block: None,
            block_offset: 0,
            current_channels: 0,
        }
    }
}

impl Plugin for OpenAudioDawReceiver {
    const NAME: &'static str = "OpenAudio DAW Receiver";
    const VENDOR: &'static str = "OpenAudio";
    const URL: &'static str = "";
    const EMAIL: &'static str = "";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const AUDIO_IO_LAYOUTS: &'static [AudioIOLayout] = &[
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(2),
            main_output_channels: NonZeroU32::new(2),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(1),
            main_output_channels: NonZeroU32::new(1),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(4),
            main_output_channels: NonZeroU32::new(4),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(6),
            main_output_channels: NonZeroU32::new(6),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(8),
            main_output_channels: NonZeroU32::new(8),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(16),
            main_output_channels: NonZeroU32::new(16),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(32),
            main_output_channels: NonZeroU32::new(32),
            ..AudioIOLayout::const_default()
        },
        AudioIOLayout {
            main_input_channels: NonZeroU32::new(64),
            main_output_channels: NonZeroU32::new(64),
            ..AudioIOLayout::const_default()
        },
    ];

    type SysExMessage = ();
    type BackgroundTask = ();
    type Editor = ();

    fn params(&self) -> Arc<dyn Params> {
        self.params.clone()
    }

    fn activate(
        &mut self,
        audio_io_layout: &AudioIOLayout,
        buffer_config: &BufferConfig,
        _context: &mut impl ActivateContext<Self>,
    ) -> bool {
        let channels = audio_io_layout
            .main_output_channels
            .or(audio_io_layout.main_input_channels)
            .map(NonZeroU32::get)
            .unwrap_or(2) as usize;
        self.receive_enabled
            .store(self.params.receive.value(), Ordering::Release);
        let slot = self.params.slot.value().clamp(1, 32) as u32;
        self.active_slot.store(slot, Ordering::Release);
        let port = self.params.port.value().clamp(1024, 65535) as u16;
        self.active_port.store(port as u32, Ordering::Release);
        let is_decrypt = self.params.decrypt.value();
        self.decrypt_enabled.store(is_decrypt, Ordering::Release);
        let pin = self.params.pin.value().clamp(0, 999_999) as u32;
        self.active_pin.store(pin, Ordering::Release);

        match ReceiverRuntime::start(
            channels,
            buffer_config.sample_rate,
            buffer_config.max_buffer_size as usize,
            slot,
            port,
            is_decrypt,
            pin,
        ) {
            Ok(runtime) => {
                self.runtime = Some(runtime);
                true
            }
            Err(e) => {
                eprintln!("OpenAudio VST3 Receiver: activation failed: {e}");
                false
            }
        }
    }

    fn process(
        &mut self,
        buffer: &mut Buffer,
        _aux: &mut AuxiliaryBuffers,
        _context: &mut impl ProcessContext<Self>,
    ) -> ProcessStatus {
        let Some(runtime) = &self.runtime else {
            return ProcessStatus::Normal;
        };

        let cur_slot = self.active_slot.load(Ordering::Acquire);
        let cur_port = self.active_port.load(Ordering::Acquire) as u16;
        let cur_decrypt = self.decrypt_enabled.load(Ordering::Acquire);
        let cur_pin = self.active_pin.load(Ordering::Acquire);

        if runtime.slot != cur_slot
            || runtime.port != cur_port
            || runtime.decrypt_enabled != cur_decrypt
            || runtime.pin != cur_pin
        {
            let channels = runtime.channels;
            let sample_rate = runtime.sample_rate as f32;
            let max_frames = runtime.maximum_block_frames;
            if let Some(mut old) = self.runtime.take() {
                old.stop();
            }
            if let Ok(new_rt) = ReceiverRuntime::start(
                channels,
                sample_rate,
                max_frames,
                cur_slot,
                cur_port,
                cur_decrypt,
                cur_pin,
            ) {
                self.runtime = Some(new_rt);
            }
            return ProcessStatus::Normal;
        }

        let is_receiving = self.params.receive.value();
        self.receive_enabled.store(is_receiving, Ordering::Release);
        if !is_receiving {
            return ProcessStatus::Normal;
        }

        if buffer.is_empty() {
            return ProcessStatus::Normal;
        }

        for frame in buffer.iter_samples() {
            while self.current_block.is_none()
                || (self.current_channels > 0
                    && self.block_offset + self.current_channels
                        > self.current_block.as_ref().unwrap().samples.len())
            {
                if let Some(old_block) = self.current_block.take() {
                    let _ = runtime.free_blocks.push(old_block);
                }
                if let Some(next_block) = runtime.ready_blocks.pop() {
                    self.current_channels = next_block.channels;
                    self.current_block = Some(next_block);
                    self.block_offset = 0;
                } else {
                    break;
                }
            }

            let mut ch = 0;
            for sample in frame {
                let out = if let Some(ref block) = self.current_block {
                    if ch < self.current_channels && self.block_offset + ch < block.samples.len() {
                        block.samples[self.block_offset + ch]
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                *sample = out;
                ch += 1;
            }

            if self.current_block.is_some() && self.current_channels > 0 {
                self.block_offset += self.current_channels;
            }
        }

        ProcessStatus::Normal
    }

    fn deactivate(&mut self) {
        self.receive_enabled.store(false, Ordering::Release);
        if let Some(mut runtime) = self.runtime.take() {
            runtime.stop();
        }
        if let Some(block) = self.current_block.take() {
            drop(block);
        }
    }
}

impl Vst3Plugin for OpenAudioDawReceiver {
    const VST3_CLASS_ID: [u8; 16] = *b"OpenAudioRecv001";
    const VST3_SUBCATEGORIES: &'static [Vst3SubCategory] =
        &[Vst3SubCategory::Fx, Vst3SubCategory::Tools];
}

nice_export_vst3!(OpenAudioDawSender, OpenAudioDawReceiver);

#[cfg(test)]
mod tests {
    use super::{acquire_audio_block, discard_queued_audio, AudioBlock};
    use crossbeam::queue::ArrayQueue;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn callback_reuses_oldest_queued_block_to_keep_stream_latency_bounded() {
        let free_blocks = ArrayQueue::new(1);
        let ready_blocks = ArrayQueue::new(1);
        assert!(ready_blocks
            .push(AudioBlock {
                samples: vec![0.25, -0.25, 0.5, -0.5],
                presentation_timestamp_ns: 10,
            })
            .is_ok());
        let dropped_frames = AtomicU64::new(0);

        let block = acquire_audio_block(&free_blocks, &ready_blocks, 2, &dropped_frames).unwrap();

        assert!(block.samples.is_empty());
        assert_eq!(block.samples.capacity(), 4);
        assert_eq!(dropped_frames.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert!(ready_blocks.is_empty());
    }

    #[test]
    fn disabling_publishing_discards_waiting_audio_blocks() {
        let free_blocks = ArrayQueue::new(2);
        let ready_blocks = ArrayQueue::new(2);
        assert!(ready_blocks
            .push(AudioBlock {
                samples: vec![0.25, -0.25],
                presentation_timestamp_ns: 10,
            })
            .is_ok());

        discard_queued_audio(&free_blocks, &ready_blocks);

        assert!(ready_blocks.is_empty());
        let recycled = free_blocks.pop().unwrap();
        assert!(recycled.samples.is_empty());
        assert_eq!(recycled.samples.capacity(), 2);
    }

    #[test]
    fn multichannel_block_accounting_supports_surround_and_bus() {
        let free_blocks = ArrayQueue::new(1);
        let ready_blocks = ArrayQueue::new(1);

        // Simulate 6 channels (5.1 surround) with 24 samples (4 frames)
        let samples_5_1 = vec![0.1f32; 24];
        assert!(ready_blocks
            .push(AudioBlock {
                samples: samples_5_1,
                presentation_timestamp_ns: 20,
            })
            .is_ok());
        let dropped_frames = AtomicU64::new(0);

        let block = acquire_audio_block(&free_blocks, &ready_blocks, 6, &dropped_frames).unwrap();
        assert!(block.samples.is_empty());
        assert_eq!(block.samples.capacity(), 24);
        // 24 samples / 6 channels = 4 frames dropped
        assert_eq!(dropped_frames.load(std::sync::atomic::Ordering::Relaxed), 4);

        // Simulate 16 channels with 64 samples (4 frames)
        let samples_16ch = vec![0.2f32; 64];
        assert!(ready_blocks
            .push(AudioBlock {
                samples: samples_16ch,
                presentation_timestamp_ns: 30,
            })
            .is_ok());
        let block16 = acquire_audio_block(&free_blocks, &ready_blocks, 16, &dropped_frames).unwrap();
        assert!(block16.samples.is_empty());
        assert_eq!(block16.samples.capacity(), 64);
        // previous 4 frames + (64 / 16 = 4 frames) = 8 frames dropped total
        assert_eq!(dropped_frames.load(std::sync::atomic::Ordering::Relaxed), 8);
    }

    #[test]
    fn multichannel_packet_frame_budgets_stay_within_mtu() {
        use network_core::protocol::max_float32_frames_per_packet;

        for &channels in &[1, 2, 4, 6, 8, 16, 32, 64] {
            let max_frames = max_float32_frames_per_packet(channels).expect("valid channel budget");
            let payload_bytes = max_frames * channels * 4;
            let total_packet_bytes = 32 + payload_bytes; // 32 byte OAv1 header
            assert!(
                total_packet_bytes <= 1200,
                "channel count {channels} produced {total_packet_bytes} bytes which exceeds 1200 MTU limit"
            );
        }
    }

    #[test]
    fn track_slot_parameter_initialization_and_range() {
        use super::OpenAudioDawSender;

        let plugin = OpenAudioDawSender::default();
        let slot_val = plugin.params.slot.value();
        assert!(slot_val >= 1 && slot_val <= 32);
        assert_eq!(plugin.active_slot.load(std::sync::atomic::Ordering::Relaxed), slot_val as u32);
    }

    #[test]
    fn receiver_parameter_initialization_and_range() {
        use super::OpenAudioDawReceiver;

        let receiver = OpenAudioDawReceiver::default();
        assert!(!receiver.params.receive.value());
        let slot_val = receiver.params.slot.value();
        assert!(slot_val >= 1 && slot_val <= 32);
        let port_val = receiver.params.port.value();
        assert!(port_val >= 1024 && port_val <= 65535);
        assert_eq!(receiver.active_slot.load(std::sync::atomic::Ordering::Relaxed), slot_val as u32);
        assert_eq!(receiver.active_port.load(std::sync::atomic::Ordering::Relaxed), port_val as u32);
    }

    #[test]
    fn encryption_parameters_initialization_and_range() {
        use super::{OpenAudioDawReceiver, OpenAudioDawSender};

        let sender = OpenAudioDawSender::default();
        assert!(!sender.params.encrypt.value());
        assert_eq!(sender.params.pin.value(), 1234);
        assert_eq!(sender.encrypt_enabled.load(std::sync::atomic::Ordering::Relaxed), false);
        assert_eq!(sender.active_pin.load(std::sync::atomic::Ordering::Relaxed), 1234);

        let receiver = OpenAudioDawReceiver::default();
        assert!(!receiver.params.decrypt.value());
        assert_eq!(receiver.params.pin.value(), 1234);
        assert_eq!(receiver.decrypt_enabled.load(std::sync::atomic::Ordering::Relaxed), false);
        assert_eq!(receiver.active_pin.load(std::sync::atomic::Ordering::Relaxed), 1234);
    }

    #[test]
    fn encryption_audio_packet_round_trip() {
        use network_core::protocol::{encode_float32_packet, PacketHeader};

        let header = PacketHeader {
            sub_stream_index: 0,
            stream_id: 101,
            sequence_number: 1,
            presentation_timestamp_ns: 1_000_000,
        };
        let samples = vec![0.5f32, -0.25f32, 0.125f32, -0.0625f32];
        let raw = encode_float32_packet(&header, 2, 48000, &samples).unwrap();

        let pin = 987654u32;
        let sender_cipher = audio_core::crypto::StreamCipher::from_passphrase(&pin.to_string());
        let encrypted = audio_core::crypto::encrypt_audio_packet(&sender_cipher, &raw).unwrap();
        assert_eq!(encrypted[5], network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED);

        let receiver_cipher = audio_core::crypto::StreamCipher::from_passphrase(&pin.to_string());
        let decrypted = audio_core::crypto::decrypt_audio_packet(&receiver_cipher, &encrypted).unwrap();
        assert_eq!(decrypted[5], network_core::protocol::PACKET_TYPE_AUDIO);
        assert_eq!(decrypted, raw);
    }
}
