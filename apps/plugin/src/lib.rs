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
    ) -> Result<Self, String> {
        if !(channels == 1 || channels == 2) {
            return Err(format!(
                "the VST3 sender supports mono or stereo tracks, not {channels} channels"
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

        let stream_sequence = STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stream_id = std::process::id()
            .wrapping_mul(2_654_435_761)
            .wrapping_add(stream_sequence)
            .max(1);
        let stream_name = format!("DAW Track {stream_sequence}");

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
        let publisher_ready_blocks = ready_blocks.clone();
        let publisher_free_blocks = free_blocks.clone();
        let publisher_dropped_frames = dropped_frames.clone();
        let publisher_send_errors = send_errors.clone();
        let publisher_packets_sent = packets_sent.clone();
        let publisher_subscribers = control_service.subscribers.clone();

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
                while publisher_running.load(Ordering::Acquire)
                    || !publisher_ready_blocks.is_empty()
                {
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
                                    Ok(packet) => {
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
        let advertiser = match thread::Builder::new()
            .name(format!("openaudio-vst3-discovery-{stream_id}"))
            .spawn(move || {
                if let Err(error) = audio_core::start_advertising(
                    "OpenAudio VST3".to_string(),
                    stream_id,
                    stream_name,
                    channels as u8,
                    control_port,
                    advertiser_running,
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


struct OpenAudioDawSender {
    params: Arc<OpenAudioDawSenderParams>,
    runtime: Option<PublisherRuntime>,
}

#[derive(Params)]
struct OpenAudioDawSenderParams {
    #[id = "publish"]
    publish: BoolParam,
}

impl Default for OpenAudioDawSender {
    fn default() -> Self {
        Self {
            params: Arc::new(OpenAudioDawSenderParams {
                publish: BoolParam::new("Publish to OpenAudio", false),
            }),
            runtime: None,
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
        match PublisherRuntime::start(
            channels,
            buffer_config.sample_rate,
            buffer_config.max_buffer_size as usize,
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
        if !self.params.publish.value() {
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

nice_export_vst3!(OpenAudioDawSender);

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
}
