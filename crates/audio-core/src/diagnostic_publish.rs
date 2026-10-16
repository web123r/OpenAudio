//! Synthetic multichannel OpenAudio publisher for diagnostics.
//!
//! This module generates deterministic sine waves without opening an
//! audio input device. It exercises the real discovery, subscription,
//! packetization, UDP transport, and signal-monitor paths.
//!
//! Typical uses:
//!
//! - Reproduce 32-channel network load without a console.
//! - Compare 2, 8, 16, 24, 32, and 64-channel performance.
//! - Test localhost, wired LAN, and Wi-Fi independently.
//! - Verify that packets remain below the fragmentation limit.
//! - Distinguish publisher/network problems from capture problems.

use crate::discovery::{start_advertising, SubscriberRegistry};
use crate::ensure_realtime_audio_thread;
use crate::protocol::{
    sample_rate_to_code, AudioPayloadHeader, PacketHeader, SAMPLE_FORMAT_FLOAT32,
};
use crate::{register_scoped_signal_meter, SignalDirection};

use socket2::SockRef;
use std::f64::consts::TAU;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const SAFE_UDP_PAYLOAD_BYTES: usize = network_core::protocol::MAX_SAFE_UDP_PAYLOAD_BYTES;
const OPENAUDIO_HEADER_BYTES: usize = network_core::protocol::OPENAUDIO_HEADER_BYTES;
const BYTES_PER_SAMPLE: usize = std::mem::size_of::<f32>();

const UDP_SEND_BUFFER_BYTES: usize = 4 * 1024 * 1024;

const IDLE_SLEEP: Duration = Duration::from_micros(250);
const HIGH_RESOLUTION_WAIT_THRESHOLD: Duration = Duration::from_millis(2);
const HIGH_RESOLUTION_SPIN_WINDOW: Duration = Duration::from_micros(500);

/// Prevents a large transmission burst if the process is suspended,
/// paused in a debugger, or delayed by heavy system load.
const MAX_CATCH_UP_MILLISECONDS: u32 = 20;

/// Conservative level so routing several diagnostic channels into one
/// output does not immediately clip.
const TEST_SIGNAL_AMPLITUDE: f64 = 0.20;

/// Configuration for a synthetic OpenAudio publisher.
#[derive(Debug, Clone)]
pub struct DiagnosticPublishConfig {
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub channel_count: usize,
    pub sample_rate: u32,
}

impl DiagnosticPublishConfig {
    pub fn new(
        node_name: impl Into<String>,
        stream_name: impl Into<String>,
        stream_id: u32,
        channel_count: usize,
        sample_rate: u32,
    ) -> Result<Self, String> {
        let config = Self {
            node_name: node_name.into(),
            stream_name: stream_name.into(),
            stream_id,
            channel_count,
            sample_rate,
        };

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.node_name.trim().is_empty() {
            return Err("diagnostic publisher node name must not \
                 be empty"
                .to_string());
        }

        if self.stream_name.trim().is_empty() {
            return Err("diagnostic publisher stream name must not \
                 be empty"
                .to_string());
        }

        if self.channel_count == 0 {
            return Err("diagnostic publisher must have at least \
                 one channel"
                .to_string());
        }

        if self.channel_count > u8::MAX as usize {
            return Err(format!(
                "diagnostic publisher channel count {} \
                 exceeds the OpenAudio protocol limit of {}",
                self.channel_count,
                u8::MAX
            ));
        }

        if sample_rate_to_code(self.sample_rate) == 0 {
            return Err(format!(
                "diagnostic sample rate {}Hz is not \
                 supported. Use 44100Hz or 48000Hz.",
                self.sample_rate
            ));
        }

        calculate_max_frames_per_packet(self.channel_count)?;

        Ok(())
    }
}

/// Final counters returned when the diagnostic publisher stops.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticPublishReport {
    pub elapsed: Duration,
    pub subscriber_elapsed: Duration,
    pub generated_frames: u64,
    pub packets_built: u64,
    pub datagrams_sent: u64,
    pub send_errors: u64,
    pub skipped_catch_up_frames: u64,
    pub maximum_packet_bytes: usize,
    pub maximum_frames_per_packet: usize,
}

impl DiagnosticPublishReport {
    pub fn generated_audio_seconds(&self, sample_rate: u32) -> f64 {
        if sample_rate == 0 {
            return 0.0;
        }

        self.generated_frames as f64 / sample_rate as f64
    }

    pub fn average_packets_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();

        if seconds <= 0.0 {
            return 0.0;
        }

        self.packets_built as f64 / seconds
    }

    pub fn average_datagrams_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();

        if seconds <= 0.0 {
            return 0.0;
        }

        self.datagrams_sent as f64 / seconds
    }

    pub fn subscriber_packets_per_second(&self) -> f64 {
        let seconds = self.subscriber_elapsed.as_secs_f64();

        if seconds <= 0.0 {
            return 0.0;
        }

        self.packets_built as f64 / seconds
    }

    pub fn subscriber_datagrams_per_second(&self) -> f64 {
        let seconds = self.subscriber_elapsed.as_secs_f64();

        if seconds <= 0.0 {
            return 0.0;
        }

        self.datagrams_sent as f64 / seconds
    }
}

/// Runs a synthetic multichannel publisher until `keep_running`
/// becomes false.
///
/// Each channel receives a different sine-wave frequency. The function
/// advertises itself like a real publisher, waits for normal OpenAudio
/// subscription requests, and sends MTU-safe Float32 packets.
///
/// The diagnostic signal meter measures exactly the synthetic samples
/// passed to packet construction. Since generation pauses when no
/// subscriber exists, the meter remains registered but displays silence
/// until a subscriber connects.
pub fn publish_diagnostic_stream_with_discovery(
    config: DiagnosticPublishConfig,
    subscribers_by_stream: SubscriberRegistry,
    keep_running: Arc<AtomicBool>,
) -> Result<DiagnosticPublishReport, String> {
    config.validate()?;
    ensure_realtime_audio_thread();

    let meter_label = format!(
        "{} — {} — Diagnostic {}ch",
        config.node_name, config.stream_name, config.channel_count,
    );

    let (signal_meter, _signal_meter_guard) = register_scoped_signal_meter(
        format!("diagnostic-publish:{}", config.stream_id),
        meter_label,
        SignalDirection::Diagnostic,
        config.channel_count,
    );

    let sample_rate_code = sample_rate_to_code(config.sample_rate);

    let maximum_frames_per_packet = calculate_max_frames_per_packet(config.channel_count)?;

    let maximum_packet_bytes = OPENAUDIO_HEADER_BYTES
        + maximum_frames_per_packet * config.channel_count * BYTES_PER_SAMPLE;

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|error| {
        format!(
            "failed to bind diagnostic publisher \
                     UDP socket: {error}"
        )
    })?;

    configure_send_socket(&socket);

    let advertising_flag = keep_running.clone();

    let advertising_node_name = config.node_name.clone();

    let advertising_stream_name = config.stream_name.clone();

    let advertising_stream_id = config.stream_id;

    let advertised_channels = config.channel_count as u8;

    std::thread::spawn(move || {
        if let Err(error) = start_advertising(
            advertising_node_name,
            advertising_stream_id,
            advertising_stream_name,
            advertised_channels,
            crate::discovery::CONTROL_PORT,
            advertising_flag,
        ) {
            eprintln!(
                "audio-core: diagnostic stream \
                 advertising stopped: {error}"
            );
        }
    });

    let start = Instant::now();
    let mut last_report = Instant::now();

    let mut report = DiagnosticPublishReport {
        maximum_packet_bytes,
        maximum_frames_per_packet,
        ..DiagnosticPublishReport::default()
    };

    let mut sequence_number = 0u32;
    let mut generated_timeline_frames = 0u64;
    let mut subscriber_started_at: Option<Instant> = None;

    let mut oscillators = create_channel_oscillators(config.channel_count, config.sample_rate);

    let sample_capacity = maximum_frames_per_packet.saturating_mul(config.channel_count);

    let mut interleaved_samples = Vec::<f32>::with_capacity(sample_capacity);

    let mut packet = Vec::<u8>::with_capacity(maximum_packet_bytes);

    println!(
        "Diagnostic Publish: '{}' / '{}' (stream {})",
        config.node_name, config.stream_name, config.stream_id
    );

    println!(
        "Diagnostic Publish: {}ch @ {}Hz Float32, raw \
         PCM {:.2} Mbps",
        config.channel_count,
        config.sample_rate,
        estimated_pcm_megabits_per_second(config.channel_count, config.sample_rate,)
    );

    println!(
        "Diagnostic Publish: up to {} frame(s) per \
         packet, maximum UDP payload {} bytes",
        maximum_frames_per_packet, maximum_packet_bytes
    );

    println!("Diagnostic Publish: waiting for a subscriber...");

    let mut had_subscriber = false;

    while keep_running.load(Ordering::Relaxed) {
        let destinations =
            crate::discovery::active_subscribers(&subscribers_by_stream, config.stream_id)
                .filter(|addresses| !addresses.is_empty());

        let elapsed_target_frames = elapsed_frames(start.elapsed(), config.sample_rate);

        if destinations.is_none() {
            if let Some(subscriber_start) = subscriber_started_at.take() {
                report.subscriber_elapsed += subscriber_start.elapsed();
            }

            // Keep the logical generator clock aligned with real time
            // while no receiver is subscribed. This avoids sending a
            // large catch-up burst when a subscriber appears.
            generated_timeline_frames = elapsed_target_frames;

            if had_subscriber {
                println!(
                    "Diagnostic Publish: no active \
                     subscriber; pausing packet \
                     transmission."
                );

                had_subscriber = false;
            }

            if last_report.elapsed() >= Duration::from_secs(5) {
                print_periodic_report(
                    &config,
                    &report,
                    start.elapsed(),
                    current_subscriber_elapsed(&report, subscriber_started_at),
                );

                last_report = Instant::now();
            }

            std::thread::sleep(Duration::from_millis(2));

            continue;
        }

        if !had_subscriber {
            println!(
                "Diagnostic Publish: subscriber detected; \
                 transmission started."
            );

            had_subscriber = true;
            subscriber_started_at = Some(Instant::now());
        }

        let destinations = destinations.unwrap_or_default();

        if elapsed_target_frames <= generated_timeline_frames {
            wait_for_next_audio_frame(start, generated_timeline_frames, config.sample_rate);
            continue;
        }

        let maximum_catch_up_frames =
            ((config.sample_rate as u64 * MAX_CATCH_UP_MILLISECONDS as u64) / 1_000)
                .max(maximum_frames_per_packet as u64);

        let frames_due = elapsed_target_frames - generated_timeline_frames;

        if frames_due > maximum_catch_up_frames {
            let skipped = frames_due - maximum_catch_up_frames;

            generated_timeline_frames = generated_timeline_frames.saturating_add(skipped);

            report.skipped_catch_up_frames = report.skipped_catch_up_frames.saturating_add(skipped);

            eprintln!(
                "audio-core: diagnostic publisher was \
                 delayed; skipped {skipped} stale \
                 frame(s) instead of sending a large \
                 catch-up burst"
            );
        }

        let frames_due = elapsed_target_frames - generated_timeline_frames;

        let frames_to_generate = (frames_due as usize).min(maximum_frames_per_packet);

        if frames_to_generate == 0 {
            std::thread::sleep(IDLE_SLEEP);
            continue;
        }

        interleaved_samples.clear();

        generate_interleaved_samples(
            &mut interleaved_samples,
            frames_to_generate,
            &mut oscillators,
        );

        // Meter the exact generated interleaved audio that will be
        // encoded into the OpenAudio packet.
        signal_meter.observe_interleaved(&interleaved_samples, config.channel_count);

        let presentation_timestamp_ns =
            frames_to_nanoseconds(generated_timeline_frames, config.sample_rate);

        build_audio_packet(
            &mut packet,
            config.stream_id,
            sequence_number,
            presentation_timestamp_ns,
            config.channel_count,
            sample_rate_code,
            frames_to_generate,
            &interleaved_samples,
        )?;

        if packet.len() > SAFE_UDP_PAYLOAD_BYTES {
            return Err(format!(
                "diagnostic publisher generated a \
                 {}-byte UDP payload, exceeding the \
                 configured safe limit of {} bytes",
                packet.len(),
                SAFE_UDP_PAYLOAD_BYTES
            ));
        }

        report.packets_built = report.packets_built.saturating_add(1);

        for destination in &destinations {
            match socket.send_to(&packet, destination) {
                Ok(bytes_sent) if bytes_sent == packet.len() => {
                    report.datagrams_sent = report.datagrams_sent.saturating_add(1);
                }

                Ok(bytes_sent) => {
                    report.send_errors = report.send_errors.saturating_add(1);

                    eprintln!(
                        "audio-core: partial diagnostic \
                         UDP send to {destination}: sent \
                         {bytes_sent} of {} bytes",
                        packet.len()
                    );
                }

                Err(error) => {
                    report.send_errors = report.send_errors.saturating_add(1);

                    eprintln!(
                        "audio-core: diagnostic UDP send \
                         to {destination} failed: {error}"
                    );
                }
            }
        }

        report.generated_frames = report
            .generated_frames
            .saturating_add(frames_to_generate as u64);

        generated_timeline_frames =
            generated_timeline_frames.saturating_add(frames_to_generate as u64);

        sequence_number = sequence_number.wrapping_add(1);

        if last_report.elapsed() >= Duration::from_secs(5) {
            print_periodic_report(
                &config,
                &report,
                start.elapsed(),
                current_subscriber_elapsed(&report, subscriber_started_at),
            );

            last_report = Instant::now();
        }
    }

    report.elapsed = start.elapsed();
    if let Some(subscriber_start) = subscriber_started_at {
        report.subscriber_elapsed += subscriber_start.elapsed();
    }

    println!(
        "Diagnostic Publish stopped: {}ch @ {}Hz, \
         {:.2}s elapsed, {:.2}s subscriber-active, \
         {} generated frame(s), {} \
         packet(s), {} datagram(s), {} send error(s), \
         {} skipped catch-up frame(s); active rates \
         {:.0} packets/s and {:.0} datagrams/s.",
        config.channel_count,
        config.sample_rate,
        report.elapsed.as_secs_f64(),
        report.subscriber_elapsed.as_secs_f64(),
        report.generated_frames,
        report.packets_built,
        report.datagrams_sent,
        report.send_errors,
        report.skipped_catch_up_frames,
        report.subscriber_packets_per_second(),
        report.subscriber_datagrams_per_second()
    );

    Ok(report)
}

fn wait_for_next_audio_frame(start: Instant, current_frame: u64, sample_rate: u32) {
    if sample_rate == 0 {
        thread::sleep(IDLE_SLEEP);
        return;
    }

    let next_frame_ns = frames_to_nanoseconds(current_frame.saturating_add(1), sample_rate);
    loop {
        let elapsed_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let remaining_ns = next_frame_ns.saturating_sub(elapsed_ns);
        if remaining_ns == 0 {
            return;
        }

        let remaining = Duration::from_nanos(remaining_ns);
        if remaining >= HIGH_RESOLUTION_WAIT_THRESHOLD {
            thread::sleep(remaining.saturating_sub(HIGH_RESOLUTION_SPIN_WINDOW));
        } else {
            std::hint::spin_loop();
        }
    }
}

#[derive(Debug, Clone)]
struct SineOscillator {
    phase_radians: f64,
    phase_increment: f64,
}

fn create_channel_oscillators(channel_count: usize, sample_rate: u32) -> Vec<SineOscillator> {
    (0..channel_count)
        .map(|channel_index| {
            let frequency = diagnostic_frequency(channel_index);

            SineOscillator {
                phase_radians: 0.0,
                phase_increment: TAU * frequency / sample_rate as f64,
            }
        })
        .collect()
}

/// Assigns each channel a recognizable frequency while remaining
/// below Nyquist for both supported sample rates.
fn diagnostic_frequency(channel_index: usize) -> f64 {
    const FREQUENCIES: [f64; 16] = [
        110.0, 137.0, 173.0, 211.0, 257.0, 307.0, 367.0, 431.0, 503.0, 587.0, 683.0, 787.0, 907.0,
        1_031.0, 1_163.0, 1_301.0,
    ];

    let bank = channel_index / FREQUENCIES.len();

    let base = FREQUENCIES[channel_index % FREQUENCIES.len()];

    // Higher banks receive a small offset so channels 1 and 17 remain
    // distinguishable without creating extreme high-frequency tones.
    base + bank as f64 * 17.0
}

fn generate_interleaved_samples(
    destination: &mut Vec<f32>,
    frame_count: usize,
    oscillators: &mut [SineOscillator],
) {
    destination.reserve(frame_count.saturating_mul(oscillators.len()));

    for _ in 0..frame_count {
        for oscillator in oscillators.iter_mut() {
            let sample = oscillator.phase_radians.sin() * TEST_SIGNAL_AMPLITUDE;

            destination.push(sample as f32);

            oscillator.phase_radians += oscillator.phase_increment;

            if oscillator.phase_radians >= TAU {
                oscillator.phase_radians -= TAU;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_audio_packet(
    packet: &mut Vec<u8>,
    stream_id: u32,
    sequence_number: u32,
    presentation_timestamp_ns: u64,
    channel_count: usize,
    sample_rate_code: u16,
    frames_per_channel: usize,
    interleaved_samples: &[f32],
) -> Result<(), String> {
    if channel_count == 0 || channel_count > u8::MAX as usize {
        return Err(format!(
            "invalid diagnostic packet channel count: \
             {channel_count}"
        ));
    }

    if frames_per_channel == 0 || frames_per_channel > u16::MAX as usize {
        return Err(format!(
            "invalid diagnostic packet frame count: \
             {frames_per_channel}"
        ));
    }

    let expected_samples = frames_per_channel
        .checked_mul(channel_count)
        .ok_or_else(|| {
            "diagnostic packet sample-count \
                 overflow"
                .to_string()
        })?;

    if interleaved_samples.len() != expected_samples {
        return Err(format!(
            "diagnostic packet expected \
             {expected_samples} sample(s), received {}",
            interleaved_samples.len()
        ));
    }

    let packet_header = PacketHeader {
        sub_stream_index: 0,
        stream_id,
        sequence_number,
        presentation_timestamp_ns,
    };

    let payload_header = AudioPayloadHeader {
        channel_count: channel_count as u8,
        sample_format: SAMPLE_FORMAT_FLOAT32,
        sample_rate_code,
        samples_per_channel: frames_per_channel as u16,
    };

    packet.clear();

    packet.extend_from_slice(&packet_header.to_bytes());

    packet.extend_from_slice(&payload_header.to_bytes());

    for &sample in interleaved_samples {
        packet.extend_from_slice(&sample.to_le_bytes());
    }

    Ok(())
}

fn calculate_max_frames_per_packet(channel_count: usize) -> Result<usize, String> {
    network_core::protocol::max_float32_frames_per_packet(channel_count)
}

fn configure_send_socket(socket: &UdpSocket) {
    let socket_ref = SockRef::from(socket);

    match socket_ref.set_send_buffer_size(UDP_SEND_BUFFER_BYTES) {
        Ok(()) => {
            println!(
                "Diagnostic Publish: requested {} MiB \
                 UDP send buffer",
                UDP_SEND_BUFFER_BYTES / (1024 * 1024)
            );
        }

        Err(error) => {
            eprintln!(
                "audio-core: could not increase the \
                 diagnostic UDP send buffer: {error}. \
                 Continuing with the operating system \
                 default."
            );
        }
    }
}

fn elapsed_frames(elapsed: Duration, sample_rate: u32) -> u64 {
    let whole_seconds = elapsed.as_secs().saturating_mul(sample_rate as u64);

    let fractional_frames =
        (elapsed.subsec_nanos() as u128 * sample_rate as u128 / 1_000_000_000u128) as u64;

    whole_seconds.saturating_add(fractional_frames)
}

fn frames_to_nanoseconds(frame_position: u64, sample_rate: u32) -> u64 {
    if sample_rate == 0 {
        return 0;
    }

    ((frame_position as u128 * 1_000_000_000u128) / sample_rate as u128).min(u64::MAX as u128)
        as u64
}

fn estimated_pcm_megabits_per_second(channel_count: usize, sample_rate: u32) -> f64 {
    channel_count as f64 * sample_rate as f64 * BYTES_PER_SAMPLE as f64 * 8.0 / 1_000_000.0
}

fn print_periodic_report(
    config: &DiagnosticPublishConfig,
    report: &DiagnosticPublishReport,
    elapsed: Duration,
    subscriber_elapsed: Duration,
) {
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    let subscriber_seconds = subscriber_elapsed.as_secs_f64().max(f64::EPSILON);

    println!(
        "Diagnostic Publish metrics: {}ch, \
         elapsed={:.1}s, generated={} frames, \
         packets={} ({:.0}/s), datagrams={} \
         ({:.0}/s), subscriber-active={:.1}s, \
         active-packets/s={:.0}, active-datagrams/s={:.0}, \
         send-errors={}, \
         skipped-catch-up={}",
        config.channel_count,
        seconds,
        report.generated_frames,
        report.packets_built,
        report.packets_built as f64 / seconds,
        report.datagrams_sent,
        report.datagrams_sent as f64 / seconds,
        subscriber_elapsed.as_secs_f64(),
        report.packets_built as f64 / subscriber_seconds,
        report.datagrams_sent as f64 / subscriber_seconds,
        report.send_errors,
        report.skipped_catch_up_frames
    );
}

fn current_subscriber_elapsed(
    report: &DiagnosticPublishReport,
    subscriber_started_at: Option<Instant>,
) -> Duration {
    report.subscriber_elapsed
        + subscriber_started_at
            .map(|started_at| started_at.elapsed())
            .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use network_core::protocol::{decode_float32, parse_packet};

    #[test]
    fn report_separates_wall_clock_and_subscriber_active_rates() {
        let report = DiagnosticPublishReport {
            elapsed: Duration::from_secs(10),
            subscriber_elapsed: Duration::from_secs(2),
            packets_built: 24_000,
            datagrams_sent: 48_000,
            ..DiagnosticPublishReport::default()
        };

        assert_eq!(report.average_packets_per_second(), 2_400.0);
        assert_eq!(report.average_datagrams_per_second(), 4_800.0);
        assert_eq!(report.subscriber_packets_per_second(), 12_000.0);
        assert_eq!(report.subscriber_datagrams_per_second(), 24_000.0);
    }

    #[test]
    fn thirty_two_channels_fit_below_safe_udp_limit() {
        let frames = calculate_max_frames_per_packet(32).unwrap();

        let packet_bytes = OPENAUDIO_HEADER_BYTES + frames * 32 * BYTES_PER_SAMPLE;

        assert_eq!(frames, 9);

        assert!(packet_bytes <= SAFE_UDP_PAYLOAD_BYTES);
    }

    #[test]
    fn sixty_four_channels_fit_below_safe_udp_limit() {
        let frames = calculate_max_frames_per_packet(64).unwrap();

        let packet_bytes = OPENAUDIO_HEADER_BYTES + frames * 64 * BYTES_PER_SAMPLE;

        assert_eq!(frames, 4);

        assert!(packet_bytes <= SAFE_UDP_PAYLOAD_BYTES);
    }

    #[test]
    fn generated_sample_count_is_interleaved_correctly() {
        let mut oscillators = create_channel_oscillators(32, 48_000);

        let mut samples = Vec::new();

        generate_interleaved_samples(&mut samples, 9, &mut oscillators);

        assert_eq!(samples.len(), 9 * 32);
    }

    #[test]
    fn packet_builder_produces_expected_size() {
        let channels = 32;
        let frames = 9;

        let samples = vec![0.0f32; channels * frames];

        let mut packet = Vec::new();

        build_audio_packet(
            &mut packet,
            9_001,
            1,
            0,
            channels,
            sample_rate_to_code(48_000),
            frames,
            &samples,
        )
        .unwrap();

        assert_eq!(
            packet.len(),
            OPENAUDIO_HEADER_BYTES + channels * frames * BYTES_PER_SAMPLE
        );

        assert!(packet.len() <= SAFE_UDP_PAYLOAD_BYTES);
    }

    #[test]
    fn one_second_64_channel_packetization_preserves_samples_and_sequence() {
        const CHANNELS: usize = 64;
        const SAMPLE_RATE: usize = 48_000;
        const FRAMES_PER_PACKET: usize = 4;

        let mut packet = Vec::with_capacity(SAFE_UDP_PAYLOAD_BYTES);
        let mut samples = Vec::with_capacity(CHANNELS * FRAMES_PER_PACKET);
        let mut expected_sequence = 0u32;
        let mut total_frames = 0usize;

        for packet_index in 0..(SAMPLE_RATE / FRAMES_PER_PACKET) {
            samples.clear();
            for frame in 0..FRAMES_PER_PACKET {
                let frame_number = packet_index * FRAMES_PER_PACKET + frame;
                for channel in 0..CHANNELS {
                    samples.push((frame_number * CHANNELS + channel) as f32);
                }
            }

            build_audio_packet(
                &mut packet,
                123,
                expected_sequence,
                (total_frames as u64 * 1_000_000_000) / SAMPLE_RATE as u64,
                CHANNELS,
                sample_rate_to_code(SAMPLE_RATE as u32),
                FRAMES_PER_PACKET,
                &samples,
            )
            .unwrap();

            assert!(packet.len() <= SAFE_UDP_PAYLOAD_BYTES);
            let parsed = parse_packet(&packet).expect("generated packet parses");
            assert_eq!(parsed.sequence_number, expected_sequence);
            assert_eq!(parsed.channel_count as usize, CHANNELS);
            assert_eq!(parsed.samples_per_channel as usize, FRAMES_PER_PACKET);
            assert_eq!(
                decode_float32(&packet, &parsed).expect("complete float payload"),
                samples
            );

            expected_sequence = expected_sequence.wrapping_add(1);
            total_frames += FRAMES_PER_PACKET;
        }

        assert_eq!(total_frames, SAMPLE_RATE);
        assert_eq!(expected_sequence, 12_000);
    }

    #[test]
    fn invalid_sample_rate_is_rejected() {
        let config = DiagnosticPublishConfig {
            node_name: "Test Node".to_string(),
            stream_name: "Test Stream".to_string(),
            stream_id: 9_001,
            channel_count: 32,
            sample_rate: 123_456,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn zero_channels_are_rejected() {
        let config = DiagnosticPublishConfig {
            node_name: "Test Node".to_string(),
            stream_name: "Test Stream".to_string(),
            stream_id: 9_001,
            channel_count: 0,
            sample_rate: 48_000,
        };

        assert!(config.validate().is_err());
    }
}
