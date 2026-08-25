//! Desktop UI for synthetic OpenAudio network diagnostics.
//!
//! This panel starts diagnostic publishers without requiring an audio
//! console, input device, or ASIO capture driver.

use audio_core::{
    publish_diagnostic_stream_with_discovery, DiagnosticPublishConfig, DiagnosticPublishReport,
    SubscriberRegistry,
};
use eframe::egui;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_CHANNEL_COUNT: usize = 32;
const DEFAULT_SAMPLE_RATE: u32 = 48_000;
const DIAGNOSTIC_STREAM_ID_BASE: u32 = 90_000;
const SAFE_UDP_PAYLOAD_BYTES: usize = 1_200;
const OPENAUDIO_HEADER_BYTES: usize = 32;
const BYTES_PER_SAMPLE: usize = std::mem::size_of::<f32>();

const CHANNEL_OPTIONS: [usize; 6] = [2, 8, 16, 24, 32, 64];
const SAMPLE_RATE_OPTIONS: [u32; 2] = [44_100, 48_000];

/// Owns all synthetic diagnostic publisher sessions.
pub struct DiagnosticPanel {
    sessions: Vec<DiagnosticSession>,
    next_session_id: u64,
}

struct DiagnosticSession {
    id: u64,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    channel_count: usize,
    sample_rate: u32,

    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    final_report: Arc<Mutex<Option<DiagnosticPublishReport>>>,

    started_at: Option<Instant>,
    last_toggle: Instant,
}

impl Default for DiagnosticPanel {
    fn default() -> Self {
        Self {
            sessions: Vec::new(),
            next_session_id: 1,
        }
    }
}

impl Drop for DiagnosticPanel {
    fn drop(&mut self) {
        for session in &self.sessions {
            session.running.store(false, Ordering::Release);
        }
    }
}

impl DiagnosticPanel {
    /// Draws the complete Network Stress Test panel.
    pub fn show(&mut self, ui: &mut egui::Ui, subscribers_by_stream: &SubscriberRegistry) {
        egui::CollapsingHeader::new("🧪 Network Stress Test")
            .default_open(false)
            .show(ui, |ui| {
                ui.label(
                    "Generate a real multichannel OpenAudio stream without \
                 opening a console or audio input device.",
                );

                ui.add_space(4.0);

                ui.horizontal(|ui| {
                    if ui.button("➕ Add test").clicked() {
                        self.add_session();
                    }

                    if ui.button("Add 32-channel test").clicked() {
                        self.add_session_with_channels(32);
                    }
                });

                if self.sessions.is_empty() {
                    ui.add_space(8.0);
                    ui.weak(
                        "Add a test, start it, then subscribe using your \
                     normal Browser, WASAPI, Split, or ASIO subscriber.",
                    );
                    return;
                }

                ui.add_space(8.0);

                let mut remove_index = None;

                for index in 0..self.sessions.len() {
                    let session = &mut self.sessions[index];

                    ui.push_id(session.id, |ui| {
                        let frame =
                            egui::Frame::group(ui.style()).inner_margin(egui::Margin::same(10.0));

                        frame.show(ui, |ui| {
                            let running = session.running.load(Ordering::Acquire);

                            ui.horizontal(|ui| {
                                ui.strong(format!("Diagnostic stream {}", session.id));

                                ui.separator();

                                if running {
                                    ui.colored_label(egui::Color32::LIGHT_GREEN, "● Running");
                                } else {
                                    ui.weak("● Stopped");
                                }

                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        let remove_button =
                                            ui.add_enabled(!running, egui::Button::new("🗑 Remove"));

                                        if remove_button.clicked() {
                                            remove_index = Some(index);
                                        }
                                    },
                                );
                            });

                            ui.add_space(6.0);

                            ui.add_enabled_ui(!running, |ui| {
                                egui::Grid::new(format!("diagnostic_grid_{}", session.id))
                                    .num_columns(2)
                                    .spacing([12.0, 6.0])
                                    .show(ui, |ui| {
                                        ui.label("Node name");
                                        ui.text_edit_singleline(&mut session.node_name);
                                        ui.end_row();

                                        ui.label("Stream name");
                                        ui.text_edit_singleline(&mut session.stream_name);
                                        ui.end_row();

                                        ui.label("Stream ID");
                                        ui.add(
                                            egui::DragValue::new(&mut session.stream_id).speed(1),
                                        );
                                        ui.end_row();

                                        ui.label("Channels");
                                        egui::ComboBox::from_id_source(format!(
                                            "diagnostic_channels_{}",
                                            session.id
                                        ))
                                        .selected_text(format!(
                                            "{} channels",
                                            session.channel_count
                                        ))
                                        .show_ui(
                                            ui,
                                            |ui| {
                                                for channels in CHANNEL_OPTIONS {
                                                    ui.selectable_value(
                                                        &mut session.channel_count,
                                                        channels,
                                                        format!("{channels} channels"),
                                                    );
                                                }
                                            },
                                        );
                                        ui.end_row();

                                        ui.label("Sample rate");
                                        egui::ComboBox::from_id_source(format!(
                                            "diagnostic_rate_{}",
                                            session.id
                                        ))
                                        .selected_text(format!("{} Hz", session.sample_rate))
                                        .show_ui(
                                            ui,
                                            |ui| {
                                                for sample_rate in SAMPLE_RATE_OPTIONS {
                                                    ui.selectable_value(
                                                        &mut session.sample_rate,
                                                        sample_rate,
                                                        format!("{sample_rate} Hz"),
                                                    );
                                                }
                                            },
                                        );
                                        ui.end_row();
                                    });
                            });

                            ui.add_space(8.0);

                            show_load_estimate(ui, session);

                            ui.add_space(8.0);

                            ui.horizontal(|ui| {
                                if running {
                                    if ui.button("⏹ Stop test").clicked() {
                                        stop_session(session);
                                    }
                                } else if ui.button("▶ Start test").clicked() {
                                    start_session(session, subscribers_by_stream.clone());
                                }

                                if running {
                                    if let Some(started_at) = session.started_at {
                                        ui.label(format!(
                                            "Elapsed: {}",
                                            format_duration(started_at.elapsed())
                                        ));
                                    }
                                }
                            });

                            ui.add_space(5.0);

                            let status = read_status(&session.status);

                            if status.starts_with("Failed") {
                                ui.colored_label(egui::Color32::LIGHT_RED, status);
                            } else {
                                ui.label(status);
                            }

                            let final_report = session
                                .final_report
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .clone();

                            if let Some(report) = final_report {
                                show_final_report(ui, &report, session.sample_rate);
                            }
                        });
                    });

                    ui.add_space(8.0);
                }

                if let Some(index) = remove_index {
                    let session = self.sessions.remove(index);
                    session.running.store(false, Ordering::Release);
                }
            });

        if self
            .sessions
            .iter()
            .any(|session| session.running.load(Ordering::Acquire))
        {
            ui.ctx().request_repaint_after(Duration::from_millis(250));
        }
    }

    fn add_session(&mut self) {
        self.add_session_with_channels(DEFAULT_CHANNEL_COUNT);
    }

    fn add_session_with_channels(&mut self, channel_count: usize) {
        let id = self.next_session_id;
        self.next_session_id = self.next_session_id.saturating_add(1);

        let stream_id = DIAGNOSTIC_STREAM_ID_BASE.saturating_add(id.min(u32::MAX as u64) as u32);

        self.sessions.push(DiagnosticSession {
            id,
            node_name: format!("OpenAudio Diagnostic {id}"),
            stream_name: format!("{channel_count}ch Network Test"),
            stream_id,
            channel_count,
            sample_rate: DEFAULT_SAMPLE_RATE,
            running: Arc::new(AtomicBool::new(false)),
            status: Arc::new(Mutex::new(
                "Ready. Start the test, then subscribe normally.".to_string(),
            )),
            final_report: Arc::new(Mutex::new(None)),
            started_at: None,
            last_toggle: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        });
    }
}

fn start_session(session: &mut DiagnosticSession, subscribers_by_stream: SubscriberRegistry) {
    if session.last_toggle.elapsed() < Duration::from_millis(300) {
        return;
    }

    session.last_toggle = Instant::now();

    if session.running.load(Ordering::Acquire) {
        return;
    }

    let config = match DiagnosticPublishConfig::new(
        session.node_name.trim(),
        session.stream_name.trim(),
        session.stream_id,
        session.channel_count,
        session.sample_rate,
    ) {
        Ok(config) => config,
        Err(error) => {
            write_status(&session.status, format!("Failed: {error}"));
            return;
        }
    };

    session.running.store(true, Ordering::Release);
    session.started_at = Some(Instant::now());

    {
        let mut report = session
            .final_report
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *report = None;
    }

    write_status(
        &session.status,
        "Advertising. Waiting for a subscriber...".to_string(),
    );

    let running = session.running.clone();
    let status = session.status.clone();
    let final_report = session.final_report.clone();

    let spawn_result = std::thread::Builder::new()
        .name(format!("diagnostic-publisher-{}", session.id))
        .spawn(move || {
            let result = publish_diagnostic_stream_with_discovery(
                config,
                subscribers_by_stream,
                running.clone(),
            );

            running.store(false, Ordering::Release);

            match result {
                Ok(report) => {
                    let summary = format!(
                        "Stopped: {} packets, {} datagrams, {} \
                     send errors, {} skipped frames.",
                        report.packets_built,
                        report.datagrams_sent,
                        report.send_errors,
                        report.skipped_catch_up_frames,
                    );

                    {
                        let mut destination = final_report
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());

                        *destination = Some(report);
                    }

                    write_status(&status, summary);
                }
                Err(error) => {
                    write_status(&status, format!("Failed: {error}"));
                }
            }
        });

    if let Err(error) = spawn_result {
        session.running.store(false, Ordering::Release);
        session.started_at = None;

        write_status(
            &session.status,
            format!("Failed to start diagnostic thread: {error}"),
        );
    }
}

fn stop_session(session: &mut DiagnosticSession) {
    if session.last_toggle.elapsed() < Duration::from_millis(300) {
        return;
    }

    session.last_toggle = Instant::now();
    session.running.store(false, Ordering::Release);

    write_status(
        &session.status,
        "Stopping diagnostic publisher...".to_string(),
    );
}

fn show_load_estimate(ui: &mut egui::Ui, session: &DiagnosticSession) {
    let frames_per_packet = calculate_frames_per_packet(session.channel_count);

    let packet_bytes =
        OPENAUDIO_HEADER_BYTES + frames_per_packet * session.channel_count * BYTES_PER_SAMPLE;

    let packets_per_second = if frames_per_packet == 0 {
        0.0
    } else {
        session.sample_rate as f64 / frames_per_packet as f64
    };

    let raw_pcm_mbps =
        session.channel_count as f64 * session.sample_rate as f64 * BYTES_PER_SAMPLE as f64 * 8.0
            / 1_000_000.0;

    egui::Grid::new(format!("diagnostic_estimate_{}", session.id))
        .num_columns(2)
        .striped(true)
        .show(ui, |ui| {
            ui.weak("Frames per packet");
            ui.label(frames_per_packet.to_string());
            ui.end_row();

            ui.weak("UDP payload");
            ui.label(format!("{packet_bytes} bytes"));
            ui.end_row();

            ui.weak("Expected packet rate");
            ui.label(format!("{packets_per_second:.0} packets/s"));
            ui.end_row();

            ui.weak("Raw Float32 audio");
            ui.label(format!("{raw_pcm_mbps:.2} Mbps"));
            ui.end_row();
        });

    if packet_bytes <= SAFE_UDP_PAYLOAD_BYTES {
        ui.colored_label(
            egui::Color32::LIGHT_GREEN,
            "✓ Packet size is below the 1200-byte safety limit.",
        );
    } else {
        ui.colored_label(
            egui::Color32::LIGHT_RED,
            "Packet size exceeds the configured safety limit.",
        );
    }
}

fn show_final_report(ui: &mut egui::Ui, report: &DiagnosticPublishReport, sample_rate: u32) {
    ui.separator();
    ui.strong("Last completed test");

    egui::Grid::new("diagnostic_final_report")
        .num_columns(2)
        .striped(true)
        .show(ui, |ui| {
            ui.label("Duration");
            ui.label(format_duration(report.elapsed));
            ui.end_row();

            ui.label("Generated audio");
            ui.label(format!(
                "{:.2} seconds",
                report.generated_audio_seconds(sample_rate)
            ));
            ui.end_row();

            ui.label("Packets built");
            ui.label(format!(
                "{} ({:.0}/s)",
                report.packets_built,
                report.average_packets_per_second()
            ));
            ui.end_row();

            ui.label("Datagrams sent");
            ui.label(format!(
                "{} ({:.0}/s)",
                report.datagrams_sent,
                report.average_datagrams_per_second()
            ));
            ui.end_row();

            ui.label("Send errors");
            ui.label(report.send_errors.to_string());
            ui.end_row();

            ui.label("Skipped catch-up frames");
            ui.label(report.skipped_catch_up_frames.to_string());
            ui.end_row();

            ui.label("Maximum packet");
            ui.label(format!("{} bytes", report.maximum_packet_bytes));
            ui.end_row();
        });

    if report.send_errors == 0 && report.skipped_catch_up_frames == 0 {
        ui.colored_label(
            egui::Color32::LIGHT_GREEN,
            "✓ Publisher completed without detected pressure.",
        );
    } else {
        ui.colored_label(
            egui::Color32::YELLOW,
            "⚠ Publisher pressure was detected. Check send errors and \
             skipped frames.",
        );
    }
}

fn calculate_frames_per_packet(channel_count: usize) -> usize {
    if channel_count == 0 {
        return 0;
    }

    let audio_budget = SAFE_UDP_PAYLOAD_BYTES - OPENAUDIO_HEADER_BYTES;

    (audio_budget / (channel_count * BYTES_PER_SAMPLE)).max(1)
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;

    if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn read_status(status: &Arc<Mutex<String>>) -> String {
    status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn write_status(status: &Arc<Mutex<String>>, value: String) {
    let mut destination = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    *destination = value;
}
