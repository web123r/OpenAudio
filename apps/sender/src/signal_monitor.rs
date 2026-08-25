use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const METER_FLOOR_DB: f32 = -60.0;
const CLIP_THRESHOLD_DB: f32 = -0.1;
const SIGNAL_THRESHOLD_DB: f32 = -59.0;
const SIGNAL_TIMEOUT_MS: u64 = 1_000;
const PEAK_HOLD_DECAY_DB_PER_SECOND: f32 = 18.0;
const MIN_CHANNEL_CELL_WIDTH: f32 = 215.0;
const MAX_GRID_COLUMNS: usize = 8;
const LIVE_REPAINT_INTERVAL: Duration = Duration::from_millis(50);

pub struct SignalMonitor {
    expanded: bool,
    held_peaks_db: HashMap<String, Vec<f32>>,
    last_update: Instant,
}

impl Default for SignalMonitor {
    fn default() -> Self {
        Self {
            expanded: true,
            held_peaks_db: HashMap::new(),
            last_update: Instant::now(),
        }
    }
}

impl SignalMonitor {
    pub fn ui(&mut self, ui: &mut egui::Ui) {
        // Keep the monitor responsive without forcing the entire
        // application to repaint at full audio-callback speed.
        ui.ctx().request_repaint_after(LIVE_REPAINT_INTERVAL);

        let snapshots = audio_core::signal_meter_snapshots();

        let now = Instant::now();
        let elapsed_seconds = now
            .saturating_duration_since(self.last_update)
            .as_secs_f32()
            .min(1.0);

        self.last_update = now;

        let active_ids: HashSet<String> = snapshots
            .iter()
            .map(|snapshot| snapshot.id.clone())
            .collect();

        self.held_peaks_db.retain(|id, _| active_ids.contains(id));

        egui::Frame::none()
            .fill(egui::Color32::from_rgb(28, 28, 32))
            .rounding(12.0)
            .inner_margin(16.0)
            .show(ui, |ui| {
                self.render_header(ui, snapshots.len());

                if !self.expanded {
                    return;
                }

                ui.add_space(10.0);

                if snapshots.is_empty() {
                    render_empty_state(ui);
                    return;
                }

                for snapshot in snapshots {
                    let peaks = snapshot
                        .channel_peaks_db
                        .iter()
                        .copied()
                        .map(sanitize_db)
                        .collect::<Vec<_>>();

                    let held_peaks = self.update_peak_holds(&snapshot.id, &peaks, elapsed_seconds);

                    render_stream(
                        ui,
                        &snapshot.id,
                        &snapshot.label,
                        snapshot.direction,
                        snapshot.last_signal_age_ms,
                        &peaks,
                        &held_peaks,
                    );

                    ui.add_space(8.0);
                }
            });
    }

    fn render_header(&mut self, ui: &mut egui::Ui, active_stream_count: usize) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("📊 Signal Monitor")
                    .size(18.0)
                    .strong()
                    .color(egui::Color32::WHITE),
            );

            let stream_text = if active_stream_count == 1 {
                "1 active stream".to_string()
            } else {
                format!("{active_stream_count} active streams")
            };

            ui.label(
                egui::RichText::new(stream_text)
                    .size(11.0)
                    .color(secondary_text()),
            );

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let button_text = if self.expanded { "Collapse" } else { "Expand" };

                if ui.small_button(button_text).clicked() {
                    self.expanded = !self.expanded;
                }
            });
        });
    }

    fn update_peak_holds(
        &mut self,
        meter_id: &str,
        current_peaks_db: &[f32],
        elapsed_seconds: f32,
    ) -> Vec<f32> {
        let held = self
            .held_peaks_db
            .entry(meter_id.to_string())
            .or_insert_with(|| vec![METER_FLOOR_DB; current_peaks_db.len()]);

        if held.len() != current_peaks_db.len() {
            held.resize(current_peaks_db.len(), METER_FLOOR_DB);
        }

        let decay = PEAK_HOLD_DECAY_DB_PER_SECOND * elapsed_seconds;

        for (held_peak, current_peak) in held.iter_mut().zip(current_peaks_db) {
            if *current_peak >= *held_peak {
                *held_peak = *current_peak;
            } else {
                *held_peak = (*held_peak - decay).max(*current_peak).max(METER_FLOOR_DB);
            }
        }

        held.clone()
    }
}

#[allow(clippy::too_many_arguments)]
fn render_stream(
    ui: &mut egui::Ui,
    meter_id: &str,
    label: &str,
    direction: audio_core::SignalDirection,
    last_signal_age_ms: u64,
    peaks_db: &[f32],
    held_peaks_db: &[f32],
) {
    let maximum_peak = peaks_db.iter().copied().fold(METER_FLOOR_DB, f32::max);

    let signal_is_recent = last_signal_age_ms <= SIGNAL_TIMEOUT_MS;

    let has_signal = signal_is_recent && maximum_peak > SIGNAL_THRESHOLD_DB;

    let is_clipping = peaks_db.iter().any(|peak| *peak >= CLIP_THRESHOLD_DB);

    egui::Frame::none()
        .fill(egui::Color32::from_rgb(38, 38, 42))
        .rounding(8.0)
        .inner_margin(10.0)
        .show(ui, |ui| {
            render_stream_header(
                ui,
                label,
                direction,
                peaks_db.len(),
                has_signal,
                is_clipping,
                last_signal_age_ms,
            );

            ui.add_space(8.0);

            if peaks_db.is_empty() {
                ui.label(
                    egui::RichText::new("This meter has no channels.")
                        .italics()
                        .color(secondary_text()),
                );

                return;
            }

            render_channel_grid(ui, meter_id, peaks_db, held_peaks_db);
        });
}

fn render_stream_header(
    ui: &mut egui::Ui,
    label: &str,
    direction: audio_core::SignalDirection,
    channel_count: usize,
    has_signal: bool,
    is_clipping: bool,
    last_signal_age_ms: u64,
) {
    ui.horizontal(|ui| {
        let direction_text = direction_label(direction);

        egui::Frame::none()
            .fill(direction_color(direction))
            .rounding(4.0)
            .inner_margin(egui::Margin::symmetric(6.0, 2.0))
            .show(ui, |ui| {
                ui.label(
                    egui::RichText::new(direction_text)
                        .size(10.0)
                        .strong()
                        .color(egui::Color32::WHITE),
                );
            });

        ui.label(
            egui::RichText::new(label)
                .strong()
                .color(egui::Color32::WHITE),
        );

        ui.label(
            egui::RichText::new(format!("{channel_count}ch"))
                .size(10.0)
                .color(secondary_text()),
        );

        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            if is_clipping {
                ui.label(
                    egui::RichText::new("● CLIP")
                        .size(11.0)
                        .strong()
                        .color(clip_color()),
                );
            } else if has_signal {
                ui.label(
                    egui::RichText::new("● SIGNAL")
                        .size(11.0)
                        .strong()
                        .color(signal_color()),
                );
            } else {
                ui.label(
                    egui::RichText::new("○ NO SIGNAL")
                        .size(11.0)
                        .strong()
                        .color(secondary_text()),
                );
            }

            let age_text = signal_age_text(last_signal_age_ms);

            ui.label(
                egui::RichText::new(age_text)
                    .size(9.0)
                    .color(secondary_text()),
            );
        });
    });
}

fn render_channel_grid(ui: &mut egui::Ui, meter_id: &str, peaks_db: &[f32], held_peaks_db: &[f32]) {
    let available_width = ui.available_width().max(MIN_CHANNEL_CELL_WIDTH);

    let columns = ((available_width / MIN_CHANNEL_CELL_WIDTH).floor() as usize)
        .clamp(1, MAX_GRID_COLUMNS)
        .min(peaks_db.len().max(1));

    egui::Grid::new(format!("signal_meter_grid_{meter_id}"))
        .num_columns(columns)
        .spacing([12.0, 7.0])
        .show(ui, |ui| {
            for (channel_index, current_peak) in peaks_db.iter().copied().enumerate() {
                let held_peak = held_peaks_db
                    .get(channel_index)
                    .copied()
                    .unwrap_or(current_peak);

                render_channel(ui, channel_index, current_peak, held_peak);

                if (channel_index + 1) % columns == 0 {
                    ui.end_row();
                }
            }

            if peaks_db.len() % columns != 0 {
                ui.end_row();
            }
        });
}

fn render_channel(ui: &mut egui::Ui, channel: usize, peak_db: f32, held_peak_db: f32) {
    let clipped = peak_db >= CLIP_THRESHOLD_DB;

    ui.vertical(|ui| {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new(format!("Ch {}", channel + 1))
                    .size(10.0)
                    .strong()
                    .color(if clipped {
                        clip_color()
                    } else {
                        secondary_text()
                    }),
            );

            if clipped {
                ui.label(
                    egui::RichText::new("CLIP")
                        .size(9.0)
                        .strong()
                        .color(clip_color()),
                );
            }

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(
                    egui::RichText::new(format_db(peak_db))
                        .size(10.0)
                        .monospace()
                        .color(meter_color(peak_db)),
                );
            });
        });

        let desired_width = (ui.available_width() - 4.0).clamp(120.0, 220.0);

        render_meter_bar(ui, desired_width, peak_db, held_peak_db);
    });
}

fn render_meter_bar(ui: &mut egui::Ui, desired_width: f32, peak_db: f32, held_peak_db: f32) {
    let desired_size = egui::vec2(desired_width, 14.0);

    let (rect, response) = ui.allocate_exact_size(desired_size, egui::Sense::hover());

    let painter = ui.painter();

    painter.rect_filled(rect, 3.0, egui::Color32::from_rgb(18, 18, 21));

    let normalized = normalize_db(peak_db);

    if normalized > 0.0 {
        let fill_rect = egui::Rect::from_min_max(
            rect.min,
            egui::pos2(rect.left() + rect.width() * normalized, rect.bottom()),
        );

        painter.rect_filled(fill_rect, 3.0, meter_color(peak_db));
    }

    let yellow_position = normalize_db(-12.0);

    let red_position = normalize_db(-1.0);

    for position in [yellow_position, red_position] {
        let x = rect.left() + rect.width() * position;

        painter.line_segment(
            [egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())],
            egui::Stroke::new(
                1.0,
                egui::Color32::from_rgba_unmultiplied(255, 255, 255, 55),
            ),
        );
    }

    let held_normalized = normalize_db(held_peak_db);

    if held_normalized > 0.0 {
        let held_x = rect.left() + rect.width() * held_normalized;

        painter.line_segment(
            [
                egui::pos2(held_x, rect.top() + 1.0),
                egui::pos2(held_x, rect.bottom() - 1.0),
            ],
            egui::Stroke::new(2.0, peak_hold_color(held_peak_db)),
        );
    }

    let hover_text = format!(
        "Current peak: {}\nPeak hold: {}\n\
         Green: below -12 dBFS\n\
         Yellow: -12 to -1 dBFS\n\
         Red: above -1 dBFS",
        format_db(peak_db),
        format_db(held_peak_db),
    );

    response.on_hover_text(hover_text);
}

fn render_empty_state(ui: &mut egui::Ui) {
    egui::Frame::none()
        .fill(egui::Color32::from_rgb(35, 35, 39))
        .rounding(8.0)
        .inner_margin(14.0)
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("○").size(20.0).color(secondary_text()));

                ui.vertical(|ui| {
                    ui.label(
                        egui::RichText::new("No active signal meters")
                            .strong()
                            .color(egui::Color32::WHITE),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Start Publish, Subscribe, \
                             Combine, Split, ASIO, or a \
                             Diagnostic stream to monitor \
                             its channels.",
                        )
                        .size(11.0)
                        .color(secondary_text()),
                    );
                });
            });
        });
}

fn direction_label(direction: audio_core::SignalDirection) -> &'static str {
    match direction {
        audio_core::SignalDirection::Input => "IN",
        audio_core::SignalDirection::Output => "OUT",
        audio_core::SignalDirection::Diagnostic => "TEST",
    }
}

fn direction_color(direction: audio_core::SignalDirection) -> egui::Color32 {
    match direction {
        audio_core::SignalDirection::Input => egui::Color32::from_rgb(0, 122, 255),
        audio_core::SignalDirection::Output => egui::Color32::from_rgb(175, 82, 222),
        audio_core::SignalDirection::Diagnostic => egui::Color32::from_rgb(255, 149, 0),
    }
}

fn meter_color(db: f32) -> egui::Color32 {
    if db >= -1.0 {
        clip_color()
    } else if db >= -12.0 {
        warning_color()
    } else {
        signal_color()
    }
}

fn peak_hold_color(db: f32) -> egui::Color32 {
    if db >= -1.0 {
        egui::Color32::WHITE
    } else if db >= -12.0 {
        egui::Color32::from_rgb(255, 244, 170)
    } else {
        egui::Color32::from_rgb(205, 255, 215)
    }
}

fn signal_color() -> egui::Color32 {
    egui::Color32::from_rgb(52, 199, 89)
}

fn warning_color() -> egui::Color32 {
    egui::Color32::from_rgb(255, 204, 0)
}

fn clip_color() -> egui::Color32 {
    egui::Color32::from_rgb(255, 69, 58)
}

fn secondary_text() -> egui::Color32 {
    egui::Color32::from_rgb(152, 152, 157)
}

fn normalize_db(db: f32) -> f32 {
    ((sanitize_db(db) - METER_FLOOR_DB) / -METER_FLOOR_DB).clamp(0.0, 1.0)
}

fn sanitize_db(db: f32) -> f32 {
    if db.is_finite() {
        db.clamp(METER_FLOOR_DB, 0.0)
    } else {
        METER_FLOOR_DB
    }
}

fn format_db(db: f32) -> String {
    let db = sanitize_db(db);

    if db <= METER_FLOOR_DB {
        "-∞ dBFS".to_string()
    } else {
        format!("{db:.1} dBFS")
    }
}

fn signal_age_text(last_signal_age_ms: u64) -> String {
    if last_signal_age_ms <= LIVE_REPAINT_INTERVAL.as_millis() as u64 {
        "live".to_string()
    } else if last_signal_age_ms < 1_000 {
        format!("{last_signal_age_ms}ms ago")
    } else if last_signal_age_ms < 60_000 {
        format!("{:.1}s ago", last_signal_age_ms as f32 / 1_000.0)
    } else {
        "inactive".to_string()
    }
}
