use eframe::egui;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct PublishSession {
    id: u64,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    selected_input: Option<String>,
    is_loopback: bool,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: std::time::Instant,
}

struct CombinePublishSession {
    id: u64,
    session_tag: String,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    channel_count: usize,
    channel_sources: Vec<(Option<String>, bool)>,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: std::time::Instant,
}

struct AsioPublishSession {
    id: u64,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    selected_driver: Option<String>,
    driver_channel_count: usize,
    channel_indices: Vec<usize>,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: std::time::Instant,
}

struct SubscribeSession {
    id: u64,
    selected_discovered_node_id: Option<String>,
    bind_port: String,
    selected_output: Option<String>,
    volume: audio_core::VolumeControl,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: std::time::Instant,
}

struct SplitSubscribeSession {
    id: u64,
    session_tag: String,
    selected_discovered_node_id: Option<String>,
    bind_port: String,
    channel_devices: Vec<Option<String>>,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: std::time::Instant,
    record: bool,
}

// NOTE: WebSubscribeSession removed. Browser playback is now a single
// always-on gateway (started once in Default::default()), not a
// per-click session -- the browser itself lists + selects streams via
// /api/streams, so there's nothing left for the desktop UI to manage
// per-session here.

struct OpenAudioApp {
    next_id: u64,
    publish_sessions: Vec<PublishSession>,
    combine_publish_sessions: Vec<CombinePublishSession>,
    asio_publish_sessions: Vec<AsioPublishSession>,
    subscribe_sessions: Vec<SubscribeSession>,
    split_subscribe_sessions: Vec<SplitSubscribeSession>,
    input_devices: Vec<audio_core::DeviceInfo>,
    output_devices: Vec<audio_core::DeviceInfo>,
    asio_drivers: Vec<audio_core::AsioDriverInfo>,
    asio_drivers_error: Option<String>,
    discovery_directory: Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>>,
    subscribers_by_stream: audio_core::SubscriberRegistry,
    error_banner: Arc<Mutex<Option<String>>>,
    info_banner: Arc<Mutex<Option<String>>>,
    refreshing_devices: Arc<AtomicBool>,
    pending_device_refresh: Arc<Mutex<Option<(Vec<audio_core::DeviceInfo>, Vec<audio_core::DeviceInfo>)>>>,
}

impl Default for OpenAudioApp {
    fn default() -> Self {
        let discovery_directory: Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let subscribers_by_stream: audio_core::SubscriberRegistry =
            Arc::new(Mutex::new(HashMap::new()));
        let always_on = Arc::new(AtomicBool::new(true));

        let dir_for_thread = discovery_directory.clone();
        let discovery_flag = always_on.clone();
        thread::spawn(move || {
            audio_core::ensure_realtime_audio_thread();
            if let Err(e) = audio_core::start_discovery_listener(dir_for_thread, discovery_flag) {
                eprintln!("discovery listener error: {e}");
            }
        });

        let subs_for_thread = subscribers_by_stream.clone();
        let control_flag = always_on.clone();
        thread::spawn(move || {
            audio_core::ensure_realtime_audio_thread();
            if let Err(e) = audio_core::start_control_listener(subs_for_thread, control_flag) {
                eprintln!("control listener error: {e}");
            }
        });

        // Single always-on browser-playback gateway for the whole app.
        // HTTP on :7100 serves the player page + /api/streams (live
        // list from `discovery_directory`); WebSocket on :7101 handles
        // per-client stream selection and relay.
        let web_gateway_dir = discovery_directory.clone();
        let web_gateway_flag = always_on.clone();
        thread::spawn(move || {
            audio_core::ensure_realtime_audio_thread();
            if let Err(e) = audio_core::run_web_gateway(7100, web_gateway_dir, web_gateway_flag) {
                eprintln!("web gateway error: {e}");
            }
        });

        let (asio_drivers, asio_drivers_error) = match audio_core::list_asio_drivers() {
            Ok(drivers) => (drivers, None),
            Err(e) => (Vec::new(), Some(e)),
        };

        Self {
            next_id: 1,
            publish_sessions: Vec::new(),
            combine_publish_sessions: Vec::new(),
            asio_publish_sessions: Vec::new(),
            subscribe_sessions: Vec::new(),
            split_subscribe_sessions: Vec::new(),
            input_devices: audio_core::list_input_devices(),
            output_devices: audio_core::list_output_devices(),
            asio_drivers,
            asio_drivers_error,
            discovery_directory,
            subscribers_by_stream,
            refreshing_devices: Arc::new(AtomicBool::new(false)),
            error_banner: Arc::new(Mutex::new(None)),
            info_banner: Arc::new(Mutex::new(None)),
            pending_device_refresh: Arc::new(Mutex::new(None)),
        }
    }
}

struct Theme;
impl Theme {
    const BG_PRIMARY:    egui::Color32 = egui::Color32::from_rgb(18, 18, 20);
    const BG_SECONDARY:  egui::Color32 = egui::Color32::from_rgb(28, 28, 32);
    const BG_CARD:       egui::Color32 = egui::Color32::from_rgb(38, 38, 42);
    const ACCENT_BLUE:   egui::Color32 = egui::Color32::from_rgb(0, 122, 255);
    const ACCENT_GREEN:  egui::Color32 = egui::Color32::from_rgb(52, 199, 89);
    const ACCENT_RED:    egui::Color32 = egui::Color32::from_rgb(255, 69, 58);
    const ACCENT_PURPLE: egui::Color32 = egui::Color32::from_rgb(175, 82, 222);
    const TEXT_PRIMARY:  egui::Color32 = egui::Color32::from_rgb(255, 255, 255);
    const TEXT_SECONDARY:egui::Color32 = egui::Color32::from_rgb(152, 152, 157);
}

fn friendly_error(raw: &str) -> String {
    if raw.contains("0x8889000A") {
        "That device is already in use by another app. Close other audio apps and try again.".to_string()
    } else if raw.contains("no default input device") {
        "No microphone/input device found. Check it's plugged in and enabled in Windows Sound settings.".to_string()
    } else if raw.contains("no default output device") {
        "No speaker/output device found. Check it's plugged in and enabled in Windows Sound settings.".to_string()
    } else if raw.contains("Resampling isn't implemented") {
        format!("Format mismatch between the incoming stream and your output device: {raw}")
    } else if raw.contains("device may not support WASAPI loopback") {
        format!("That device doesn't support loopback capture: {raw}")
    } else if raw.contains("must match exactly") {
        format!("Channel/device count mismatch: {raw}")
    } else if raw.contains("must share one sample rate") {
        format!("Sample rate mismatch across channels: {raw}")
    } else if raw.contains("couldn't find SAR's default.json") {
        "Couldn't find SAR's config file -- has SAR been configured at least once via its own GUI first?".to_string()
    } else if raw.contains("ASIO host unavailable") {
        "ASIO host unavailable. Make sure the app was built with --features asio and CPAL_ASIO_DIR was set.".to_string()
    } else if raw.contains("ASIO driver") && raw.contains("not found") {
        format!("ASIO driver not found. Is the console driver installed? {raw}")
    } else if raw.contains("not compiled in") {
        "ASIO is not enabled in this build. Rebuild with: cargo build --features asio (after setting CPAL_ASIO_DIR).".to_string()
    } else {
        raw.to_string()
    }
}

// ══════════════════════════════════════════════════════════════════
// PART 2 STARTS WITH: impl eframe::App for OpenAudioApp { ... }
// ══════════════════════════════════════════════════════════════════

impl eframe::App for OpenAudioApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut style = (*ctx.style()).clone();
        style.visuals.window_fill = Theme::BG_PRIMARY;
        style.visuals.panel_fill = Theme::BG_PRIMARY;
        style.visuals.widgets.inactive.bg_fill = Theme::BG_CARD;
        style.visuals.widgets.inactive.rounding = egui::Rounding::same(8.0);
        style.visuals.widgets.hovered.rounding = egui::Rounding::same(8.0);
        style.spacing.item_spacing = egui::vec2(10.0, 8.0);
        ctx.set_style(style);

        egui::CentralPanel::default()
            .frame(egui::Frame::none().fill(Theme::BG_PRIMARY).inner_margin(20.0))
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {

                        // ── HEADER ─────────────────────────────────────────
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("Ferronme's Open Audio")
                                    .size(28.0).strong().color(Theme::TEXT_PRIMARY),
                            );
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                let is_refreshing = self.refreshing_devices.load(Ordering::Relaxed);
                                let label = if is_refreshing { "Refreshing..." } else { "🔄 Refresh Devices" };
                                if ui.add_enabled(!is_refreshing, egui::Button::new(label)).clicked() {
                                    self.refreshing_devices.store(true, Ordering::Relaxed);
                                    let refreshing = self.refreshing_devices.clone();
                                    let pending = self.pending_device_refresh.clone();
                                    thread::spawn(move || {
                                        let inputs = audio_core::list_input_devices();
                                        let outputs = audio_core::list_output_devices();
                                        *pending.lock().unwrap() = Some((inputs, outputs));
                                        refreshing.store(false, Ordering::Relaxed);
                                    });
                                }
                                if let Some((inputs, outputs)) = self.pending_device_refresh.lock().unwrap().take() {
                                    self.input_devices = inputs;
                                    self.output_devices = outputs;
                                    match audio_core::list_asio_drivers() {
                                        Ok(d) => { self.asio_drivers = d; self.asio_drivers_error = None; }
                                        Err(e) => { self.asio_drivers = Vec::new(); self.asio_drivers_error = Some(e); }
                                    }
                                }
                            });
                        });
                        ui.label(egui::RichText::new("Audio Networking • Open • Free(for now lol)").size(13.0).color(Theme::TEXT_SECONDARY));
                        ui.add_space(16.0);

                        // ── BANNERS ────────────────────────────────────────
                        if let Some(err) = self.error_banner.lock().unwrap().clone() {
                            egui::Frame::none().fill(Theme::ACCENT_RED).rounding(10.0).inner_margin(12.0).show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new("⚠").size(16.0).color(egui::Color32::WHITE));
                                    ui.label(egui::RichText::new(err).color(egui::Color32::WHITE));
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        if ui.small_button("✕").clicked() { *self.error_banner.lock().unwrap() = None; }
                                    });
                                });
                            });
                            ui.add_space(10.0);
                        }
                        if let Some(info) = self.info_banner.lock().unwrap().clone() {
                            egui::Frame::none().fill(Theme::ACCENT_BLUE).rounding(10.0).inner_margin(12.0).show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new("ℹ").size(16.0).color(egui::Color32::WHITE));
                                    ui.label(egui::RichText::new(info).color(egui::Color32::WHITE));
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        if ui.small_button("✕").clicked() { *self.info_banner.lock().unwrap() = None; }
                                    });
                                });
                            });
                            ui.add_space(10.0);
                        }

                        // ── ASIO PUBLISH ───────────────────────────────────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new("🎛 ASIO Publish Streams").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                                ui.label(egui::RichText::new("(any installed ASIO driver)").size(11.0).color(Theme::TEXT_SECONDARY));
                            });
                            ui.add_space(6.0);

                            if let Some(ref err) = self.asio_drivers_error.clone() {
                                egui::Frame::none().fill(egui::Color32::from_rgb(55,35,15)).rounding(8.0).inner_margin(10.0).show(ui, |ui| {
                                    ui.label(egui::RichText::new(format!("⚠  {}", friendly_error(err))).size(11.0).color(egui::Color32::YELLOW));
                                });
                            } else if self.asio_drivers.is_empty() {
                                egui::Frame::none().fill(Theme::BG_CARD).rounding(8.0).inner_margin(10.0).show(ui, |ui| {
                                    ui.label(egui::RichText::new("No ASIO drivers detected. Install your console's USB driver then click 🔄 Refresh Devices.").size(11.0).color(Theme::TEXT_SECONDARY));
                                });
                            } else {
                                ui.label(egui::RichText::new(format!("✅  {} ASIO driver(s) detected", self.asio_drivers.len())).size(11.0).color(Theme::ACCENT_GREEN));
                            }
                            ui.add_space(8.0);

                            let mut to_remove_asio: Option<usize> = None;
                            for (idx, session) in self.asio_publish_sessions.iter_mut().enumerate() {
                                let is_running = session.running.load(Ordering::Relaxed);
                                let debounce_ok = session.last_toggle.elapsed() > Duration::from_millis(400);
                                egui::Frame::none().fill(Theme::BG_CARD).rounding(10.0).inner_margin(14.0).show(ui, |ui| {
                                    ui.add_enabled_ui(!is_running, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(egui::RichText::new("ASIO Driver:").color(Theme::TEXT_SECONDARY));
                                            let driver_label = session.selected_driver.clone().unwrap_or_else(|| "— select driver —".to_string());
                                            let combo_label = if session.driver_channel_count > 0 {
                                                format!("{driver_label}  ({} ch in)", session.driver_channel_count)
                                            } else { driver_label };
                                            egui::ComboBox::from_id_source(format!("asio_driver_{}", session.id))
                                                .selected_text(combo_label).width(320.0)
                                                .show_ui(ui, |ui| {
                                                    ui.selectable_value(&mut session.selected_driver, None, "— select driver —");
                                                    for driver in &self.asio_drivers {
                                                        let label = format!("{}  ({} in / {} out)", driver.name, driver.max_input_channels, driver.max_output_channels);
                                                        let prev = session.selected_driver.clone();
                                                        ui.selectable_value(&mut session.selected_driver, Some(driver.name.clone()), label);
                                                        if session.selected_driver != prev && session.selected_driver == Some(driver.name.clone()) {
                                                            session.driver_channel_count = driver.max_input_channels as usize;
                                                            session.channel_indices = (0..session.driver_channel_count).collect();
                                                        }
                                                    }
                                                });
                                        });

                                        if session.driver_channel_count > 0 {
                                            ui.add_space(8.0);
                                            ui.label(egui::RichText::new(format!("Channels to stream  ({} available):", session.driver_channel_count)).size(11.0).color(Theme::TEXT_SECONDARY));
                                            ui.add_space(4.0);
                                            ui.horizontal(|ui| {
                                                if ui.small_button("All").clicked()  { session.channel_indices = (0..session.driver_channel_count).collect(); }
                                                if ui.small_button("None").clicked() { session.channel_indices.clear(); }
                                                if ui.small_button("1-2").clicked()  { session.channel_indices = vec![0,1]; }
                                                if session.driver_channel_count >= 8  { if ui.small_button("1-8").clicked()  { session.channel_indices = (0..8).collect(); } }
                                                if session.driver_channel_count >= 16 { if ui.small_button("1-16").clicked() { session.channel_indices = (0..16).collect(); } }
                                                if session.driver_channel_count >= 32 { if ui.small_button("1-32").clicked() { session.channel_indices = (0..32).collect(); } }
                                            });
                                            ui.add_space(4.0);
                                            let cols = 8usize;
                                            egui::Grid::new(format!("ch_grid_{}", session.id)).num_columns(cols).spacing([4.0,4.0]).show(ui, |ui| {
                                                for ch in 0..session.driver_channel_count {
                                                    let is_sel = session.channel_indices.contains(&ch);
                                                    let btn = egui::Button::new(
                                                        egui::RichText::new(format!("Ch {}", ch+1)).size(10.0)
                                                            .color(if is_sel { egui::Color32::WHITE } else { Theme::TEXT_SECONDARY })
                                                    )
                                                    .fill(if is_sel { Theme::ACCENT_PURPLE } else { Theme::BG_SECONDARY })
                                                    .rounding(4.0).min_size(egui::vec2(44.0, 22.0));
                                                    if ui.add(btn).clicked() {
                                                        if is_sel { session.channel_indices.retain(|&x| x != ch); }
                                                        else { session.channel_indices.push(ch); session.channel_indices.sort(); }
                                                    }
                                                    if (ch+1) % cols == 0 { ui.end_row(); }
                                                }
                                            });
                                            ui.add_space(4.0);
                                            ui.label(egui::RichText::new(format!("{} channel(s) selected", session.channel_indices.len())).size(11.0).color(Theme::ACCENT_GREEN));
                                        }
                                    });

                                    ui.add_space(10.0);
                                    ui.columns(3, |cols| {
                                        cols[0].vertical(|ui| { ui.label(egui::RichText::new("Node Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.node_name).desired_width(150.0)); });
                                        cols[1].vertical(|ui| { ui.label(egui::RichText::new("Stream Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.stream_name).desired_width(150.0)); });
                                        cols[2].vertical(|ui| { ui.label(egui::RichText::new("Stream ID").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::DragValue::new(&mut session.stream_id)); });
                                    });
                                    ui.add_space(10.0); ui.separator(); ui.add_space(6.0);

                                    ui.horizontal(|ui| {
                                        if is_running {
                                            if ui.add_enabled(debounce_ok, egui::Button::new(egui::RichText::new("⏹ Stop").color(egui::Color32::WHITE)).fill(Theme::ACCENT_RED).rounding(8.0).min_size(egui::vec2(70.0,28.0))).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                session.running.store(false, Ordering::Relaxed);
                                                *session.status.lock().unwrap() = "Stopping...".to_string();
                                            }
                                        } else {
                                            let can_start = session.selected_driver.is_some() && !session.channel_indices.is_empty();
                                            if ui.add_enabled(debounce_ok && can_start,
                                                egui::Button::new(egui::RichText::new("▶ Start ASIO Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_PURPLE).rounding(8.0).min_size(egui::vec2(130.0,28.0))
                                            ).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                *self.error_banner.lock().unwrap() = None;
                                                let node_name       = session.node_name.clone();
                                                let stream_name     = session.stream_name.clone();
                                                let stream_id       = session.stream_id;
                                                let driver_name     = session.selected_driver.clone().unwrap();
                                                let channel_indices = session.channel_indices.clone();
                                                let subs            = self.subscribers_by_stream.clone();
                                                let running         = session.running.clone();
                                                let status          = session.status.clone();
                                                let error_banner    = self.error_banner.clone();
                                                running.store(true, Ordering::Relaxed);
                                                *status.lock().unwrap() = format!("ASIO: streaming {} ch from '{}'...", channel_indices.len(), driver_name);
                                                let rft = running.clone();
                                                thread::spawn(move || {
                                                    let result = audio_core::capture_asio_with_discovery(node_name, stream_name, stream_id, driver_name, channel_indices, subs, rft.clone());
                                                    match result {
                                                        Ok(()) => *status.lock().unwrap() = "Stopped.".to_string(),
                                                        Err(e) => { *status.lock().unwrap() = format!("Error: {e}"); *error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                    }
                                                    rft.store(false, Ordering::Relaxed);
                                                });
                                            }
                                            if !can_start {
                                                ui.label(egui::RichText::new(if session.selected_driver.is_none() { "← select a driver first" } else { "← select at least one channel" }).size(11.0).color(Theme::TEXT_SECONDARY));
                                            }
                                            if ui.add(egui::Button::new("🗑 Remove").fill(Theme::BG_SECONDARY).rounding(8.0)).clicked() { to_remove_asio = Some(idx); }
                                        }
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            let (color, icon) = if is_running { (Theme::ACCENT_PURPLE, "●") } else { (Theme::TEXT_SECONDARY, "○") };
                                            ui.label(egui::RichText::new(&*session.status.lock().unwrap()).color(Theme::TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(icon).size(14.0).color(color));
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }
                            if let Some(idx) = to_remove_asio { self.asio_publish_sessions.remove(idx); }
                            ui.add_space(6.0);
                            if ui.add(egui::Button::new(egui::RichText::new("+ Add ASIO Publish Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_PURPLE).rounding(8.0)).clicked() {
                                let id = self.next_id; self.next_id += 1;
                                self.asio_publish_sessions.push(AsioPublishSession {
                                    id, node_name: format!("OpenAudio Node {id}"), stream_name: format!("ASIO Stream {id}"),
                                    stream_id: 6000 + id as u32, selected_driver: None, driver_channel_count: 0,
                                    channel_indices: Vec::new(), running: Arc::new(AtomicBool::new(false)),
                                    status: Arc::new(Mutex::new("Not started.".to_string())),
                                    last_toggle: std::time::Instant::now() - Duration::from_secs(1),
                                });
                            }
                        });
                        ui.add_space(12.0);

                        // ── WASAPI SINGLE PUBLISH ──────────────────────────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.label(egui::RichText::new("📤 Publish Streams (single source)").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                            ui.add_space(10.0);
                            let mut to_remove: Option<usize> = None;
                            for (idx, session) in self.publish_sessions.iter_mut().enumerate() {
                                let is_running = session.running.load(Ordering::Relaxed);
                                let debounce_ok = session.last_toggle.elapsed() > Duration::from_millis(400);
                                egui::Frame::none().fill(Theme::BG_CARD).rounding(10.0).inner_margin(14.0).show(ui, |ui| {
                                    ui.add_enabled_ui(!is_running, |ui| {
                                        ui.checkbox(&mut session.is_loopback, egui::RichText::new("🔁 Loopback (capture output device)").color(Theme::TEXT_PRIMARY));
                                        ui.checkbox(&mut session.record, egui::RichText::new("⏺ Record this stream to WAV").color(Theme::TEXT_PRIMARY));
                                        ui.add_space(6.0);
                                        ui.horizontal(|ui| {
                                            ui.label(egui::RichText::new(if session.is_loopback { "Output Device:" } else { "Input Device:" }).color(Theme::TEXT_SECONDARY));
                                            let current_label = session.selected_input.clone().unwrap_or_else(|| "System Default".to_string());
                                            let device_list = if session.is_loopback { &self.output_devices } else { &self.input_devices };
                                            egui::ComboBox::from_id_source(format!("input_device_{}", session.id)).selected_text(current_label).width(280.0).show_ui(ui, |ui| {
                                                ui.selectable_value(&mut session.selected_input, None, "System Default");
                                                ui.selectable_value(&mut session.selected_input, Some(audio_core::NONE_DEVICE.to_string()), "🚫 None"); // ADDED
                                                for d in device_list { ui.selectable_value(&mut session.selected_input, Some(d.name.clone()), &d.name); }
                                            });
                                        });
                                    });
                                    ui.add_space(10.0);
                                    ui.columns(3, |cols| {
                                        cols[0].vertical(|ui| { ui.label(egui::RichText::new("Node Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.node_name).desired_width(150.0)); });
                                        cols[1].vertical(|ui| { ui.label(egui::RichText::new("Stream Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.stream_name).desired_width(150.0)); });
                                        cols[2].vertical(|ui| { ui.label(egui::RichText::new("Stream ID").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::DragValue::new(&mut session.stream_id)); });
                                    });
                                    ui.add_space(10.0); ui.separator(); ui.add_space(6.0);
                                    ui.horizontal(|ui| {
                                        if is_running {
                                            if ui.add_enabled(debounce_ok, egui::Button::new(egui::RichText::new("⏹ Stop").color(egui::Color32::WHITE)).fill(Theme::ACCENT_RED).rounding(8.0).min_size(egui::vec2(70.0,28.0))).clicked() {
                                                session.last_toggle = std::time::Instant::now(); session.running.store(false, Ordering::Relaxed); *session.status.lock().unwrap() = "Stopping...".to_string();
                                            }
                                        } else {
                                            if ui.add_enabled(debounce_ok, egui::Button::new(egui::RichText::new("▶ Start").color(egui::Color32::WHITE)).fill(Theme::ACCENT_GREEN).rounding(8.0).min_size(egui::vec2(70.0,28.0))).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                *self.error_banner.lock().unwrap() = None;
                                                let node_name = session.node_name.clone(); let stream_name = session.stream_name.clone();
                                                let stream_id = session.stream_id; let device_name = session.selected_input.clone();
                                                let is_loopback = session.is_loopback;
                                                let record_path = if session.record { Some(audio_core::generate_record_path(&format!("publish_{stream_id}"))) } else { None };
                                                let subs = self.subscribers_by_stream.clone(); let running = session.running.clone();
                                                let status = session.status.clone(); let error_banner = self.error_banner.clone();
                                                running.store(true, Ordering::Relaxed); *status.lock().unwrap() = "Advertising + transmitting...".to_string();
                                                let rft = running.clone();
                                                thread::spawn(move || {
                                                    let result = if is_loopback {
                                                        audio_core::transmit_loopback_with_discovery(node_name, stream_name, stream_id, device_name, subs, record_path, rft.clone())
                                                    } else {
                                                        audio_core::transmit_with_discovery(node_name, stream_name, stream_id, device_name, subs, record_path, rft.clone())
                                                    };
                                                    match result {
                                                        Ok(()) => *status.lock().unwrap() = "Stopped.".to_string(),
                                                        Err(e) => { *status.lock().unwrap() = format!("Error: {e}"); *error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                    }
                                                    rft.store(false, Ordering::Relaxed);
                                                });
                                            }
                                            if ui.add(egui::Button::new("🗑 Remove").fill(Theme::BG_SECONDARY).rounding(8.0)).clicked() { to_remove = Some(idx); }
                                        }
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            let (color, icon) = if is_running { (Theme::ACCENT_RED, "●") } else { (Theme::TEXT_SECONDARY, "○") };
                                            ui.label(egui::RichText::new(&*session.status.lock().unwrap()).color(Theme::TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(icon).size(14.0).color(color));
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }
                            if let Some(idx) = to_remove { self.publish_sessions.remove(idx); }
                            ui.add_space(6.0);
                            if ui.add(egui::Button::new(egui::RichText::new("+ Add Publish Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_BLUE).rounding(8.0)).clicked() {
                                let id = self.next_id; self.next_id += 1;
                                self.publish_sessions.push(PublishSession {
                                    id, node_name: format!("OpenAudio Node {id}"), stream_name: format!("Stream {id}"),
                                    stream_id: 3000 + id as u32, selected_input: None, is_loopback: false, record: false,
                                    running: Arc::new(AtomicBool::new(false)), status: Arc::new(Mutex::new("Not started.".to_string())),
                                    last_toggle: std::time::Instant::now() - Duration::from_secs(1),
                                });
                            }
                        });
                        ui.add_space(12.0);

                        // ── COMBINE PUBLISH ────────────────────────────────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.label(egui::RichText::new("📤 Publish Streams (combine multiple devices)").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                            ui.add_space(10.0);
                            let mut to_remove_combine: Option<usize> = None;
                            for (idx, session) in self.combine_publish_sessions.iter_mut().enumerate() {
                                let is_running = session.running.load(Ordering::Relaxed);
                                let debounce_ok = session.last_toggle.elapsed() > Duration::from_millis(400);
                                egui::Frame::none().fill(Theme::BG_CARD).rounding(10.0).inner_margin(14.0).show(ui, |ui| {
                                    ui.columns(3, |cols| {
                                        cols[0].vertical(|ui| { ui.label(egui::RichText::new("Node Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.node_name).desired_width(150.0)); });
                                        cols[1].vertical(|ui| { ui.label(egui::RichText::new("Stream Name").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.stream_name).desired_width(150.0)); });
                                        cols[2].vertical(|ui| { ui.label(egui::RichText::new("Stream ID").size(11.0).color(Theme::TEXT_SECONDARY)); ui.add_enabled(!is_running, egui::DragValue::new(&mut session.stream_id)); });
                                    });
                                    ui.add_space(10.0);
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new("Channels:").color(Theme::TEXT_SECONDARY));
                                        let mut count = session.channel_count;
                                        if ui.add_enabled(!is_running, egui::DragValue::new(&mut count).clamp_range(1..=64)).changed() {
                                            session.channel_count = count; session.channel_sources.resize(count, (None, false));
                                        }
                                    });
                                    ui.add_space(8.0);
                                    ui.add_enabled_ui(!is_running, |ui| {
                                        if ui.add(egui::Button::new(egui::RichText::new("🔧 Auto-create SAR Recording endpoints").color(egui::Color32::WHITE)).fill(Theme::ACCENT_BLUE).rounding(8.0)).clicked() {
                                            match audio_core::ensure_openaudio_endpoints(&session.session_tag, audio_core::EndpointKind::Recording, session.channel_count) {
                                                Ok(names) => { for (i, name) in names.into_iter().enumerate() { session.channel_sources[i] = (Some(name), false); } *self.info_banner.lock().unwrap() = Some("SAR endpoints created. Restart your DAW's ASIO connection to SAR, route tracks to the new 'OpenAudio-...' Recording endpoints, then Refresh Devices before hitting Start.".to_string()); }
                                                Err(e) => { *self.error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                            }
                                        }
                                        ui.checkbox(&mut session.record, egui::RichText::new("⏺ Record each channel separately to WAV").color(Theme::TEXT_PRIMARY));
                                        ui.add_space(8.0);
                                        ui.label(egui::RichText::new("Channel Sources:").size(12.0).color(Theme::TEXT_SECONDARY));
                                        for (ch_idx, (dev, is_loopback)) in session.channel_sources.iter_mut().enumerate() {
                                            ui.horizontal(|ui| {
                                                ui.label(egui::RichText::new(format!("Ch {ch_idx}:")).color(Theme::TEXT_SECONDARY));
                                                ui.checkbox(is_loopback, "Loopback");
                                                let current_label = dev.clone().unwrap_or_else(|| "System Default".to_string());
                                                let device_list = if *is_loopback { &self.output_devices } else { &self.input_devices };
                                                egui::ComboBox::from_id_source(format!("combine_device_{}_{ch_idx}", session.id)).selected_text(current_label).width(220.0).show_ui(ui, |ui| {
                                                    ui.selectable_value(dev, None, "System Default");
                                                    ui.selectable_value(dev, Some(audio_core::NONE_DEVICE.to_string()), "🚫 None"); // ADDED
                                                    for d in device_list { ui.selectable_value(dev, Some(d.name.clone()), &d.name); }
                                                });
                                            });
                                        }
                                    });
                                    ui.add_space(10.0); ui.separator(); ui.add_space(6.0);
                                    ui.horizontal(|ui| {
                                        if is_running {
                                            if ui.add_enabled(debounce_ok, egui::Button::new(egui::RichText::new("⏹ Stop").color(egui::Color32::WHITE)).fill(Theme::ACCENT_RED).rounding(8.0).min_size(egui::vec2(70.0,28.0))).clicked() {
                                                session.last_toggle = std::time::Instant::now(); session.running.store(false, Ordering::Relaxed); *session.status.lock().unwrap() = "Stopping...".to_string();
                                            }
                                        } else {
                                            if ui.add_enabled(debounce_ok, egui::Button::new(egui::RichText::new("▶ Start").color(egui::Color32::WHITE)).fill(Theme::ACCENT_GREEN).rounding(8.0).min_size(egui::vec2(70.0,28.0))).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                *self.error_banner.lock().unwrap() = None;
                                                let node_name = session.node_name.clone(); let stream_name = session.stream_name.clone(); let stream_id = session.stream_id;
                                                let sources: Vec<audio_core::ChannelSource> = session.channel_sources.iter().map(|(name, loopback)| audio_core::ChannelSource { device_name: name.clone(), is_loopback: *loopback }).collect();
                                                let record_each = session.record; let subs = self.subscribers_by_stream.clone();
                                                let running = session.running.clone(); let status = session.status.clone(); let error_banner = self.error_banner.clone();
                                                running.store(true, Ordering::Relaxed); *status.lock().unwrap() = format!("Advertising + combining {} channel(s)...", sources.len());
                                                let rft = running.clone();
                                                thread::spawn(move || {
                                                    let result = audio_core::capture_and_combine_with_discovery(node_name, stream_name, stream_id, sources, subs, record_each, rft.clone());
                                                    match result {
                                                        Ok(()) => *status.lock().unwrap() = "Stopped.".to_string(),
                                                        Err(e) => { *status.lock().unwrap() = format!("Error: {e}"); *error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                    }
                                                    rft.store(false, Ordering::Relaxed);
                                                });
                                            }
                                            if ui.add(egui::Button::new("🗑 Remove").fill(Theme::BG_SECONDARY).rounding(8.0)).clicked() {
                                                let _ = audio_core::remove_openaudio_endpoints(&session.session_tag); to_remove_combine = Some(idx);
                                            }
                                        }
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            let (color, icon) = if is_running { (Theme::ACCENT_RED, "●") } else { (Theme::TEXT_SECONDARY, "○") };
                                            ui.label(egui::RichText::new(&*session.status.lock().unwrap()).color(Theme::TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(icon).size(14.0).color(color));
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }
                            if let Some(idx) = to_remove_combine { self.combine_publish_sessions.remove(idx); }
                            ui.add_space(6.0);
                            if ui.add(egui::Button::new(egui::RichText::new("+ Add Combine Publish Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_BLUE).rounding(8.0)).clicked() {
                                let id = self.next_id; self.next_id += 1;
                                self.combine_publish_sessions.push(CombinePublishSession {
                                    id, session_tag: format!("combine{id}"), node_name: format!("OpenAudio Node {id}"),
                                    stream_name: format!("Combined Stream {id}"), stream_id: 4000 + id as u32,
                                    channel_count: 2, channel_sources: vec![(None,false),(None,false)], record: false,
                                    running: Arc::new(AtomicBool::new(false)), status: Arc::new(Mutex::new("Not started.".to_string())),
                                    last_toggle: std::time::Instant::now() - Duration::from_secs(1),
                                });
                            }
                        });
                        ui.add_space(12.0);

// ══════════════════════════════════════════════════════════════════
// PART 3 STARTS WITH: // ── SUBSCRIBE STREAMS (MIXED TO ONE OUTPUT) ──
// ══════════════════════════════════════════════════════════════════

                        // ── SUBSCRIBE STREAMS (MIXED TO ONE OUTPUT) ────────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.label(egui::RichText::new("📥 Subscribe Streams (mixed to one output)").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                            ui.add_space(10.0);
                            let mut to_remove_sub: Option<usize> = None;

                            for (idx, session) in self.subscribe_sessions.iter_mut().enumerate() {
                                let is_running = session.running.load(Ordering::Relaxed);
                                let debounce_ok = session.last_toggle.elapsed() > Duration::from_millis(400);

                                egui::Frame::none().fill(Theme::BG_CARD).rounding(10.0).inner_margin(14.0).show(ui, |ui| {
                                    ui.add_enabled_ui(!is_running, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(egui::RichText::new("Output Device:").color(Theme::TEXT_SECONDARY));
                                            let current_label = session.selected_output.clone().unwrap_or_else(|| "System Default".to_string());
                                            egui::ComboBox::from_id_source(format!("output_device_{}", session.id))
                                                .selected_text(current_label).width(280.0)
                                                .show_ui(ui, |ui| {
                                                    ui.selectable_value(&mut session.selected_output, None, "System Default");
                                                    ui.selectable_value(&mut session.selected_output, Some(audio_core::NONE_DEVICE.to_string()), "🚫 None"); // ADDED
                                                    for d in &self.output_devices {
                                                        ui.selectable_value(&mut session.selected_output, Some(d.name.clone()), &d.name);
                                                    }
                                                });
                                        });
                                    });

                                    ui.add_space(8.0);
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new("Local Port:").color(Theme::TEXT_SECONDARY));
                                        ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.bind_port).desired_width(80.0));
                                    });

                                    ui.add_space(8.0);
                                    ui.horizontal(|ui| {
                                        let mut vol = audio_core::get_volume(&session.volume);
                                        if ui.add(egui::Slider::new(&mut vol, 0.0..=1.5).text("Volume")).changed() {
                                            audio_core::set_volume(&session.volume, vol);
                                        }
                                    });

                                    ui.add_enabled_ui(!is_running, |ui| {
                                        ui.checkbox(&mut session.record, egui::RichText::new("⏺ Record mixed output to WAV").color(Theme::TEXT_PRIMARY));
                                    });

                                    ui.add_space(8.0);
                                    ui.label(egui::RichText::new("Discovered Streams:").size(12.0).color(Theme::TEXT_SECONDARY));
                                    {
                                        let directory = self.discovery_directory.lock().unwrap();
                                        if directory.is_empty() {
                                            ui.label(egui::RichText::new("(none yet)").color(Theme::TEXT_SECONDARY).italics());
                                        } else {
                                            for (node_id, node) in directory.iter() {
                                                let label = format!(
                                                    "{} — \"{}\" ({}ch, {})",
                                                    node.node_name, node.stream_name, node.channel_count, node.ip
                                                );
                                                ui.radio_value(&mut session.selected_discovered_node_id, Some(node_id.clone()), label);
                                            }
                                        }
                                    }

                                    ui.add_space(10.0);
                                    ui.separator();
                                    ui.add_space(6.0);

                                    ui.horizontal(|ui| {
                                        if is_running {
                                            if ui.add_enabled(debounce_ok,
                                                egui::Button::new(egui::RichText::new("⏹ Stop").color(egui::Color32::WHITE))
                                                    .fill(Theme::ACCENT_RED).rounding(8.0).min_size(egui::vec2(70.0, 28.0))
                                            ).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                session.running.store(false, Ordering::Relaxed);
                                                *session.status.lock().unwrap() = "Stopping...".to_string();
                                            }
                                        } else {
                                            if ui.add_enabled(debounce_ok,
                                                egui::Button::new(egui::RichText::new("▶ Start").color(egui::Color32::WHITE))
                                                    .fill(Theme::ACCENT_GREEN).rounding(8.0).min_size(egui::vec2(70.0, 28.0))
                                            ).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                *self.error_banner.lock().unwrap() = None;

                                                let Some(selected_id) = session.selected_discovered_node_id.clone() else {
                                                    *self.error_banner.lock().unwrap() = Some("Select a discovered stream first.".to_string());
                                                    return;
                                                };
                                                let node = self.discovery_directory.lock().unwrap().get(&selected_id).cloned();
                                                let Some(node) = node else {
                                                    *self.error_banner.lock().unwrap() = Some("That stream is no longer available.".to_string());
                                                    return;
                                                };
                                                let port: u16 = match session.bind_port.parse() {
                                                    Ok(p) => p,
                                                    Err(_) => { *self.error_banner.lock().unwrap() = Some(format!("'{}' isn't a valid port.", session.bind_port)); return; }
                                                };
                                                if let Err(e) = audio_core::send_subscribe_request(&node.ip, node.control_port, node.stream_id, port) {
                                                    *self.error_banner.lock().unwrap() = Some(friendly_error(&e)); return;
                                                }
                                                let bind_addr    = format!("0.0.0.0:{port}");
                                                let device_name  = session.selected_output.clone();
                                                let volume       = session.volume.clone();
                                                let record_path  = if session.record { Some(audio_core::generate_record_path(&format!("subscribe_{port}"))) } else { None };
                                                let running      = session.running.clone();
                                                let status       = session.status.clone();
                                                let error_banner = self.error_banner.clone();
                                                running.store(true, Ordering::Relaxed);
                                                *status.lock().unwrap() = format!("Subscribed to '{}' on {}...", node.stream_name, node.node_name);
                                                let rft = running.clone();
                                                thread::spawn(move || {
                                                    let result = audio_core::receive_and_play_bus_with_volume(&bind_addr, device_name, volume, record_path, rft.clone());
                                                    match result {
                                                        Ok(()) => *status.lock().unwrap() = "Stopped.".to_string(),
                                                        Err(e) => { *status.lock().unwrap() = format!("Error: {e}"); *error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                    }
                                                    rft.store(false, Ordering::Relaxed);
                                                });
                                            }
                                            if ui.add(egui::Button::new("🗑 Remove").fill(Theme::BG_SECONDARY).rounding(8.0)).clicked() {
                                                to_remove_sub = Some(idx);
                                            }
                                        }
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            let (color, icon) = if is_running { (Theme::ACCENT_GREEN, "●") } else { (Theme::TEXT_SECONDARY, "○") };
                                            ui.label(egui::RichText::new(&*session.status.lock().unwrap()).color(Theme::TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(icon).size(14.0).color(color));
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }

                            if let Some(idx) = to_remove_sub { self.subscribe_sessions.remove(idx); }
                            ui.add_space(6.0);
                            if ui.add(egui::Button::new(egui::RichText::new("+ Add Subscribe Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_BLUE).rounding(8.0)).clicked() {
                                let id = self.next_id; self.next_id += 1;
                                self.subscribe_sessions.push(SubscribeSession {
                                    id, selected_discovered_node_id: None,
                                    bind_port: (6980 + id as u16).to_string(),
                                    selected_output: None,
                                    volume: audio_core::new_volume_control(1.0),
                                    record: false,
                                    running: Arc::new(AtomicBool::new(false)),
                                    status: Arc::new(Mutex::new("Not started.".to_string())),
                                    last_toggle: std::time::Instant::now() - Duration::from_secs(1),
                                });
                            }
                        });

                        ui.add_space(12.0);

                        // ── SPLIT SUBSCRIBE (ONE DEVICE PER CHANNEL) ───────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.label(egui::RichText::new("📥 Subscribe Streams (split — one device per channel)").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                            ui.add_space(10.0);
                            let mut to_remove_split: Option<usize> = None;

                            for (idx, session) in self.split_subscribe_sessions.iter_mut().enumerate() {
                                let is_running = session.running.load(Ordering::Relaxed);
                                let debounce_ok = session.last_toggle.elapsed() > Duration::from_millis(400);

                                egui::Frame::none().fill(Theme::BG_CARD).rounding(10.0).inner_margin(14.0).show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new("Local Port:").color(Theme::TEXT_SECONDARY));
                                        ui.add_enabled(!is_running, egui::TextEdit::singleline(&mut session.bind_port).desired_width(80.0));
                                    });

                                    ui.add_space(8.0);
                                    ui.label(egui::RichText::new("Discovered Streams:").size(12.0).color(Theme::TEXT_SECONDARY));
                                    let mut newly_selected_channel_count: Option<usize> = None;
                                    {
                                        let directory = self.discovery_directory.lock().unwrap();
                                        if directory.is_empty() {
                                            ui.label(egui::RichText::new("(none yet)").color(Theme::TEXT_SECONDARY).italics());
                                        } else {
                                            for (node_id, node) in directory.iter() {
                                                let label = format!(
                                                    "{} — \"{}\" ({}ch, {})",
                                                    node.node_name, node.stream_name, node.channel_count, node.ip
                                                );
                                                let was_selected = session.selected_discovered_node_id.as_deref() == Some(node_id.as_str());
                                                ui.radio_value(&mut session.selected_discovered_node_id, Some(node_id.clone()), label);
                                                let now_selected = session.selected_discovered_node_id.as_deref() == Some(node_id.as_str());
                                                if now_selected && !was_selected {
                                                    newly_selected_channel_count = Some(node.channel_count as usize);
                                                }
                                            }
                                        }
                                    }
                                    if let Some(count) = newly_selected_channel_count {
                                        session.channel_devices = vec![None; count];
                                    }

                                    ui.add_space(8.0);
                                    ui.add_enabled_ui(!is_running, |ui| {
                                        if !session.channel_devices.is_empty() {
                                            if ui.add(
                                                egui::Button::new(egui::RichText::new("🔧 Auto-create SAR Playback endpoints").color(egui::Color32::WHITE))
                                                    .fill(Theme::ACCENT_BLUE).rounding(8.0)
                                            ).clicked() {
                                                match audio_core::ensure_openaudio_endpoints(&session.session_tag, audio_core::EndpointKind::Playback, session.channel_devices.len()) {
                                                    Ok(names) => {
                                                        for (i, name) in names.into_iter().enumerate() { session.channel_devices[i] = Some(name); }
                                                        *self.info_banner.lock().unwrap() = Some("SAR endpoints created. Restart your DAW's ASIO connection to SAR, arm tracks on the new 'OpenAudio-...' Playback endpoints, then Refresh Devices before hitting Start.".to_string());
                                                    }
                                                    Err(e) => { *self.error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                }
                                            }

                                            ui.checkbox(&mut session.record, egui::RichText::new("⏺ Record each channel separately to WAV").color(Theme::TEXT_PRIMARY));
                                            ui.add_space(8.0);
                                            ui.label(egui::RichText::new(format!("Channel Devices ({}):", session.channel_devices.len())).size(12.0).color(Theme::TEXT_SECONDARY));
                                            for (ch_idx, dev) in session.channel_devices.iter_mut().enumerate() {
                                                ui.horizontal(|ui| {
                                                    ui.label(egui::RichText::new(format!("Ch {ch_idx}:")).color(Theme::TEXT_SECONDARY));
                                                    let current_label = dev.clone().unwrap_or_else(|| "System Default".to_string());
                                                    egui::ComboBox::from_id_source(format!("split_device_{}_{ch_idx}", session.id))
                                                        .selected_text(current_label).width(220.0)
                                                        .show_ui(ui, |ui| {
                                                            ui.selectable_value(dev, None, "System Default");
                                                            ui.selectable_value(dev, Some(audio_core::NONE_DEVICE.to_string()), "🚫 None"); // ADDED
                                                            for d in &self.output_devices {
                                                                ui.selectable_value(dev, Some(d.name.clone()), &d.name);
                                                            }
                                                        });
                                                });
                                            }
                                        }
                                    });

                                    ui.add_space(10.0);
                                    ui.separator();
                                    ui.add_space(6.0);

                                    ui.horizontal(|ui| {
                                        if is_running {
                                            if ui.add_enabled(debounce_ok,
                                                egui::Button::new(egui::RichText::new("⏹ Stop").color(egui::Color32::WHITE))
                                                    .fill(Theme::ACCENT_RED).rounding(8.0).min_size(egui::vec2(70.0, 28.0))
                                            ).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                session.running.store(false, Ordering::Relaxed);
                                                *session.status.lock().unwrap() = "Stopping...".to_string();
                                            }
                                        } else {
                                            if ui.add_enabled(debounce_ok,
                                                egui::Button::new(egui::RichText::new("▶ Start").color(egui::Color32::WHITE))
                                                    .fill(Theme::ACCENT_GREEN).rounding(8.0).min_size(egui::vec2(70.0, 28.0))
                                            ).clicked() {
                                                session.last_toggle = std::time::Instant::now();
                                                *self.error_banner.lock().unwrap() = None;

                                                let Some(selected_id) = session.selected_discovered_node_id.clone() else {
                                                    *self.error_banner.lock().unwrap() = Some("Select a discovered stream first.".to_string()); return;
                                                };
                                                let node = self.discovery_directory.lock().unwrap().get(&selected_id).cloned();
                                                let Some(node) = node else {
                                                    *self.error_banner.lock().unwrap() = Some("That stream is no longer available.".to_string()); return;
                                                };
                                                let port: u16 = match session.bind_port.parse() {
                                                    Ok(p) => p,
                                                    Err(_) => { *self.error_banner.lock().unwrap() = Some(format!("'{}' isn't a valid port.", session.bind_port)); return; }
                                                };
                                                if let Err(e) = audio_core::send_subscribe_request(&node.ip, node.control_port, node.stream_id, port) {
                                                    *self.error_banner.lock().unwrap() = Some(friendly_error(&e)); return;
                                                }
                                                let bind_addr       = format!("0.0.0.0:{port}");
                                                let device_targets  = session.channel_devices.clone();
                                                let record_each     = session.record;
                                                let running         = session.running.clone();
                                                let status          = session.status.clone();
                                                let error_banner    = self.error_banner.clone();
                                                running.store(true, Ordering::Relaxed);
                                                *status.lock().unwrap() = format!("Split-subscribed to '{}' ({} ch)...", node.stream_name, device_targets.len());
                                                let rft = running.clone();
                                                thread::spawn(move || {
                                                    let result = audio_core::receive_and_split_to_devices(&bind_addr, device_targets, record_each, rft.clone());
                                                    match result {
                                                        Ok(()) => *status.lock().unwrap() = "Stopped.".to_string(),
                                                        Err(e) => { *status.lock().unwrap() = format!("Error: {e}"); *error_banner.lock().unwrap() = Some(friendly_error(&e)); }
                                                    }
                                                    rft.store(false, Ordering::Relaxed);
                                                });
                                            }
                                            if ui.add(egui::Button::new("🗑 Remove").fill(Theme::BG_SECONDARY).rounding(8.0)).clicked() {
                                                let _ = audio_core::remove_openaudio_endpoints(&session.session_tag);
                                                to_remove_split = Some(idx);
                                            }
                                        }
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            let (color, icon) = if is_running { (Theme::ACCENT_GREEN, "●") } else { (Theme::TEXT_SECONDARY, "○") };
                                            ui.label(egui::RichText::new(&*session.status.lock().unwrap()).color(Theme::TEXT_SECONDARY));
                                            ui.label(egui::RichText::new(icon).size(14.0).color(color));
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }

                            if let Some(idx) = to_remove_split { self.split_subscribe_sessions.remove(idx); }
                            ui.add_space(6.0);
                            if ui.add(egui::Button::new(egui::RichText::new("+ Add Split Subscribe Stream").color(egui::Color32::WHITE)).fill(Theme::ACCENT_BLUE).rounding(8.0)).clicked() {
                                let id = self.next_id; self.next_id += 1;
                                self.split_subscribe_sessions.push(SplitSubscribeSession {
                                    id, session_tag: format!("split{id}"),
                                    selected_discovered_node_id: None,
                                    bind_port: (6990 + id as u16).to_string(),
                                    channel_devices: Vec::new(),
                                    record: false,
                                    running: Arc::new(AtomicBool::new(false)),
                                    status: Arc::new(Mutex::new("Not started.".to_string())),
                                    last_toggle: std::time::Instant::now() - Duration::from_secs(1),
                                });
                            }
                        });

                        ui.add_space(12.0);

                                                // ── WEB PLAYBACK (BROWSER) ──────────────────────────
                        egui::Frame::none().fill(Theme::BG_SECONDARY).rounding(12.0).inner_margin(16.0).show(ui, |ui| {
                            ui.label(egui::RichText::new("🌐 Browser Playback").size(18.0).strong().color(Theme::TEXT_PRIMARY));
                            ui.add_space(8.0);
                            ui.label(
                                egui::RichText::new("Always running. Open this on any device on your LAN — it lists every discovered stream and lets you pick one.")
                                    .size(12.0).color(Theme::TEXT_SECONDARY)
                            );
                            ui.add_space(8.0);
                            egui::Frame::none().fill(Theme::BG_CARD).rounding(8.0).inner_margin(10.0).show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new("●").color(Theme::ACCENT_GREEN));
                                    ui.monospace(egui::RichText::new("http://<this-machine-IP>:7100/").color(Theme::TEXT_PRIMARY));
                                });
                            });
                        });
                        ui.add_space(12.0);


// ══════════════════════════════════════════════════════════════════
// PART 4 STARTS WITH: ui.add_space(20.0); (closing braces + icon + main)
// ══════════════════════════════════════════════════════════════════
                        ui.add_space(20.0);

                    }); // end ScrollArea
                }); // end CentralPanel
        ctx.request_repaint_after(Duration::from_millis(100));
    } // end fn update
} // end impl eframe::App

// ═══════════════════════════════════════════════════════════════════════════
// ICON
// ═══════════════════════════════════════════════════════════════════════════

fn load_icon() -> egui::IconData {
    let size = 64u32;
    let mut rgba = Vec::with_capacity((size * size * 4) as usize);
    let cx = size as f32 / 2.0;
    let cy = size as f32 / 2.0;
    for y in 0..size {
        for x in 0..size {
            let dx = x as f32 - cx;
            let dy = y as f32 - cy;
            let dist = (dx * dx + dy * dy).sqrt();
            let outer = size as f32 / 2.0;
            let inner = outer * 0.55;
            if dist <= outer && dist >= inner {
                let angle = dy.atan2(dx);
                let wave = ((angle * 3.0).sin() * 0.5 + 0.5) * 0.4 + 0.6;
                rgba.push((0.0f32.max(wave * 100.0)) as u8);
                rgba.push((0.0f32.max(wave * 180.0)) as u8);
                rgba.push(255u8);
                rgba.push(255u8);
            } else if dist < inner {
                let pulse = ((dist / inner * std::f32::consts::PI).sin()).abs();
                rgba.push((pulse * 80.0) as u8);
                rgba.push((pulse * 140.0) as u8);
                rgba.push((pulse * 220.0) as u8);
                rgba.push(200u8);
            } else {
                rgba.push(0); rgba.push(0); rgba.push(0); rgba.push(0);
            }
        }
    }
    egui::IconData { rgba, width: size, height: size }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

fn main() -> eframe::Result<()> {
    audio_core::prepare_realtime_process();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 800.0])
            .with_min_inner_size([700.0, 500.0])
            .with_icon(load_icon()),
        ..Default::default()
    };

    eframe::run_native(
        "Ferronme's Open Audio",
        options,
        Box::new(|_cc| Ok(Box::new(OpenAudioApp::default()))),
    )
}