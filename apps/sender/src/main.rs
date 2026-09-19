mod asio_subscribe_ui;
mod diagnostic_panel;
mod file_dialog;
mod preset;
mod signal_monitor;
mod ui_shell;

use asio_subscribe_ui::AsioSubscribePanel;
use diagnostic_panel::DiagnosticPanel;
use signal_monitor::SignalMonitor;
use ui_shell::{AppPage, UiSummary};
use preset::{
    AsioPublishPreset, BrowserPreset, CombinePublishPreset, PresetFile,
    PublishPreset, SplitSubscribePreset, SubscribePreset,
};

use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

fn hardware_api_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "WASAPI"
    } else if cfg!(target_os = "macos") {
        "CoreAudio"
    } else {
        "Hardware"
    }
}

static ASIO_DEVICE_LOCKS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

pub(crate) fn reserve_asio_device(driver_name: &str) -> Result<(), String> {
    let driver_name = driver_name.trim();
    if driver_name.is_empty() {
        return Err("ASIO driver name is empty".to_string());
    }

    let mut locks = ASIO_DEVICE_LOCKS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if locks.contains(driver_name) {
        return Err(format!(
            "ASIO driver '{}' is already in use by another OpenAudio ASIO session. Stop the other ASIO publish or subscribe session before starting a new one.",
            driver_name
        ));
    }

    locks.insert(driver_name.to_string());
    Ok(())
}

pub(crate) fn release_asio_device(driver_name: &str) {
    let driver_name = driver_name.trim();
    if driver_name.is_empty() {
        return;
    }

    if let Ok(mut locks) = ASIO_DEVICE_LOCKS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
    {
        locks.remove(driver_name);
    }
}

#[cfg(test)]
mod tests {
    use super::{release_asio_device, reserve_asio_device};

    #[test]
    fn asio_device_lock_rejects_duplicate_driver() {
        let driver = "duplicate-lock-test";
        release_asio_device(driver);

        assert!(reserve_asio_device(driver).is_ok());
        assert!(reserve_asio_device(driver).is_err());

        release_asio_device(driver);
    }
}

// ============================================================================
// PUBLISH SESSION TYPES
// ============================================================================

struct PublishSession {
    id: u64,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    selected_input: Option<String>,
    is_loopback: bool,
    record: bool,
    channel_labels: Vec<String>,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
}

struct CombinePublishSession {
    id: u64,
    session_tag: String,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    channel_count: usize,
    channel_sources: Vec<(Option<String>, bool)>,
    channel_labels: Vec<String>,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
}

struct AsioPublishSession {
    id: u64,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    selected_driver: Option<String>,
    driver_channel_count: usize,
    channel_indices: Vec<usize>,
    channel_labels: Vec<String>,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
}

// ============================================================================
// SUBSCRIBE SESSION TYPES
// ============================================================================

struct SubscribeSession {
    id: u64,
    selected_discovered_node_id: Option<String>,
    bind_port: String,
    selected_output: Option<String>,
    volume: audio_core::VolumeControl,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
}

struct SplitSubscribeSession {
    id: u64,
    session_tag: String,
    selected_discovered_node_id: Option<String>,
    bind_port: String,
    channel_devices: Vec<Option<String>>,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
    record: bool,
}

// Browser playback uses one application-level gateway rather than one
// gateway per stream. It is disabled by default and explicitly started
// or stopped from the Browser Sharing page.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BrowserAccessMode {
    PasswordProtected,
    OpenLan,
}

// ============================================================================
// APPLICATION STATE
// ============================================================================

struct OpenAudioApp {
    next_id: u64,

    // Top-level desktop navigation.
    active_page: AppPage,

    // Publish sessions.
    publish_sessions: Vec<PublishSession>,
    combine_publish_sessions: Vec<CombinePublishSession>,
    asio_publish_sessions: Vec<AsioPublishSession>,

    // Independent feature panels.
    asio_subscribe_panel: AsioSubscribePanel,
    diagnostic_panel: DiagnosticPanel,
    signal_monitor: SignalMonitor,

    // Browser gateway configuration.
    browser_access_mode: BrowserAccessMode,
    browser_password: String,
    browser_password_confirmation: String,
    browser_show_password: bool,
    browser_gateway_running: Arc<AtomicBool>,
    browser_gateway_thread_active: Arc<AtomicBool>,
    browser_gateway_status: Arc<Mutex<String>>,
    browser_gateway_last_toggle: Instant,

    // Subscribe sessions.
    subscribe_sessions: Vec<SubscribeSession>,
    split_subscribe_sessions: Vec<SplitSubscribeSession>,

    // Available devices and drivers.
    input_devices: Vec<audio_core::DeviceInfo>,
    output_devices: Vec<audio_core::DeviceInfo>,
    asio_drivers: Vec<audio_core::AsioDriverInfo>,
    asio_drivers_error: Option<String>,

    // Shared networking state.
    discovery_directory: Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>>,
    subscribers_by_stream: audio_core::SubscriberRegistry,

    // User notifications.
    error_banner: Arc<Mutex<Option<String>>>,
    info_banner: Arc<Mutex<Option<String>>>,

    // Asynchronous device refresh.
    refreshing_devices: Arc<AtomicBool>,
    pending_device_refresh:
        Arc<Mutex<Option<(Vec<audio_core::DeviceInfo>, Vec<audio_core::DeviceInfo>)>>>,
}

// ============================================================================
// DEFAULT APPLICATION INITIALIZATION
// ============================================================================

impl Default for OpenAudioApp {
    fn default() -> Self {
        let discovery_directory: Arc<
            Mutex<HashMap<String, audio_core::DiscoveredNode>>,
        > = Arc::new(Mutex::new(HashMap::new()));

        let subscribers_by_stream: audio_core::SubscriberRegistry =
            Arc::new(Mutex::new(HashMap::new()));

        // Discovery and publisher-control listeners live for the lifetime
        // of the desktop application.
        let always_on = Arc::new(AtomicBool::new(true));

        let directory_for_discovery = discovery_directory.clone();
        let discovery_running = always_on.clone();

        thread::Builder::new()
            .name("openaudio-discovery-listener".to_string())
            .spawn(move || {
                audio_core::ensure_realtime_audio_thread();

                if let Err(error) = audio_core::start_discovery_listener(
                    directory_for_discovery,
                    discovery_running,
                ) {
                    eprintln!("Discovery listener error: {error}");
                }
            })
            .unwrap_or_else(|error| {
                panic!("Failed to start discovery listener: {error}");
            });

        let registry_for_control = subscribers_by_stream.clone();
        let control_running = always_on;

        thread::Builder::new()
            .name("openaudio-control-listener".to_string())
            .spawn(move || {
                audio_core::ensure_realtime_audio_thread();

                if let Err(error) = audio_core::start_control_listener(
                    registry_for_control,
                    control_running,
                ) {
                    eprintln!("Control listener error: {error}");
                }
            })
            .unwrap_or_else(|error| {
                panic!("Failed to start control listener: {error}");
            });

        // The application owns one configurable browser gateway. It remains
        // stopped until explicitly enabled by the user.
        let (asio_drivers, asio_drivers_error) =
            match audio_core::list_asio_drivers() {
                Ok(drivers) => (drivers, None),
                Err(error) => (Vec::new(), Some(error)),
            };

        Self {
            next_id: 1,
            active_page: AppPage::Overview,

            publish_sessions: Vec::new(),
            combine_publish_sessions: Vec::new(),
            asio_publish_sessions: Vec::new(),

            asio_subscribe_panel: AsioSubscribePanel::default(),
            diagnostic_panel: DiagnosticPanel::default(),
            signal_monitor: SignalMonitor::default(),

            browser_access_mode: BrowserAccessMode::PasswordProtected,
            browser_password: String::new(),
            browser_password_confirmation: String::new(),
            browser_show_password: false,
            browser_gateway_running: Arc::new(AtomicBool::new(false)),
            browser_gateway_thread_active: Arc::new(AtomicBool::new(false)),
            browser_gateway_status: Arc::new(Mutex::new(
                "Gateway stopped. Browser playback is not exposed.".to_string(),
            )),
            browser_gateway_last_toggle: Instant::now() - Duration::from_secs(1),

            subscribe_sessions: Vec::new(),
            split_subscribe_sessions: Vec::new(),

            input_devices: audio_core::list_input_devices(),
            output_devices: audio_core::list_output_devices(),
            asio_drivers,
            asio_drivers_error,

            discovery_directory,
            subscribers_by_stream,

            error_banner: Arc::new(Mutex::new(None)),
            info_banner: Arc::new(Mutex::new(None)),

            refreshing_devices: Arc::new(AtomicBool::new(false)),
            pending_device_refresh: Arc::new(Mutex::new(None)),
        }
    }
}

// ============================================================================
// APPLICATION HELPERS
// ============================================================================

impl OpenAudioApp {
    fn ui_summary(&self) -> UiSummary {
        let configured_publishers = self.publish_sessions.len()
            + self.combine_publish_sessions.len()
            + self.asio_publish_sessions.len();

        let running_publishers = self
            .publish_sessions
            .iter()
            .filter(|session| session.running.load(Ordering::Relaxed))
            .count()
            + self
                .combine_publish_sessions
                .iter()
                .filter(|session| session.running.load(Ordering::Relaxed))
                .count()
            + self
                .asio_publish_sessions
                .iter()
                .filter(|session| session.running.load(Ordering::Relaxed))
                .count();

        // ASIO Subscribe sessions are managed privately by
        // AsioSubscribePanel. These counts currently cover the WASAPI
        // mixed and split subscribers owned directly by OpenAudioApp.
        let configured_subscribers =
            self.subscribe_sessions.len() + self.split_subscribe_sessions.len();

        let running_subscribers = self
            .subscribe_sessions
            .iter()
            .filter(|session| session.running.load(Ordering::Relaxed))
            .count()
            + self
                .split_subscribe_sessions
                .iter()
                .filter(|session| session.running.load(Ordering::Relaxed))
                .count();

        let discovered_streams = match self.discovery_directory.lock() {
            Ok(directory) => directory.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };

        let gateway_running = self
            .browser_gateway_running
            .load(Ordering::Acquire);

        let gateway_thread_active = self
            .browser_gateway_thread_active
            .load(Ordering::Acquire);

        UiSummary {
            configured_publishers,
            running_publishers,
            configured_subscribers,
            running_subscribers,
            discovered_streams,
            gateway_running,
            gateway_stopping: gateway_thread_active && !gateway_running,
        }
    }

        fn refresh_devices(&mut self) {
        if self.refreshing_devices.swap(true, Ordering::AcqRel) {
            return;
        }

        let refreshing = Arc::clone(&self.refreshing_devices);
        let pending = Arc::clone(&self.pending_device_refresh);
        let error_banner = Arc::clone(&self.error_banner);

        let spawn_result = thread::Builder::new()
            .name("openaudio-device-refresh".to_string())
            .spawn(move || {
                let inputs = audio_core::list_input_devices();
                let outputs = audio_core::list_output_devices();

                match pending.lock() {
                    Ok(mut destination) => {
                        *destination = Some((inputs, outputs));
                    }
                    Err(poisoned) => {
                        *poisoned.into_inner() = Some((inputs, outputs));
                    }
                }

                refreshing.store(false, Ordering::Release);
            });

        if let Err(error) = spawn_result {
            self.refreshing_devices
                .store(false, Ordering::Release);

            set_shared_optional_message(
                &error_banner,
                Some(format!(
                    "Could not start the device refresh worker: {error}"
                )),
            );
        }
    }





    fn apply_pending_device_refresh(&mut self) {
        let pending_result = match self.pending_device_refresh.lock() {
            Ok(mut pending) => pending.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };

        let Some((inputs, outputs)) = pending_result else {
            return;
        };

        self.input_devices = inputs;
        self.output_devices = outputs;

        match audio_core::list_asio_drivers() {
            Ok(drivers) => {
                self.asio_drivers = drivers;
                self.asio_drivers_error = None;
            }
            Err(error) => {
                self.asio_drivers.clear();
                self.asio_drivers_error = Some(error);
            }
        }

        set_shared_optional_message(
            &self.info_banner,
            Some("Audio devices and ASIO drivers refreshed.".to_string()),
        );
    }

    fn save_preset(&self) {
        let Some(path) = file_dialog::choose_save_file() else {
            return;
        };

        let preset = self.to_preset();
        match serde_json::to_string_pretty(&preset)
            .map_err(|error| error.to_string())
            .and_then(|json| std::fs::write(&path, json).map_err(|error| error.to_string()))
        {
            Ok(()) => set_shared_optional_message(
                &self.info_banner,
                Some(format!("Preset saved to {}.", path.display())),
            ),
            Err(error) => set_shared_optional_message(
                &self.error_banner,
                Some(format!("Could not save preset: {error}")),
            ),
        }
    }

    fn load_preset(&mut self) {
        let Some(path) = file_dialog::choose_open_file() else {
            return;
        };

        let result = std::fs::read_to_string(&path)
            .map_err(|error| error.to_string())
            .and_then(|json| serde_json::from_str::<PresetFile>(&json).map_err(|error| error.to_string()))
            .and_then(|preset| self.apply_preset(preset));

        match result {
            Ok(()) => set_shared_optional_message(
                &self.info_banner,
                Some(format!("Preset loaded from {}.", path.display())),
            ),
            Err(error) => set_shared_optional_message(
                &self.error_banner,
                Some(format!("Could not load preset: {error}")),
            ),
        }
    }

    fn to_preset(&self) -> PresetFile {
        PresetFile {
            format: "OpenAudio preset".to_string(),
            version: 1,
            publish_sessions: self
                .publish_sessions
                .iter()
                .map(|session| PublishPreset {
                    node_name: session.node_name.clone(),
                    stream_name: session.stream_name.clone(),
                    stream_id: session.stream_id,
                    selected_input: session.selected_input.clone(),
                    is_loopback: session.is_loopback,
                    record: session.record,
                    channel_labels: session.channel_labels.clone(),
                })
                .collect(),
            combine_publish_sessions: self
                .combine_publish_sessions
                .iter()
                .map(|session| CombinePublishPreset {
                    session_tag: session.session_tag.clone(),
                    node_name: session.node_name.clone(),
                    stream_name: session.stream_name.clone(),
                    stream_id: session.stream_id,
                    channel_count: session.channel_count,
                    channel_sources: session.channel_sources.clone(),
                    record: session.record,
                    channel_labels: session.channel_labels.clone(),
                })
                .collect(),
            asio_publish_sessions: self
                .asio_publish_sessions
                .iter()
                .map(|session| AsioPublishPreset {
                    node_name: session.node_name.clone(),
                    stream_name: session.stream_name.clone(),
                    stream_id: session.stream_id,
                    selected_driver: session.selected_driver.clone(),
                    channel_indices: session.channel_indices.clone(),
                    channel_labels: session.channel_labels.clone(),
                })
                .collect(),
            subscribe_sessions: self
                .subscribe_sessions
                .iter()
                .map(|session| SubscribePreset {
                    selected_discovered_node_id: session.selected_discovered_node_id.clone(),
                    bind_port: session.bind_port.clone(),
                    selected_output: session.selected_output.clone(),
                    volume: audio_core::get_volume(&session.volume),
                    record: session.record,
                })
                .collect(),
            split_subscribe_sessions: self
                .split_subscribe_sessions
                .iter()
                .map(|session| SplitSubscribePreset {
                    session_tag: session.session_tag.clone(),
                    selected_discovered_node_id: session.selected_discovered_node_id.clone(),
                    bind_port: session.bind_port.clone(),
                    channel_devices: session.channel_devices.clone(),
                    record: session.record,
                })
                .collect(),
            browser: BrowserPreset {
                access_mode: match self.browser_access_mode {
                    BrowserAccessMode::PasswordProtected => "password_protected",
                    BrowserAccessMode::OpenLan => "open_lan",
                }
                .to_string(),
                password: self.browser_password.clone(),
            },
        }
    }

    fn apply_preset(&mut self, preset: PresetFile) -> Result<(), String> {
        if preset.format != "OpenAudio preset" || preset.version != 1 {
            return Err("unsupported OpenAudio preset format or version".to_string());
        }

        let any_running = self.publish_sessions.iter().any(|session| session.running.load(Ordering::Acquire))
            || self.combine_publish_sessions.iter().any(|session| session.running.load(Ordering::Acquire))
            || self.asio_publish_sessions.iter().any(|session| session.running.load(Ordering::Acquire))
            || self.subscribe_sessions.iter().any(|session| session.running.load(Ordering::Acquire))
            || self.split_subscribe_sessions.iter().any(|session| session.running.load(Ordering::Acquire));

        if any_running {
            return Err("stop all active audio sessions before loading a preset".to_string());
        }

        self.publish_sessions.clear();
        self.combine_publish_sessions.clear();
        self.asio_publish_sessions.clear();
        self.subscribe_sessions.clear();
        self.split_subscribe_sessions.clear();

        for item in preset.publish_sessions {
            let id = self.take_next_id();
            self.publish_sessions.push(PublishSession {
                id,
                node_name: item.node_name,
                stream_name: item.stream_name,
                stream_id: item.stream_id,
                selected_input: item.selected_input,
                is_loopback: item.is_loopback,
                record: item.record,
                channel_labels: item.channel_labels,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new("Not started.".to_string())),
                last_toggle: Instant::now() - Duration::from_secs(1),
            });
        }

        for item in preset.combine_publish_sessions {
            let id = self.take_next_id();
            self.combine_publish_sessions.push(CombinePublishSession {
                id,
                session_tag: item.session_tag,
                node_name: item.node_name,
                stream_name: item.stream_name,
                stream_id: item.stream_id,
                channel_count: item.channel_count,
                channel_sources: item.channel_sources,
                channel_labels: item.channel_labels,
                record: item.record,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new("Not started.".to_string())),
                last_toggle: Instant::now() - Duration::from_secs(1),
            });
        }

        for item in preset.asio_publish_sessions {
            let id = self.take_next_id();
            let driver_channel_count = item
                .selected_driver
                .as_ref()
                .and_then(|name| self.asio_drivers.iter().find(|driver| &driver.name == name))
                .map(|driver| driver.max_input_channels as usize)
                .unwrap_or(0);
            self.asio_publish_sessions.push(AsioPublishSession {
                id,
                node_name: item.node_name,
                stream_name: item.stream_name,
                stream_id: item.stream_id,
                selected_driver: item.selected_driver,
                driver_channel_count,
                channel_indices: item.channel_indices,
                channel_labels: item.channel_labels,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new("Not started.".to_string())),
                last_toggle: Instant::now() - Duration::from_secs(1),
            });
        }

        for item in preset.subscribe_sessions {
            let id = self.take_next_id();
            self.subscribe_sessions.push(SubscribeSession {
                id,
                selected_discovered_node_id: item.selected_discovered_node_id,
                bind_port: item.bind_port,
                selected_output: item.selected_output,
                volume: audio_core::new_volume_control(item.volume.clamp(0.0, 2.0)),
                record: item.record,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new("Not started.".to_string())),
                last_toggle: Instant::now() - Duration::from_secs(1),
            });
        }

        for item in preset.split_subscribe_sessions {
            let id = self.take_next_id();
            self.split_subscribe_sessions.push(SplitSubscribeSession {
                id,
                session_tag: item.session_tag,
                selected_discovered_node_id: item.selected_discovered_node_id,
                bind_port: item.bind_port,
                channel_devices: item.channel_devices,
                record: item.record,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new("Not started.".to_string())),
                last_toggle: Instant::now() - Duration::from_secs(1),
            });
        }

        self.browser_access_mode = if preset.browser.access_mode == "open_lan" {
            BrowserAccessMode::OpenLan
        } else {
            BrowserAccessMode::PasswordProtected
        };
        self.browser_password = preset.browser.password;
        self.browser_password_confirmation.clear();
        Ok(())
    }

    fn take_next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }
}

// ============================================================================
// THEME
// ============================================================================

struct Theme;

impl Theme {
    const BG_PRIMARY: egui::Color32 =
        egui::Color32::from_rgb(18, 18, 20);

    const BG_SECONDARY: egui::Color32 =
        egui::Color32::from_rgb(28, 28, 32);

    const BG_CARD: egui::Color32 =
        egui::Color32::from_rgb(38, 38, 42);

    const BG_WARNING: egui::Color32 =
        egui::Color32::from_rgb(70, 48, 12);

    const ACCENT_BLUE: egui::Color32 =
        egui::Color32::from_rgb(0, 122, 255);

    const ACCENT_GREEN: egui::Color32 =
        egui::Color32::from_rgb(52, 199, 89);

    const ACCENT_RED: egui::Color32 =
        egui::Color32::from_rgb(255, 69, 58);

    const ACCENT_PURPLE: egui::Color32 =
        egui::Color32::from_rgb(175, 82, 222);

    const ACCENT_ORANGE: egui::Color32 =
        egui::Color32::from_rgb(255, 149, 0);

    const TEXT_PRIMARY: egui::Color32 =
        egui::Color32::from_rgb(255, 255, 255);

    const TEXT_SECONDARY: egui::Color32 =
        egui::Color32::from_rgb(152, 152, 157);
}

// ============================================================================
// SHARED UI HELPERS
// ============================================================================

fn configure_style(ctx: &egui::Context) {
    let mut style = (*ctx.style()).clone();

    style.visuals.window_fill = Theme::BG_PRIMARY;
    style.visuals.panel_fill = Theme::BG_PRIMARY;
    style.visuals.extreme_bg_color = Theme::BG_PRIMARY;

    style.visuals.widgets.inactive.bg_fill = Theme::BG_CARD;
    style.visuals.widgets.inactive.rounding = egui::Rounding::same(8.0);
    style.visuals.widgets.hovered.rounding = egui::Rounding::same(8.0);
    style.visuals.widgets.active.rounding = egui::Rounding::same(8.0);

    style.spacing.item_spacing = egui::vec2(10.0, 8.0);
    style.spacing.button_padding = egui::vec2(10.0, 6.0);
    style.spacing.combo_width = 220.0;

    ctx.set_style(style);
}

fn shared_message(
    value: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    match value.lock() {
        Ok(message) => message.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn set_shared_optional_message(
    destination: &Arc<Mutex<Option<String>>>,
    value: Option<String>,
) {
    match destination.lock() {
        Ok(mut message) => {
            *message = value;
        }
        Err(poisoned) => {
            *poisoned.into_inner() = value;
        }
    }
}

fn shared_status(
    status: &Arc<Mutex<String>>,
    fallback: &str,
) -> String {
    match status.lock() {
        Ok(value) => value.clone(),
        Err(poisoned) => {
            let value = poisoned.into_inner().clone();

            if value.is_empty() {
                fallback.to_string()
            } else {
                value
            }
        }
    }
}

fn set_shared_status(
    status: &Arc<Mutex<String>>,
    message: impl Into<String>,
) {
    let message = message.into();

    match status.lock() {
        Ok(mut destination) => {
            *destination = message;
        }
        Err(poisoned) => {
            *poisoned.into_inner() = message;
        }
    }
}

fn section_frame() -> egui::Frame {
    egui::Frame::none()
        .fill(Theme::BG_SECONDARY)
        .rounding(12.0)
        .inner_margin(16.0)
}

fn card_frame() -> egui::Frame {
    egui::Frame::none()
        .fill(Theme::BG_CARD)
        .rounding(10.0)
        .inner_margin(14.0)
}

fn status_dot(
    ui: &mut egui::Ui,
    running: bool,
    running_color: egui::Color32,
) {
    ui.label(
        egui::RichText::new(if running { "●" } else { "○" })
            .size(14.0)
            .color(if running {
                running_color
            } else {
                Theme::TEXT_SECONDARY
            }),
    );
}

fn friendly_error(raw: &str) -> String {
    if raw.contains("0x8889000A") {
        "That device is already in use by another app. Close other audio \
         apps and try again."
            .to_string()
    } else if raw.contains("no default input device") {
        "No microphone/input device was found. Check that it is connected \
         and enabled in Windows Sound settings."
            .to_string()
    } else if raw.contains("no default output device") {
        "No speaker/output device was found. Check that it is connected \
         and enabled in Windows Sound settings."
            .to_string()
    } else if raw.contains("Resampling isn't implemented") {
        format!(
            "Format mismatch between the incoming stream and the output \
             device: {raw}"
        )
    } else if raw.contains("device may not support WASAPI loopback") {
        format!(
            "That device does not support WASAPI loopback capture: {raw}"
        )
    } else if raw.contains("must match exactly") {
        format!("Channel/device count mismatch: {raw}")
    } else if raw.contains("must share one sample rate") {
        format!("Sample-rate mismatch across channels: {raw}")
    } else if raw.contains("couldn't find SAR's default.json") {
        "Could not find SAR's configuration file. Configure SAR through \
         its own interface at least once, then try again."
            .to_string()
    } else if raw.contains("ASIO host unavailable") {
        "ASIO is unavailable. Build the application with --features asio \
         and confirm CPAL_ASIO_DIR is configured."
            .to_string()
    } else if raw.contains("ASIO driver") && raw.contains("not found") {
        format!(
            "The selected ASIO driver was not found. Confirm the console \
             driver is installed: {raw}"
        )
    } else if raw.contains("not compiled in") {
        "ASIO is not enabled in this build. Rebuild with --features asio \
         after configuring CPAL_ASIO_DIR."
            .to_string()
    } else if raw.contains("Address already in use")
        || raw.contains("os error 10048")
    {
        "The requested network port is already in use. Stop the old \
         OpenAudio process or choose another port."
            .to_string()
    } else {
        raw.to_string()
    }
}
// ============================================================================
// EFRAME APPLICATION
// ============================================================================

impl eframe::App for OpenAudioApp {
    fn update(
        &mut self,
        ctx: &egui::Context,
        _frame: &mut eframe::Frame,
    ) {
        configure_style(ctx);
        self.apply_pending_device_refresh();

        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(Theme::BG_PRIMARY)
                    .inner_margin(20.0),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        self.render_application_header(ui);

                        ui.add_space(14.0);

                        self.render_banners(ui);

                        let summary = self.ui_summary();

                        ui_shell::render_navigation(
                            ui,
                            &mut self.active_page,
                            summary,
                        );

                        ui.add_space(14.0);

                        match self.active_page {
                            AppPage::Overview => {
                                self.render_overview_page(ui);
                            }
                            AppPage::Publish => {
                                self.render_publish_page(ui);
                            }
                            AppPage::Subscribe => {
                                self.render_subscribe_page(ui);
                            }
                            AppPage::Browser => {
                                self.render_browser_page(ui);
                            }
                            AppPage::Diagnostics => {
                                self.render_diagnostics_page(ui);
                            }
                        }

                        ui.add_space(24.0);
                    });
            });

        ctx.request_repaint_after(Duration::from_millis(100));
    }
}

// ============================================================================
// APPLICATION SHELL
// ============================================================================

impl OpenAudioApp {
    fn render_application_header(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label(
                    egui::RichText::new("Ferronme's Open Audio")
                        .size(28.0)
                        .strong()
                        .color(Theme::TEXT_PRIMARY),
                );

                ui.add_space(2.0);

                ui.label(
                    egui::RichText::new(
                        "Audio networking • Open • Free (for now lol)",
                    )
                    .size(13.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.with_layout(
                egui::Layout::right_to_left(
                    egui::Align::Center,
                ),
                |ui| {
                    let refreshing = self
                        .refreshing_devices
                        .load(Ordering::Acquire);

                    let button_text = if refreshing {
                        "Refreshing devices..."
                    } else {
                        "↻ Refresh Devices"
                    };

                    if ui
                        .add_enabled(
                            !refreshing,
                            egui::Button::new(
                                egui::RichText::new(button_text)
                                    .color(Theme::TEXT_PRIMARY),
                            )
                            .fill(Theme::BG_CARD)
                            .rounding(8.0)
                            .min_size(egui::vec2(150.0, 34.0)),
                        )
                        .clicked()
                    {
                        self.refresh_devices();
                    }

                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("Load Preset")
                                    .color(Theme::TEXT_PRIMARY),
                            )
                            .fill(Theme::BG_CARD)
                            .rounding(8.0)
                            .min_size(egui::vec2(105.0, 34.0)),
                        )
                        .clicked()
                    {
                        self.load_preset();
                    }

                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("Save Preset")
                                    .color(Theme::TEXT_PRIMARY),
                            )
                            .fill(Theme::BG_CARD)
                            .rounding(8.0)
                            .min_size(egui::vec2(105.0, 34.0)),
                        )
                        .clicked()
                    {
                        self.save_preset();
                    }

                    let active_audio_sessions =
                        self.active_audio_session_count();

                    let (status_text, status_color) =
                        if active_audio_sessions > 0 {
                            (
                                format!(
                                    "{active_audio_sessions} active"
                                ),
                                Theme::ACCENT_GREEN,
                            )
                        } else {
                            (
                                "Idle".to_string(),
                                Theme::TEXT_SECONDARY,
                            )
                        };

                    ui.label(
                        egui::RichText::new(status_text)
                            .size(11.0)
                            .strong()
                            .color(status_color),
                    );

                    ui.label(
                        egui::RichText::new(
                            if active_audio_sessions > 0 {
                                "●"
                            } else {
                                "○"
                            },
                        )
                        .size(13.0)
                        .color(status_color),
                    );
                },
            );
        });
    }

    fn active_audio_session_count(&self) -> usize {
        let publisher_count = self
            .publish_sessions
            .iter()
            .filter(|session| {
                session.running.load(Ordering::Relaxed)
            })
            .count()
            + self
                .combine_publish_sessions
                .iter()
                .filter(|session| {
                    session.running.load(Ordering::Relaxed)
                })
                .count()
            + self
                .asio_publish_sessions
                .iter()
                .filter(|session| {
                    session.running.load(Ordering::Relaxed)
                })
                .count();

        let subscriber_count = self
            .subscribe_sessions
            .iter()
            .filter(|session| {
                session.running.load(Ordering::Relaxed)
            })
            .count()
            + self
                .split_subscribe_sessions
                .iter()
                .filter(|session| {
                    session.running.load(Ordering::Relaxed)
                })
                .count();

        publisher_count + subscriber_count
    }

    fn render_banners(&mut self, ui: &mut egui::Ui) {
        if let Some(error) = shared_message(&self.error_banner) {
            egui::Frame::none()
                .fill(Theme::ACCENT_RED)
                .rounding(10.0)
                .inner_margin(12.0)
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("⚠")
                                .size(16.0)
                                .color(egui::Color32::WHITE),
                        );

                        ui.label(
                            egui::RichText::new(error)
                                .color(egui::Color32::WHITE),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                if ui
                                    .small_button("Close")
                                    .clicked()
                                {
                                    set_shared_optional_message(
                                        &self.error_banner,
                                        None,
                                    );
                                }
                            },
                        );
                    });
                });

            ui.add_space(10.0);
        }

        if let Some(information) =
            shared_message(&self.info_banner)
        {
            egui::Frame::none()
                .fill(Theme::ACCENT_BLUE)
                .rounding(10.0)
                .inner_margin(12.0)
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("ℹ")
                                .size(16.0)
                                .color(egui::Color32::WHITE),
                        );

                        ui.label(
                            egui::RichText::new(information)
                                .color(egui::Color32::WHITE),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                if ui
                                    .small_button("Close")
                                    .clicked()
                                {
                                    set_shared_optional_message(
                                        &self.info_banner,
                                        None,
                                    );
                                }
                            },
                        );
                    });
                });

            ui.add_space(10.0);
        }
    }
}

// ============================================================================
// OVERVIEW PAGE
// ============================================================================

impl OpenAudioApp {
    fn render_overview_page(&mut self, ui: &mut egui::Ui) {
        self.render_overview_actions(ui);

        ui.add_space(12.0);

        self.signal_monitor.ui(ui);

        ui.add_space(12.0);

        self.render_discovered_streams_overview(ui);
    }

    fn render_overview_actions(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        section_frame().show(ui, |ui| {
            ui.label(
                egui::RichText::new("Quick Actions")
                    .size(17.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
            );

            ui.add_space(4.0);

            ui.label(
                egui::RichText::new(
                    "Move directly to the workflow you want to configure.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(10.0);

            ui.horizontal_wrapped(|ui| {
                if ui
                    .add(
                        egui::Button::new(
                            egui::RichText::new(
                                "↑ Publish Audio",
                            )
                            .color(egui::Color32::WHITE),
                        )
                        .fill(Theme::ACCENT_GREEN)
                        .rounding(8.0)
                        .min_size(egui::vec2(145.0, 34.0)),
                    )
                    .clicked()
                {
                    self.active_page = AppPage::Publish;
                }

                if ui
                    .add(
                        egui::Button::new(
                            egui::RichText::new(
                                "↓ Receive Audio",
                            )
                            .color(egui::Color32::WHITE),
                        )
                        .fill(Theme::ACCENT_PURPLE)
                        .rounding(8.0)
                        .min_size(egui::vec2(145.0, 34.0)),
                    )
                    .clicked()
                {
                    self.active_page = AppPage::Subscribe;
                }

                if ui
                    .add(
                        egui::Button::new(
                            egui::RichText::new(
                                "◎ Browser Sharing",
                            )
                            .color(egui::Color32::WHITE),
                        )
                        .fill(Theme::ACCENT_BLUE)
                        .rounding(8.0)
                        .min_size(egui::vec2(155.0, 34.0)),
                    )
                    .clicked()
                {
                    self.active_page = AppPage::Browser;
                }

                if ui
                    .add(
                        egui::Button::new(
                            egui::RichText::new(
                                "⚙ Network Test",
                            )
                            .color(egui::Color32::WHITE),
                        )
                        .fill(Theme::ACCENT_ORANGE)
                        .rounding(8.0)
                        .min_size(egui::vec2(145.0, 34.0)),
                    )
                    .clicked()
                {
                    self.active_page =
                        AppPage::Diagnostics;
                }
            });
        });
    }

    fn render_discovered_streams_overview(
        &self,
        ui: &mut egui::Ui,
    ) {
        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        "Network Discovery",
                    )
                    .size(17.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.with_layout(
                    egui::Layout::right_to_left(
                        egui::Align::Center,
                    ),
                    |ui| {
                        ui.label(
                            egui::RichText::new(
                                "Updates automatically",
                            )
                            .size(10.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    },
                );
            });

            ui.add_space(8.0);

            let directory = match self
                .discovery_directory
                .lock()
            {
                Ok(directory) => directory,
                Err(poisoned) => poisoned.into_inner(),
            };

            if directory.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("○")
                                .size(20.0)
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                        );

                        ui.vertical(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "No streams discovered",
                                )
                                .strong()
                                .color(
                                    Theme::TEXT_PRIMARY,
                                ),
                            );

                            ui.label(
                                egui::RichText::new(
                                    "Start a publisher on this \
                                     computer or another device \
                                     on the same network.",
                                )
                                .size(11.0)
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );
                        });
                    });
                });

                return;
            }

            let mut streams = directory
                .iter()
                .map(|(node_id, node)| {
                    (
                        node_id.clone(),
                        node.node_name.clone(),
                        node.stream_name.clone(),
                        node.channel_count,
                        node.ip.clone(),
                    )
                })
                .collect::<Vec<_>>();

            streams.sort_by(|left, right| {
                left.1
                    .to_lowercase()
                    .cmp(&right.1.to_lowercase())
                    .then_with(|| {
                        left.2
                            .to_lowercase()
                            .cmp(&right.2.to_lowercase())
                    })
            });

            for (
                _node_id,
                node_name,
                stream_name,
                channel_count,
                ip,
            ) in streams
            {
                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new("●")
                                .size(11.0)
                                .color(Theme::ACCENT_GREEN),
                        );

                        ui.vertical(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    stream_name,
                                )
                                .strong()
                                .color(
                                    Theme::TEXT_PRIMARY,
                                ),
                            );

                            ui.label(
                                egui::RichText::new(
                                    node_name,
                                )
                                .size(11.0)
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );
                        });

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(ip)
                                        .size(11.0)
                                        .monospace()
                                        .color(
                                            Theme::TEXT_SECONDARY,
                                        ),
                                );

                                ui.label(
                                    egui::RichText::new(
                                        format!(
                                            "{channel_count} ch"
                                        ),
                                    )
                                    .size(11.0)
                                    .strong()
                                    .color(
                                        Theme::ACCENT_BLUE,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(6.0);
            }
        });
    }
}

// ============================================================================
// PUBLISH PAGE ENTRY
// ============================================================================

impl OpenAudioApp {
    fn render_publish_page(&mut self, ui: &mut egui::Ui) {
        self.render_publish_page_notice(ui);

        ui.add_space(12.0);

        self.render_asio_publish_section(ui);

        ui.add_space(12.0);

        // Implemented in Part 3.
        self.render_wasapi_publish_section(ui);

        ui.add_space(12.0);

        // Implemented in Part 3.
        self.render_combine_publish_section(ui);
    }

    fn render_publish_page_notice(
        &self,
        ui: &mut egui::Ui,
    ) {
        egui::Frame::none()
            .fill(egui::Color32::from_rgb(24, 47, 32))
            .rounding(10.0)
            .inner_margin(12.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("↑")
                            .size(18.0)
                            .color(Theme::ACCENT_GREEN),
                    );

                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(
                                "Publishing sends audio to \
                                 subscribed devices.",
                            )
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.label(
                            egui::RichText::new(
                                "Raw Float32 bandwidth grows with \
                                 channel count. Select only the \
                                 ASIO channels you need.",
                            )
                            .size(11.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    });
                });
            });
    }
}

// ============================================================================
// ASIO PUBLISH
// ============================================================================

impl OpenAudioApp {
    fn render_asio_publish_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let asio_drivers = &self.asio_drivers;
        let asio_drivers_error =
            self.asio_drivers_error.clone();

        let subscriber_registry =
            self.subscribers_by_stream.clone();

        let error_banner = self.error_banner.clone();

        let mut remove_index: Option<usize> = None;
        let mut add_requested = false;

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        "ASIO Publish Streams",
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.label(
                    egui::RichText::new(
                        "Direct multichannel console capture",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.add_space(6.0);

            match asio_drivers_error.as_deref() {
                Some(error) => {
                    egui::Frame::none()
                        .fill(Theme::BG_WARNING)
                        .rounding(8.0)
                        .inner_margin(10.0)
                        .show(ui, |ui| {
                            ui.label(
                                egui::RichText::new(
                                    format!(
                                        "⚠ {}",
                                        friendly_error(error)
                                    ),
                                )
                                .size(11.0)
                                .color(
                                    egui::Color32::YELLOW,
                                ),
                            );
                        });
                }
                None if asio_drivers.is_empty() => {
                    card_frame().show(ui, |ui| {
                        ui.label(
                            egui::RichText::new(
                                "No ASIO drivers detected. \
                                 Install your console driver, \
                                 then refresh devices.",
                            )
                            .size(11.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    });
                }
                None => {
                    ui.label(
                        egui::RichText::new(
                            format!(
                                "{} ASIO driver(s) available",
                                asio_drivers.len()
                            ),
                        )
                        .size(11.0)
                        .color(Theme::ACCENT_GREEN),
                    );
                }
            }

            ui.add_space(10.0);

            for (
                index,
                session,
            ) in self
                .asio_publish_sessions
                .iter_mut()
                .enumerate()
            {
                let running =
                    session.running.load(Ordering::Acquire);

                let debounce_ready =
                    session.last_toggle.elapsed()
                        > Duration::from_millis(400);

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(
                                format!(
                                    "ASIO Stream {}",
                                    session.id
                                ),
                            )
                            .size(14.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                let color = if running {
                                    Theme::ACCENT_PURPLE
                                } else {
                                    Theme::TEXT_SECONDARY
                                };

                                ui.label(
                                    egui::RichText::new(
                                        if running {
                                            "RUNNING"
                                        } else {
                                            "STOPPED"
                                        },
                                    )
                                    .size(10.0)
                                    .strong()
                                    .color(color),
                                );

                                status_dot(
                                    ui,
                                    running,
                                    Theme::ACCENT_PURPLE,
                                );
                            },
                        );
                    });

                    ui.add_space(10.0);

                    ui.add_enabled_ui(!running, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "ASIO Driver",
                                )
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );

                            let driver_label = session
                                .selected_driver
                                .clone()
                                .unwrap_or_else(|| {
                                    "Select a driver"
                                        .to_string()
                                });

                            let selected_text =
                                if session
                                    .driver_channel_count
                                    > 0
                                {
                                    format!(
                                        "{} ({} inputs)",
                                        driver_label,
                                        session
                                            .driver_channel_count
                                    )
                                } else {
                                    driver_label
                                };

                            egui::ComboBox::from_id_source(
                                format!(
                                    "asio_publish_driver_{}",
                                    session.id
                                ),
                            )
                            .selected_text(selected_text)
                            .width(360.0)
                            .show_ui(
                                ui,
                                |ui| {
                                    if ui
                                        .selectable_label(
                                            session
                                                .selected_driver
                                                .is_none(),
                                            "Select a driver",
                                        )
                                        .clicked()
                                    {
                                        session
                                            .selected_driver =
                                            None;

                                        session
                                            .driver_channel_count =
                                            0;

                                        session
                                            .channel_indices
                                            .clear();
                                        session.channel_labels.clear();
                                    }

                                    for driver in asio_drivers {
                                        if driver
                                            .max_input_channels
                                            == 0
                                        {
                                            continue;
                                        }

                                        let selected = session
                                            .selected_driver
                                            .as_deref()
                                            == Some(
                                                driver
                                                    .name
                                                    .as_str(),
                                            );

                                        let label = format!(
                                            "{} ({} in / {} out)",
                                            driver.name,
                                            driver
                                                .max_input_channels,
                                            driver
                                                .max_output_channels,
                                        );

                                        if ui
                                            .selectable_label(
                                                selected,
                                                label,
                                            )
                                            .clicked()
                                        {
                                            session
                                                .selected_driver =
                                                Some(
                                                    driver
                                                        .name
                                                        .clone(),
                                                );

                                            session
                                                .driver_channel_count =
                                                driver
                                                    .max_input_channels
                                                    as usize;

                                            session
                                                .channel_indices =
                                                (0..session
                                                    .driver_channel_count)
                                                    .collect();
                                            session.channel_labels =
                                                (0..session.driver_channel_count)
                                                    .map(|index| format!("Channel {}", index + 1))
                                                    .collect();
                                        }
                                    }
                                },
                            );
                        });

                        if session.driver_channel_count > 0 {
                            ui.add_space(10.0);

                            ui.horizontal_wrapped(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Quick selection",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                if ui.small_button("All").clicked()
                                {
                                    session.channel_indices =
                                        (0..session
                                            .driver_channel_count)
                                            .collect();
                                }

                                if ui
                                    .small_button("None")
                                    .clicked()
                                {
                                    session
                                        .channel_indices
                                        .clear();
                                }

                                if ui
                                    .small_button("1–2")
                                    .clicked()
                                {
                                    session.channel_indices =
                                        (0..session
                                            .driver_channel_count
                                            .min(2))
                                            .collect();
                                }

                                if session
                                    .driver_channel_count
                                    >= 8
                                    && ui
                                        .small_button("1–8")
                                        .clicked()
                                {
                                    session.channel_indices =
                                        (0..8).collect();
                                }

                                if session
                                    .driver_channel_count
                                    >= 16
                                    && ui
                                        .small_button("1–16")
                                        .clicked()
                                {
                                    session.channel_indices =
                                        (0..16).collect();
                                }

                                if session
                                    .driver_channel_count
                                    >= 32
                                    && ui
                                        .small_button("1–32")
                                        .clicked()
                                {
                                    session.channel_indices =
                                        (0..32).collect();
                                }
                            });

                            ui.add_space(8.0);

                            let available_width =
                                ui.available_width();

                            let columns =
                                if available_width >= 900.0 {
                                    12
                                } else if available_width >= 650.0 {
                                    8
                                } else {
                                    4
                                };

                            egui::Grid::new(format!(
                                "asio_publish_channels_{}",
                                session.id
                            ))
                            .num_columns(columns)
                            .spacing([5.0, 5.0])
                            .show(ui, |ui| {
                                for channel in
                                    0..session
                                        .driver_channel_count
                                {
                                    let selected = session
                                        .channel_indices
                                        .contains(&channel);

                                    let fill = if selected {
                                        Theme::ACCENT_PURPLE
                                    } else {
                                        Theme::BG_SECONDARY
                                    };

                                    let text_color = if selected {
                                        egui::Color32::WHITE
                                    } else {
                                        Theme::TEXT_SECONDARY
                                    };

                                    let button =
                                        egui::Button::new(
                                            egui::RichText::new(
                                                format!(
                                                    "{}",
                                                    channel + 1
                                                ),
                                            )
                                            .size(10.0)
                                            .strong()
                                            .color(text_color),
                                        )
                                        .fill(fill)
                                        .rounding(5.0)
                                        .min_size(
                                            egui::vec2(
                                                42.0,
                                                26.0,
                                            ),
                                        );

                                    if ui.add(button).clicked() {
                                        if selected {
                                            session
                                                .channel_indices
                                                .retain(
                                                    |selected_channel| {
                                                        *selected_channel
                                                            != channel
                                                    },
                                                );
                                        } else {
                                            session
                                                .channel_indices
                                                .push(channel);

                                            session
                                                .channel_indices
                                                .sort_unstable();

                                            session
                                                .channel_indices
                                                .dedup();
                                        }
                                    }

                                    if (channel + 1) % columns
                                        == 0
                                    {
                                        ui.end_row();
                                    }
                                }

                                if session.driver_channel_count
                                    % columns
                                    != 0
                                {
                                    ui.end_row();
                                }
                            });

                            ui.add_space(6.0);
                            ui.label(
                                egui::RichText::new("Channel labels")
                                    .size(11.0)
                                    .color(Theme::TEXT_SECONDARY),
                            );
                            for channel in session.channel_indices.clone() {
                                if let Some(label) = session.channel_labels.get_mut(channel) {
                                    ui.horizontal(|ui| {
                                        ui.label(format!("ASIO {}", channel + 1));
                                        ui.add(
                                            egui::TextEdit::singleline(label)
                                                .desired_width(180.0),
                                        );
                                    });
                                }
                            }

                            ui.add_space(8.0);

                            self::render_asio_bandwidth_estimate(
                                ui,
                                session.channel_indices.len(),
                            );
                        }

                        ui.add_space(10.0);

                        ui.columns(3, |columns| {
                            columns[0].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Node Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.node_name,
                                    )
                                    .desired_width(180.0),
                                );
                            });

                            columns[1].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.stream_name,
                                    )
                                    .desired_width(180.0),
                                );
                            });

                            columns[2].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream ID",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::DragValue::new(
                                        &mut session.stream_id,
                                    ),
                                );
                            });
                        });
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(7.0);

                    ui.horizontal(|ui| {
                        if running {
                            if ui
                                .add_enabled(
                                    debounce_ready,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "■ Stop Stream",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_RED)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        120.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                session.running.store(
                                    false,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &session.status,
                                    "Stopping...",
                                );
                            }
                        } else {
                            let can_start = session
                                .selected_driver
                                .is_some()
                                && !session
                                    .channel_indices
                                    .is_empty()
                                && !session
                                    .node_name
                                    .trim()
                                    .is_empty()
                                && !session
                                    .stream_name
                                    .trim()
                                    .is_empty();

                            if ui
                                .add_enabled(
                                    debounce_ready
                                        && can_start,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "▶ Start ASIO Stream",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(
                                        Theme::ACCENT_PURPLE,
                                    )
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        160.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                set_shared_optional_message(
                                    &error_banner,
                                    None,
                                );

                                let node_name =
                                    session.node_name.clone();

                                let stream_name =
                                    session.stream_name.clone();

                                let stream_id =
                                    session.stream_id;

                                let driver_name = session
                                    .selected_driver
                                    .clone()
                                    .unwrap_or_default();

                                let channel_indices =
                                    session
                                        .channel_indices
                                        .clone();
                                let channel_labels = session
                                    .channel_indices
                                    .iter()
                                    .map(|&index| {
                                        session
                                            .channel_labels
                                            .get(index)
                                            .filter(|label| !label.trim().is_empty())
                                            .cloned()
                                            .unwrap_or_else(|| format!("Channel {}", index + 1))
                                    })
                                    .collect::<Vec<_>>();

                                let subscribers =
                                    subscriber_registry
                                        .clone();

                                let running =
                                    session.running.clone();

                                let worker_running =
                                    running.clone();

                                let status =
                                    session.status.clone();

                                let worker_status =
                                    status.clone();

                                let worker_error_banner =
                                    error_banner.clone();

                                running.store(
                                    true,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &status,
                                    format!(
                                        "Streaming {} channel(s) \
                                         from '{}'...",
                                        channel_indices.len(),
                                        driver_name,
                                    ),
                                );

                                if let Err(error) = reserve_asio_device(&driver_name) {
                                    set_shared_optional_message(
                                        &worker_error_banner,
                                        Some(error),
                                    );
                                    set_shared_status(
                                        &status,
                                        "ASIO driver busy.",
                                    );
                                    running.store(false, Ordering::Release);
                                    return;
                                }

                                let worker_driver_name = driver_name.clone();

                                let spawn_result =
                                    thread::Builder::new()
                                        .name(format!(
                                            "asio-publish-{}",
                                            stream_id
                                        ))
                                        .spawn(move || {
                                            let result =
                                                audio_core::
                                                capture_asio_with_channel_labels(
                                                    node_name,
                                                    stream_name,
                                                    stream_id,
                                                    worker_driver_name.clone(),
                                                    channel_indices,
                                                    channel_labels,
                                                    subscribers,
                                                    worker_running
                                                        .clone(),
                                                );

                                            match result {
                                                Ok(()) => {
                                                    set_shared_status(
                                                        &worker_status,
                                                        "Stopped.",
                                                    );
                                                }
                                                Err(error) => {
                                                    let friendly =
                                                        friendly_error(
                                                            &error,
                                                        );

                                                    set_shared_status(
                                                        &worker_status,
                                                        format!(
                                                            "Error: \
                                                             {friendly}"
                                                        ),
                                                    );

                                                    set_shared_optional_message(
                                                        &worker_error_banner,
                                                        Some(
                                                            friendly,
                                                        ),
                                                    );
                                                }
                                            }

                                            release_asio_device(&worker_driver_name);
                                            worker_running.store(
                                                false,
                                                Ordering::Release,
                                            );
                                        });

                                if let Err(error) =
                                    spawn_result
                                {
                                    release_asio_device(&driver_name);
                                    running.store(
                                        false,
                                        Ordering::Release,
                                    );

                                    let message = format!(
                                        "Could not start ASIO \
                                         publisher thread: {error}"
                                    );

                                    set_shared_status(
                                        &status,
                                        &message,
                                    );

                                    set_shared_optional_message(
                                        &error_banner,
                                        Some(message),
                                    );
                                }
                            }

                            if !can_start {
                                let reason = if session
                                    .selected_driver
                                    .is_none()
                                {
                                    "Select an ASIO driver"
                                } else if session
                                    .channel_indices
                                    .is_empty()
                                {
                                    "Select at least one channel"
                                } else {
                                    "Enter node and stream names"
                                };

                                ui.label(
                                    egui::RichText::new(reason)
                                        .size(11.0)
                                        .color(
                                            Theme::TEXT_SECONDARY,
                                        ),
                                );
                            }

                            if ui
                                .add(
                                    egui::Button::new(
                                        "Remove",
                                    )
                                    .fill(
                                        Theme::BG_SECONDARY,
                                    )
                                    .rounding(8.0),
                                )
                                .clicked()
                            {
                                remove_index = Some(index);
                            }
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        shared_status(
                                            &session.status,
                                            "Status unavailable.",
                                        ),
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(8.0);
            }

            if self.asio_publish_sessions.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(
                            "No ASIO publish streams configured.",
                        )
                        .color(Theme::TEXT_SECONDARY),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Add a stream to select an ASIO \
                             driver and the exact channels to \
                             transmit.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                });

                ui.add_space(8.0);
            }

            if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(
                            "+ Add ASIO Publish Stream",
                        )
                        .color(egui::Color32::WHITE),
                    )
                    .fill(Theme::ACCENT_PURPLE)
                    .rounding(8.0)
                    .min_size(egui::vec2(200.0, 32.0)),
                )
                .clicked()
            {
                add_requested = true;
            }
        });

        if let Some(index) = remove_index {
            if index < self.asio_publish_sessions.len()
                && !self.asio_publish_sessions[index]
                    .running
                    .load(Ordering::Acquire)
            {
                self.asio_publish_sessions.remove(index);
            }
        }

        if add_requested {
            let id = self.next_id;
            self.next_id =
                self.next_id.saturating_add(1);

            self.asio_publish_sessions.push(
                AsioPublishSession {
                    id,
                    node_name: format!(
                        "OpenAudio Node {id}"
                    ),
                    stream_name: format!(
                        "ASIO Stream {id}"
                    ),
                    stream_id: 6000u32
                        .saturating_add(
                            id.min(u32::MAX as u64)
                                as u32,
                        ),
                    selected_driver: None,
                    driver_channel_count: 0,
                    channel_indices: Vec::new(),
                    channel_labels: Vec::new(),
                    running: Arc::new(
                        AtomicBool::new(false),
                    ),
                    status: Arc::new(Mutex::new(
                        "Not started.".to_string(),
                    )),
                    last_toggle: Instant::now()
                        - Duration::from_secs(1),
                },
            );
        }
    }
}

fn render_asio_bandwidth_estimate(
    ui: &mut egui::Ui,
    selected_channels: usize,
) {
    let raw_mbps = selected_channels as f64
        * 48_000.0
        * std::mem::size_of::<f32>() as f64
        * 8.0
        / 1_000_000.0;

    let (color, message) = if selected_channels == 0 {
        (
            Theme::ACCENT_RED,
            "No channels selected".to_string(),
        )
    } else if selected_channels <= 2 {
        (
            Theme::ACCENT_GREEN,
            "Low channel load".to_string(),
        )
    } else if selected_channels <= 16 {
        (
            Theme::ACCENT_ORANGE,
            "Moderate channel load".to_string(),
        )
    } else {
        (
            egui::Color32::YELLOW,
            "High-bandwidth multichannel stream".to_string(),
        )
    };

    egui::Frame::none()
        .fill(Theme::BG_SECONDARY)
        .rounding(7.0)
        .inner_margin(9.0)
        .show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label(
                    egui::RichText::new(
                        format!(
                            "{} selected",
                            selected_channels
                        ),
                    )
                    .size(11.0)
                    .strong()
                    .color(color),
                );

                ui.separator();

                ui.label(
                    egui::RichText::new(
                        format!(
                            "≈ {raw_mbps:.2} Mbps raw Float32 \
                             audio at 48 kHz"
                        ),
                    )
                    .size(11.0)
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.separator();

                ui.label(
                    egui::RichText::new(message)
                        .size(10.0)
                        .color(color),
                );
            });
        });
}

// ============================================================================
// SUBSCRIBE PAGE ENTRY
// ============================================================================

impl OpenAudioApp {
    fn render_subscribe_page(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        self.render_subscribe_page_notice(ui);

        ui.add_space(12.0);

        self.render_asio_subscribe_section(ui);

        ui.add_space(12.0);

        // Implemented in Part 4.
        self.render_wasapi_subscribe_section(ui);

        ui.add_space(12.0);

        // Implemented in Part 4.
        self.render_split_subscribe_section(ui);
    }

    fn render_subscribe_page_notice(
        &self,
        ui: &mut egui::Ui,
    ) {
        egui::Frame::none()
            .fill(egui::Color32::from_rgb(42, 29, 55))
            .rounding(10.0)
            .inner_margin(12.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("↓")
                            .size(18.0)
                            .color(Theme::ACCENT_PURPLE),
                    );

                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(
                                "Receive and route discovered \
                                 network streams.",
                            )
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.label(
                            egui::RichText::new(
                                "Choose ASIO for direct channel \
                                 routing, mixed WASAPI for normal \
                                 playback, or split routing for \
                                 one device per channel.",
                            )
                            .size(11.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    });
                });
            });
    }

    fn render_asio_subscribe_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        self.asio_subscribe_panel.ui(
            ui,
            &self.asio_drivers,
            &self.discovery_directory,
            &self.error_banner,
        );
    }
}

// ============================================================================
// DIAGNOSTICS PAGE
// ============================================================================

impl OpenAudioApp {
    fn render_diagnostics_page(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        egui::Frame::none()
            .fill(Theme::BG_WARNING)
            .rounding(10.0)
            .inner_margin(12.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("⚙")
                            .size(18.0)
                            .color(Theme::ACCENT_ORANGE),
                    );

                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(
                                "Test the transport independently \
                                 of your audio hardware.",
                            )
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.label(
                            egui::RichText::new(
                                "Generate a synthetic stream, \
                                 subscribe normally, and compare \
                                 2-channel versus 32-channel \
                                 stability.",
                            )
                            .size(11.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    });
                });
            });

        ui.add_space(12.0);

        section_frame().show(ui, |ui| {
            self.diagnostic_panel.show(
                ui,
                &self.subscribers_by_stream,
            );
        });

        ui.add_space(12.0);

        self.render_diagnostic_guidance(ui);
    }

    fn render_diagnostic_guidance(
        &self,
        ui: &mut egui::Ui,
    ) {
        section_frame().show(ui, |ui| {
            ui.label(
                egui::RichText::new(
                    "Recommended Comparison",
                )
                .size(17.0)
                .strong()
                .color(Theme::TEXT_PRIMARY),
            );

            ui.add_space(8.0);

            egui::Grid::new(
                "diagnostic_comparison_steps",
            )
            .num_columns(2)
            .spacing([16.0, 8.0])
            .striped(true)
            .show(ui, |ui| {
                ui.label(
                    egui::RichText::new("1")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "Run a 2-channel, 48 kHz diagnostic stream.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("2")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "Subscribe from the target browser or desktop receiver.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("3")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "Record underruns, crackles, packet errors, and latency.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("4")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "Repeat with 32 channels on the same network path.",
                );
                ui.end_row();
            });

            ui.add_space(10.0);

            ui.label(
                egui::RichText::new(
                    "Important: selecting one channel in the current \
                     browser player changes playback routing, but the \
                     gateway still sends the complete source stream. \
                     Transport-level channel filtering remains a \
                     separate fix.",
                )
                .size(11.0)
                .italics()
                .color(egui::Color32::YELLOW),
            );
        });
    }
}
// ============================================================================
// WASAPI PUBLISH
// ============================================================================

impl OpenAudioApp {
    fn render_wasapi_publish_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let input_devices = &self.input_devices;
        let output_devices = &self.output_devices;
        let subscriber_registry = self.subscribers_by_stream.clone();
        let error_banner = self.error_banner.clone();

        let mut remove_index: Option<usize> = None;
        let mut add_requested = false;

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        format!("{} Publish Streams", hardware_api_name()),
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.label(
                    egui::RichText::new(
                        "Single microphone, input, or loopback source",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.add_space(6.0);

            ui.label(
                egui::RichText::new(
                    "Use normal input capture for microphones and interfaces, \
                     or enable loopback to capture audio being played through \
                     an output device.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(10.0);

            for (index, session) in
                self.publish_sessions.iter_mut().enumerate()
            {
                let running =
                    session.running.load(Ordering::Acquire);

                let debounce_ready =
                    session.last_toggle.elapsed()
                        > Duration::from_millis(400);

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "Publish Stream {}",
                                session.id
                            ))
                            .size(14.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                let color = if running {
                                    Theme::ACCENT_GREEN
                                } else {
                                    Theme::TEXT_SECONDARY
                                };

                                ui.label(
                                    egui::RichText::new(
                                        if running {
                                            "RUNNING"
                                        } else {
                                            "STOPPED"
                                        },
                                    )
                                    .size(10.0)
                                    .strong()
                                    .color(color),
                                );

                                status_dot(
                                    ui,
                                    running,
                                    Theme::ACCENT_GREEN,
                                );
                            },
                        );
                    });

                    ui.add_space(10.0);

                    ui.add_enabled_ui(!running, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.checkbox(
                                &mut session.is_loopback,
                                egui::RichText::new(
                                    "Loopback capture",
                                )
                                .color(Theme::TEXT_PRIMARY),
                            );

                            ui.checkbox(
                                &mut session.record,
                                egui::RichText::new(
                                    "Record stream to WAV",
                                )
                                .color(Theme::TEXT_PRIMARY),
                            );
                        });

                        ui.add_space(8.0);

                        ui.horizontal(|ui| {
                            let device_label =
                                if session.is_loopback {
                                    "Output Device"
                                } else {
                                    "Input Device"
                                };

                            ui.label(
                                egui::RichText::new(device_label)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                            );

                            let current_label = session
                                .selected_input
                                .clone()
                                .unwrap_or_else(|| {
                                    "System Default".to_string()
                                });

                            let device_list =
                                if session.is_loopback {
                                    output_devices
                                } else {
                                    input_devices
                                };

                            egui::ComboBox::from_id_source(
                                format!(
                                    "wasapi_publish_device_{}",
                                    session.id
                                ),
                            )
                            .selected_text(current_label)
                            .width(390.0)
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut session.selected_input,
                                    None,
                                    "System Default",
                                );

                                ui.selectable_value(
                                    &mut session.selected_input,
                                    Some(
                                        audio_core::NONE_DEVICE
                                            .to_string(),
                                    ),
                                    "No device",
                                );

                                for device in device_list {
                                    ui.selectable_value(
                                        &mut session.selected_input,
                                        Some(device.name.clone()),
                                        &device.name,
                                    );
                                }
                            });
                        });

                        if session.selected_input.as_deref()
                            == Some(audio_core::NONE_DEVICE)
                        {
                            ui.label(
                                egui::RichText::new(
                                    "No physical device is selected. \
                                     Starting may fail unless the audio \
                                     core treats this source as silence.",
                                )
                                .size(10.0)
                                .color(egui::Color32::YELLOW),
                            );
                        }

                        let label_count = if session.is_loopback {
                            output_devices
                                .iter()
                                .find(|device| {
                                    Some(device.name.as_str())
                                        == session.selected_input.as_deref()
                                })
                                .map(|device| device.max_output_channels as usize)
                        } else {
                            input_devices
                                .iter()
                                .find(|device| {
                                    Some(device.name.as_str())
                                        == session.selected_input.as_deref()
                                })
                                .map(|device| device.max_input_channels as usize)
                        }
                        .unwrap_or(2)
                        .max(1);

                        while session.channel_labels.len() < label_count {
                            let index = session.channel_labels.len();
                            session.channel_labels.push(format!("Channel {}", index + 1));
                        }
                        session.channel_labels.truncate(label_count);

                        ui.label(
                            egui::RichText::new("Channel labels")
                                .size(11.0)
                                .color(Theme::TEXT_SECONDARY),
                        );
                        for (index, label) in session.channel_labels.iter_mut().enumerate() {
                            ui.horizontal(|ui| {
                                ui.label(format!("Channel {}", index + 1));
                                ui.add(
                                    egui::TextEdit::singleline(label)
                                        .desired_width(180.0),
                                );
                            });
                        }

                        ui.add_space(10.0);

                        ui.columns(3, |columns| {
                            columns[0].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Node Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.node_name,
                                    )
                                    .desired_width(190.0),
                                );
                            });

                            columns[1].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.stream_name,
                                    )
                                    .desired_width(190.0),
                                );
                            });

                            columns[2].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream ID",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::DragValue::new(
                                        &mut session.stream_id,
                                    )
                                    .speed(1),
                                );
                            });
                        });
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(7.0);

                    ui.horizontal(|ui| {
                        if running {
                            if ui
                                .add_enabled(
                                    debounce_ready,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "■ Stop Stream",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_RED)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        120.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                session.running.store(
                                    false,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &session.status,
                                    "Stopping...",
                                );
                            }
                        } else {
                            let valid_names =
                                !session.node_name.trim().is_empty()
                                    && !session
                                        .stream_name
                                        .trim()
                                        .is_empty();

                            if ui
                                .add_enabled(
                                    debounce_ready && valid_names,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "▶ Start Publishing",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_GREEN)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        145.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                set_shared_optional_message(
                                    &error_banner,
                                    None,
                                );

                                let node_name =
                                    session.node_name.clone();

                                let stream_name =
                                    session.stream_name.clone();

                                let stream_id =
                                    session.stream_id;

                                let device_name =
                                    session.selected_input.clone();

                                let is_loopback =
                                    session.is_loopback;

                                let channel_labels = session.channel_labels.clone();

                                let record_path =
                                    if session.record {
                                        Some(
                                            audio_core::
                                            generate_record_path(
                                                &format!(
                                                    "publish_{stream_id}"
                                                ),
                                            ),
                                        )
                                    } else {
                                        None
                                    };

                                let subscribers =
                                    subscriber_registry.clone();

                                let running =
                                    session.running.clone();

                                let worker_running =
                                    running.clone();

                                let status =
                                    session.status.clone();

                                let worker_status =
                                    status.clone();

                                let worker_error_banner =
                                    error_banner.clone();

                                running.store(
                                    true,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &status,
                                    if is_loopback {
                                        "Starting loopback \
                                         capture..."
                                    } else {
                                        "Starting input capture..."
                                    },
                                );

                                let spawn_result =
                                    thread::Builder::new()
                                        .name(format!(
                                            "wasapi-publish-{stream_id}"
                                        ))
                                        .spawn(move || {
                                            let result =
                                                if is_loopback {
                                                    audio_core::
                                                    transmit_loopback_with_discovery_labeled(
                                                        node_name,
                                                        stream_name,
                                                        stream_id,
                                                        device_name,
                                                        subscribers,
                                                        record_path,
                                                        Some(channel_labels),
                                                        worker_running
                                                            .clone(),
                                                    )
                                                } else {
                                                    audio_core::
                                                    transmit_with_discovery_labeled(
                                                        node_name,
                                                        stream_name,
                                                        stream_id,
                                                        device_name,
                                                        subscribers,
                                                        record_path,
                                                        Some(channel_labels),
                                                        worker_running
                                                            .clone(),
                                                    )
                                                };

                                            match result {
                                                Ok(()) => {
                                                    set_shared_status(
                                                        &worker_status,
                                                        "Stopped.",
                                                    );
                                                }
                                                Err(error) => {
                                                    let friendly =
                                                        friendly_error(
                                                            &error,
                                                        );

                                                    set_shared_status(
                                                        &worker_status,
                                                        format!(
                                                            "Error: \
                                                             {friendly}"
                                                        ),
                                                    );

                                                    set_shared_optional_message(
                                                        &worker_error_banner,
                                                        Some(friendly),
                                                    );
                                                }
                                            }

                                            worker_running.store(
                                                false,
                                                Ordering::Release,
                                            );
                                        });

                                if let Err(error) =
                                    spawn_result
                                {
                                    running.store(
                                        false,
                                        Ordering::Release,
                                    );

                                    let message = format!(
                                        "Could not start the WASAPI \
                                         publisher thread: {error}"
                                    );

                                    set_shared_status(
                                        &status,
                                        &message,
                                    );

                                    set_shared_optional_message(
                                        &error_banner,
                                        Some(message),
                                    );
                                }
                            }

                            if !valid_names {
                                ui.label(
                                    egui::RichText::new(
                                        "Enter node and stream names",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            }

                            if ui
                                .add(
                                    egui::Button::new("Remove")
                                        .fill(
                                            Theme::BG_SECONDARY,
                                        )
                                        .rounding(8.0),
                                )
                                .clicked()
                            {
                                remove_index = Some(index);
                            }
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        shared_status(
                                            &session.status,
                                            "Status unavailable.",
                                        ),
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(8.0);
            }

            if self.publish_sessions.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(
                            "No single-source WASAPI publishers \
                             configured.",
                        )
                        .color(Theme::TEXT_SECONDARY),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Add a stream to publish a microphone, \
                             interface input, or Windows loopback \
                             device.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                });

                ui.add_space(8.0);
            }

            if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(
                            format!(
                                "+ Add {} Publish Stream",
                                hardware_api_name()
                            ),
                        )
                        .color(egui::Color32::WHITE),
                    )
                    .fill(Theme::ACCENT_BLUE)
                    .rounding(8.0)
                    .min_size(egui::vec2(220.0, 32.0)),
                )
                .clicked()
            {
                add_requested = true;
            }
        });

        if let Some(index) = remove_index {
            if index < self.publish_sessions.len()
                && !self.publish_sessions[index]
                    .running
                    .load(Ordering::Acquire)
            {
                self.publish_sessions.remove(index);
            }
        }

        if add_requested {
            let id = self.next_id;
            self.next_id =
                self.next_id.saturating_add(1);

            self.publish_sessions.push(PublishSession {
                id,
                node_name: format!("OpenAudio Node {id}"),
                stream_name: format!("Stream {id}"),
                stream_id: 3000u32.saturating_add(
                    id.min(u32::MAX as u64) as u32,
                ),
                selected_input: None,
                is_loopback: false,
                record: false,
                channel_labels: vec!["Channel 1".to_string(), "Channel 2".to_string()],
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new(
                    "Not started.".to_string(),
                )),
                last_toggle: Instant::now()
                    - Duration::from_secs(1),
            });
        }
    }
}

// ============================================================================
// COMBINED WASAPI PUBLISH
// ============================================================================

impl OpenAudioApp {
    fn render_combine_publish_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let input_devices = &self.input_devices;
        let output_devices = &self.output_devices;
        let subscriber_registry = self.subscribers_by_stream.clone();
        let error_banner = self.error_banner.clone();
        let info_banner = self.info_banner.clone();

        let mut remove_index: Option<usize> = None;
        let mut remove_endpoint_tag: Option<String> = None;
        let mut add_requested = false;

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        "Combined Device Publishing",
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.label(
                    egui::RichText::new(
                        "Build one multichannel stream from \
                         multiple Windows devices",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.add_space(6.0);

            ui.label(
                egui::RichText::new(
                    "Each configured source becomes one outgoing stream \
                     channel. All active devices must support a compatible \
                     sample rate.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(10.0);

            for (index, session) in self
                .combine_publish_sessions
                .iter_mut()
                .enumerate()
            {
                let running =
                    session.running.load(Ordering::Acquire);

                let debounce_ready =
                    session.last_toggle.elapsed()
                        > Duration::from_millis(400);

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "Combined Stream {}",
                                session.id
                            ))
                            .size(14.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                let color = if running {
                                    Theme::ACCENT_GREEN
                                } else {
                                    Theme::TEXT_SECONDARY
                                };

                                ui.label(
                                    egui::RichText::new(
                                        if running {
                                            "RUNNING"
                                        } else {
                                            "STOPPED"
                                        },
                                    )
                                    .size(10.0)
                                    .strong()
                                    .color(color),
                                );

                                status_dot(
                                    ui,
                                    running,
                                    Theme::ACCENT_GREEN,
                                );
                            },
                        );
                    });

                    ui.add_space(10.0);

                    ui.add_enabled_ui(!running, |ui| {
                        ui.columns(3, |columns| {
                            columns[0].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Node Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.node_name,
                                    )
                                    .desired_width(190.0),
                                );
                            });

                            columns[1].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream Name",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::TextEdit::singleline(
                                        &mut session.stream_name,
                                    )
                                    .desired_width(190.0),
                                );
                            });

                            columns[2].vertical(|ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "Stream ID",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );

                                ui.add(
                                    egui::DragValue::new(
                                        &mut session.stream_id,
                                    )
                                    .speed(1),
                                );
                            });
                        });

                        ui.add_space(10.0);

                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "Outgoing Channels",
                                )
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );

                            let mut channel_count =
                                session.channel_count;

                            if ui
                                .add(
                                    egui::DragValue::new(
                                        &mut channel_count,
                                    )
                                    .clamp_range(1..=64)
                                    .speed(1),
                                )
                                .changed()
                            {
                                session.channel_count =
                                    channel_count;

                                session.channel_sources.resize(
                                    channel_count,
                                    (None, false),
                                );
                                while session.channel_labels.len() < channel_count {
                                    let index = session.channel_labels.len();
                                    session.channel_labels.push(format!("Channel {}", index + 1));
                                }
                                session.channel_labels.truncate(channel_count);
                            }

                            ui.label(
                                egui::RichText::new(
                                    "one source per channel",
                                )
                                .size(10.0)
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );
                        });

                        ui.label(
                            egui::RichText::new("Channel labels")
                                .size(11.0)
                                .color(Theme::TEXT_SECONDARY),
                        );
                        for (index, label) in session.channel_labels.iter_mut().enumerate() {
                            ui.horizontal(|ui| {
                                ui.label(format!("Channel {}", index + 1));
                                ui.add(
                                    egui::TextEdit::singleline(label)
                                        .desired_width(180.0),
                                );
                            });
                        }

                        ui.add_space(8.0);

                        render_combined_bandwidth_estimate(
                            ui,
                            session.channel_count,
                        );

                        ui.add_space(10.0);

                        ui.horizontal_wrapped(|ui| {
                            if ui
                                .add(
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "Create SAR Recording \
                                             Endpoints",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_BLUE)
                                    .rounding(8.0),
                                )
                                .clicked()
                            {
                                match audio_core::
                                    ensure_openaudio_endpoints(
                                        &session.session_tag,
                                        audio_core::EndpointKind::
                                            Recording,
                                        session.channel_count,
                                    )
                                {
                                    Ok(names) => {
                                        for (source, name) in session
                                            .channel_sources
                                            .iter_mut()
                                            .zip(names)
                                        {
                                            source.0 = Some(name);
                                            source.1 = false;
                                        }

                                        set_shared_optional_message(
                                            &info_banner,
                                            Some(
                                                "SAR recording \
                                                 endpoints created. \
                                                 Restart your DAW's \
                                                 ASIO connection, route \
                                                 tracks to the new \
                                                 OpenAudio recording \
                                                 endpoints, then refresh \
                                                 devices."
                                                    .to_string(),
                                            ),
                                        );
                                    }
                                    Err(error) => {
                                        set_shared_optional_message(
                                            &error_banner,
                                            Some(friendly_error(
                                                &error,
                                            )),
                                        );
                                    }
                                }
                            }

                            ui.checkbox(
                                &mut session.record,
                                egui::RichText::new(
                                    "Record each channel to WAV",
                                )
                                .color(Theme::TEXT_PRIMARY),
                            );
                        });

                        ui.add_space(10.0);

                        ui.label(
                            egui::RichText::new(
                                "Channel Sources",
                            )
                            .size(12.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.add_space(6.0);

                        for (
                            channel_index,
                            (device, is_loopback),
                        ) in session
                            .channel_sources
                            .iter_mut()
                            .enumerate()
                        {
                            egui::Frame::none()
                                .fill(Theme::BG_SECONDARY)
                                .rounding(7.0)
                                .inner_margin(9.0)
                                .show(ui, |ui| {
                                    ui.horizontal_wrapped(|ui| {
                                        ui.label(
                                            egui::RichText::new(
                                                format!(
                                                    "Channel {}",
                                                    channel_index + 1
                                                ),
                                            )
                                            .strong()
                                            .color(
                                                Theme::TEXT_PRIMARY,
                                            ),
                                        );

                                        ui.checkbox(
                                            is_loopback,
                                            "Loopback",
                                        );

                                        let current_label =
                                            device
                                                .clone()
                                                .unwrap_or_else(
                                                    || {
                                                        "System Default"
                                                            .to_string()
                                                    },
                                                );

                                        let device_list =
                                            if *is_loopback {
                                                output_devices
                                            } else {
                                                input_devices
                                            };

                                        egui::ComboBox::
                                            from_id_source(format!(
                                                "combined_source_{}_{}",
                                                session.id,
                                                channel_index
                                            ))
                                            .selected_text(
                                                current_label,
                                            )
                                            .width(360.0)
                                            .show_ui(
                                                ui,
                                                |ui| {
                                                    ui.selectable_value(
                                                        device,
                                                        None,
                                                        "System Default",
                                                    );

                                                    ui.selectable_value(
                                                        device,
                                                        Some(
                                                            audio_core::
                                                                NONE_DEVICE
                                                                .to_string(),
                                                        ),
                                                        "No device / \
                                                         silent channel",
                                                    );

                                                    for source_device in
                                                        device_list
                                                    {
                                                        ui.selectable_value(
                                                            device,
                                                            Some(
                                                                source_device
                                                                    .name
                                                                    .clone(),
                                                            ),
                                                            &source_device
                                                                .name,
                                                        );
                                                    }
                                                },
                                            );
                                    });
                                });

                            ui.add_space(5.0);
                        }
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(7.0);

                    ui.horizontal(|ui| {
                        if running {
                            if ui
                                .add_enabled(
                                    debounce_ready,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "■ Stop Stream",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_RED)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        120.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                session.running.store(
                                    false,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &session.status,
                                    "Stopping...",
                                );
                            }
                        } else {
                            let valid_configuration =
                                session.channel_count > 0
                                    && session
                                        .channel_sources
                                        .len()
                                        == session.channel_count
                                    && !session
                                        .node_name
                                        .trim()
                                        .is_empty()
                                    && !session
                                        .stream_name
                                        .trim()
                                        .is_empty();

                            if ui
                                .add_enabled(
                                    debounce_ready
                                        && valid_configuration,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "▶ Start Combined Stream",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_GREEN)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        180.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                set_shared_optional_message(
                                    &error_banner,
                                    None,
                                );

                                let node_name =
                                    session.node_name.clone();

                                let stream_name =
                                    session.stream_name.clone();

                                let stream_id =
                                    session.stream_id;

                                let sources = session
                                    .channel_sources
                                    .iter()
                                    .map(
                                        |(
                                            device_name,
                                            is_loopback,
                                        )| {
                                            audio_core::
                                                ChannelSource {
                                                device_name:
                                                    device_name
                                                        .clone(),
                                                is_loopback:
                                                    *is_loopback,
                                            }
                                        },
                                    )
                                    .collect::<Vec<_>>();

                                let source_count =
                                    sources.len();

                                let record_each =
                                    session.record;

                                let channel_labels = session.channel_labels.clone();

                                let subscribers =
                                    subscriber_registry.clone();

                                let running =
                                    session.running.clone();

                                let worker_running =
                                    running.clone();

                                let status =
                                    session.status.clone();

                                let worker_status =
                                    status.clone();

                                let worker_error_banner =
                                    error_banner.clone();

                                running.store(
                                    true,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &status,
                                    format!(
                                        "Combining and advertising \
                                         {source_count} channel(s)..."
                                    ),
                                );

                                let spawn_result =
                                    thread::Builder::new()
                                        .name(format!(
                                            "combined-publish-{stream_id}"
                                        ))
                                        .spawn(move || {
                                            let result = audio_core::
                                                capture_and_combine_with_labels(
                                                    node_name,
                                                    stream_name,
                                                    stream_id,
                                                    sources,
                                                    subscribers,
                                                    record_each,
                                                    Some(channel_labels),
                                                    worker_running
                                                        .clone(),
                                                );

                                            match result {
                                                Ok(()) => {
                                                    set_shared_status(
                                                        &worker_status,
                                                        "Stopped.",
                                                    );
                                                }
                                                Err(error) => {
                                                    let friendly =
                                                        friendly_error(
                                                            &error,
                                                        );

                                                    set_shared_status(
                                                        &worker_status,
                                                        format!(
                                                            "Error: \
                                                             {friendly}"
                                                        ),
                                                    );

                                                    set_shared_optional_message(
                                                        &worker_error_banner,
                                                        Some(friendly),
                                                    );
                                                }
                                            }

                                            worker_running.store(
                                                false,
                                                Ordering::Release,
                                            );
                                        });

                                if let Err(error) =
                                    spawn_result
                                {
                                    running.store(
                                        false,
                                        Ordering::Release,
                                    );

                                    let message = format!(
                                        "Could not start the combined \
                                         publisher thread: {error}"
                                    );

                                    set_shared_status(
                                        &status,
                                        &message,
                                    );

                                    set_shared_optional_message(
                                        &error_banner,
                                        Some(message),
                                    );
                                }
                            }

                            if !valid_configuration {
                                ui.label(
                                    egui::RichText::new(
                                        "Complete the stream names \
                                         and channel configuration",
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            }

                            if ui
                                .add(
                                    egui::Button::new("Remove")
                                        .fill(
                                            Theme::BG_SECONDARY,
                                        )
                                        .rounding(8.0),
                                )
                                .clicked()
                            {
                                remove_endpoint_tag =
                                    Some(
                                        session
                                            .session_tag
                                            .clone(),
                                    );

                                remove_index = Some(index);
                            }
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        shared_status(
                                            &session.status,
                                            "Status unavailable.",
                                        ),
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(8.0);
            }

            if self.combine_publish_sessions.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(
                            "No combined publishers configured.",
                        )
                        .color(Theme::TEXT_SECONDARY),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Add a combined stream when each outgoing \
                             channel must come from a separate Windows \
                             audio endpoint.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                });

                ui.add_space(8.0);
            }

            if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(
                            "+ Add Combined Publish Stream",
                        )
                        .color(egui::Color32::WHITE),
                    )
                    .fill(Theme::ACCENT_BLUE)
                    .rounding(8.0)
                    .min_size(egui::vec2(235.0, 32.0)),
                )
                .clicked()
            {
                add_requested = true;
            }
        });

        if let Some(index) = remove_index {
            if index < self.combine_publish_sessions.len()
                && !self.combine_publish_sessions[index]
                    .running
                    .load(Ordering::Acquire)
            {
                if let Some(session_tag) =
                    remove_endpoint_tag
                {
                    if let Err(error) =
                        audio_core::remove_openaudio_endpoints(
                            &session_tag,
                        )
                    {
                        set_shared_optional_message(
                            &error_banner,
                            Some(format!(
                                "The stream was removed, but its SAR \
                                 endpoints could not be removed: {}",
                                friendly_error(&error),
                            )),
                        );
                    }
                }

                self.combine_publish_sessions.remove(index);
            }
        }

        if add_requested {
            let id = self.next_id;
            self.next_id =
                self.next_id.saturating_add(1);

            self.combine_publish_sessions.push(
                CombinePublishSession {
                    id,
                    session_tag: format!("combine{id}"),
                    node_name: format!(
                        "OpenAudio Node {id}"
                    ),
                    stream_name: format!(
                        "Combined Stream {id}"
                    ),
                    stream_id: 4000u32.saturating_add(
                        id.min(u32::MAX as u64) as u32,
                    ),
                    channel_count: 2,
                    channel_sources: vec![
                        (None, false),
                        (None, false),
                    ],
                    channel_labels: vec![
                        "Channel 1".to_string(),
                        "Channel 2".to_string(),
                    ],
                    record: false,
                    running: Arc::new(
                        AtomicBool::new(false),
                    ),
                    status: Arc::new(Mutex::new(
                        "Not started.".to_string(),
                    )),
                    last_toggle: Instant::now()
                        - Duration::from_secs(1),
                },
            );
        }
    }
}

// ============================================================================
// PUBLISH BANDWIDTH ESTIMATE
// ============================================================================

fn render_combined_bandwidth_estimate(
    ui: &mut egui::Ui,
    channel_count: usize,
) {
    const ASSUMED_SAMPLE_RATE: f64 = 48_000.0;
    const FLOAT32_BYTES: f64 = 4.0;

    let raw_mbps = channel_count as f64
        * ASSUMED_SAMPLE_RATE
        * FLOAT32_BYTES
        * 8.0
        / 1_000_000.0;

    let color = if channel_count <= 2 {
        Theme::ACCENT_GREEN
    } else if channel_count <= 16 {
        Theme::ACCENT_ORANGE
    } else {
        egui::Color32::YELLOW
    };

    egui::Frame::none()
        .fill(Theme::BG_SECONDARY)
        .rounding(7.0)
        .inner_margin(9.0)
        .show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label(
                    egui::RichText::new(format!(
                        "{channel_count} channel(s)"
                    ))
                    .size(11.0)
                    .strong()
                    .color(color),
                );

                ui.separator();

                ui.label(
                    egui::RichText::new(format!(
                        "Approximately {raw_mbps:.2} Mbps raw \
                         Float32 audio at 48 kHz"
                    ))
                    .size(11.0)
                    .color(Theme::TEXT_PRIMARY),
                );

                if channel_count > 16 {
                    ui.separator();

                    ui.label(
                        egui::RichText::new(
                            "Use wired Ethernet where possible",
                        )
                        .size(10.0)
                        .strong()
                        .color(egui::Color32::YELLOW),
                    );
                }
            });
        });
}
// ============================================================================
// WASAPI SUBSCRIBE
// ============================================================================

impl OpenAudioApp {
    fn render_wasapi_subscribe_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let output_devices = &self.output_devices;
        let error_banner = self.error_banner.clone();

        let discovered_nodes = match self.discovery_directory.lock() {
            Ok(directory) => directory.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        let mut discovered_entries = discovered_nodes
            .iter()
            .map(|(node_id, node)| {
                (node_id.clone(), node.clone())
            })
            .collect::<Vec<_>>();

        discovered_entries.sort_by(|left, right| {
            left.1
                .node_name
                .to_lowercase()
                .cmp(&right.1.node_name.to_lowercase())
                .then_with(|| {
                    left.1
                        .stream_name
                        .to_lowercase()
                        .cmp(&right.1.stream_name.to_lowercase())
                })
        });

        let mut remove_index: Option<usize> = None;
        let mut add_requested = false;

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        format!("{} Mixed Playback", hardware_api_name()),
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.label(
                    egui::RichText::new(
                        "Mix one discovered stream to a Windows output",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.add_space(6.0);

            ui.label(
                egui::RichText::new(
                    "Use this mode for speakers, headphones, and ordinary \
                     Windows playback devices.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(10.0);

            for (index, session) in
                self.subscribe_sessions.iter_mut().enumerate()
            {
                let running =
                    session.running.load(Ordering::Acquire);

                let debounce_ready =
                    session.last_toggle.elapsed()
                        > Duration::from_millis(400);

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "Playback Session {}",
                                session.id
                            ))
                            .size(14.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                let color = if running {
                                    Theme::ACCENT_GREEN
                                } else {
                                    Theme::TEXT_SECONDARY
                                };

                                ui.label(
                                    egui::RichText::new(
                                        if running {
                                            "RUNNING"
                                        } else {
                                            "STOPPED"
                                        },
                                    )
                                    .size(10.0)
                                    .strong()
                                    .color(color),
                                );

                                status_dot(
                                    ui,
                                    running,
                                    Theme::ACCENT_GREEN,
                                );
                            },
                        );
                    });

                    ui.add_space(10.0);

                    ui.add_enabled_ui(!running, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "Output Device",
                                )
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );

                            let current_label = session
                                .selected_output
                                .clone()
                                .unwrap_or_else(|| {
                                    "System Default".to_string()
                                });

                            egui::ComboBox::from_id_source(
                                format!(
                                    "wasapi_subscribe_output_{}",
                                    session.id
                                ),
                            )
                            .selected_text(current_label)
                            .width(390.0)
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut session.selected_output,
                                    None,
                                    "System Default",
                                );

                                ui.selectable_value(
                                    &mut session.selected_output,
                                    Some(
                                        audio_core::NONE_DEVICE
                                            .to_string(),
                                    ),
                                    "No output device",
                                );

                                for device in output_devices {
                                    ui.selectable_value(
                                        &mut session.selected_output,
                                        Some(device.name.clone()),
                                        &device.name,
                                    );
                                }
                            });
                        });

                        ui.add_space(8.0);

                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "Local UDP Port",
                                )
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );

                            ui.add(
                                egui::TextEdit::singleline(
                                    &mut session.bind_port,
                                )
                                .desired_width(90.0),
                            );

                            ui.checkbox(
                                &mut session.record,
                                egui::RichText::new(
                                    "Record mixed output to WAV",
                                )
                                .color(Theme::TEXT_PRIMARY),
                            );
                        });
                    });

                    ui.add_space(8.0);

                    ui.horizontal(|ui| {
                        let mut volume =
                            audio_core::get_volume(
                                &session.volume,
                            );

                        if ui
                            .add(
                                egui::Slider::new(
                                    &mut volume,
                                    0.0..=1.5,
                                )
                                .text("Volume")
                                .show_value(true),
                            )
                            .changed()
                        {
                            audio_core::set_volume(
                                &session.volume,
                                volume,
                            );
                        }

                        ui.label(
                            egui::RichText::new(format!(
                                "{:.0}%",
                                volume * 100.0
                            ))
                            .size(11.0)
                            .color(Theme::TEXT_SECONDARY),
                        );
                    });

                    ui.add_space(10.0);

                    ui.label(
                        egui::RichText::new(
                            "Discovered Stream",
                        )
                        .size(12.0)
                        .strong()
                        .color(Theme::TEXT_PRIMARY),
                    );

                    ui.add_space(5.0);

                    if discovered_entries.is_empty() {
                        egui::Frame::none()
                            .fill(Theme::BG_SECONDARY)
                            .rounding(7.0)
                            .inner_margin(10.0)
                            .show(ui, |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "No streams discovered. \
                                         Start a publisher first.",
                                    )
                                    .italics()
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            });
                    } else {
                        ui.add_enabled_ui(!running, |ui| {
                            for (node_id, node) in
                                &discovered_entries
                            {
                                let selected = session
                                    .selected_discovered_node_id
                                    .as_deref()
                                    == Some(node_id.as_str());

                                let label = format!(
                                    "{} — \"{}\"  •  {} ch  •  {}",
                                    node.node_name,
                                    node.stream_name,
                                    node.channel_count,
                                    node.ip,
                                );

                                let response = ui.selectable_label(
                                    selected,
                                    egui::RichText::new(label)
                                        .color(
                                            if selected {
                                                Theme::TEXT_PRIMARY
                                            } else {
                                                Theme::TEXT_SECONDARY
                                            },
                                        ),
                                );

                                if response.clicked() {
                                    session
                                        .selected_discovered_node_id =
                                        Some(node_id.clone());
                                }
                            }
                        });
                    }

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(7.0);

                    ui.horizontal(|ui| {
                        if running {
                            if ui
                                .add_enabled(
                                    debounce_ready,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "■ Stop Playback",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_RED)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        130.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                session.running.store(
                                    false,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &session.status,
                                    "Stopping...",
                                );
                            }
                        } else {
                            let selected_node = session
                                .selected_discovered_node_id
                                .as_ref()
                                .and_then(|node_id| {
                                    discovered_nodes
                                        .get(node_id)
                                });

                            let port_result =
                                session.bind_port.parse::<u16>();

                            let can_start =
                                selected_node.is_some()
                                    && matches!(
                                        port_result,
                                        Ok(port) if port != 0
                                    );

                            if ui
                                .add_enabled(
                                    debounce_ready && can_start,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "▶ Start Playback",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_GREEN)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        140.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                set_shared_optional_message(
                                    &error_banner,
                                    None,
                                );

                                let selected_id = session
                                    .selected_discovered_node_id
                                    .clone();

                                let node = selected_id
                                    .as_ref()
                                    .and_then(|node_id| {
                                        discovered_nodes
                                            .get(node_id)
                                    })
                                    .cloned();

                                let port =
                                    session.bind_port.parse::<u16>();

                                match (node, port) {
                                    (Some(node), Ok(port))
                                        if port != 0 =>
                                    {
                                        let subscribe_result =
                                            audio_core::
                                            send_subscribe_request(
                                                &node.ip,
                                                node.control_port,
                                                node.stream_id,
                                                port,
                                            );

                                        match subscribe_result {
                                            Ok(()) => {
                                                let bind_addr =
                                                    format!(
                                                        "0.0.0.0:{port}"
                                                    );

                                                let device_name =
                                                    session
                                                        .selected_output
                                                        .clone();

                                                let volume =
                                                    session
                                                        .volume
                                                        .clone();

                                                let record_path =
                                                    if session.record {
                                                        Some(
                                                            audio_core::
                                                            generate_record_path(
                                                                &format!(
                                                                    "subscribe_{port}"
                                                                ),
                                                            ),
                                                        )
                                                    } else {
                                                        None
                                                    };

                                                let running =
                                                    session
                                                        .running
                                                        .clone();

                                                let worker_running =
                                                    running.clone();

                                                let status =
                                                    session
                                                        .status
                                                        .clone();

                                                let worker_status =
                                                    status.clone();

                                                let worker_banner =
                                                    error_banner
                                                        .clone();

                                                let stream_name =
                                                    node
                                                        .stream_name
                                                        .clone();

                                                let node_name =
                                                    node
                                                        .node_name
                                                        .clone();

                                                running.store(
                                                    true,
                                                    Ordering::Release,
                                                );

                                                let reconnect_running = running.clone();
                                                let reconnect_ip = node.ip.clone();
                                                let reconnect_control_port = node.control_port;
                                                let reconnect_stream_id = node.stream_id;
                                                thread::spawn(move || {
                                                    while reconnect_running.load(Ordering::Acquire) {
                                                        let _ = audio_core::send_subscribe_request(
                                                            &reconnect_ip,
                                                            reconnect_control_port,
                                                            reconnect_stream_id,
                                                            port,
                                                        );
                                                        for _ in 0..10 {
                                                            if !reconnect_running.load(Ordering::Acquire) {
                                                                return;
                                                            }
                                                            thread::sleep(Duration::from_millis(100));
                                                        }
                                                    }
                                                });

                                                set_shared_status(
                                                    &status,
                                                    format!(
                                                        "Receiving '{}' \
                                                         from '{}'...",
                                                        stream_name,
                                                        node_name,
                                                    ),
                                                );

                                                let spawn_result =
                                                    thread::Builder::new()
                                                        .name(format!(
                                                            "wasapi-subscribe-{port}"
                                                        ))
                                                        .spawn(move || {
                                                            let result =
                                                                audio_core::
                                                                receive_and_play_bus_with_volume(
                                                                    &bind_addr,
                                                                    device_name,
                                                                    volume,
                                                                    record_path,
                                                                    worker_running
                                                                        .clone(),
                                                                );

                                                            match result {
                                                                Ok(()) => {
                                                                    set_shared_status(
                                                                        &worker_status,
                                                                        "Stopped.",
                                                                    );
                                                                }
                                                                Err(error) => {
                                                                    let friendly =
                                                                        friendly_error(
                                                                            &error,
                                                                        );

                                                                    set_shared_status(
                                                                        &worker_status,
                                                                        format!(
                                                                            "Error: \
                                                                             {friendly}"
                                                                        ),
                                                                    );

                                                                    set_shared_optional_message(
                                                                        &worker_banner,
                                                                        Some(friendly),
                                                                    );
                                                                }
                                                            }

                                                            worker_running
                                                                .store(
                                                                    false,
                                                                    Ordering::Release,
                                                                );
                                                        });

                                                if let Err(error) =
                                                    spawn_result
                                                {
                                                    running.store(
                                                        false,
                                                        Ordering::Release,
                                                    );

                                                    let message =
                                                        format!(
                                                            "Could not start \
                                                             playback thread: \
                                                             {error}"
                                                        );

                                                    set_shared_status(
                                                        &status,
                                                        &message,
                                                    );

                                                    set_shared_optional_message(
                                                        &error_banner,
                                                        Some(message),
                                                    );
                                                }
                                            }
                                            Err(error) => {
                                                set_shared_optional_message(
                                                    &error_banner,
                                                    Some(friendly_error(
                                                        &error,
                                                    )),
                                                );
                                            }
                                        }
                                    }
                                    _ => {
                                        set_shared_optional_message(
                                            &error_banner,
                                            Some(
                                                "Select a discovered \
                                                 stream and enter a \
                                                 valid UDP port."
                                                    .to_string(),
                                            ),
                                        );
                                    }
                                }
                            }

                            if !can_start {
                                let reason =
                                    if selected_node.is_none() {
                                        "Select a discovered stream"
                                    } else {
                                        "Enter a valid UDP port"
                                    };

                                ui.label(
                                    egui::RichText::new(reason)
                                        .size(11.0)
                                        .color(
                                            Theme::TEXT_SECONDARY,
                                        ),
                                );
                            }

                            if ui
                                .add(
                                    egui::Button::new("Remove")
                                        .fill(
                                            Theme::BG_SECONDARY,
                                        )
                                        .rounding(8.0),
                                )
                                .clicked()
                            {
                                remove_index = Some(index);
                            }
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        shared_status(
                                            &session.status,
                                            "Status unavailable.",
                                        ),
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(8.0);
            }

            if self.subscribe_sessions.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(
                            "No mixed playback sessions configured.",
                        )
                        .color(Theme::TEXT_SECONDARY),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Add a session to receive a discovered \
                             stream through a normal Windows output.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                });

                ui.add_space(8.0);
            }

            if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(
                            format!(
                                "+ Add {} Playback",
                                hardware_api_name()
                            ),
                        )
                        .color(egui::Color32::WHITE),
                    )
                    .fill(Theme::ACCENT_BLUE)
                    .rounding(8.0)
                    .min_size(egui::vec2(200.0, 32.0)),
                )
                .clicked()
            {
                add_requested = true;
            }
        });

        if let Some(index) = remove_index {
            if index < self.subscribe_sessions.len()
                && !self.subscribe_sessions[index]
                    .running
                    .load(Ordering::Acquire)
            {
                self.subscribe_sessions.remove(index);
            }
        }

        if add_requested {
            let id = self.next_id;
            self.next_id =
                self.next_id.saturating_add(1);

            let port = 6980u64
                .saturating_add(id)
                .min(u16::MAX as u64);

            self.subscribe_sessions.push(SubscribeSession {
                id,
                selected_discovered_node_id: None,
                bind_port: port.to_string(),
                selected_output: None,
                volume: audio_core::new_volume_control(1.0),
                record: false,
                running: Arc::new(AtomicBool::new(false)),
                status: Arc::new(Mutex::new(
                    "Not started.".to_string(),
                )),
                last_toggle: Instant::now()
                    - Duration::from_secs(1),
            });
        }
    }
}

// ============================================================================
// SPLIT SUBSCRIBE
// ============================================================================

impl OpenAudioApp {
    fn render_split_subscribe_section(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let output_devices = &self.output_devices;
        let error_banner = self.error_banner.clone();
        let info_banner = self.info_banner.clone();

        let discovered_nodes = match self.discovery_directory.lock() {
            Ok(directory) => directory.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        let mut discovered_entries = discovered_nodes
            .iter()
            .map(|(node_id, node)| {
                (node_id.clone(), node.clone())
            })
            .collect::<Vec<_>>();

        discovered_entries.sort_by(|left, right| {
            left.1
                .node_name
                .to_lowercase()
                .cmp(&right.1.node_name.to_lowercase())
                .then_with(|| {
                    left.1
                        .stream_name
                        .to_lowercase()
                        .cmp(&right.1.stream_name.to_lowercase())
                })
        });

        let mut remove_index: Option<usize> = None;
        let mut remove_endpoint_tag: Option<String> = None;
        let mut add_requested = false;

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        "Split Channel Playback",
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                ui.label(
                    egui::RichText::new(
                        "Route each incoming channel to a separate device",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.add_space(6.0);

            ui.label(
                egui::RichText::new(
                    "Use split playback for virtual devices, SAR endpoints, \
                     or workflows where each incoming channel requires an \
                     independent Windows output.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(10.0);

            for (index, session) in self
                .split_subscribe_sessions
                .iter_mut()
                .enumerate()
            {
                let running =
                    session.running.load(Ordering::Acquire);

                let debounce_ready =
                    session.last_toggle.elapsed()
                        > Duration::from_millis(400);

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "Split Session {}",
                                session.id
                            ))
                            .size(14.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                let color = if running {
                                    Theme::ACCENT_PURPLE
                                } else {
                                    Theme::TEXT_SECONDARY
                                };

                                ui.label(
                                    egui::RichText::new(
                                        if running {
                                            "RUNNING"
                                        } else {
                                            "STOPPED"
                                        },
                                    )
                                    .size(10.0)
                                    .strong()
                                    .color(color),
                                );

                                status_dot(
                                    ui,
                                    running,
                                    Theme::ACCENT_PURPLE,
                                );
                            },
                        );
                    });

                    ui.add_space(10.0);

                    ui.add_enabled_ui(!running, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                egui::RichText::new(
                                    "Local UDP Port",
                                )
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );

                            ui.add(
                                egui::TextEdit::singleline(
                                    &mut session.bind_port,
                                )
                                .desired_width(90.0),
                            );

                            ui.checkbox(
                                &mut session.record,
                                egui::RichText::new(
                                    "Record each channel to WAV",
                                )
                                .color(Theme::TEXT_PRIMARY),
                            );
                        });

                        ui.add_space(10.0);

                        ui.label(
                            egui::RichText::new(
                                "Discovered Stream",
                            )
                            .size(12.0)
                            .strong()
                            .color(Theme::TEXT_PRIMARY),
                        );

                        ui.add_space(5.0);

                        if discovered_entries.is_empty() {
                            ui.label(
                                egui::RichText::new(
                                    "No streams discovered.",
                                )
                                .italics()
                                .color(
                                    Theme::TEXT_SECONDARY,
                                ),
                            );
                        } else {
                            for (node_id, node) in
                                &discovered_entries
                            {
                                let selected = session
                                    .selected_discovered_node_id
                                    .as_deref()
                                    == Some(node_id.as_str());

                                let label = format!(
                                    "{} — \"{}\"  •  {} ch  •  {}",
                                    node.node_name,
                                    node.stream_name,
                                    node.channel_count,
                                    node.ip,
                                );

                                if ui
                                    .selectable_label(
                                        selected,
                                        label,
                                    )
                                    .clicked()
                                {
                                    session
                                        .selected_discovered_node_id =
                                        Some(node_id.clone());

                                    session.channel_devices = vec![
                                        None;
                                        node.channel_count
                                            as usize
                                    ];
                                }
                            }
                        }

                        if !session.channel_devices.is_empty() {
                            ui.add_space(10.0);

                            ui.horizontal_wrapped(|ui| {
                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new(
                                                "Create SAR Playback \
                                                 Endpoints",
                                            )
                                            .color(
                                                egui::Color32::WHITE,
                                            ),
                                        )
                                        .fill(Theme::ACCENT_BLUE)
                                        .rounding(8.0),
                                    )
                                    .clicked()
                                {
                                    match audio_core::
                                        ensure_openaudio_endpoints(
                                            &session.session_tag,
                                            audio_core::EndpointKind::
                                                Playback,
                                            session
                                                .channel_devices
                                                .len(),
                                        )
                                    {
                                        Ok(names) => {
                                            for (
                                                destination,
                                                name,
                                            ) in session
                                                .channel_devices
                                                .iter_mut()
                                                .zip(names)
                                            {
                                                *destination =
                                                    Some(name);
                                            }

                                            set_shared_optional_message(
                                                &info_banner,
                                                Some(
                                                    "SAR playback \
                                                     endpoints created. \
                                                     Restart your DAW's \
                                                     ASIO connection, \
                                                     configure the new \
                                                     playback endpoints, \
                                                     then refresh devices."
                                                        .to_string(),
                                                ),
                                            );
                                        }
                                        Err(error) => {
                                            set_shared_optional_message(
                                                &error_banner,
                                                Some(friendly_error(
                                                    &error,
                                                )),
                                            );
                                        }
                                    }
                                }

                                if ui.small_button("Clear All").clicked()
                                {
                                    for destination in
                                        &mut session.channel_devices
                                    {
                                        *destination = None;
                                    }
                                }
                            });

                            ui.add_space(10.0);

                            ui.label(
                                egui::RichText::new(format!(
                                    "Channel Outputs ({})",
                                    session.channel_devices.len()
                                ))
                                .size(12.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                            );

                            ui.add_space(5.0);

                            for (
                                channel_index,
                                destination,
                            ) in session
                                .channel_devices
                                .iter_mut()
                                .enumerate()
                            {
                                egui::Frame::none()
                                    .fill(Theme::BG_SECONDARY)
                                    .rounding(7.0)
                                    .inner_margin(9.0)
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(
                                                egui::RichText::new(
                                                    format!(
                                                        "Channel {}",
                                                        channel_index
                                                            + 1
                                                    ),
                                                )
                                                .strong()
                                                .color(
                                                    Theme::TEXT_PRIMARY,
                                                ),
                                            );

                                            let current_label =
                                                destination
                                                    .clone()
                                                    .unwrap_or_else(
                                                        || {
                                                            "System Default"
                                                                .to_string()
                                                        },
                                                    );

                                            egui::ComboBox::
                                                from_id_source(format!(
                                                    "split_output_{}_{}",
                                                    session.id,
                                                    channel_index
                                                ))
                                                .selected_text(
                                                    current_label,
                                                )
                                                .width(390.0)
                                                .show_ui(
                                                    ui,
                                                    |ui| {
                                                        ui.selectable_value(
                                                            destination,
                                                            None,
                                                            "System Default",
                                                        );

                                                        ui.selectable_value(
                                                            destination,
                                                            Some(
                                                                audio_core::
                                                                    NONE_DEVICE
                                                                    .to_string(),
                                                            ),
                                                            "Disabled",
                                                        );

                                                        for device in
                                                            output_devices
                                                        {
                                                            ui.selectable_value(
                                                                destination,
                                                                Some(
                                                                    device
                                                                        .name
                                                                        .clone(),
                                                                ),
                                                                &device.name,
                                                            );
                                                        }
                                                    },
                                                );
                                        });
                                    });

                                ui.add_space(5.0);
                            }
                        }
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(7.0);

                    ui.horizontal(|ui| {
                        if running {
                            if ui
                                .add_enabled(
                                    debounce_ready,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "■ Stop Split Playback",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_RED)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        160.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                session.running.store(
                                    false,
                                    Ordering::Release,
                                );

                                set_shared_status(
                                    &session.status,
                                    "Stopping...",
                                );
                            }
                        } else {
                            let selected_node = session
                                .selected_discovered_node_id
                                .as_ref()
                                .and_then(|node_id| {
                                    discovered_nodes.get(node_id)
                                });

                            let valid_port = matches!(
                                session.bind_port.parse::<u16>(),
                                Ok(port) if port != 0
                            );

                            let channel_map_valid = selected_node
                                .map(|node| {
                                    session.channel_devices.len()
                                        == node.channel_count as usize
                                        && !session
                                            .channel_devices
                                            .is_empty()
                                })
                                .unwrap_or(false);

                            let can_start = selected_node.is_some()
                                && valid_port
                                && channel_map_valid;

                            if ui
                                .add_enabled(
                                    debounce_ready && can_start,
                                    egui::Button::new(
                                        egui::RichText::new(
                                            "▶ Start Split Playback",
                                        )
                                        .color(
                                            egui::Color32::WHITE,
                                        ),
                                    )
                                    .fill(Theme::ACCENT_PURPLE)
                                    .rounding(8.0)
                                    .min_size(egui::vec2(
                                        175.0,
                                        30.0,
                                    )),
                                )
                                .clicked()
                            {
                                session.last_toggle =
                                    Instant::now();

                                set_shared_optional_message(
                                    &error_banner,
                                    None,
                                );

                                let node = session
                                    .selected_discovered_node_id
                                    .as_ref()
                                    .and_then(|node_id| {
                                        discovered_nodes
                                            .get(node_id)
                                    })
                                    .cloned();

                                let port =
                                    session.bind_port.parse::<u16>();

                                match (node, port) {
                                    (Some(node), Ok(port))
                                        if port != 0 =>
                                    {
                                        match audio_core::
                                            send_subscribe_request(
                                                &node.ip,
                                                node.control_port,
                                                node.stream_id,
                                                port,
                                            )
                                        {
                                            Ok(()) => {
                                                let bind_addr =
                                                    format!(
                                                        "0.0.0.0:{port}"
                                                    );

                                                let device_targets =
                                                    session
                                                        .channel_devices
                                                        .clone();

                                                let target_count =
                                                    device_targets
                                                        .len();

                                                let record_each =
                                                    session.record;

                                                let running =
                                                    session
                                                        .running
                                                        .clone();

                                                let worker_running =
                                                    running.clone();

                                                let status =
                                                    session
                                                        .status
                                                        .clone();

                                                let worker_status =
                                                    status.clone();

                                                let worker_banner =
                                                    error_banner
                                                        .clone();

                                                running.store(
                                                    true,
                                                    Ordering::Release,
                                                );

                                                let reconnect_running = running.clone();
                                                let reconnect_ip = node.ip.clone();
                                                let reconnect_control_port = node.control_port;
                                                let reconnect_stream_id = node.stream_id;
                                                thread::spawn(move || {
                                                    while reconnect_running.load(Ordering::Acquire) {
                                                        let _ = audio_core::send_subscribe_request(
                                                            &reconnect_ip,
                                                            reconnect_control_port,
                                                            reconnect_stream_id,
                                                            port,
                                                        );
                                                        for _ in 0..10 {
                                                            if !reconnect_running.load(Ordering::Acquire) {
                                                                return;
                                                            }
                                                            thread::sleep(Duration::from_millis(100));
                                                        }
                                                    }
                                                });

                                                set_shared_status(
                                                    &status,
                                                    format!(
                                                        "Receiving '{}' \
                                                         and routing {} \
                                                         channel(s)...",
                                                        node.stream_name,
                                                        target_count,
                                                    ),
                                                );

                                                let spawn_result =
                                                    thread::Builder::new()
                                                        .name(format!(
                                                            "split-subscribe-{port}"
                                                        ))
                                                        .spawn(move || {
                                                            let result =
                                                                audio_core::
                                                                receive_and_split_to_devices(
                                                                    &bind_addr,
                                                                    device_targets,
                                                                    record_each,
                                                                    worker_running
                                                                        .clone(),
                                                                );

                                                            match result {
                                                                Ok(()) => {
                                                                    set_shared_status(
                                                                        &worker_status,
                                                                        "Stopped.",
                                                                    );
                                                                }
                                                                Err(error) => {
                                                                    let friendly =
                                                                        friendly_error(
                                                                            &error,
                                                                        );

                                                                    set_shared_status(
                                                                        &worker_status,
                                                                        format!(
                                                                            "Error: \
                                                                             {friendly}"
                                                                        ),
                                                                    );

                                                                    set_shared_optional_message(
                                                                        &worker_banner,
                                                                        Some(friendly),
                                                                    );
                                                                }
                                                            }

                                                            worker_running
                                                                .store(
                                                                    false,
                                                                    Ordering::Release,
                                                                );
                                                        });

                                                if let Err(error) =
                                                    spawn_result
                                                {
                                                    running.store(
                                                        false,
                                                        Ordering::Release,
                                                    );

                                                    let message =
                                                        format!(
                                                            "Could not start \
                                                             split playback \
                                                             thread: {error}"
                                                        );

                                                    set_shared_status(
                                                        &status,
                                                        &message,
                                                    );

                                                    set_shared_optional_message(
                                                        &error_banner,
                                                        Some(message),
                                                    );
                                                }
                                            }
                                            Err(error) => {
                                                set_shared_optional_message(
                                                    &error_banner,
                                                    Some(friendly_error(
                                                        &error,
                                                    )),
                                                );
                                            }
                                        }
                                    }
                                    _ => {
                                        set_shared_optional_message(
                                            &error_banner,
                                            Some(
                                                "Select a stream and \
                                                 enter a valid UDP port."
                                                    .to_string(),
                                            ),
                                        );
                                    }
                                }
                            }

                            if !can_start {
                                let reason =
                                    if selected_node.is_none() {
                                        "Select a discovered stream"
                                    } else if !valid_port {
                                        "Enter a valid UDP port"
                                    } else {
                                        "Channel routing does not \
                                         match the stream"
                                    };

                                ui.label(
                                    egui::RichText::new(reason)
                                        .size(11.0)
                                        .color(
                                            Theme::TEXT_SECONDARY,
                                        ),
                                );
                            }

                            if ui
                                .add(
                                    egui::Button::new("Remove")
                                        .fill(
                                            Theme::BG_SECONDARY,
                                        )
                                        .rounding(8.0),
                                )
                                .clicked()
                            {
                                remove_endpoint_tag =
                                    Some(
                                        session
                                            .session_tag
                                            .clone(),
                                    );

                                remove_index = Some(index);
                            }
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(
                                egui::Align::Center,
                            ),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        shared_status(
                                            &session.status,
                                            "Status unavailable.",
                                        ),
                                    )
                                    .size(11.0)
                                    .color(
                                        Theme::TEXT_SECONDARY,
                                    ),
                                );
                            },
                        );
                    });
                });

                ui.add_space(8.0);
            }

            if self.split_subscribe_sessions.is_empty() {
                card_frame().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(
                            "No split playback sessions configured.",
                        )
                        .color(Theme::TEXT_SECONDARY),
                    );

                    ui.label(
                        egui::RichText::new(
                            "Add a session to route individual incoming \
                             channels to separate Windows devices.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                });

                ui.add_space(8.0);
            }

            if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(
                            "+ Add Split Playback",
                        )
                        .color(egui::Color32::WHITE),
                    )
                    .fill(Theme::ACCENT_PURPLE)
                    .rounding(8.0)
                    .min_size(egui::vec2(195.0, 32.0)),
                )
                .clicked()
            {
                add_requested = true;
            }
        });

        if let Some(index) = remove_index {
            if index < self.split_subscribe_sessions.len()
                && !self.split_subscribe_sessions[index]
                    .running
                    .load(Ordering::Acquire)
            {
                if let Some(session_tag) =
                    remove_endpoint_tag
                {
                    if let Err(error) =
                        audio_core::remove_openaudio_endpoints(
                            &session_tag,
                        )
                    {
                        set_shared_optional_message(
                            &error_banner,
                            Some(format!(
                                "The session was removed, but its SAR \
                                 endpoints could not be removed: {}",
                                friendly_error(&error),
                            )),
                        );
                    }
                }

                self.split_subscribe_sessions.remove(index);
            }
        }

        if add_requested {
            let id = self.next_id;
            self.next_id =
                self.next_id.saturating_add(1);

            let port = 6990u64
                .saturating_add(id)
                .min(u16::MAX as u64);

            self.split_subscribe_sessions.push(
                SplitSubscribeSession {
                    id,
                    session_tag: format!("split{id}"),
                    selected_discovered_node_id: None,
                    bind_port: port.to_string(),
                    channel_devices: Vec::new(),
                    record: false,
                    running: Arc::new(
                        AtomicBool::new(false),
                    ),
                    status: Arc::new(Mutex::new(
                        "Not started.".to_string(),
                    )),
                    last_toggle: Instant::now()
                        - Duration::from_secs(1),
                },
            );
        }
    }
}

// ============================================================================
// BROWSER GATEWAY PAGE
// ============================================================================

impl OpenAudioApp {
    fn render_browser_page(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        let gateway_running = self
            .browser_gateway_running
            .load(Ordering::Acquire);

        let gateway_thread_active = self
            .browser_gateway_thread_active
            .load(Ordering::Acquire);

        let gateway_stopping =
            gateway_thread_active && !gateway_running;

        let debounce_ready =
            self.browser_gateway_last_toggle.elapsed()
                > Duration::from_millis(500);

        let discovered_count = match self
            .discovery_directory
            .lock()
        {
            Ok(directory) => directory.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };

        self.render_browser_status_summary(
            ui,
            gateway_running,
            gateway_stopping,
            discovered_count,
        );

        ui.add_space(12.0);

        section_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(
                        "Browser Playback Gateway",
                    )
                    .size(18.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
                );

                let (status_label, status_color) =
                    if gateway_running {
                        (
                            "● RUNNING",
                            Theme::ACCENT_GREEN,
                        )
                    } else if gateway_stopping {
                        (
                            "● STOPPING",
                            Theme::ACCENT_ORANGE,
                        )
                    } else {
                        (
                            "○ STOPPED",
                            Theme::TEXT_SECONDARY,
                        )
                    };

                ui.label(
                    egui::RichText::new(status_label)
                        .size(11.0)
                        .strong()
                        .color(status_color),
                );
            });

            ui.add_space(6.0);

            ui.label(
                egui::RichText::new(
                    "Serve discovered OpenAudio streams to browsers \
                     connected to this network.",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            ui.add_space(12.0);

            ui.add_enabled_ui(!gateway_thread_active, |ui| {
                ui.label(
                    egui::RichText::new("Access Mode")
                        .size(12.0)
                        .strong()
                        .color(Theme::TEXT_PRIMARY),
                );

                ui.add_space(5.0);

                ui.radio_value(
                    &mut self.browser_access_mode,
                    BrowserAccessMode::PasswordProtected,
                    "Password Protected — recommended",
                );

                ui.radio_value(
                    &mut self.browser_access_mode,
                    BrowserAccessMode::OpenLan,
                    "Open LAN — no password",
                );

                ui.add_space(10.0);

                match self.browser_access_mode {
                    BrowserAccessMode::PasswordProtected => {
                        self.render_browser_password_fields(ui);
                    }
                    BrowserAccessMode::OpenLan => {
                        egui::Frame::none()
                            .fill(Theme::BG_WARNING)
                            .rounding(8.0)
                            .inner_margin(12.0)
                            .show(ui, |ui| {
                                ui.label(
                                    egui::RichText::new(
                                        "⚠ Anyone who can reach this \
                                         computer on the local network \
                                         can discover and play streams.",
                                    )
                                    .size(11.0)
                                    .color(
                                        egui::Color32::YELLOW,
                                    ),
                                );
                            });
                    }
                }
            });

            ui.add_space(12.0);

            if gateway_thread_active {
                self.render_active_gateway_details(ui);

                ui.add_space(10.0);

                if gateway_running {
                    if ui
                        .add_enabled(
                            debounce_ready,
                            egui::Button::new(
                                egui::RichText::new(
                                    "■ Stop Browser Gateway",
                                )
                                .color(egui::Color32::WHITE),
                            )
                            .fill(Theme::ACCENT_RED)
                            .rounding(8.0)
                            .min_size(egui::vec2(
                                190.0,
                                32.0,
                            )),
                        )
                        .clicked()
                    {
                        self.browser_gateway_last_toggle =
                            Instant::now();

                        self.browser_gateway_running.store(
                            false,
                            Ordering::Release,
                        );

                        set_shared_status(
                            &self.browser_gateway_status,
                            "Stopping browser gateway...",
                        );
                    }
                } else {
                    ui.add_enabled(
                        false,
                        egui::Button::new(
                            "Stopping browser gateway...",
                        ),
                    );
                }
            } else {
                let password_valid =
                    self.browser_password_valid();

                let can_start =
                    debounce_ready && password_valid;

                if ui
                    .add_enabled(
                        can_start,
                        egui::Button::new(
                            egui::RichText::new(
                                "▶ Start Browser Gateway",
                            )
                            .color(egui::Color32::WHITE),
                        )
                        .fill(Theme::ACCENT_GREEN)
                        .rounding(8.0)
                        .min_size(egui::vec2(
                            200.0,
                            32.0,
                        )),
                    )
                    .clicked()
                {
                    self.start_browser_gateway();
                }

                if !password_valid
                    && self.browser_access_mode
                        == BrowserAccessMode::PasswordProtected
                {
                    ui.label(
                        egui::RichText::new(
                            "Enter matching passwords containing \
                             at least eight characters.",
                        )
                        .size(11.0)
                        .color(Theme::TEXT_SECONDARY),
                    );
                }
            }

            ui.add_space(10.0);

            ui.label(
                egui::RichText::new(shared_status(
                    &self.browser_gateway_status,
                    "Gateway status unavailable.",
                ))
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );
        });

        ui.add_space(12.0);

        self.render_browser_security_notice(ui);
    }

    fn render_browser_status_summary(
        &self,
        ui: &mut egui::Ui,
        running: bool,
        stopping: bool,
        discovered_count: usize,
    ) {
        let (state, color, detail) = if running {
            (
                "Browser sharing is active",
                Theme::ACCENT_GREEN,
                format!(
                    "{discovered_count} discovered stream(s) \
                     available to authenticated browser clients."
                ),
            )
        } else if stopping {
            (
                "Browser sharing is stopping",
                Theme::ACCENT_ORANGE,
                "Waiting for the HTTP and WebSocket gateway \
                 threads to exit."
                    .to_string(),
            )
        } else {
            (
                "Browser sharing is private",
                Theme::TEXT_SECONDARY,
                "The gateway is disabled and no browser playback \
                 endpoint is exposed."
                    .to_string(),
            )
        };

        egui::Frame::none()
            .fill(Theme::BG_SECONDARY)
            .rounding(12.0)
            .inner_margin(16.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(
                            if running {
                                "●"
                            } else {
                                "○"
                            },
                        )
                        .size(20.0)
                        .color(color),
                    );

                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(state)
                                .size(16.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );

                        ui.label(
                            egui::RichText::new(detail)
                                .size(11.0)
                                .color(Theme::TEXT_SECONDARY),
                        );
                    });
                });
            });
    }

    fn render_browser_password_fields(
        &mut self,
        ui: &mut egui::Ui,
    ) {
        egui::Frame::none()
            .fill(Theme::BG_CARD)
            .rounding(8.0)
            .inner_margin(12.0)
            .show(ui, |ui| {
                ui.label(
                    egui::RichText::new(
                        "Visitors must authenticate before they can \
                         discover or play streams.",
                    )
                    .size(11.0)
                    .color(Theme::TEXT_SECONDARY),
                );

                ui.add_space(8.0);

                egui::Grid::new(
                    "browser_gateway_password_grid",
                )
                .num_columns(2)
                .spacing([14.0, 8.0])
                .show(ui, |ui| {
                    ui.label(
                        egui::RichText::new("Password")
                            .color(Theme::TEXT_SECONDARY),
                    );

                    ui.add(
                        egui::TextEdit::singleline(
                            &mut self.browser_password,
                        )
                        .password(
                            !self.browser_show_password,
                        )
                        .desired_width(280.0)
                        .hint_text("At least 8 characters"),
                    );

                    ui.end_row();

                    ui.label(
                        egui::RichText::new("Confirm")
                            .color(Theme::TEXT_SECONDARY),
                    );

                    ui.add(
                        egui::TextEdit::singleline(
                            &mut self
                                .browser_password_confirmation,
                        )
                        .password(
                            !self.browser_show_password,
                        )
                        .desired_width(280.0)
                        .hint_text("Enter it again"),
                    );

                    ui.end_row();
                });

                ui.checkbox(
                    &mut self.browser_show_password,
                    "Show password",
                );

                if !self.browser_password.is_empty()
                    && self
                        .browser_password
                        .chars()
                        .count()
                        < 8
                {
                    ui.label(
                        egui::RichText::new(
                            "Password must contain at least \
                             eight characters.",
                        )
                        .size(11.0)
                        .color(Theme::ACCENT_RED),
                    );
                } else if !self
                    .browser_password_confirmation
                    .is_empty()
                    && self.browser_password
                        != self.browser_password_confirmation
                {
                    ui.label(
                        egui::RichText::new(
                            "The passwords do not match.",
                        )
                        .size(11.0)
                        .color(Theme::ACCENT_RED),
                    );
                }
            });
    }

    fn render_active_gateway_details(
        &self,
        ui: &mut egui::Ui,
    ) {
        card_frame().show(ui, |ui| {
            ui.label(
                egui::RichText::new("Gateway Address")
                    .size(12.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
            );

            ui.add_space(5.0);

            ui.monospace(
                egui::RichText::new(
                    "http://<this-machine-IP>:7100/",
                )
                .color(Theme::ACCENT_BLUE),
            );

            ui.add_space(5.0);

            ui.label(
                egui::RichText::new(
                    "HTTP player/API: 7100  •  WebSocket relay: 7101",
                )
                .size(11.0)
                .color(Theme::TEXT_SECONDARY),
            );

            let access_label =
                match self.browser_access_mode {
                    BrowserAccessMode::PasswordProtected => {
                        "Access: password protected"
                    }
                    BrowserAccessMode::OpenLan => {
                        "Access: open LAN"
                    }
                };

            let access_color =
                match self.browser_access_mode {
                    BrowserAccessMode::PasswordProtected => {
                        Theme::ACCENT_GREEN
                    }
                    BrowserAccessMode::OpenLan => {
                        egui::Color32::YELLOW
                    }
                };

            ui.label(
                egui::RichText::new(access_label)
                    .size(11.0)
                    .strong()
                    .color(access_color),
            );
        });
    }

    fn browser_password_valid(&self) -> bool {
        match self.browser_access_mode {
            BrowserAccessMode::OpenLan => true,
            BrowserAccessMode::PasswordProtected => {
                self.browser_password.chars().count() >= 8
                    && self.browser_password
                        == self.browser_password_confirmation
            }
        }
    }

    fn start_browser_gateway(&mut self) {
        if self
            .browser_gateway_thread_active
            .load(Ordering::Acquire)
        {
            return;
        }

        if !self.browser_password_valid() {
            set_shared_optional_message(
                &self.error_banner,
                Some(
                    "Enter matching passwords containing at least \
                     eight characters."
                        .to_string(),
                ),
            );

            return;
        }

        self.browser_gateway_last_toggle =
            Instant::now();

        set_shared_optional_message(
            &self.error_banner,
            None,
        );

        let access = match self.browser_access_mode {
            BrowserAccessMode::OpenLan => {
                audio_core::GatewayAccess::Open
            }
            BrowserAccessMode::PasswordProtected => {
                audio_core::GatewayAccess::PasswordProtected {
                    password: self.browser_password.clone(),
                }
            }
        };

        let config = audio_core::WebGatewayConfig {
            http_port: 7100,
            access,
            max_clients: 8,
            session_duration: Duration::from_secs(
                8 * 60 * 60,
            ),
        };

        let directory =
            self.discovery_directory.clone();

        let running =
            self.browser_gateway_running.clone();

        let thread_active =
            self.browser_gateway_thread_active.clone();

        let status =
            self.browser_gateway_status.clone();

        let error_banner =
            self.error_banner.clone();

        running.store(true, Ordering::Release);
        thread_active.store(true, Ordering::Release);

        set_shared_status(
            &status,
            "Starting browser gateway...",
        );

        // The gateway configuration owns its password copy.
        self.browser_password.clear();
        self.browser_password_confirmation.clear();
        self.browser_show_password = false;
                let worker_running = Arc::clone(&running);
        let worker_thread_active = Arc::clone(&thread_active);
        let worker_status = Arc::clone(&status);
        let worker_error_banner = Arc::clone(&error_banner);


                let spawn_result = thread::Builder::new()
            .name("openaudio-browser-gateway".to_string())
            .spawn(move || {
                audio_core::ensure_realtime_audio_thread();

                set_shared_status(
                    &worker_status,
                    "Browser gateway running.",
                );

                let result = audio_core::run_web_gateway(
                    config,
                    directory,
                    Arc::clone(&worker_running),
                );

                worker_running.store(false, Ordering::Release);

                match result {
                    Ok(()) => {
                        set_shared_status(
                            &worker_status,
                            "Gateway stopped. Browser playback is not exposed.",
                        );
                    }
                    Err(error) => {
                        let friendly = friendly_error(&error);

                        set_shared_status(
                            &worker_status,
                            format!("Gateway error: {friendly}"),
                        );

                        set_shared_optional_message(
                            &worker_error_banner,
                            Some(format!(
                                "Browser gateway failed: {friendly}"
                            )),
                        );
                    }
                }

                worker_thread_active.store(
                    false,
                    Ordering::Release,
                );
            });

        if let Err(error) = spawn_result {
            running.store(false, Ordering::Release);
            thread_active.store(false, Ordering::Release);

            let message = format!(
                "Could not start the browser gateway thread: {error}"
            );

            set_shared_status(&status, &message);

            set_shared_optional_message(
                &self.error_banner,
                Some(message),
            );
        }

    }

    fn render_browser_security_notice(
        &self,
        ui: &mut egui::Ui,
    ) {
        section_frame().show(ui, |ui| {
            ui.label(
                egui::RichText::new(
                    "Security and Bandwidth",
                )
                .size(17.0)
                .strong()
                .color(Theme::TEXT_PRIMARY),
            );

            ui.add_space(8.0);

            egui::Grid::new(
                "browser_security_information",
            )
            .num_columns(2)
            .spacing([16.0, 8.0])
            .striped(true)
            .show(ui, |ui| {
                ui.label(
                    egui::RichText::new("Authentication")
                        .strong()
                        .color(Theme::ACCENT_GREEN),
                );
                ui.label(
                    "Password mode protects stream discovery and \
                     WebSocket access.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("Encryption")
                        .strong()
                        .color(egui::Color32::YELLOW),
                );
                ui.label(
                    "HTTP does not encrypt audio or credentials in \
                     transit. Use only on a trusted LAN.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("Bandwidth")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "The current gateway sends every source channel \
                     to each browser client.",
                );
                ui.end_row();

                ui.label(
                    egui::RichText::new("32-channel load")
                        .strong()
                        .color(Theme::ACCENT_ORANGE),
                );
                ui.label(
                    "Approximately 49.15 Mbps per browser at 48 kHz \
                     Float32, before protocol overhead.",
                );
                ui.end_row();
            });

            ui.add_space(10.0);

            ui.label(
                egui::RichText::new(
                    "Selecting one channel in the browser currently \
                     changes the audible mix only. Gateway-side channel \
                     filtering is still required to reduce transmitted \
                     browser bandwidth.",
                )
                .size(11.0)
                .italics()
                .color(egui::Color32::YELLOW),
            );
        });
    }
}

// ============================================================================
// APPLICATION SHUTDOWN
// ============================================================================

impl Drop for OpenAudioApp {
    fn drop(&mut self) {
        for session in &self.publish_sessions {
            session.running.store(false, Ordering::Release);
        }

        for session in &self.combine_publish_sessions {
            session.running.store(false, Ordering::Release);
        }

        for session in &self.asio_publish_sessions {
            session.running.store(false, Ordering::Release);
        }

        for session in &self.subscribe_sessions {
            session.running.store(false, Ordering::Release);
        }

        for session in &self.split_subscribe_sessions {
            session.running.store(false, Ordering::Release);
        }

        self.browser_gateway_running
            .store(false, Ordering::Release);
    }
}

// ============================================================================
// APPLICATION ICON
// ============================================================================

fn load_icon() -> egui::IconData {
    let size = 64u32;
    let mut rgba =
        Vec::with_capacity((size * size * 4) as usize);

    let center_x = size as f32 / 2.0;
    let center_y = size as f32 / 2.0;

    for y in 0..size {
        for x in 0..size {
            let dx = x as f32 - center_x;
            let dy = y as f32 - center_y;
            let distance = (dx * dx + dy * dy).sqrt();

            let outer_radius = size as f32 / 2.0;
            let inner_radius = outer_radius * 0.55;

            if distance <= outer_radius
                && distance >= inner_radius
            {
                let angle = dy.atan2(dx);

                let wave =
                    ((angle * 3.0).sin() * 0.5 + 0.5)
                        * 0.4
                        + 0.6;

                rgba.push((wave * 100.0) as u8);
                rgba.push((wave * 180.0) as u8);
                rgba.push(255);
                rgba.push(255);
            } else if distance < inner_radius {
                let pulse = (
                    distance
                        / inner_radius
                        * std::f32::consts::PI
                )
                    .sin()
                    .abs();

                rgba.push((pulse * 80.0) as u8);
                rgba.push((pulse * 140.0) as u8);
                rgba.push((pulse * 220.0) as u8);
                rgba.push(200);
            } else {
                rgba.extend_from_slice(&[0, 0, 0, 0]);
            }
        }
    }

    egui::IconData {
        rgba,
        width: size,
        height: size,
    }
}

// ============================================================================
// MAIN
// ============================================================================

fn main() -> eframe::Result<()> {
    audio_core::prepare_realtime_process();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1180.0, 820.0])
            .with_min_inner_size([820.0, 600.0])
            .with_icon(load_icon()),
        ..Default::default()
    };

    eframe::run_native(
        "Ferronme's Open Audio",
        options,
        Box::new(|_creation_context| {
            Ok(Box::new(OpenAudioApp::default()))
        }),
    )
}
