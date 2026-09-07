//! Desktop UI and session management for routing discovered OpenAudio
//! network streams to ASIO output channels.

use eframe::egui;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct AsioSubscribePanel {
    next_id: u64,
    sessions: Vec<AsioSubscribeSession>,
}

struct AsioSubscribeSession {
    id: u64,
    selected_node_id: Option<String>,
    selected_driver: Option<String>,
    incoming_channel_count: usize,
    channel_labels: Vec<String>,
    driver_output_count: usize,
    output_routes: Vec<Option<usize>>,
    bind_port: String,
    record: bool,
    running: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    last_toggle: Instant,
}

#[derive(Clone)]
struct DiscoveredStream {
    node_id: String,
    node_name: String,
    stream_name: String,
    stream_id: u32,
    channel_count: usize,
    channel_labels: Vec<String>,
    ip: String,
    control_port: u16,
}

impl Default for AsioSubscribePanel {
    fn default() -> Self {
        Self {
            next_id: 1,
            sessions: Vec::new(),
        }
    }
}

impl AsioSubscribePanel {
    pub fn ui(
        &mut self,
        ui: &mut egui::Ui,
        asio_drivers: &[audio_core::AsioDriverInfo],
        discovery_directory: &Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>>,
        error_banner: &Arc<Mutex<Option<String>>>,
    ) {
        let discovered_streams = snapshot_discovered_streams(discovery_directory);

        egui::Frame::none()
            .fill(egui::Color32::from_rgb(28, 28, 32))
            .rounding(12.0)
            .inner_margin(16.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("🎛 ASIO Subscribe")
                            .size(18.0)
                            .strong()
                            .color(egui::Color32::WHITE),
                    );
                    ui.label(
                        egui::RichText::new("(route network channels directly to ASIO outputs)")
                            .size(11.0)
                            .color(egui::Color32::from_rgb(152, 152, 157)),
                    );
                });

                ui.add_space(6.0);

                if asio_drivers.is_empty() {
                    egui::Frame::none()
                        .fill(egui::Color32::from_rgb(38, 38, 42))
                        .rounding(8.0)
                        .inner_margin(10.0)
                        .show(ui, |ui| {
                            ui.label(
                                egui::RichText::new(
                                    "No ASIO drivers detected. Install or enable an ASIO \
                                     driver, then click Refresh Devices.",
                                )
                                .size(11.0)
                                .color(egui::Color32::from_rgb(152, 152, 157)),
                            );
                        });
                }

                ui.add_space(8.0);

                let mut remove_index = None;

                for (index, session) in self.sessions.iter_mut().enumerate() {
                    let is_running = session.running.load(Ordering::Relaxed);
                    let debounce_ready = session.last_toggle.elapsed() > Duration::from_millis(400);

                    egui::Frame::none()
                        .fill(egui::Color32::from_rgb(38, 38, 42))
                        .rounding(10.0)
                        .inner_margin(14.0)
                        .show(ui, |ui| {
                            render_stream_selector(ui, session, &discovered_streams, is_running);

                            ui.add_space(10.0);

                            render_driver_selector(ui, session, asio_drivers, is_running);

                            ui.add_space(10.0);

                            ui.horizontal(|ui| {
                                ui.label(
                                    egui::RichText::new("Local UDP Port:").color(secondary_text()),
                                );

                                ui.add_enabled(
                                    !is_running,
                                    egui::TextEdit::singleline(&mut session.bind_port)
                                        .desired_width(90.0),
                                );

                                ui.add_enabled_ui(!is_running, |ui| {
                                    ui.checkbox(
                                        &mut session.record,
                                        egui::RichText::new("⏺ Record routed ASIO output")
                                            .color(egui::Color32::WHITE),
                                    );
                                });
                            });

                            if session.incoming_channel_count > 0 {
                                ui.add_space(12.0);
                                render_quick_routes(ui, session, is_running);

                                ui.add_space(8.0);
                                render_routing_grid(ui, session, is_running);
                            }

                            ui.add_space(10.0);
                            ui.separator();
                            ui.add_space(6.0);

                            ui.horizontal(|ui| {
                                if is_running {
                                    if ui
                                        .add_enabled(
                                            debounce_ready,
                                            egui::Button::new(
                                                egui::RichText::new("⏹ Stop")
                                                    .color(egui::Color32::WHITE),
                                            )
                                            .fill(egui::Color32::from_rgb(255, 69, 58))
                                            .rounding(8.0)
                                            .min_size(egui::vec2(70.0, 28.0)),
                                        )
                                        .clicked()
                                    {
                                        session.last_toggle = Instant::now();
                                        session.running.store(false, Ordering::Relaxed);
                                        set_status(session, "Stopping...");
                                    }
                                } else {
                                    let validation = validate_session(
                                        session,
                                        asio_drivers,
                                        &discovered_streams,
                                    );
                                    let can_start = validation.is_ok();

                                    if ui
                                        .add_enabled(
                                            debounce_ready && can_start,
                                            egui::Button::new(
                                                egui::RichText::new("▶ Start ASIO Subscribe")
                                                    .color(egui::Color32::WHITE),
                                            )
                                            .fill(egui::Color32::from_rgb(175, 82, 222))
                                            .rounding(8.0)
                                            .min_size(egui::vec2(155.0, 28.0)),
                                        )
                                        .clicked()
                                    {
                                        start_session(session, &discovered_streams, error_banner);
                                    }

                                    if let Err(reason) = validation {
                                        ui.label(
                                            egui::RichText::new(reason)
                                                .size(11.0)
                                                .color(secondary_text()),
                                        );
                                    }

                                    if ui
                                        .add(
                                            egui::Button::new("🗑 Remove")
                                                .fill(egui::Color32::from_rgb(28, 28, 32))
                                                .rounding(8.0),
                                        )
                                        .clicked()
                                    {
                                        remove_index = Some(index);
                                    }
                                }

                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        let color = if is_running {
                                            egui::Color32::from_rgb(175, 82, 222)
                                        } else {
                                            secondary_text()
                                        };

                                        ui.label(
                                            egui::RichText::new(session_status(session))
                                                .color(secondary_text()),
                                        );
                                        ui.label(
                                            egui::RichText::new(if is_running {
                                                "●"
                                            } else {
                                                "○"
                                            })
                                            .size(14.0)
                                            .color(color),
                                        );
                                    },
                                );
                            });
                        });

                    ui.add_space(8.0);
                }

                if let Some(index) = remove_index {
                    if index < self.sessions.len() {
                        self.sessions.remove(index);
                    }
                }

                ui.add_space(6.0);

                if ui
                    .add(
                        egui::Button::new(
                            egui::RichText::new("+ Add ASIO Subscribe Stream")
                                .color(egui::Color32::WHITE),
                        )
                        .fill(egui::Color32::from_rgb(175, 82, 222))
                        .rounding(8.0),
                    )
                    .clicked()
                {
                    self.add_session();
                }
            });
    }

    fn add_session(&mut self) {
        let id = self.next_id;
        self.next_id += 1;

        self.sessions.push(AsioSubscribeSession {
            id,
            selected_node_id: None,
            selected_driver: None,
            incoming_channel_count: 0,
            channel_labels: Vec::new(),
            driver_output_count: 0,
            output_routes: Vec::new(),
            bind_port: (7050u64.saturating_add(id)).to_string(),
            record: false,
            running: Arc::new(AtomicBool::new(false)),
            status: Arc::new(Mutex::new("Not started.".to_string())),
            last_toggle: Instant::now() - Duration::from_secs(1),
        });
    }
}

fn render_stream_selector(
    ui: &mut egui::Ui,
    session: &mut AsioSubscribeSession,
    streams: &[DiscoveredStream],
    is_running: bool,
) {
    ui.label(
        egui::RichText::new("Discovered Stream:")
            .size(12.0)
            .color(secondary_text()),
    );

    if streams.is_empty() {
        ui.label(
            egui::RichText::new("(none discovered yet)")
                .italics()
                .color(secondary_text()),
        );
        return;
    }

    ui.add_enabled_ui(!is_running, |ui| {
        for stream in streams {
            let label = format!(
                "{} — \"{}\" ({}ch, {})",
                stream.node_name, stream.stream_name, stream.channel_count, stream.ip
            );

            let was_selected = session.selected_node_id.as_deref() == Some(stream.node_id.as_str());

            ui.radio_value(
                &mut session.selected_node_id,
                Some(stream.node_id.clone()),
                label,
            );

            let is_selected = session.selected_node_id.as_deref() == Some(stream.node_id.as_str());

            if is_selected && !was_selected {
                session.incoming_channel_count = stream.channel_count;
                session.channel_labels = stream.channel_labels.clone();
                session.output_routes =
                    default_routes(stream.channel_count, session.driver_output_count);
            }
        }
    });
}

fn render_driver_selector(
    ui: &mut egui::Ui,
    session: &mut AsioSubscribeSession,
    asio_drivers: &[audio_core::AsioDriverInfo],
    is_running: bool,
) {
    ui.add_enabled_ui(!is_running, |ui| {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("ASIO Output Driver:").color(secondary_text()));

            let selected_label = session
                .selected_driver
                .clone()
                .unwrap_or_else(|| "— select ASIO output driver —".to_string());

            egui::ComboBox::from_id_source(format!("asio_sub_driver_{}", session.id))
                .selected_text(selected_label)
                .width(350.0)
                .show_ui(ui, |ui| {
                    if ui
                        .selectable_label(
                            session.selected_driver.is_none(),
                            "— select ASIO output driver —",
                        )
                        .clicked()
                    {
                        session.selected_driver = None;
                        session.driver_output_count = 0;
                        session.output_routes = vec![None; session.incoming_channel_count];
                    }

                    for driver in asio_drivers {
                        // Subscribe requires an output-capable driver.
                        if driver.max_output_channels == 0 {
                            continue;
                        }

                        let selected =
                            session.selected_driver.as_deref() == Some(driver.name.as_str());

                        let label = format!(
                            "{} ({} in / {} out{})",
                            driver.name,
                            driver.max_input_channels,
                            driver.max_output_channels,
                            driver
                                .default_sample_rate
                                .map(|rate| format!(", {rate}Hz"))
                                .unwrap_or_default()
                        );

                        if ui.selectable_label(selected, label).clicked() {
                            session.selected_driver = Some(driver.name.clone());
                            session.driver_output_count = driver.max_output_channels as usize;
                            session.output_routes = default_routes(
                                session.incoming_channel_count,
                                session.driver_output_count,
                            );
                        }
                    }
                });
        });

        if let Some(driver_name) = session.selected_driver.as_deref() {
            ui.label(
                egui::RichText::new(format!(
                    "{} exposes {} output channel(s)",
                    driver_name, session.driver_output_count
                ))
                .size(11.0)
                .color(egui::Color32::from_rgb(52, 199, 89)),
            );
        }
    });
}

fn render_quick_routes(ui: &mut egui::Ui, session: &mut AsioSubscribeSession, is_running: bool) {
    ui.add_enabled_ui(!is_running, |ui| {
        ui.horizontal_wrapped(|ui| {
            ui.label(egui::RichText::new("Quick routing:").color(secondary_text()));

            if ui.small_button("Sequential").clicked() {
                session.output_routes =
                    default_routes(session.incoming_channel_count, session.driver_output_count);
            }

            if ui.small_button("Clear").clicked() {
                session.output_routes = vec![None; session.incoming_channel_count];
            }

            if session.incoming_channel_count >= 2
                && session.driver_output_count >= 2
                && ui.small_button("Stream 1–2 → Out 1–2").clicked()
            {
                session.output_routes = vec![None; session.incoming_channel_count];
                session.output_routes[0] = Some(0);
                session.output_routes[1] = Some(1);
            }

            if session.incoming_channel_count >= 2
                && session.driver_output_count >= 4
                && ui.small_button("Stream 1–2 → Out 3–4").clicked()
            {
                session.output_routes = vec![None; session.incoming_channel_count];
                session.output_routes[0] = Some(2);
                session.output_routes[1] = Some(3);
            }

            if session.incoming_channel_count >= 2
                && session.driver_output_count >= 8
                && ui.small_button("Stream 1–2 → Out 7–8").clicked()
            {
                session.output_routes = vec![None; session.incoming_channel_count];
                session.output_routes[0] = Some(6);
                session.output_routes[1] = Some(7);
            }
        });
    });
}

fn render_routing_grid(ui: &mut egui::Ui, session: &mut AsioSubscribeSession, is_running: bool) {
    ui.label(
        egui::RichText::new(format!(
            "Channel Routes ({} incoming → {} ASIO outputs):",
            session.incoming_channel_count, session.driver_output_count
        ))
        .size(12.0)
        .color(secondary_text()),
    );

    ui.add_space(4.0);

    egui::Grid::new(format!("asio_sub_routes_{}", session.id))
        .num_columns(3)
        .striped(true)
        .spacing([12.0, 6.0])
        .show(ui, |ui| {
            ui.label(
                egui::RichText::new("Stream channel")
                    .strong()
                    .color(secondary_text()),
            );
            ui.label(
                egui::RichText::new("Route")
                    .strong()
                    .color(secondary_text()),
            );
            ui.label(
                egui::RichText::new("ASIO output")
                    .strong()
                    .color(secondary_text()),
            );
            ui.end_row();

            for incoming_index in 0..session.incoming_channel_count {
                ui.label(
                    egui::RichText::new(
                        session_channel_label(session, incoming_index),
                    )
                        .color(egui::Color32::WHITE),
                );

                ui.label(egui::RichText::new("→").color(secondary_text()));

                let current_route = session.output_routes.get(incoming_index).copied().flatten();

                let current_label = current_route
                    .map(|output| format!("ASIO Out {}", output + 1))
                    .unwrap_or_else(|| "Disabled".to_string());

                ui.add_enabled_ui(!is_running, |ui| {
                    egui::ComboBox::from_id_source(format!(
                        "asio_sub_route_{}_{}",
                        session.id, incoming_index
                    ))
                    .selected_text(current_label)
                    .width(145.0)
                    .show_ui(ui, |ui| {
                        if let Some(route) = session.output_routes.get_mut(incoming_index) {
                            ui.selectable_value(route, None, "Disabled");

                            for output_index in 0..session.driver_output_count {
                                ui.selectable_value(
                                    route,
                                    Some(output_index),
                                    format!("ASIO Out {}", output_index + 1),
                                );
                            }
                        }
                    });
                });

                ui.end_row();
            }
        });

    let active_routes = session
        .output_routes
        .iter()
        .filter(|route| route.is_some())
        .count();

    ui.add_space(4.0);
    ui.label(
        egui::RichText::new(format!(
            "{active_routes} of {} incoming channel(s) routed",
            session.incoming_channel_count
        ))
        .size(11.0)
        .color(if active_routes > 0 {
            egui::Color32::from_rgb(52, 199, 89)
        } else {
            egui::Color32::from_rgb(255, 69, 58)
        }),
    );
}

fn session_channel_label(session: &AsioSubscribeSession, channel_index: usize) -> String {
    session
        .channel_labels
        .get(channel_index)
        .cloned()
        .unwrap_or_else(|| format!("Channel {}", channel_index + 1))
}

fn validate_session(
    session: &AsioSubscribeSession,
    asio_drivers: &[audio_core::AsioDriverInfo],
    streams: &[DiscoveredStream],
) -> Result<(), &'static str> {
    let Some(node_id) = session.selected_node_id.as_deref() else {
        return Err("← select a discovered stream");
    };

    let Some(stream) = streams.iter().find(|item| item.node_id == node_id) else {
        return Err("← selected stream is no longer available");
    };

    if stream.channel_count == 0 {
        return Err("← selected stream reports zero channels");
    }

    let Some(driver_name) = session.selected_driver.as_deref() else {
        return Err("← select an ASIO output driver");
    };

    let Some(driver) = asio_drivers
        .iter()
        .find(|driver| driver.name == driver_name)
    else {
        return Err("← selected ASIO driver is no longer available");
    };

    if driver.max_output_channels == 0 {
        return Err("← selected ASIO driver has no output channels");
    }

    if session.output_routes.len() != stream.channel_count {
        return Err("← routing map does not match the stream channel count");
    }

    if session.output_routes.iter().all(Option::is_none) {
        return Err("← route at least one incoming channel");
    }

    if session
        .output_routes
        .iter()
        .flatten()
        .any(|output| *output >= driver.max_output_channels as usize)
    {
        return Err("← one or more routes exceed the driver output count");
    }

    if session.bind_port.parse::<u16>().is_err() {
        return Err("← enter a valid UDP port");
    }

    Ok(())
}

fn start_session(
    session: &mut AsioSubscribeSession,
    streams: &[DiscoveredStream],
    error_banner: &Arc<Mutex<Option<String>>>,
) {
    session.last_toggle = Instant::now();

    if let Ok(mut banner) = error_banner.lock() {
        *banner = None;
    }

    let Some(node_id) = session.selected_node_id.as_deref() else {
        set_error(error_banner, "Select a discovered stream first.");
        return;
    };

    let Some(stream) = streams
        .iter()
        .find(|stream| stream.node_id == node_id)
        .cloned()
    else {
        set_error(error_banner, "The selected stream is no longer available.");
        return;
    };

    let Some(driver_name) = session.selected_driver.clone() else {
        set_error(error_banner, "Select an ASIO output driver first.");
        return;
    };

    let port = match session.bind_port.parse::<u16>() {
        Ok(port) if port != 0 => port,
        _ => {
            set_error(
                error_banner,
                &format!("'{}' is not a valid UDP port.", session.bind_port),
            );
            return;
        }
    };

    if session.output_routes.len() != stream.channel_count {
        set_error(
            error_banner,
            "The routing map no longer matches the selected stream.",
        );
        return;
    }

    if session.output_routes.iter().all(Option::is_none) {
        set_error(
            error_banner,
            "Route at least one incoming channel before starting.",
        );
        return;
    }

    if let Err(error) = crate::reserve_asio_device(&driver_name) {
        set_error(error_banner, &error);
        return;
    }

    if let Err(error) =
        audio_core::send_subscribe_request(&stream.ip, stream.control_port, stream.stream_id, port)
    {
        crate::release_asio_device(&driver_name);
        set_error(
            error_banner,
            &format!("Failed to subscribe to the publisher: {error}"),
        );
        return;
    }

    let bind_addr = format!("0.0.0.0:{port}");
    let output_routes = session.output_routes.clone();
    let record_path = if session.record {
        Some(audio_core::generate_record_path(&format!(
            "asio_subscribe_{}_{}",
            stream.stream_id, port
        )))
    } else {
        None
    };

    let running = session.running.clone();
    let worker_running = running.clone();
    let status = session.status.clone();
    let worker_status = status.clone();
    let worker_error_banner = error_banner.clone();
    let stream_name = stream.stream_name.clone();
    let node_name = stream.node_name.clone();
    let driver_status_name = driver_name.clone();

    running.store(true, Ordering::Relaxed);

    let reconnect_running = running.clone();
    let reconnect_ip = stream.ip.clone();
    let reconnect_control_port = stream.control_port;
    let reconnect_stream_id = stream.stream_id;
    thread::spawn(move || {
        while reconnect_running.load(Ordering::Acquire) {
            if let Err(error) = audio_core::send_subscribe_request(
                &reconnect_ip,
                reconnect_control_port,
                reconnect_stream_id,
                port,
            ) {
                eprintln!(
                    "audio-core: ASIO subscription renewal failed: {error}"
                );
            }

            for _ in 0..10 {
                if !reconnect_running.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    });

    set_status(
        session,
        &format!("Starting '{}' → '{}'...", stream.stream_name, driver_name),
    );

    thread::spawn(move || {
        let result = audio_core::receive_and_play_asio(
            &bind_addr,
            driver_name.clone(),
            output_routes,
            record_path,
            worker_running.clone(),
        );

        match result {
            Ok(()) => {
                set_shared_status(&worker_status, "Stopped.");
            }
            Err(error) => {
                let message = format!(
                    "ASIO Subscribe failed for '{}' on '{}' → '{}': {}",
                    stream_name, node_name, driver_status_name, error
                );

                set_shared_status(&worker_status, &format!("Error: {error}"));
                set_error(&worker_error_banner, &message);
            }
        }

        crate::release_asio_device(&driver_name);
        worker_running.store(false, Ordering::Relaxed);
    });
}

fn snapshot_discovered_streams(
    discovery_directory: &Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>>,
) -> Vec<DiscoveredStream> {
    let mut result = match discovery_directory.lock() {
        Ok(directory) => directory
            .iter()
            .map(|(node_id, node)| DiscoveredStream {
                node_id: node_id.clone(),
                node_name: node.node_name.clone(),
                stream_name: node.stream_name.clone(),
                stream_id: node.stream_id,
                channel_count: node.channel_count as usize,
                channel_labels: node.channel_labels.clone(),
                ip: node.ip.clone(),
                control_port: node.control_port,
            })
            .collect::<Vec<_>>(),
        Err(poisoned) => poisoned
            .into_inner()
            .iter()
            .map(|(node_id, node)| DiscoveredStream {
                node_id: node_id.clone(),
                node_name: node.node_name.clone(),
                stream_name: node.stream_name.clone(),
                stream_id: node.stream_id,
                channel_count: node.channel_count as usize,
                channel_labels: node.channel_labels.clone(),
                ip: node.ip.clone(),
                control_port: node.control_port,
            })
            .collect::<Vec<_>>(),
    };

    result.sort_by(|a, b| {
        a.node_name
            .to_lowercase()
            .cmp(&b.node_name.to_lowercase())
            .then_with(|| {
                a.stream_name
                    .to_lowercase()
                    .cmp(&b.stream_name.to_lowercase())
            })
    });

    result
}

fn default_routes(
    incoming_channel_count: usize,
    output_channel_count: usize,
) -> Vec<Option<usize>> {
    (0..incoming_channel_count)
        .map(|incoming_index| {
            if incoming_index < output_channel_count {
                Some(incoming_index)
            } else {
                None
            }
        })
        .collect()
}

fn session_status(session: &AsioSubscribeSession) -> String {
    match session.status.lock() {
        Ok(status) => status.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn set_status(session: &AsioSubscribeSession, message: &str) {
    set_shared_status(&session.status, message);
}

fn set_shared_status(status: &Arc<Mutex<String>>, message: &str) {
    match status.lock() {
        Ok(mut guard) => *guard = message.to_string(),
        Err(poisoned) => {
            *poisoned.into_inner() = message.to_string();
        }
    }
}

fn set_error(error_banner: &Arc<Mutex<Option<String>>>, message: &str) {
    match error_banner.lock() {
        Ok(mut banner) => *banner = Some(message.to_string()),
        Err(poisoned) => {
            *poisoned.into_inner() = Some(message.to_string());
        }
    }
}

fn secondary_text() -> egui::Color32 {
    egui::Color32::from_rgb(152, 152, 157)
}
