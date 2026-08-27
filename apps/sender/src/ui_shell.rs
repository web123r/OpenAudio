use eframe::egui;

const BG_SECONDARY: egui::Color32 = egui::Color32::from_rgb(28, 28, 32);
const BG_CARD: egui::Color32 = egui::Color32::from_rgb(38, 38, 42);
const TEXT_PRIMARY: egui::Color32 = egui::Color32::from_rgb(255, 255, 255);
const TEXT_SECONDARY: egui::Color32 = egui::Color32::from_rgb(152, 152, 157);
const ACCENT_BLUE: egui::Color32 = egui::Color32::from_rgb(0, 122, 255);
const ACCENT_GREEN: egui::Color32 = egui::Color32::from_rgb(52, 199, 89);
const ACCENT_PURPLE: egui::Color32 = egui::Color32::from_rgb(175, 82, 222);
const ACCENT_ORANGE: egui::Color32 = egui::Color32::from_rgb(255, 149, 0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AppPage {
    #[default]
    Overview,
    Publish,
    Subscribe,
    Browser,
    Diagnostics,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UiSummary {
    pub configured_publishers: usize,
    pub running_publishers: usize,
    pub configured_subscribers: usize,
    pub running_subscribers: usize,
    pub discovered_streams: usize,
    pub gateway_running: bool,
    pub gateway_stopping: bool,
}

impl AppPage {
    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Publish => "Publish Audio",
            Self::Subscribe => "Receive Audio",
            Self::Browser => "Browser Sharing",
            Self::Diagnostics => "Diagnostics",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Overview => {
                "Monitor active signals and review the current OpenAudio system state."
            }
            Self::Publish => {
                "Capture audio devices or ASIO channels and publish them to the network."
            }
            Self::Subscribe => {
                "Receive discovered streams and route them to WASAPI, split devices, or ASIO."
            }
            Self::Browser => {
                "Control secure browser playback and trusted-LAN access."
            }
            Self::Diagnostics => {
                "Generate synthetic streams and test multichannel network performance."
            }
        }
    }
}

pub fn render_navigation(
    ui: &mut egui::Ui,
    active_page: &mut AppPage,
    summary: UiSummary,
) {
    egui::Frame::none()
        .fill(BG_SECONDARY)
        .rounding(12.0)
        .inner_margin(12.0)
        .show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                navigation_button(
                    ui,
                    active_page,
                    AppPage::Overview,
                    "⌂",
                    "Overview",
                    ACCENT_BLUE,
                );

                navigation_button(
                    ui,
                    active_page,
                    AppPage::Publish,
                    "↑",
                    "Publish",
                    ACCENT_GREEN,
                );

                navigation_button(
                    ui,
                    active_page,
                    AppPage::Subscribe,
                    "↓",
                    "Subscribe",
                    ACCENT_PURPLE,
                );

                navigation_button(
                    ui,
                    active_page,
                    AppPage::Browser,
                    "◎",
                    "Browser",
                    ACCENT_BLUE,
                );

                navigation_button(
                    ui,
                    active_page,
                    AppPage::Diagnostics,
                    "⚙",
                    "Diagnostics",
                    ACCENT_ORANGE,
                );
            });
        });

    ui.add_space(12.0);

    render_page_heading(ui, *active_page);

    ui.add_space(12.0);

    render_summary(ui, summary);
}

fn navigation_button(
    ui: &mut egui::Ui,
    active_page: &mut AppPage,
    page: AppPage,
    icon: &str,
    label: &str,
    accent: egui::Color32,
) {
    let selected = *active_page == page;

    let fill = if selected {
        accent
    } else {
        egui::Color32::TRANSPARENT
    };

    let text_color = if selected {
        egui::Color32::WHITE
    } else {
        TEXT_SECONDARY
    };

    let response = ui.add(
        egui::Button::new(
            egui::RichText::new(format!("{icon}  {label}"))
                .size(12.0)
                .strong()
                .color(text_color),
        )
        .fill(fill)
        .rounding(8.0)
        .min_size(egui::vec2(112.0, 34.0)),
    );

    if response.clicked() {
        *active_page = page;
    }
}

fn render_page_heading(ui: &mut egui::Ui, active_page: AppPage) {
    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.label(
                egui::RichText::new(active_page.title())
                    .size(22.0)
                    .strong()
                    .color(TEXT_PRIMARY),
            );

            ui.add_space(2.0);

            ui.label(
                egui::RichText::new(active_page.description())
                    .size(12.0)
                    .color(TEXT_SECONDARY),
            );
        });
    });
}

fn render_summary(ui: &mut egui::Ui, summary: UiSummary) {
    let available_width = ui.available_width();

    if available_width >= 780.0 {
        ui.columns(4, |columns| {
            summary_card(
                &mut columns[0],
                "Publishers",
                &format!(
                    "{} running",
                    summary.running_publishers
                ),
                &format!(
                    "{} configured",
                    summary.configured_publishers
                ),
                ACCENT_GREEN,
            );

            summary_card(
                &mut columns[1],
                "Subscribers",
                &format!(
                    "{} running",
                    summary.running_subscribers
                ),
                &format!(
                    "{} configured",
                    summary.configured_subscribers
                ),
                ACCENT_PURPLE,
            );

            summary_card(
                &mut columns[2],
                "Discovered",
                &summary.discovered_streams.to_string(),
                "network streams",
                ACCENT_BLUE,
            );

            gateway_summary_card(&mut columns[3], summary);
        });
    } else {
        egui::Grid::new("responsive_summary_grid")
            .num_columns(2)
            .spacing([8.0, 8.0])
            .show(ui, |ui| {
                compact_summary_card(
                    ui,
                    "Publishers",
                    &format!(
                        "{}/{}",
                        summary.running_publishers,
                        summary.configured_publishers
                    ),
                    ACCENT_GREEN,
                );

                compact_summary_card(
                    ui,
                    "Subscribers",
                    &format!(
                        "{}/{}",
                        summary.running_subscribers,
                        summary.configured_subscribers
                    ),
                    ACCENT_PURPLE,
                );

                ui.end_row();

                compact_summary_card(
                    ui,
                    "Discovered",
                    &summary.discovered_streams.to_string(),
                    ACCENT_BLUE,
                );

                let gateway_text = if summary.gateway_running {
                    "Running"
                } else if summary.gateway_stopping {
                    "Stopping"
                } else {
                    "Stopped"
                };

                let gateway_color = if summary.gateway_running {
                    ACCENT_GREEN
                } else if summary.gateway_stopping {
                    ACCENT_ORANGE
                } else {
                    TEXT_SECONDARY
                };

                compact_summary_card(
                    ui,
                    "Gateway",
                    gateway_text,
                    gateway_color,
                );

                ui.end_row();
            });
    }
}

fn summary_card(
    ui: &mut egui::Ui,
    title: &str,
    primary: &str,
    secondary: &str,
    accent: egui::Color32,
) {
    egui::Frame::none()
        .fill(BG_CARD)
        .rounding(10.0)
        .inner_margin(12.0)
        .show(ui, |ui| {
            ui.set_min_height(72.0);

            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new("●")
                        .size(10.0)
                        .color(accent),
                );

                ui.label(
                    egui::RichText::new(title)
                        .size(11.0)
                        .strong()
                        .color(TEXT_SECONDARY),
                );
            });

            ui.add_space(5.0);

            ui.label(
                egui::RichText::new(primary)
                    .size(18.0)
                    .strong()
                    .color(TEXT_PRIMARY),
            );

            ui.label(
                egui::RichText::new(secondary)
                    .size(10.0)
                    .color(TEXT_SECONDARY),
            );
        });
}

fn gateway_summary_card(
    ui: &mut egui::Ui,
    summary: UiSummary,
) {
    let (primary, secondary, color) = if summary.gateway_running {
        (
            "Running",
            "browser playback exposed",
            ACCENT_GREEN,
        )
    } else if summary.gateway_stopping {
        (
            "Stopping",
            "waiting for gateway thread",
            ACCENT_ORANGE,
        )
    } else {
        (
            "Stopped",
            "browser playback private",
            TEXT_SECONDARY,
        )
    };

    summary_card(
        ui,
        "Browser Gateway",
        primary,
        secondary,
        color,
    );
}

fn compact_summary_card(
    ui: &mut egui::Ui,
    title: &str,
    value: &str,
    accent: egui::Color32,
) {
    let available = (ui.available_width() * 0.5 - 6.0).max(135.0);

    egui::Frame::none()
        .fill(BG_CARD)
        .rounding(8.0)
        .inner_margin(10.0)
        .show(ui, |ui| {
            ui.set_min_width(available);
            ui.set_min_height(54.0);

            ui.label(
                egui::RichText::new(title)
                    .size(10.0)
                    .color(TEXT_SECONDARY),
            );

            ui.label(
                egui::RichText::new(value)
                    .size(15.0)
                    .strong()
                    .color(accent),
            );
        });
}
