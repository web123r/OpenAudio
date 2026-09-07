use crate::backend::get_backend;

/// Sentinel string the GUI can store in an `Option<String>` device
/// field to mean "explicitly disabled -- open no device", distinct
/// from `None` (Rust's Option) which means "use system default".
/// Using a plain string keeps every existing `Option<String>`
/// field in main.rs (selected_input, selected_output, dev, etc.)
/// unchanged -- no struct/type changes needed there.
pub const NONE_DEVICE: &str = "None";

#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub name: String,
    pub is_default: bool,
    pub max_input_channels: u16,
    pub max_output_channels: u16,
    pub default_sample_rate: Option<u32>,
}

pub fn list_input_devices() -> Vec<DeviceInfo> {
    get_backend().list_input_devices()
}

pub fn list_output_devices() -> Vec<DeviceInfo> {
    get_backend().list_output_devices()
}

/// Three-state device selection: explicitly disabled, default, or
/// a specific named device. Parses the raw `Option<String>` value
/// GUI code already stores (e.g. `session.selected_output`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceSelection<'a> {
    NoDevice,
    Default,
    Named(&'a str),
}

impl<'a> DeviceSelection<'a> {
    pub fn parse(value: Option<&'a str>) -> Self {
        match value {
            None => DeviceSelection::Default,
            Some(n) if n == NONE_DEVICE => DeviceSelection::NoDevice,
            Some(n) => DeviceSelection::Named(n),
        }
    }
}

/// True if `value` is the explicit "skip this device" sentinel
/// (`Some(NONE_DEVICE)`). `None` (no selection / system default) is
/// NOT a skip -- callers that only need a bool check (bus.rs,
/// combine.rs, split.rs) use this instead of matching on
/// `DeviceSelection` directly.
pub fn is_skip(value: &Option<String>) -> bool {
    matches!(
        DeviceSelection::parse(value.as_deref()),
        DeviceSelection::NoDevice
    )
}

