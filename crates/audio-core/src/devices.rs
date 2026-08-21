use cpal::traits::{DeviceTrait, HostTrait};

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

enum Direction {
    Input,
    Output,
}

pub fn list_input_devices() -> Vec<DeviceInfo> {
    list_devices_for_host(cpal::default_host(), Direction::Input)
}

pub fn list_output_devices() -> Vec<DeviceInfo> {
    list_devices_for_host(cpal::default_host(), Direction::Output)
}

fn list_devices_for_host(host: cpal::Host, direction: Direction) -> Vec<DeviceInfo> {
    let default_name = match direction {
        Direction::Input => host.default_input_device().and_then(|d| d.name().ok()),
        Direction::Output => host.default_output_device().and_then(|d| d.name().ok()),
    };

    let devices = match host.devices() {
        Ok(devices) => devices,
        Err(err) => {
            eprintln!("audio-core: failed to enumerate devices: {err}");
            return Vec::new();
        }
    };

    let mut result = Vec::new();

    for device in devices {
        let raw_name = match device.name() {
            Ok(n) => n,
            Err(_) => continue,
        };

        let max_input_channels = match device.supported_input_configs() {
            Ok(cfgs) => cfgs.map(|c| c.channels()).max().unwrap_or(0),
            Err(_) => 0,
        };

        let max_output_channels = match device.supported_output_configs() {
            Ok(cfgs) => cfgs.map(|c| c.channels()).max().unwrap_or(0),
            Err(_) => 0,
        };

        let relevant = match direction {
            Direction::Input => max_input_channels > 0,
            Direction::Output => max_output_channels > 0,
        };
        if !relevant {
            continue;
        }

        let default_sample_rate = match direction {
            Direction::Input => device.default_input_config().ok().map(|c| c.sample_rate().0),
            Direction::Output => device.default_output_config().ok().map(|c| c.sample_rate().0),
        };

        let is_default = default_name.as_deref() == Some(raw_name.as_str());

        result.push(DeviceInfo {
            name: raw_name,
            is_default,
            max_input_channels,
            max_output_channels,
            default_sample_rate,
        });
    }

    result
}

pub fn get_input_device(name: Option<&str>) -> Result<cpal::Device, String> {
    match name {
        None => cpal::default_host()
            .default_input_device()
            .ok_or_else(|| "no default input device found".to_string()),
        Some(n) => find_device_in_host(cpal::default_host(), n),
    }
}

pub fn get_output_device(name: Option<&str>) -> Result<cpal::Device, String> {
    match name {
        None => cpal::default_host()
            .default_output_device()
            .ok_or_else(|| "no default output device found".to_string()),
        Some(n) => find_device_in_host(cpal::default_host(), n),
    }
}

fn find_device_in_host(host: cpal::Host, name: &str) -> Result<cpal::Device, String> {
    let all_names: Vec<String> = host.devices()
        .map_err(|e| format!("failed to enumerate devices: {e}"))?
        .filter_map(|d| d.name().ok())
        .collect();

    if !all_names.iter().any(|dn| dn == name) {
        return Err(format!("device not found: '{name}'. Devices seen: {all_names:?}"));
    }

    host.devices()
        .map_err(|e| format!("failed to enumerate devices: {e}"))?
        .find(|d| d.name().map(|dn| dn == name).unwrap_or(false))
        .ok_or_else(|| format!("device not found: '{name}'"))
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

/// Resolves a selection to Some(device) to open, or None meaning
/// "the caller should skip opening any output stream at all."
pub fn resolve_output_device(selection: DeviceSelection) -> Result<Option<cpal::Device>, String> {
    match selection {
        DeviceSelection::NoDevice => Ok(None),
        DeviceSelection::Default => get_output_device(None).map(Some),
        DeviceSelection::Named(name) => get_output_device(Some(name)).map(Some),
    }
}

pub fn resolve_input_device(selection: DeviceSelection) -> Result<Option<cpal::Device>, String> {
    match selection {
        DeviceSelection::NoDevice => Ok(None),
        DeviceSelection::Default => get_input_device(None).map(Some),
        DeviceSelection::Named(name) => get_input_device(Some(name)).map(Some),
    }
}

/// True if `value` is the explicit "skip this device" sentinel
/// (`Some(NONE_DEVICE)`). `None` (no selection / system default) is
/// NOT a skip -- callers that only need a bool check (bus.rs,
/// combine.rs, split.rs) use this instead of matching on
/// `DeviceSelection` directly.
pub fn is_skip(value: &Option<String>) -> bool {
    matches!(DeviceSelection::parse(value.as_deref()), DeviceSelection::NoDevice)
}