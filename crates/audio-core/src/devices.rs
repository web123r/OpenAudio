use cpal::traits::{DeviceTrait, HostTrait};

/// Basic information about a single audio device, enough to display
/// it in a device picker and to sanity-check its capabilities before
/// we try to open a stream on it in a later milestone.
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub name: String,
    pub is_default: bool,
    pub max_input_channels: u16,
    pub max_output_channels: u16,
    pub default_sample_rate: Option<u32>,
}

pub fn list_input_devices() -> Vec<DeviceInfo> {
    list_devices(Direction::Input)
}

pub fn list_output_devices() -> Vec<DeviceInfo> {
    list_devices(Direction::Output)
}

enum Direction {
    Input,
    Output,
}

fn list_devices(direction: Direction) -> Vec<DeviceInfo> {
    let host = cpal::default_host();

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
        let name = match device.name() {
            Ok(n) => n,
            Err(_) => continue,
        };

        let max_input_channels = device
            .supported_input_configs()
            .ok()
            .and_then(|cfgs| cfgs.map(|c| c.channels()).max())
            .unwrap_or(0);

        let max_output_channels = device
            .supported_output_configs()
            .ok()
            .and_then(|cfgs| cfgs.map(|c| c.channels()).max())
            .unwrap_or(0);

        let relevant = match direction {
            Direction::Input => max_input_channels > 0,
            Direction::Output => max_output_channels > 0,
        };
        if !relevant {
            continue;
        }

        let default_sample_rate = match direction {
            Direction::Input => device
                .default_input_config()
                .ok()
                .map(|c| c.sample_rate().0),
            Direction::Output => device
                .default_output_config()
                .ok()
                .map(|c| c.sample_rate().0),
        };

        let is_default = default_name.as_deref() == Some(name.as_str());

        result.push(DeviceInfo {
            name,
            is_default,
            max_input_channels,
            max_output_channels,
            default_sample_rate,
        });
    }

    result
}