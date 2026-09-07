use crate::backend::{AudioBackend, AudioStream, StreamConfig};
use crate::devices::DeviceInfo;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::SampleFormat;
use std::sync::Arc;

pub struct CpalBackend;

impl CpalBackend {
    pub fn new() -> Arc<dyn AudioBackend> {
        Arc::new(Self)
    }
}

struct CpalStream {
    stream: cpal::Stream,
}

impl AudioStream for CpalStream {
    fn play(&self) -> Result<(), String> {
        self.stream.play().map_err(|e| format!("failed to start stream: {e}"))
    }
    fn pause(&self) -> Result<(), String> {
        self.stream.pause().map_err(|e| format!("failed to pause stream: {e}"))
    }
}

enum Direction {
    Input,
    Output,
}

fn list_devices_for_host(host: &cpal::Host, direction: Direction) -> Vec<DeviceInfo> {
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

fn get_input_device(host: &cpal::Host, name: Option<&str>) -> Result<cpal::Device, String> {
    match name {
        None => host.default_input_device().ok_or_else(|| "no default input device found".to_string()),
        Some(n) => find_device_in_host(host, n),
    }
}

fn get_output_device(host: &cpal::Host, name: Option<&str>) -> Result<cpal::Device, String> {
    match name {
        None => host.default_output_device().ok_or_else(|| "no default output device found".to_string()),
        Some(n) => find_device_in_host(host, n),
    }
}

fn find_device_in_host(host: &cpal::Host, name: &str) -> Result<cpal::Device, String> {
    let all_names: Vec<String> = host
        .devices()
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

impl AudioBackend for CpalBackend {
    fn name(&self) -> &'static str {
        "CPAL"
    }

    fn list_input_devices(&self) -> Vec<DeviceInfo> {
        list_devices_for_host(&cpal::default_host(), Direction::Input)
    }

    fn list_output_devices(&self) -> Vec<DeviceInfo> {
        list_devices_for_host(&cpal::default_host(), Direction::Output)
    }

    fn get_input_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String> {
        let device = get_input_device(&cpal::default_host(), device_name)?;
        let name = device.name().unwrap_or_else(|_| "Default Input".to_string());
        let config = device.default_input_config().map_err(|e| format!("failed to get config: {e}"))?;
        Ok((name, StreamConfig {
            channels: config.channels(),
            sample_rate: config.sample_rate().0,
        }))
    }

    fn get_output_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String> {
        let device = get_output_device(&cpal::default_host(), device_name)?;
        let name = device.name().unwrap_or_else(|_| "Default Output".to_string());
        let config = device.default_output_config().map_err(|e| format!("failed to get config: {e}"))?;
        Ok((name, StreamConfig {
            channels: config.channels(),
            sample_rate: config.sample_rate().0,
        }))
    }

    fn get_loopback_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String> {
        if cfg!(target_family = "unix") {
            // On Linux (PulseAudio/PipeWire) and macOS (BlackHole), loopback is typically
            // captured from an input device rather than an output device API.
            self.get_input_config(device_name)
        } else {
            let device = get_output_device(&cpal::default_host(), device_name)?;
            let name = device.name().unwrap_or_else(|_| "Default Output".to_string());
            let config = device.default_output_config().map_err(|e| format!("failed to get loopback config: {e}"))?;
            Ok((name, StreamConfig {
                channels: config.channels(),
                sample_rate: config.sample_rate().0,
            }))
        }
    }

    fn build_input_stream(
        &self,
        device_name: Option<&str>,
        mut callback: Box<dyn FnMut(&[f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String> {
        let device = get_input_device(&cpal::default_host(), device_name)?;
        let config = device.default_input_config().map_err(|e| format!("failed to get config: {e}"))?;
        let sample_format = config.sample_format();
        let stream_config: cpal::StreamConfig = config.into();

        let err_fn = |err| eprintln!("audio-core: input stream error: {err}");

        let stream = match sample_format {
            SampleFormat::F32 => {
                device.build_input_stream(&stream_config, move |data: &[f32], _| { callback(data); }, err_fn, None)
            },
            SampleFormat::I16 => {
                device.build_input_stream(&stream_config, move |data: &[i16], _| {
                    let mut f32_data = Vec::with_capacity(data.len());
                    for &s in data { f32_data.push(s as f32 / i16::MAX as f32); }
                    callback(&f32_data);
                }, err_fn, None)
            },
            SampleFormat::U16 => {
                device.build_input_stream(&stream_config, move |data: &[u16], _| {
                    let mut f32_data = Vec::with_capacity(data.len());
                    for &s in data { f32_data.push((s as f32 - (u16::MAX as f32 / 2.0)) / (u16::MAX as f32 / 2.0)); }
                    callback(&f32_data);
                }, err_fn, None)
            },
            _ => return Err("unsupported sample format".to_string()),
        }.map_err(|e| format!("failed to build input stream: {e}"))?;

        Ok(Box::new(CpalStream { stream }))
    }

    fn build_output_stream(
        &self,
        device_name: Option<&str>,
        mut callback: Box<dyn FnMut(&mut [f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String> {
        let device = get_output_device(&cpal::default_host(), device_name)?;
        let config = device.default_output_config().map_err(|e| format!("failed to get config: {e}"))?;
        let sample_format = config.sample_format();
        let stream_config: cpal::StreamConfig = config.into();

        let err_fn = |err| eprintln!("audio-core: output stream error: {err}");

        let stream = match sample_format {
            SampleFormat::F32 => {
                device.build_output_stream(&stream_config, move |data: &mut [f32], _| { callback(data); }, err_fn, None)
            },
            SampleFormat::I16 => {
                device.build_output_stream(&stream_config, move |data: &mut [i16], _| {
                    let mut f32_data = vec![0.0f32; data.len()];
                    callback(&mut f32_data);
                    for (i, &s) in f32_data.iter().enumerate() {
                        data[i] = (s * i16::MAX as f32) as i16;
                    }
                }, err_fn, None)
            },
            SampleFormat::U16 => {
                device.build_output_stream(&stream_config, move |data: &mut [u16], _| {
                    let mut f32_data = vec![0.0f32; data.len()];
                    callback(&mut f32_data);
                    for (i, &s) in f32_data.iter().enumerate() {
                        data[i] = (s * (u16::MAX as f32 / 2.0) + (u16::MAX as f32 / 2.0)) as u16;
                    }
                }, err_fn, None)
            },
            _ => return Err("unsupported sample format".to_string()),
        }.map_err(|e| format!("failed to build output stream: {e}"))?;

        Ok(Box::new(CpalStream { stream }))
    }

    fn build_loopback_stream(
        &self,
        device_name: Option<&str>,
        mut callback: Box<dyn FnMut(&[f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String> {
        if cfg!(target_family = "unix") {
            // On Linux and macOS, loopback capture acts like a normal input capture from a monitor sink.
            return self.build_input_stream(device_name, callback);
        }

        let device = get_output_device(&cpal::default_host(), device_name)?;
        let config = device.default_output_config().map_err(|e| format!("failed to get config: {e}"))?;
        let sample_format = config.sample_format();
        let stream_config: cpal::StreamConfig = config.into();

        let err_fn = |err| eprintln!("audio-core: loopback stream error: {err}");

        let stream = match sample_format {
            SampleFormat::F32 => {
                device.build_input_stream(&stream_config, move |data: &[f32], _| { callback(data); }, err_fn, None)
            },
            SampleFormat::I16 => {
                device.build_input_stream(&stream_config, move |data: &[i16], _| {
                    let mut f32_data = Vec::with_capacity(data.len());
                    for &s in data { f32_data.push(s as f32 / i16::MAX as f32); }
                    callback(&f32_data);
                }, err_fn, None)
            },
            SampleFormat::U16 => {
                device.build_input_stream(&stream_config, move |data: &[u16], _| {
                    let mut f32_data = Vec::with_capacity(data.len());
                    for &s in data { f32_data.push((s as f32 - (u16::MAX as f32 / 2.0)) / (u16::MAX as f32 / 2.0)); }
                    callback(&f32_data);
                }, err_fn, None)
            },
            _ => return Err("unsupported sample format".to_string()),
        }.map_err(|e| format!("failed to build loopback stream (device may not support WASAPI loopback): {e}"))?;

        Ok(Box::new(CpalStream { stream }))
    }
}
