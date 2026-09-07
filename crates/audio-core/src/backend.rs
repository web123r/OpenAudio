use crate::devices::DeviceInfo;
use std::sync::Arc;

pub struct StreamConfig {
    pub channels: u16,
    pub sample_rate: u32,
}

pub trait AudioStream {
    fn play(&self) -> Result<(), String>;
    fn pause(&self) -> Result<(), String>;
}

pub trait AudioBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn list_input_devices(&self) -> Vec<DeviceInfo>;
    fn list_output_devices(&self) -> Vec<DeviceInfo>;

    fn get_input_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String>;
    fn get_output_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String>;
    fn get_loopback_config(&self, device_name: Option<&str>) -> Result<(String, StreamConfig), String>;

    fn build_input_stream(
        &self,
        device_name: Option<&str>,
        callback: Box<dyn FnMut(&[f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String>;

    fn build_output_stream(
        &self,
        device_name: Option<&str>,
        callback: Box<dyn FnMut(&mut [f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String>;

    fn build_loopback_stream(
        &self,
        device_name: Option<&str>,
        callback: Box<dyn FnMut(&[f32]) + Send + 'static>,
    ) -> Result<Box<dyn AudioStream>, String>;
}

pub fn get_backend() -> Arc<dyn AudioBackend> {
    crate::cpal_backend::CpalBackend::new()
}
