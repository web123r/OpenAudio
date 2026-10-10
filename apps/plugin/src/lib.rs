//! OpenAudio DAW Audio Plugin Bridge.
//! Provides audio processing callbacks for DAW hosts (Ableton Live, FL Studio, Reaper, Logic Pro)
//! to stream track audio over IP.

use audio_core::register_signal_meter;
use audio_core::signal_meter::{SignalDirection, SignalMeter};
use std::sync::Arc;

pub struct OpenAudioPlugin {
    pub sample_rate: f32,
    pub channels: usize,
    pub stream_name: String,
    pub is_active: bool,
    pub meter: Arc<SignalMeter>,
}

impl Default for OpenAudioPlugin {
    fn default() -> Self {
        Self::new(48000.0, 2)
    }
}

impl OpenAudioPlugin {
    pub fn new(sample_rate: f32, channels: usize) -> Self {
        let meter = register_signal_meter(
            "daw-plugin-track-1",
            "DAW Track 1",
            SignalDirection::Output,
            channels,
        );

        Self {
            sample_rate,
            channels,
            stream_name: "DAW Track 1".to_string(),
            is_active: true,
            meter,
        }
    }

    /// Process audio buffers received from the DAW host callback thread.
    pub fn process(&mut self, buffer: &mut [f32]) {
        if !self.is_active || buffer.is_empty() {
            return;
        }

        // Measure peak signal levels for UI display
        self.meter.observe_interleaved(buffer, self.channels);
    }

    pub fn set_stream_name(&mut self, name: impl Into<String>) {
        self.stream_name = name.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_core::signal_meter_snapshots;

    #[test]
    fn test_plugin_audio_processing() {
        let mut plugin = OpenAudioPlugin::new(48000.0, 2);
        let mut audio_buffer = vec![0.5f32, -0.5f32, 0.2f32, -0.2f32];

        plugin.process(&mut audio_buffer);
        let snapshots = signal_meter_snapshots();

        assert!(!snapshots.is_empty());
        assert_eq!(plugin.stream_name, "DAW Track 1");
    }
}
