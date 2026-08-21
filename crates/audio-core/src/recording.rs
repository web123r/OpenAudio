use hound::{SampleFormat, WavSpec, WavWriter};
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex};

pub type SharedWavWriter = Arc<Mutex<Option<WavWriter<BufWriter<File>>>>>;

/// Creates a 32-bit float WAV writer at `path`, creating parent
/// directories if needed (e.g. "recordings/"). Best-effort: recording
/// failures never crash the actual audio pipeline, they just get
/// logged and recording silently stops for that stream.
pub fn create_wav_writer(path: &str, channels: u16, sample_rate: u32) -> Result<SharedWavWriter, String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create recording directory: {e}"))?;
        }
    }
    let spec = WavSpec {
        channels,
        sample_rate,
        bits_per_sample: 32,
        sample_format: SampleFormat::Float,
    };
    let writer = WavWriter::create(path, spec)
        .map_err(|e| format!("failed to create recording file {path}: {e}"))?;
    Ok(Arc::new(Mutex::new(Some(writer))))
}

/// Writes samples, silently no-oping on a poisoned lock rather than
/// panicking -- recording is a best-effort side channel, it should
/// never be the thing that brings down live audio.
pub fn write_samples(writer: &SharedWavWriter, samples: &[f32]) {
    if let Ok(mut guard) = writer.lock() {
        if let Some(w) = guard.as_mut() {
            for &s in samples {
                let _ = w.write_sample(s);
            }
        }
    }
}

pub fn finalize(writer: &SharedWavWriter) {
    if let Ok(mut guard) = writer.lock() {
        if let Some(w) = guard.take() {
            let _ = w.finalize();
        }
    }
}

/// Generates a timestamped, filesystem-safe path under "recordings/".
pub fn generate_record_path(label: &str) -> String {
    let safe: String = label
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("recordings/{safe}_{ts}.wav")
}