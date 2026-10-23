use hound::{SampleFormat, WavSpec, WavWriter};
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex};

pub enum AudioWriter {
    Wav(WavWriter<BufWriter<File>>),
    Flac {
        path: String,
        channels: u16,
        sample_rate: u32,
        samples: Vec<f32>,
    },
}

pub type SharedAudioWriter = Arc<Mutex<Option<AudioWriter>>>;
pub type SharedWavWriter = SharedAudioWriter;

/// Creates an audio writer (WAV or FLAC depending on file extension) at `path`.
pub fn create_audio_writer(
    path: &str,
    channels: u16,
    sample_rate: u32,
) -> Result<SharedAudioWriter, String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create recording directory: {e}"))?;
        }
    }

    if path.to_lowercase().ends_with(".flac") {
        Ok(Arc::new(Mutex::new(Some(AudioWriter::Flac {
            path: path.to_string(),
            channels,
            sample_rate,
            samples: Vec::new(),
        }))))
    } else {
        let spec = WavSpec {
            channels,
            sample_rate,
            bits_per_sample: 32,
            sample_format: SampleFormat::Float,
        };
        let writer = WavWriter::create(path, spec)
            .map_err(|e| format!("failed to create recording file {path}: {e}"))?;
        Ok(Arc::new(Mutex::new(Some(AudioWriter::Wav(writer)))))
    }
}

/// Backwards-compatible constructor for WAV writers.
pub fn create_wav_writer(
    path: &str,
    channels: u16,
    sample_rate: u32,
) -> Result<SharedWavWriter, String> {
    create_audio_writer(path, channels, sample_rate)
}

/// Writes samples, silently no-oping on a poisoned lock rather than
/// panicking -- recording is a best-effort side channel, it should
/// never be the thing that brings down live audio.
pub fn write_samples(writer: &SharedAudioWriter, samples: &[f32]) {
    if let Ok(mut guard) = writer.lock() {
        if let Some(w) = guard.as_mut() {
            match w {
                AudioWriter::Wav(wav) => {
                    for &s in samples {
                        let _ = wav.write_sample(s);
                    }
                }
                AudioWriter::Flac { samples: flac_buf, .. } => {
                    flac_buf.extend_from_slice(samples);
                }
            }
        }
    }
}

pub fn finalize(writer: &SharedAudioWriter) {
    if let Ok(mut guard) = writer.lock() {
        if let Some(w) = guard.take() {
            match w {
                AudioWriter::Wav(wav) => {
                    let _ = wav.finalize();
                }
                AudioWriter::Flac {
                    path,
                    channels,
                    sample_rate,
                    samples,
                } => {
                    let _ = encode_and_save_flac(&path, channels, sample_rate, &samples);
                }
            }
        }
    }
}

fn encode_and_save_flac(
    path: &str,
    channels: u16,
    sample_rate: u32,
    samples: &[f32],
) -> Result<(), String> {
    use flacenc::bitsink::ByteSink;
    use flacenc::component::BitRepr;
    use flacenc::config;
    use flacenc::error::Verify;
    use flacenc::source::MemSource;

    if channels == 0 || sample_rate == 0 || samples.is_empty() {
        return Ok(());
    }

    // Convert f32 [-1.0, 1.0] to 24-bit PCM signed integers
    let samples_i32: Vec<i32> = samples
        .iter()
        .map(|&s| {
            let clamped = s.clamp(-1.0, 1.0);
            (clamped * 8_388_607.0) as i32
        })
        .collect();

    let bits_per_sample = 24;
    let source = MemSource::from_samples(&samples_i32, channels as usize, bits_per_sample, sample_rate as usize);
    let mut encoder_config = config::Encoder::default();
    encoder_config.block_size = 4096;
    let verified_config = encoder_config
        .into_verified()
        .map_err(|e| format!("invalid FLAC config: {e:?}"))?;

    let flac_stream = flacenc::encode_with_fixed_block_size(&verified_config, source, 4096)
        .map_err(|e| format!("FLAC encoding error: {e:?}"))?;

    let mut sink = ByteSink::new();
    flac_stream
        .write(&mut sink)
        .map_err(|e| format!("FLAC sink write error: {e:?}"))?;

    std::fs::write(path, sink.as_slice())
        .map_err(|e| format!("failed to write FLAC file {path}: {e}"))?;

    Ok(())
}

/// Generates a timestamped, filesystem-safe path under "recordings/".
pub fn generate_record_path(label: &str) -> String {
    let safe: String = label
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("recordings/{safe}_{ts}.wav")
}

/// Generates a timestamped, filesystem-safe FLAC path under "recordings/".
pub fn generate_flac_record_path(label: &str) -> String {
    let safe: String = label
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("recordings/{safe}_{ts}.flac")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flac_recording_creation_and_finalize() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir
            .join(format!("openaudio_test_{}.flac", std::process::id()))
            .to_string_lossy()
            .to_string();

        let writer = create_audio_writer(&path, 2, 48000).expect("failed to create flac writer");
        let samples = vec![0.1f32, -0.1f32, 0.5f32, -0.5f32, 0.0f32, 0.0f32];
        write_samples(&writer, &samples);
        finalize(&writer);

        let metadata = std::fs::metadata(&path).expect("flac file was not created");
        assert!(metadata.len() > 0, "flac file was empty");
        let _ = std::fs::remove_file(&path);
    }
}
