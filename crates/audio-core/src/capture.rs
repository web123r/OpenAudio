use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{SampleFormat, StreamConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Captures `duration_secs` of audio from the default input device and
/// writes it to a 32-bit float WAV file at `output_path`.
///
/// This is a deliberately simple, blocking implementation for
/// Milestone 1 -- it exists to prove the capture path works, not to
/// be the final architecture. Later milestones replace the WAV
/// writer with a network packetizer.
pub fn capture_to_wav(duration_secs: u64, output_path: &str) -> Result<(), String> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .ok_or_else(|| "no default input device found".to_string())?;

    let config = device
        .default_input_config()
        .map_err(|e| format!("failed to get default input config: {e}"))?;

    let sample_format = config.sample_format();
    let stream_config: StreamConfig = config.into();

    let spec = hound::WavSpec {
        channels: stream_config.channels,
        sample_rate: stream_config.sample_rate.0,
        bits_per_sample: 32,
        sample_format: hound::SampleFormat::Float,
    };

    let writer = hound::WavWriter::create(output_path, spec)
        .map_err(|e| format!("failed to create wav file: {e}"))?;
    let writer = Arc::new(Mutex::new(Some(writer)));
    let writer_clone = writer.clone();

    let err_fn = |err| eprintln!("audio-core: stream error: {err}");

    let stream = match sample_format {
        SampleFormat::F32 => device.build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                if let Ok(mut guard) = writer_clone.lock() {
                    if let Some(w) = guard.as_mut() {
                        for &sample in data {
                            let _ = w.write_sample(sample);
                        }
                    }
                }
            },
            err_fn,
            None,
        ),
        SampleFormat::I16 => device.build_input_stream(
            &stream_config,
            move |data: &[i16], _| {
                if let Ok(mut guard) = writer_clone.lock() {
                    if let Some(w) = guard.as_mut() {
                        for &sample in data {
                            let _ = w.write_sample(sample as f32 / i16::MAX as f32);
                        }
                    }
                }
            },
            err_fn,
            None,
        ),
        SampleFormat::U16 => device.build_input_stream(
            &stream_config,
            move |data: &[u16], _| {
                if let Ok(mut guard) = writer_clone.lock() {
                    if let Some(w) = guard.as_mut() {
                        for &sample in data {
                            let centered = sample as f32 - (u16::MAX as f32 / 2.0);
                            let _ = w.write_sample(centered / (u16::MAX as f32 / 2.0));
                        }
                    }
                }
            },
            err_fn,
            None,
        ),
        _ => return Err("unsupported sample format".to_string()),
    }
    .map_err(|e| format!("failed to build input stream: {e}"))?;

    stream
        .play()
        .map_err(|e| format!("failed to start stream: {e}"))?;

    println!("Recording {duration_secs}s of audio from default input device...");
    std::thread::sleep(Duration::from_secs(duration_secs));
    drop(stream); // stops capture

    if let Ok(mut guard) = writer.lock() {
        if let Some(w) = guard.take() {
            w.finalize()
                .map_err(|e| format!("failed to finalize wav: {e}"))?;
        }
    }

    println!("Wrote {output_path}");
    Ok(())
}