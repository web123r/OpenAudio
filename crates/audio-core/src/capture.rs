use crate::backend::get_backend;
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
    let backend = get_backend();
    let (_device_label, config) = backend.get_input_config(None)?;

    let spec = hound::WavSpec {
        channels: config.channels,
        sample_rate: config.sample_rate,
        bits_per_sample: 32,
        sample_format: hound::SampleFormat::Float,
    };

    let writer = hound::WavWriter::create(output_path, spec)
        .map_err(|e| format!("failed to create wav file: {e}"))?;
    let writer = Arc::new(Mutex::new(Some(writer)));
    let writer_clone = writer.clone();

    let stream = backend.build_input_stream(
        None,
        Box::new(move |data: &[f32]| {
            if let Ok(mut guard) = writer_clone.lock() {
                if let Some(w) = guard.as_mut() {
                    for &sample in data {
                        let _ = w.write_sample(sample);
                    }
                }
            }
        }),
    )?;

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
