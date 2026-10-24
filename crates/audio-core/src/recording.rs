use hound::{SampleFormat, WavSpec, WavWriter};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

enum RecordMessage {
    Samples(Vec<f32>),
}

/// Asynchronous audio writer that delegates all disk operations and encoding
/// to a background thread so the real-time audio thread is never blocked.
pub struct AsyncAudioWriter {
    sender: SyncSender<RecordMessage>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

pub type AudioWriter = AsyncAudioWriter;
pub type SharedAudioWriter = Arc<Mutex<Option<AsyncAudioWriter>>>;
pub type SharedWavWriter = SharedAudioWriter;

/// Creates an asynchronous audio writer (24-bit broadcast WAV or FLAC) at `path`.
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

    // 512 queue capacity is ~5.4 seconds of 10ms audio blocks:
    // absorbs any Windows NTFS filesystem latency spikes without stalling the audio engine.
    let (sender, receiver) = sync_channel::<RecordMessage>(512);
    let path_buf = path.to_string();

    let worker = if path.to_lowercase().ends_with(".flac") {
        thread::Builder::new()
            .name("openaudio-flac-writer".to_string())
            .spawn(move || run_flac_worker(path_buf, channels, sample_rate, receiver))
            .map_err(|e| format!("failed to spawn recording worker thread: {e}"))?
    } else {
        thread::Builder::new()
            .name("openaudio-wav-writer".to_string())
            .spawn(move || run_wav_worker(path_buf, channels, sample_rate, receiver))
            .map_err(|e| format!("failed to spawn recording worker thread: {e}"))?
    };

    Ok(Arc::new(Mutex::new(Some(AsyncAudioWriter {
        sender,
        worker: Some(worker),
    }))))
}

/// Backwards-compatible constructor for WAV writers.
pub fn create_wav_writer(
    path: &str,
    channels: u16,
    sample_rate: u32,
) -> Result<SharedWavWriter, String> {
    create_audio_writer(path, channels, sample_rate)
}

/// Writes samples to the recording buffer via non-blocking try-send.
///
/// This function is safe to call directly from high-priority audio callbacks:
/// it performs zero disk I/O, does not block on filesystem locks, and will
/// never compromise live audio playback timing.
pub fn write_samples(writer: &SharedAudioWriter, samples: &[f32]) {
    if samples.is_empty() {
        return;
    }

    // Non-blocking try_lock: if another thread is finalizing or busy, don't stall the audio callback.
    if let Ok(guard) = writer.try_lock() {
        if let Some(ref w) = *guard {
            match w.sender.try_send(RecordMessage::Samples(samples.to_vec())) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    eprintln!("audio-core: recording queue full; disk write falling behind");
                }
                Err(TrySendError::Disconnected(_)) => {}
            }
        }
    }
}

/// Flushes and finalizes the recording file cleanly, waiting for the background
/// writer to finish writing all pending frames before returning.
pub fn finalize(writer: &SharedAudioWriter) {
    let mut writer_opt = match writer.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if let Some(mut w) = writer_opt.take() {
        // Dropping sender signals the worker thread's receiver loop to terminate.
        drop(w.sender);
        if let Some(handle) = w.worker.take() {
            let _ = handle.join();
        }
    }
}

fn run_wav_worker(
    path: String,
    channels: u16,
    sample_rate: u32,
    receiver: Receiver<RecordMessage>,
) -> Result<(), String> {
    // 24-bit PCM Integer format (AES/EBU broadcast standard).
    // Provides 144 dB of pristine dynamic range and avoids the digital clipping / wrapping
    // bugs present in Windows Media Player and Groove Music when playing 32-bit float WAVs.
    let spec = WavSpec {
        channels,
        sample_rate,
        bits_per_sample: 24,
        sample_format: SampleFormat::Int,
    };

    let mut writer = WavWriter::create(&path, spec)
        .map_err(|e| format!("failed to create recording file '{path}': {e}"))?;

    while let Ok(RecordMessage::Samples(samples)) = receiver.recv() {
        for s in samples {
            let clamped = s.clamp(-1.0, 1.0);
            // Scale float [-1.0, 1.0] to 24-bit signed integer [-8388608, 8388607]
            let sample_i32 = (clamped * 8_388_607.0).round() as i32;
            let _ = writer.write_sample(sample_i32);
        }
    }

    writer
        .finalize()
        .map_err(|e| format!("failed to finalize WAV '{path}': {e}"))?;
    Ok(())
}

fn run_flac_worker(
    path: String,
    channels: u16,
    sample_rate: u32,
    receiver: Receiver<RecordMessage>,
) -> Result<(), String> {
    let mut accumulated = Vec::new();

    while let Ok(RecordMessage::Samples(samples)) = receiver.recv() {
        accumulated.extend_from_slice(&samples);
    }

    encode_and_save_flac(&path, channels, sample_rate, &accumulated)
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
            (clamped * 8_388_607.0).round() as i32
        })
        .collect();

    let bits_per_sample = 24;
    let source = MemSource::from_samples(
        &samples_i32,
        channels as usize,
        bits_per_sample,
        sample_rate as usize,
    );
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

    #[test]
    fn test_wav_24bit_async_recording_creation_and_finalize() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir
            .join(format!("openaudio_test_{}.wav", std::process::id()))
            .to_string_lossy()
            .to_string();

        let writer = create_audio_writer(&path, 2, 48000).expect("failed to create wav writer");
        let samples = vec![0.2f32, -0.2f32, 0.8f32, -0.8f32, 0.0f32, 0.0f32];
        write_samples(&writer, &samples);
        finalize(&writer);

        let metadata = std::fs::metadata(&path).expect("wav file was not created");
        assert!(metadata.len() > 0, "wav file was empty");

        // Verify the wav file has 24-bit depth and can be read back cleanly
        let mut reader = hound::WavReader::open(&path).expect("failed to open generated wav");
        assert_eq!(reader.spec().bits_per_sample, 24);
        assert_eq!(reader.spec().sample_format, SampleFormat::Int);
        let read_samples: Vec<i32> = reader.samples::<i32>().map(|s| s.unwrap()).collect();
        assert_eq!(read_samples.len(), 6);

        let _ = std::fs::remove_file(&path);
    }
}
