//! Milestone 1: Capture audio to a WAV file.
//!
//! Run with: cargo run --example capture_to_wav -p audio-core

fn main() {
    let duration_secs = 5;
    let output_path = "capture_test.wav";

    println!("=== OpenAudio Milestone 1: Audio Capture ===\n");

    if let Err(e) = audio_core::capture_to_wav(duration_secs, output_path) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}