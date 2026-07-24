//! Milestone 3: Receive UDP packets and reconstruct audio to WAV.
//!
//! Run with: cargo run --example receive -p audio-core

fn main() {
    let duration_secs = 8; // a bit longer than transmit's 5s so you have time to start both
    let bind_addr = "0.0.0.0:6970";
    let output_path = "receive_test.wav";

    println!("=== OpenAudio Milestone 3: Receive ===\n");

    if let Err(e) = audio_core::receive_to_wav(duration_secs, bind_addr, output_path) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}