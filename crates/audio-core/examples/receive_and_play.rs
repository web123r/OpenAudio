//! Milestone 4: Receive UDP packets and play them back live.
//!
//! Run with: cargo run --example receive_and_play -p audio-core

fn main() {
    let duration_secs = 8;
    let bind_addr = "0.0.0.0:6970";

    println!("=== OpenAudio Milestone 4: Live Playback ===\n");

    if let Err(e) = audio_core::receive_and_play(bind_addr, duration_secs) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}