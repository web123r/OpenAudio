//! Milestone 2: Transmit captured audio as UDP packets.
//!
//! Run with: cargo run --example transmit -p audio-core

fn main() {
    let duration_secs = 5;
    let dest_addr = "127.0.0.1:6970";
    let stream_id = 1001;

    println!("=== OpenAudio Milestone 2: Transmit ===\n");

    if let Err(e) = audio_core::transmit(duration_secs, dest_addr, stream_id) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}