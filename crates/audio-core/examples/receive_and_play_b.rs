//! Milestone 5: Second receiver instance, bound to a different port,
//! to prove one Publisher can serve two Subscribers at once.
//!
//! Run with: cargo run --example receive_and_play_b -p audio-core

fn main() {
    let duration_secs = 8;
    let bind_addr = "0.0.0.0:6971";

    println!("=== OpenAudio Milestone 5: Live Playback (Receiver B) ===\n");

    if let Err(e) = audio_core::receive_and_play(bind_addr, duration_secs) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}