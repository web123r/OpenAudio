//! Milestone 5: Transmit to multiple simultaneous subscribers.
//!
//! Run with: cargo run --example transmit_multi -p audio-core

fn main() {
    let duration_secs = 6;
    let dest_addrs = ["127.0.0.1:6970", "127.0.0.1:6971"];
    let stream_id = 1001;

    println!("=== OpenAudio Milestone 5: Multi-Subscriber Transmit ===\n");

    if let Err(e) = audio_core::transmit_multi(duration_secs, &dest_addrs, stream_id) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}