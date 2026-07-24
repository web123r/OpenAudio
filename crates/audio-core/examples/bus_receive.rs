//! Milestone 6: Bus -- receive and mix multiple streams.
//!
//! Run with: cargo run --example bus_receive -p audio-core

fn main() {
    let bind_addr = "0.0.0.0:6972";
    let duration_secs = 8;

    println!("=== OpenAudio Milestone 6: Bus (Mixing) ===\n");

    if let Err(e) = audio_core::receive_and_play_bus(bind_addr, duration_secs) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}