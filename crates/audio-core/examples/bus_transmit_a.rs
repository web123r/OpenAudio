//! Milestone 6: First stream feeding the bus.
//!
//! Run with: cargo run --example bus_transmit_a -p audio-core

fn main() {
    let duration_secs = 20;
    let dest_addr = "127.0.0.1:6972";
    let stream_id = 2001;

    if let Err(e) = audio_core::transmit(duration_secs, dest_addr, stream_id) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}