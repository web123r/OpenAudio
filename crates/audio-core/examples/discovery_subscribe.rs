//! CLI test: Subscribe via discovery (no GUI, no IP typing).
//!
//! Run with: cargo run --example discovery_subscribe -p audio-core

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    let listen_secs_for_discovery = 15;
    let play_duration_secs = 20;
    let my_bind_addr = "0.0.0.0:6980";
    let my_receive_port: u16 = 6980;

    println!("=== OpenAudio: Discovery Subscribe (CLI) ===\n");

    let directory: Arc<Mutex<HashMap<String, audio_core::DiscoveredNode>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let listener_keep_running = Arc::new(AtomicBool::new(true));
    let listener_flag = listener_keep_running.clone();
    let dir_for_thread = directory.clone();
    std::thread::spawn(move || {
        if let Err(e) = audio_core::start_discovery_listener(dir_for_thread, listener_flag) {
            eprintln!("discovery listener error: {e}");
        }
    });

    println!("Listening for Publishers on the network for up to {listen_secs_for_discovery}s...");
    let search_start = Instant::now();
    let found = loop {
        if let Some(node) = directory.lock().unwrap().values().next().cloned() {
            break Some(node);
        }
        if search_start.elapsed().as_secs() >= listen_secs_for_discovery {
            break None;
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    let Some(node) = found else {
        println!("No Publisher found on the network within {listen_secs_for_discovery}s.");
        listener_keep_running.store(false, Ordering::Relaxed);
        return;
    };

    println!(
        "Found: '{}' from node '{}' at {}",
        node.stream_name, node.node_name, node.ip
    );

    if let Err(e) =
        audio_core::send_subscribe_request(&node.ip, node.control_port, node.stream_id, my_receive_port)
    {
        eprintln!("Failed to send subscribe request: {e}");
        listener_keep_running.store(false, Ordering::Relaxed);
        return;
    }
    println!("Subscribe request sent. Starting playback for {play_duration_secs}s...");

    let play_keep_running = Arc::new(AtomicBool::new(true));
    let play_timer_flag = play_keep_running.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(play_duration_secs));
        play_timer_flag.store(false, Ordering::Relaxed);
    });

    if let Err(e) =
        audio_core::receive_and_play_bus_with_control(my_bind_addr, None, play_keep_running)
    {
        eprintln!("Error: {e}");
    }

    listener_keep_running.store(false, Ordering::Relaxed);
    println!("Done.");
}