use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn main() {
    let duration_secs = 30;
    let node_name = "CLI Test Node".to_string();
    let stream_name = "CLI Test Stream".to_string();
    let stream_id = 3001;

    println!("=== OpenAudio: Discovery Transmit (CLI) ===\n");

    let subscribers_by_stream: Arc<Mutex<HashMap<u32, Vec<std::net::SocketAddr>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let control_keep_running = Arc::new(AtomicBool::new(true));
    let control_flag = control_keep_running.clone();
    let subs_for_thread = subscribers_by_stream.clone();
    std::thread::spawn(move || {
        if let Err(e) = audio_core::start_control_listener(subs_for_thread, control_flag) {
            eprintln!("control listener error: {e}");
        }
    });

    let keep_running = Arc::new(AtomicBool::new(true));
    let timer_flag = keep_running.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(duration_secs));
        timer_flag.store(false, Ordering::Relaxed);
    });

    println!("Advertising '{stream_name}' as '{node_name}' for {duration_secs}s...\n");

    if let Err(e) = audio_core::transmit_with_discovery(
        node_name,
        stream_name,
        stream_id,
        None,
        subscribers_by_stream,
        keep_running,
    ) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    control_keep_running.store(false, Ordering::Relaxed);
    println!("Done.");
}