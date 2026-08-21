//! CLI test: subscribe directly (no discovery needed) and split N
//! channels to N separate named output devices (e.g. SAR endpoints).
//!
//! Use this version when the Publisher is on the SAME machine as this
//! CLI test -- our GUI app already owns the fixed discovery port
//! (5450) on this machine, so a second discovery listener here would
//! just collide with it. Since you already know the Publisher's
//! address (localhost) and Stream ID (whatever you set in the GUI's
//! "Stream ID" field for that Publish session), we can skip discovery
//! entirely and register directly.
//!
//! EDIT the constants below to match your GUI's Publish session, then:
//! cargo run --example discovery_subscribe_split -p audio-core



fn main() {

    println!("=== Output devices cpal actually sees ===");
    for d in audio_core::list_output_devices() {
        println!("{:?}", d.name);
    }
    println!("==========================================\n");
    // EDIT THESE to match the Publish session running in your GUI app:
    let publisher_ip = "127.0.0.1";
    let publisher_control_port: u16 = 7000; // fixed control port, matches the GUI
    let stream_id: u32 = 3001; // whatever "Stream ID" shows in that Publish session's card

    // EDIT THIS: exact Windows device names for your SAR endpoints, in
    // the order you want network channels routed to them.
    let device_names: Vec<String> = vec![
    "Ferronme 1 (Synchronous Audio Router)".to_string(),
    "Ferronme 1 (Synchronous Audio Router)".to_string(),
];
    let play_duration_secs = 60;
    let my_receive_port: u16 = 6981;
    let my_bind_addr = format!("0.0.0.0:{my_receive_port}");

    println!("=== OpenAudio: Direct Subscribe (Split to Devices) ===\n");
    println!("Registering with Publisher at {publisher_ip}:{publisher_control_port} for stream {stream_id}...");

    if let Err(e) = audio_core::send_subscribe_request(
        publisher_ip,
        publisher_control_port,
        stream_id,
        my_receive_port,
    ) {
        eprintln!("Failed to send subscribe request: {e}");
        return;
    }

    println!("Subscribe request sent. Splitting to {} device(s) for {play_duration_secs}s...", device_names.len());

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let keep_running = Arc::new(AtomicBool::new(true));
    let timer_flag = keep_running.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(play_duration_secs));
        timer_flag.store(false, Ordering::Relaxed);
    });

    if let Err(e) = audio_core::receive_and_split_to_devices(&my_bind_addr, device_names, keep_running) {
        eprintln!("Error: {e}");
    }

    println!("Done.");
}