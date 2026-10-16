//! Milestone 0: Enumerate audio devices.
//!
//! Run with: cargo run --example list_devices

fn print_device(d: &audio_core::DeviceInfo) {
    let default_marker = if d.is_default { " (default)" } else { "" };
    let sample_rate = d
        .default_sample_rate
        .map(|r| format!("{r} Hz"))
        .unwrap_or_else(|| "unknown".to_string());

    println!(
        "  - {}{}\n      in: {} ch | out: {} ch | default rate: {}",
        d.name, default_marker, d.max_input_channels, d.max_output_channels, sample_rate
    );
}

fn main() {
    println!("=== OpenAudio Milestone 0: Device Enumeration ===\n");

    println!("Input devices:");
    let inputs = audio_core::list_input_devices();
    if inputs.is_empty() {
        println!("  (none found)");
    }
    for d in &inputs {
        print_device(d);
    }

    println!("\nOutput devices:");
    let outputs = audio_core::list_output_devices();
    if outputs.is_empty() {
        println!("  (none found)");
    }
    for d in &outputs {
        print_device(d);
    }
}
