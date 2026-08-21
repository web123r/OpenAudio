//! Quick test: writes 3 OpenAudio-managed SAR endpoints (Playback
//! type) into SAR's default.json, tagged "test1", then prints what it
//! did. Check default.json.bak exists and default.json has the new
//! entries alongside your existing manual ones afterward.
//!
//! Run with: cargo run --example sar_config_test -p audio-core

fn main() {
    println!("=== SAR Config Automation Test ===\n");

    match audio_core::find_sar_config_path() {
        Some(path) => println!("Found SAR config at: {}", path.display()),
        None => {
            eprintln!("Could not find SAR's default.json -- is SAR installed and configured at least once?");
            std::process::exit(1);
        }
    }

    println!("\nAdding 3 Playback endpoints tagged 'test1'...");
    match audio_core::ensure_openaudio_endpoints("test1", audio_core::EndpointKind::Playback, 3) {
        Ok(names) => {
            println!("Success. Created/ensured these endpoint names:");
            for name in &names {
                println!("  - {name}");
            }
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }

    println!("\nNow open default.json yourself and confirm:");
    println!("  1. Your existing manual endpoints (Test1, Output1, etc.) are still there, unchanged.");
    println!("  2. Three new entries exist: OpenAudio-test1-Ch0, OpenAudio-test1-Ch1, OpenAudio-test1-Ch2.");
    println!("  3. A default.json.bak file exists in the same folder (the pre-change backup).");
}