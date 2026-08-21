//! audio-core
//!
//! Milestone 0:  enumerate audio devices.
//! Milestone 1:  capture audio to a WAV file.
//! Milestone 2:  transmit captured audio as UDP packets.
//! Milestone 3:  receive UDP packets and reconstruct audio to WAV.
//! Milestone 4:  receive UDP packets and play them back live.
//! Milestone 5:  transmit to multiple simultaneous subscribers.
//! Milestone 6:  mix multiple Publisher streams into one Subscriber Bus.
//! Milestone 7.5: cancellation + device selection for the GUI.
//! Milestone 9:  network discovery -- no more typed IP addresses.
//! Milestone 10: multiple concurrent Publish/Subscribe sessions per app instance.
//! Milestone 11: split a multichannel stream out to N separate named
//!               output devices (e.g. SAR endpoints), for DAW multichannel routing.
//! Milestone 12: ASIO driver support -- enumerate and capture from any
//!               installed ASIO driver (Midas, Yamaha, A&H, DiGiCo, etc.)
//!               exactly as a DAW would. Build with --features asio.

mod devices;
mod capture;
mod protocol;
mod transmit;
mod receive;
mod playback;
mod bus;
mod discovery;
mod split;
mod util;
mod combine;
mod recording;
mod sar_config;
mod web_stream;
mod web_gateway;
mod platform;
pub mod asio;

pub use platform::{
    boost_audio_thread_priority, ensure_realtime_audio_thread, prepare_realtime_process,
    JITTER_BUFFER_TARGET_SECS,
};

// ── WASAPI (default host) ────────────────────────────────────────────────
pub use devices::{
    get_input_device, get_output_device, list_input_devices, list_output_devices, DeviceInfo,
};
pub use capture::capture_to_wav;
pub use transmit::{
    transmit, transmit_multi, transmit_with_control, transmit_with_discovery,
    transmit_loopback_with_discovery,
};
pub use receive::receive_to_wav;
pub use playback::receive_and_play;
pub use bus::{
    get_volume, new_volume_control, receive_and_play_bus, receive_and_play_bus_with_control,
    receive_and_play_bus_with_volume, set_volume, VolumeControl,
};
pub use discovery::{
    send_subscribe_request, start_advertising, start_control_listener, start_discovery_listener,
    DiscoveredNode, NodeAdvertisement, SubscriberRegistry,
};
pub use split::receive_and_split_to_devices;
pub use combine::{capture_and_combine_with_discovery, ChannelSource};
pub use sar_config::{
    ensure_openaudio_endpoints, find_sar_config_path, openaudio_endpoint_name,
    remove_openaudio_endpoints, EndpointKind,
};
pub use web_stream::receive_and_serve_web;
pub use web_gateway::run_web_gateway; 
pub use recording::{create_wav_writer, finalize as finalize_recording, generate_record_path};
pub use devices::NONE_DEVICE;


// ── ASIO ─────────────────────────────────────────────────────────────────
// Always exported. When the `asio` feature is off, these return a clear
// "not compiled in" error instead of a compile failure.
pub use asio::{capture_asio_with_discovery, list_asio_drivers, AsioDriverInfo};