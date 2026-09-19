//! audio-core
//!
//! OpenAudio's audio capture, transport, discovery, playback, routing,
//! recording, ASIO, browser gateway, and diagnostic engine.
//!
//! Milestone 0:  Enumerate audio devices.
//! Milestone 1:  Capture audio to WAV.
//! Milestone 2:  Transmit captured audio as UDP packets.
//! Milestone 3:  Receive UDP packets and reconstruct audio to WAV.
//! Milestone 4:  Receive UDP packets and play them live.
//! Milestone 5:  Transmit to multiple simultaneous subscribers.
//! Milestone 6:  Mix multiple publisher streams into one subscriber bus.
//! Milestone 7.5: Cancellation and device selection for the GUI.
//! Milestone 9:  Network discovery.
//! Milestone 10: Multiple concurrent publish/subscribe sessions.
//! Milestone 11: Split multichannel streams across output devices.
//! Milestone 12: ASIO driver enumeration and multichannel capture.
//! Milestone 13: Route received streams directly to ASIO outputs.
//! Milestone 14: Synthetic multichannel publishing for network diagnostics.

pub mod backend;
pub mod cpal_backend;
mod bus;
mod capture;
mod combine;
mod devices;
mod diagnostic_publish;
mod discovery;
mod platform;
mod playback;
mod protocol;
mod receive;
mod recording;
mod sar_config;
mod split;
mod stream_control;
mod transmit;
mod util;
mod web_gateway;
pub mod web_publish;
mod web_stream;

pub mod asio;
mod asio_subscribe;
pub mod signal_meter;

pub use signal_meter::{
    register_scoped_signal_meter, register_signal_meter, remove_signal_meter,
    signal_meter_snapshots, SignalDirection, SignalMeter, SignalMeterGuard, SignalMeterSnapshot,
};

// ── Real-time process and thread configuration ──────────────────────────

pub use platform::{
    boost_audio_thread_priority, ensure_realtime_audio_thread, prepare_realtime_process,
    JITTER_BUFFER_TARGET_SECS,
};

pub use stream_control::{
    resample_interleaved_linear, AdaptiveJitterController, ClockSynchronizer,
};

// ── Standard audio-device discovery ─────────────────────────────────────

pub use devices::{
    list_input_devices, list_output_devices, DeviceInfo, NONE_DEVICE,
};

// ── Basic capture and receive operations ────────────────────────────────

pub use capture::capture_to_wav;
pub use playback::receive_and_play;
pub use receive::receive_to_wav;

// ── Network publishing ──────────────────────────────────────────────────

pub use transmit::{
    transmit, transmit_loopback_with_discovery, transmit_loopback_with_discovery_labeled,
    transmit_multi, transmit_with_control, transmit_with_discovery,
    transmit_with_discovery_labeled,
};

// ── Synthetic diagnostic publishing ─────────────────────────────────────
//
// Generates deterministic multichannel Float32 audio without requiring a
// physical console or input device. It uses the same discovery,
// subscription, packet format, and UDP transport as production publishers.

pub use diagnostic_publish::{
    publish_diagnostic_stream_with_discovery, DiagnosticPublishConfig, DiagnosticPublishReport,
};

// ── Mixed subscriber bus ────────────────────────────────────────────────

pub use bus::{
    get_volume, new_volume_control, receive_and_play_bus, receive_and_play_bus_with_control,
    receive_and_play_bus_with_volume, set_volume, VolumeControl,
};

// ── Discovery and subscription control ──────────────────────────────────

pub use discovery::{
    send_subscribe_request, start_advertising, start_advertising_with_labels,
    start_control_listener, start_discovery_listener,
    DiscoveredNode, NodeAdvertisement, SubscriberRegistry,
};

// ── Multichannel standard-device and SAR routing ────────────────────────

pub use combine::{
    capture_and_combine_with_discovery, capture_and_combine_with_labels, ChannelSource,
};

pub use split::receive_and_split_to_devices;

pub use sar_config::{
    ensure_openaudio_endpoints, find_sar_config_path, openaudio_endpoint_name,
    remove_openaudio_endpoints, EndpointKind,
};

// ── Browser playback ────────────────────────────────────────────────────
pub use web_gateway::{run_web_gateway, GatewayAccess, WebGatewayConfig};

pub use web_stream::receive_and_serve_web;

// ── Recording ───────────────────────────────────────────────────────────

pub use recording::{create_wav_writer, finalize as finalize_recording, generate_record_path};

// ── ASIO publishing and device enumeration ──────────────────────────────
//
// These remain exported without the `asio` feature. In that configuration,
// their stub implementations return descriptive runtime errors, allowing
// the rest of the application to compile normally.

pub use asio::{
    capture_asio_with_channel_labels, capture_asio_with_discovery, list_asio_drivers,
    AsioDriverInfo,
};

// ── ASIO subscription and output routing ────────────────────────────────

pub use asio_subscribe::receive_and_play_asio;
