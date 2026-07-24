//! audio-core
//!
//! Milestone 0: enumerate audio devices.
//! Milestone 1: capture audio to a WAV file.
//! Milestone 2: transmit captured audio as UDP packets.
//! Milestone 3: receive UDP packets and reconstruct audio to WAV.
//! Milestone 4: receive UDP packets and play them back live.
//! Milestone 5: transmit to multiple simultaneous subscribers.
//! Milestone 6: mix multiple Publisher streams into one Subscriber Bus.

mod devices;
mod capture;
mod protocol;
mod transmit;
mod receive;
mod playback;
mod bus;

pub use devices::{list_input_devices, list_output_devices, DeviceInfo};
pub use capture::capture_to_wav;
pub use transmit::{transmit, transmit_multi};
pub use receive::receive_to_wav;
pub use playback::receive_and_play;
pub use bus::receive_and_play_bus;