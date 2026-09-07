//! Platform-neutral OpenAudio wire-format primitives.
//!
//! This crate deliberately contains no audio-device or operating-system code,
//! so network clients and future Linux/macOS backends can share the protocol.

pub mod protocol;
pub mod discovery;
pub mod receiver;
