//! Platform-neutral OpenAudio wire-format primitives.
//!
//! This crate deliberately contains no audio-device or operating-system code,
//! so network clients and future Linux/macOS backends can share the protocol.

pub mod aes67;
pub mod discovery;
pub mod fec;
pub mod plc;
pub mod protocol;
pub mod receiver;
