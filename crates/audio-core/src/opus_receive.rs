//! Re-export of the shared packet decoder from `network-core`.
//!
//! The decoder lives in `network_core::opus_receive` so both crates and the
//! DAW plugin share one multichannel Opus assembler.

pub use network_core::opus_receive::*;
