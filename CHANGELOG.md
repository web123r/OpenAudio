# OpenAudio Changelog

All notable changes to the OpenAudio project will be documented in this file.

## [0.3.3] - 2026-11-13

### 🎹 **Studio One DAW Integration**
* Added full Studio One compatibility documentation, plug-in deployment guidance, and host validation procedures for **OpenAudio DAW Sender** and receiver workflows.
* Verified VST3 bundle discovery via Studio One Plug-in Manager (`%LOCALAPPDATA%\Programs\Common\VST3` and `%CommonProgramFiles%\VST3`), including blocklist recovery and transport start/stop testing.
* Validated mono, stereo, and multichannel surround/bus channel layouts (up to 64 channels) under 44.1 kHz and 48 kHz host sample rates across multiple audio buffer block sizes.
* Updated test checklists and DAW deployment guides with Studio One specific verification steps.

## [0.3.2] - 2026-10-31

### 🎛️ **VST3 Plugin Opus Support**
* The plugin now decodes Opus: its receiver declares PCM and Opus and decodes through the shared `AudioPacketDecoder`, so it receives multichannel Opus streams that previously reached it as nothing.
* Added an opt-in `Publish Opus` parameter (default off). When enabled, the plugin resamples non-48 kHz host audio, encodes 20 ms frames, advertises the Opus wire format, and filters subscribers by declared capability.
* Plugin publishing and subscription now use `active_subscribers_for_format` and `send_subscribe_request_with_formats` instead of the format-blind variants.

### 🌐 **Browser Publisher Format Support**
* The browser microphone publisher now honours format negotiation: it uses the shared subscriber registry and `active_subscribers_for_format`, so it stops inserting every subscriber and reports skipped incompatible ones like the native publishers do.
* Its handshake accepts `format` (`float32`, `opus`, or `auto`, defaulting to `float32` for older pages), and the publish page gained a format selector. Opus publishing resamples non-48 kHz capture, encodes 20 ms multichannel frames, and advertises the concrete format.
* Publish timestamps are now derived from the sent frame count instead of wall-clock reads, so packet spacing matches audio time for both formats.
* The publisher handshake validates the channel count (1–64) before audio is split; a zero-channel handshake previously divided by zero.

### 🐛 **Discovery and Format Fixes**
* The discovery listener now sizes its receive buffer from the protocol limits (`MAX_ADVERTISEMENT_BYTES`) instead of a fixed 1 KiB array, so 64-channel advertisements — previously ~1035 bytes and silently dropped — are received. A test pins the worst-case escaped advertisement below the buffer size.
* `PublishFormat::Auto` now resolves against the subscribers already waiting on the stream: a multichannel stream whose waiting subscribers all declared PCM only stays on Float32 rather than publishing Opus to nobody.
* The receiver's nominal packet-rate report used integer division and under-reported rates (328 packets/s instead of 328.77 at 2 ch), which skewed the expected-packet-loss estimate.

### 📡 **Opus Format Negotiation**
* Discovery advertisements now carry the concrete wire format a stream sends (`sample_format`), and subscribe requests declare every format the receiver can decode (`supported_formats`).
* Publishers only stream to subscribers that declared support for the stream's format, and report how many subscribers were skipped for being incompatible instead of sending undecodable packets.
* Requests without `supported_formats` are treated as Float32-only, preserving compatibility with older clients.
* Every native receiver — mixed bus, split receiver, browser gateway, network receiver, receiver CLI, ASIO subscriber, and VST3 plugin — declares both PCM and Opus.

### 🔊 **Universal Native Opus Receiving**
* Added a shared `AudioPacketDecoder` that assembles multichannel Opus substreams, applies packet-loss concealment to missing substreams, and decodes plain Float32 packets, so the simple receivers no longer carry format-specific logic.
* Simple live playback, WAV capture, ASIO playback, and the `receive_and_serve_web` relay now accept Opus as well as Float32 streams.
* The ASIO subscriber now declares PCM and Opus support, removing the last native subscriber that rejected Opus.
* Every receive path (bus, split, ASIO subscriber, shared receiver, playback, WAV capture, web relay, and browser gateway) now decodes through that one assembler instead of four hand-written copies, so substream completeness, duplicate rejection, stale-frame concealment, and the bounded pending-frame limit behave identically everywhere.
* A quiet socket no longer strands a partly received frame: each receiver calls the decoder's new `flush_expired` on its read timeout, so the tail of a stream and gaps where packets stop are concealed after one 20 ms interval instead of being held until the next packet. The browser gateway had lost this behaviour when its private assembler was removed.
* Added `DecodeStats` so dropped Opus traffic is explainable rather than silent: duplicate substreams, malformed/unsupported packets, and concealed substreams are counted in the decoder and surfaced in the bus status line and shutdown summary, the ASIO subscriber's timing line, and the browser gateway's metrics message (shown in the player's stats bar).

### 🎛️ **Selectable Opus Publishing**
* Added a `PublishFormat::Auto` policy that selects Opus at 8 or more channels and Float32 below that; Float32 remains the default.
* ASIO publishing now honours the selected publish format instead of always sending Float32, and the setting is persisted in presets.

---

## [0.3.0] - 2026-10-16

### 🚀 Highlights & New Features

#### 🔔 **Automatic Background GitHub Update Checker**
* Integrated background update checker (`apps/sender/src/updater.rs`) that periodically queries the GitHub Releases API for `web123r/OpenAudio`.
* Added non-intrusive in-app banner alert showing the newly published version, release title, and immediate one-click action buttons to download or view the release notes on GitHub.
* Added manual "Check for Updates" trigger and status indicator to the desktop GUI status bar.

#### ⚡ **Real-Time Audio Safety & Zero-Allocation Queues**
* Re-engineered `BoundedAudioQueue` (`crates/audio-core/src/util.rs`) with pre-allocated buffer recycling to guarantee zero heap allocations on high-priority audio callback threads.
* Eliminated blocking `eprintln!` I/O logging inside audio callback loops in `crates/audio-core/src/transmit.rs`, preventing priority inversion and audio dropouts under network backpressure.
* Added atomic drop metrics tracking for explicit, observable buffer overrun monitoring.

#### 🎛️ **Non-Blocking VST3 DAW Plugin Lifecycle**
* Decoupled runtime initialization, socket rebinding, and thread joins from the DAW audio `process()` path (`apps/plugin/src/lib.rs`).
* Introduced staged parameter updates via atomic dirty flags, ensuring REAPER, Ableton Live, and Cubase audio processing continues uninterrupted when toggling publishing or changing PINs.
* Hardened multi-instance DAW isolation with strict per-packet stream identity validation.

#### 🎯 **Precision Packet Loss Concealment (PLC)**
* Dynamically sized PLC concealment synthesis in `crates/audio-core/src/bus.rs` and `apps/receiver/src/main.rs` to match exact stream packet duration and channel count rather than a fixed 128-sample estimate.
* Ensured sequence continuity and phase alignment across packet loss events.

#### 🔒 **Cryptographic Hardening & Key Derivation**
* Upgraded stream password key derivation from SHA-256 to memory-hard `Argon2id` using a dedicated domain salt (`OpenAudio-V1-Argon2id-Salt`) (`crates/audio-core/src/crypto.rs`).
* Hardened AES-256-GCM symmetric transport by binding the 24-byte packet header as Associated Authenticated Data (AAD), thwarting header tampering and stream spoofing.
* Enforced nonce uniqueness by incorporating nanosecond presentation timestamps into AES-GCM nonces, preventing nonce reuse across restarts and sequence rollovers.

#### 🛡️ **Preset Security & Windows Firewall Hardening**
* Prevented accidental credential leakage by skipping serialization of sensitive passphrases in preset files (`.oapreset`).
* Restricted firewall configuration script (`scripts/setup-firewall.ps1`) to `Private,Domain` network profiles, avoiding open port exposure on untrusted public Wi-Fi networks.

---

## [0.2.0] - 2026-10-10

### 🚀 Added Features & Major Architecture Upgrades

#### 📦 **Workspace & Crate Structuring**
* Renamed `apps/sender` crate to `openaudio-gui` to eliminate cargo workspace name collisions, while preserving binary output as `OpenAudio.exe`.
* Added new `openaudio-plugin` crate (`apps/plugin`) to workspace members for DAW (VST3 / CLAP / AudioUnit) host buffer processing.

#### 🔄 **Network Resiliency & Packet Re-ordering**
* Added `PacketReorderWindow` in `network-core::receiver`. Implements a 32-packet sliding sequence reorder buffer that holds out-of-order UDP frames and restores consecutive packet order before playback.

#### 🔒 **Security & Payload Encryption**
* Added `aes-gcm` crate dependency to `audio-core`.
* Implemented `StreamCipher` module (`crypto.rs`) providing authenticated AES-256-GCM symmetric encryption and decryption for live audio stream payloads.

#### 🎼 **Audio Codec Protocol Extensions**
* Extended wire-format protocol in `network-core::protocol` with `SAMPLE_FORMAT_OPUS` (`0x10`) and `SAMPLE_FORMAT_FLAC` (`0x11`) payload format codes.
* Updated `ParsedPacket` struct to expose `pub sample_format: u8`.

#### ⏱️ **Precision Time Protocol (PTP IEEE 1588)**
* Created `PtpClock` module (`ptp.rs`) in `audio-core` for grandmaster-aligned nanosecond timestamping to prevent hardware clock drift across network nodes.

#### 📻 **AES67 / RFC 4566 Broadcast Interoperability**
* Created `Aes67StreamInfo` module (`aes67.rs`) in `network-core` for generating standard Session Description Protocol (SDP) metadata, enabling stream discovery by commercial broadcast hardware.

#### 🌐 **WebRTC Browser Gateway**
* Created `WebRtcAudioSession` module (`webrtc_gateway.rs`) in `audio-core` for sub-10ms browser audio publishing and playback using WebRTC SDP offers/answers.

#### 🎛️ **Visual Drag-and-Drop Patchbay UI**
* Created `PatchbayMatrix` module (`patchbay.rs`) in `apps/sender` to render a 2D drag-and-drop routing matrix grid in `egui`.

---

## [0.1.0] - 2026-09-19

### Initial Release
* Cross-platform Audio over IP engine using `cpal` for Windows (WASAPI/ASIO), macOS (CoreAudio), and Linux (ALSA/PulseAudio/PipeWire).
* Real-time thread priority boosting, UDP multicast node discovery, eframe/egui desktop GUI, and CLI receiver.
