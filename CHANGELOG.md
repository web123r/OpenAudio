# OpenAudio Changelog

All notable changes to the OpenAudio project will be documented in this file.

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
