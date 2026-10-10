# OpenAudio Changelog

All notable changes to the OpenAudio project will be documented in this file.

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
