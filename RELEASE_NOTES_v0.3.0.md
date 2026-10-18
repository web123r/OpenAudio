# OpenAudio v0.3.0

High-performance, ultra-low-latency Audio-over-IP engine with native ASIO, DAW VST3 plugin, zero-allocation real-time audio pipeline, memory-hard Argon2id encryption, and automatic update notifications.

---

## What's New in v0.3.0

### 🚀 Automatic In-App GitHub Update Checker
- Added automated background update checker (`apps/sender/src/updater.rs`) that periodically queries GitHub Releases on `web123r/OpenAudio`.
- Non-intrusive in-app banner alerts you when a newer version is published, with direct "Download Update" and "View on GitHub" actions.
- Added a manual "Check for Updates" trigger and version status indicator to the desktop status bar.

### ⚡ Real-Time Audio Callback Safety & Zero-Allocation Queues
- Re-engineered `BoundedAudioQueue` with pre-allocated buffer recycling, eliminating dynamic memory allocations from real-time audio callbacks.
- Removed blocking `eprintln!` I/O logging inside audio callback loops to protect against priority inversion and audio dropouts under network backpressure.
- Added atomic metrics tracking for observable queue overruns (`drops_total`).

### 🎛️ Non-Blocking VST3 DAW Plugin Lifecycle
- Decoupled runtime initialization, socket rebinding, and thread joins from the DAW audio `process()` path (`apps/plugin/src/lib.rs`).
- Staged parameter updates via atomic dirty flags, ensuring REAPER, Ableton Live, and Cubase audio processing continues uninterrupted when toggling publishing or updating PINs.
- Hardened multi-instance DAW isolation with strict per-packet stream identity validation.

### 🎯 Dynamic Packet Loss Concealment (PLC) & RFC 5109 FEC
- Concealment frame size dynamically matches the active stream format and packetization rather than a fixed 128-sample estimate.
- Preserves exact phase continuity and sample alignment during UDP packet loss.

### 🔒 Cryptographic Hardening & Key Derivation
- Upgraded symmetric stream password derivation to memory-hard `Argon2id` with a dedicated domain salt (`OpenAudio-V1-Argon2id-Salt`).
- Hardened AES-256-GCM symmetric transport by binding the 24-byte packet header as Associated Authenticated Data (AAD), preventing packet header tampering and stream spoofing.
- Enforced nonce uniqueness by embedding nanosecond presentation timestamps into AES-GCM nonces to prevent nonce reuse across stream restarts or sequence rollovers.

### 🛡️ Preset Security & Windows Firewall Hardening
- Plain-text credentials are automatically omitted when saving `.oapreset` files, allowing presets to be shared safely.
- Restricted `setup-firewall.ps1` rules to `Private,Domain` network profiles, avoiding exposure on untrusted public Wi-Fi networks.

---

## Quickstart Guide

1. **Windows Firewall Setup**: Run `setup-firewall.bat` as Administrator to permit UDP audio multicast and unicast streaming.
2. **DAW VST3 Plugin**: Run `install-vst3.bat` to install `OpenAudio.vst3` into `%CommonProgramFiles%\VST3`.
3. **Launch**: Open `OpenAudio.exe`. Use the **Patchbay** tab for 1-click matrix routing or the **Publish** tab to select your ASIO / WASAPI interface.
