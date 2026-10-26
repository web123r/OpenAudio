# OpenAudio by Ferronme

OpenAudio by Ferronme is an open-source, cross-platform audio-over-IP project written in
Rust. It includes a desktop sender/router, a headless receiver and publisher CLI,
and browser playback and publishing gateways. Supported audio wire formats include
uncompressed Float32 PCM for bit-perfect low-latency studio networks, and RFC 6716
Opus compression for bandwidth-efficient multi-track streaming over Wi-Fi and WAN.

The project is usable for development, studio routing, and LAN distribution.
See [Current limitations](#current-limitations) for details on current cross-platform
certification status.

The agreed first-release target and its proposed validation gates are recorded
in [docs/release-scope.md](./docs/release-scope.md), including the Phase 1
hardware/software test runbook.
The current implementation and continuation plan are summarized in
[docs/implementation-guide.md](./docs/implementation-guide.md).
Windows portable-package instructions are in
[docs/windows-install.md](./docs/windows-install.md).

## What is included

- **Desktop GUI (`OpenAudio`)**: Complete control room for publishing device audio,
  routing subscribers, combining and splitting channel streams, monitoring real-time
  latency / jitter / packet loss, archival FLAC/WAV recording, and synthetic diagnostics.
- **Headless CLI (`receiver`)**: Lightweight command-line binary supporting:
  - Stream discovery, playback, and detailed RFC 3550 packet inspection.
  - Channel selection (`--channels 0,1`) and stereo downmixing (`--downmix stereo`).
  - Headless audio publishing (`receiver publish`) for Linux servers and headless rigs.
  - Per-stream archival recording (WAV and lossless FLAC) on the receiver side.
  - Autonomous discovery-aware reconnection loops.
- **Audio Codecs & Archival**:
  - Uncompressed 32-bit floating-point PCM over UDP.
  - RFC 6716 Opus encoding and decoding for ultra-low-bandwidth multi-channel streaming.
  - Pure-Rust lossless FLAC and broadcast WAV recording engines.
- **Real-Time Audio Safety**:
  - Zero-heap allocation bounded audio queues (`BoundedAudioQueue`) with recycled pools.
  - Real-time thread scheduling: Windows MMCSS "Pro Audio" (`THREAD_PRIORITY_TIME_CRITICAL`),
    Linux `SCHED_RR` real-time priorities, and macOS Mach absolute timebase thread priority.
  - Adaptive jitter buffer with exponential adjustment and Packet Loss Concealment (PLC).
- **Security & Integrity**:
  - AES-256-GCM encryption with Argon2id password-based key derivation.
  - Packet headers bound as Additional Authenticated Data (AAD) to prevent tampering.
- **Hardware & Host Integration**:
  - Optional Windows ASIO integration for low-latency studio interfaces.
  - Cross-platform CPAL backend (WASAPI on Windows, CoreAudio on macOS, ALSA/PipeWire on Linux).
  - Experimental Windows VST3 sender plugin for streaming DAW tracks directly into OpenAudio.
- **Web & Browser Gateway**:
  - Browser playback gateway for listening to network streams from mobile or desktop browsers.
  - Browser microphone publishing gateway.

## Requirements

- Rust stable toolchain and Cargo (1.75+).
- Node.js for the browser playback regression tests.
- Linux: ALSA development headers and `pkg-config`:

  ```sh
  sudo apt-get update
  sudo apt-get install -y libasound2-dev pkg-config
  ```

- macOS desktop audio loopback requires a virtual input driver such as
  [BlackHole](https://github.com/ExistentialAudio/BlackHole).
- Windows ASIO requires compatible installed hardware/drivers. ASIO support is
  optional and is not required for the normal WASAPI build.

## Build and test

Run from the repository root:

```sh
cargo check --workspace
cargo test --workspace
node --test crates/audio-core/tests/web_player.test.js
cargo build -p openaudio-gui --bin OpenAudio
cargo build -p receiver
cargo xtask bundle openaudio-plugin --release
```

Enable the optional Windows ASIO integration with:

```sh
cargo build -p openaudio-gui --bin OpenAudio --features asio
```

Linux builds need the ALSA packages above. The CI workflow checks and tests on
Windows, macOS, and Ubuntu; local hardware behavior still depends on each
machine's audio devices and drivers.

## Quick start

### Desktop application

Launch the GUI:

```sh
cargo run -p openaudio-gui --bin OpenAudio
```

- Use **Publish** to choose an input device, configure channel count/sample rate, select
  codec (PCM or Opus), and optionally enable AES-256-GCM encryption or local recording.
- Use **Receive Audio** to connect to discovered streams. Displays real-time round-trip latency,
  one-way delay, adaptive jitter buffer health, packet loss %, and RFC 3550 jitter.
  Also features automatic stream reconnection if the publisher drops and restarts.
- Explore **Browser Sharing** to enable HTML5 audio streaming and **Diagnostics** for loopback tests.

### Headless receiver & publisher CLI

#### 1. Discover local streams
```sh
cargo run -p receiver -- --list
```

#### 2. Play a stream with channel mapping or stereo downmix
Play channels 0 and 1:
```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --channels 0,1
```
Play a multi-channel stream downmixed to stereo:
```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --downmix stereo
```

#### 3. Stream inspection and network quality metrics
Inspect packet headers, RFC 3550 interarrival jitter, and packet loss without audio output:
```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --inspect --seconds 30
```

#### 4. Per-stream recording on receiver
Record incoming stream directly to a lossless FLAC or WAV file:
```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --record live-session.flac
```

#### 5. Headless publisher mode
Publish an audio device directly from the CLI on headless servers:
```sh
cargo run -p receiver -- publish --device "Microphone" --channels 2 --sample-rate 48000 --codec opus --bitrate 128000
```
Add `--record output.flac` to capture an archival copy simultaneously.

### Browser playback and publishing

Start the **Browser Sharing** gateway in the desktop app and open
`http://<computer-ip>:7100/` for playback or
`http://<computer-ip>:7100/publish` for browser microphone publishing. The
gateway's default HTTP port is `7100`; its WebSocket port is `7101`. Use the
access mode displayed in the app. The gateway is stopped by default.

Browser playback accepts Float32 PCM and resamples to the browser's output
sample rate. For multichannel streams it defaults to a selected stereo pair;
the optional alternate L/R mode averages even and odd channels.

Browser microphone capture requires a secure browser context. With native
HTTPS / TLS enabled, OpenAudio generates an automatic self-signed TLS certificate
covering localhost and local LAN IPs, allowing iOS Safari and Android Chrome to
access `https://<computer-ip>:7100/publish` as a wireless microphone without
external reverse proxies. The mobile publisher features in-browser WebCodecs Opus
compression (32–128 kbps), live animated VU meters, mic gain, and mute controls.

## Network and security

OpenAudio discovery uses IPv4 multicast address `239.19.84.1` on UDP port `5450`;
the native control service uses UDP port `7000`. Audio packets travel over UDP
ports negotiated during subscription. The browser gateway uses TCP ports `7100` (HTTP)
and `7101` (WebSocket) by default. Host firewalls must allow the relevant traffic
between participating devices.

Native streams can be protected with **AES-256-GCM** encryption by setting a passphrase.
Key derivation uses **Argon2id** with salt hashing. Packet headers are authenticated as
AAD (Additional Authenticated Data) to defend against packet-injection and header-tampering attacks.

## Audio formats and interoperability

- **Float32 PCM**: Uncompressed little-endian 32-bit IEEE float. Zero codec latency,
  pristine studio quality. Payload rate: `channels × sample_rate × 32` bits/s
  (e.g., 64 channels @ 48 kHz is 98.3 Mbit/s).
- **Opus (RFC 6716)**: Variable/constant bitrate speech and music compression (6 kbps to 510 kbps).
  Allows high-channel-count audio over standard Wi-Fi or constrained network links.
- **FLAC & WAV Recording**: Real-time archival recording engine. Supports broadcast WAV
  and pure-Rust FLAC compression without external C library dependencies.

## Current limitations

- Browser playback currently decodes Float32 PCM; Opus decoding in the browser gateway
  is handled when served as transcoded PCM streams.
- Browser sample-rate conversion uses linear interpolation. It handles source
  and output rate differences but is not an oversampled sinc resampler.
- The VST3 sender is an initial implementation, tested on major Windows DAWs (Reaper, Ableton).
  It publishes mono/stereo tracks, and its host-visible Publish parameter must be enabled.
- Browser microphone publishing requires HTTPS when accessed from non-localhost devices.
- Local hardware behavior depends on each machine's audio devices, buffer sizes, and drivers.

## Troubleshooting

- **No streams appear:** Check that devices are on a network that forwards IPv4
  multicast and that UDP `5450` is allowed through host firewalls. Guest Wi-Fi
  and client-isolation settings commonly block device discovery.
- **A stream appears but does not play:** Check UDP firewall rules. If stream channel
  count exceeds your output hardware, pass `--downmix stereo` or `--channels 0,1` in the CLI.
- **Browser playback is silent or distorted:** Confirm the stream is Float32 PCM,
  select the intended stereo pair, and check browser audio output/permissions.
- **Browser microphone permission is denied:** Enable HTTPS in Browser Sharing,
  open `https://<ip>:7100/publish`, and accept the browser's self-signed certificate
  prompt (tap Advanced > Proceed). Modern mobile browsers require HTTPS for microphone access.
- **ASIO is unavailable:** Ensure compatible ASIO drivers are installed, build with
  `--features asio`, and verify no exclusive DAW session is locking the ASIO driver.

## Project layout

- `crates/network-core`: packet protocol, discovery, Opus codec, and receiver transport.
- `crates/audio-core`: audio backends (CPAL/ASIO), routing, FLAC/WAV recording, and web gateways.
- `apps/sender`: desktop GUI with real-time latency & quality monitoring.
- `apps/receiver`: headless receiver and publisher CLI with downmixing and recording.
- `apps/plugin`: experimental Windows VST3 DAW track sender.
