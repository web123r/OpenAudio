# OpenAudio

A cross-platform, ultra-low latency, multi-channel audio over IP solution designed to seamlessly bridge realtime audio streams across Windows, macOS, and Linux without the complexity of traditional broadcast hardware.

## Features
- **Mobile Web Publishing:** Stream microphone audio directly from any smartphone or browser via high-performance WebSockets (`/publish`), without installing any apps.
- **ASIO Routing Integration:** Seamlessly route incoming network streams (including browser-based streams) directly into professional ASIO audio hardware.
- **Cross-Platform:** Native audio bridging across Windows (WASAPI/ASIO), macOS (CoreAudio), and Linux (ALSA/PulseAudio/PipeWire).
- **Zero-Configuration Discovery:** Multicast-based automatic discovery of streams across your local network.

## Building and Compiling

OpenAudio relies on native audio hardware access via `cpal` to provide minimal latency playback and capture.

### Prerequisites

You will need the Rust toolchain installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Windows (Primary Support)
Windows requires no special packages out of the box. Audio capture and playback utilizes WASAPI by default.

```bash
cargo build --release
```

### Linux (PulseAudio/PipeWire)
Linux targets require the ALSA development headers so `cpal` can interface with `libasound`.

```bash
sudo apt-get update
sudo apt-get install -y libasound2-dev pkg-config
cargo build --release
```

Note on loopback: Linux loopback capturing requires you to select a "monitor" input device (e.g. provided automatically by PulseAudio or PipeWire for your active sinks).

### macOS (CoreAudio)
macOS utilizes CoreAudio bindings. No third-party system dependencies are required; the system SDK headers provide everything necessary natively.

```bash
cargo build --release
```

Note on loopback: Apple does not provide OS-level desktop loopback audio. You must install a third-party kernel-level driver like [BlackHole](https://github.com/ExistentialAudio/BlackHole), [Soundflower](https://github.com/mattingalls/Soundflower), or Loopback, and select its input stream.

## Usage

Start the sender:
```bash
cargo run -p sender
```

Start the receiver (will discover publishers automatically on the local network):
```bash
cargo run -p receiver -- --list
cargo run -p receiver -- --publisher <IP> --stream-id <ID>
```
