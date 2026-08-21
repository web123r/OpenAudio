# OpenAudio by Ferronme

Professional audio networking. Open. Free. Windows-first.

OpenAudio is a low-latency audio networking application built in Rust. It provides a free, open-source alternative to proprietary audio routing protocols by transmitting high-quality, uncompressed audio over UDP across your local network.

## Features

- **Low-Latency Audio I/O:** Built on top of Windows WASAPI for fast, direct access to consumer audio devices.
- **ASIO Support:** Native support for professional ASIO drivers (consoles, interfaces) for multi-channel, ultra-low latency routing.
- **Immediate-Mode GUI:** A fast, responsive desktop application built with `egui`.
- **Zero-Config Networking:** Features auto-discovery of streams across the local network—no need to manually type IP addresses.
- **Flexible Routing:** 
  - Mix multiple incoming streams to a single output (Bus Mixing).
  - Combine multiple local devices into a single broadcast stream.
  - Split multi-channel incoming streams into separate local output devices.
- **Loopback Capture:** Easily capture desktop audio output and broadcast it.
- **Recording:** Record streams directly to WAV files while transmitting or receiving.

## Prerequisites

- **OS:** Windows 10 or Windows 11
- **Toolchain:** Rust (MSVC toolchain)
- **Build Tools:** Visual Studio Build Tools with "Desktop development with C++"
- **Version Control:** Git

## Building & Running

Clone the repository and run the main desktop application (the `sender` app) using Cargo:

```bash
cd OpenAudio
cargo run -p ferronme --release
```
*(Note: The main UI application crate is named `ferronme` and is located in the `apps/sender` directory).*

### Enabling ASIO Support
To build with ASIO support, you must have the ASIO SDK configured and build with the appropriate features enabled:

```bash
cargo run -p ferronme --release --features asio
```

## Architecture

The project is structured into a workspace containing the core logic and the frontend applications:

- `crates/audio-core`: The monolithic core of the application. It handles WASAPI and ASIO interactions (via `cpal`), UDP networking, UDP discovery, web streaming gateways, and routing/mixing logic.
- `apps/sender`: The `egui`-based graphical user interface that manages publish and subscribe sessions.

## Project Status

**Current Status:** Core Milestones Completed (GUI & Routing)

### Milestone Tracker

- [x] Milestone 0: Enumerate audio devices
- [x] Milestone 1: Capture audio
- [x] Milestone 2: Transmit packets
- [x] Milestone 3: Receive packets
- [x] Milestone 4: Playback
- [x] Milestone 5: Multiple receivers
- [x] Milestone 6: Routing (Bus mixing)
- [x] Milestone 7: GUI (Publish/Subscribe, Stop button, device selection, error banner)
- [ ] Milestone 8: Virtual audio driver -- DE-SCOPED for now.
      Investigated SYSVAD (simulates capture via tone generator, not real audio) and VirtualDrivers/Virtual-Audio-Driver. A true "cable" driver or custom kernel work is needed to make network audio appear as a selectable mic in other apps.
- [ ] Milestone 9: (next -- TBD)

## Troubleshooting

- **Audio crackles when switching windows:** This has been resolved by elevating the process priority to `HIGH_PRIORITY_CLASS`, preventing Windows 11 EcoQoS and the scheduler from throttling the audio threads when the application loses focus.
- **Device already in use (0x8889000A):** Ensure no other applications are holding an exclusive lock on your audio device.
- **ASIO driver not found:** Make sure your ASIO hardware is connected, its driver is installed, and the app was built with the `--features asio` flag.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
