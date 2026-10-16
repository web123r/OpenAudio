# OpenAudio

OpenAudio is an open-source, cross-platform audio-over-IP project written in
Rust. It includes a desktop sender/router, a headless receiver, and browser
playback and publishing gateways. The v1 wire format is uncompressed
interleaved little-endian Float32 PCM over UDP on a trusted local network.

The project is usable for development and LAN testing, but is not yet a
production-certified replacement for established broadcast audio systems. See
[Current limitations](#current-limitations) before relying on it for a live
production.

The agreed first-release target and its proposed validation gates are recorded
in [docs/release-scope.md](./docs/release-scope.md), including the Phase 1
hardware/software test runbook.
The current implementation and continuation plan are summarized in
[docs/implementation-guide.md](./docs/implementation-guide.md).
Windows portable-package instructions are in
[docs/windows-install.md](./docs/windows-install.md).

## What is included

- Desktop GUI for publishing device audio, routing subscribers, combining and
  splitting channel streams, recording, and synthetic diagnostics.
- Automatic stream discovery and a headless receiver with playback and packet
  inspection modes.
- Optional Windows ASIO integration.
- Browser gateway for listening to network streams and publishing microphone
  input.
- Experimental Windows VST3 sender for publishing mono/stereo DAW tracks to
  OpenAudio receivers.
- Cross-platform audio backend support through CPAL (WASAPI on Windows,
  CoreAudio on macOS, and ALSA/PipeWire/PulseAudio device access on Linux).

## Requirements

- Rust stable toolchain and Cargo.
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

Use **Publish** to choose an input and start a stream. Use **Receive Audio** to
choose a discovered stream and an output. The app also contains **Browser
Sharing** and **Diagnostics** pages.

### Headless receiver

Discover streams on the local network:

```sh
cargo run -p receiver -- --list
```

Play a discovered stream for the default 10 seconds:

```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID>
```

Choose a duration or inspect packet statistics instead of playing audio:

```sh
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --seconds 60
cargo run -p receiver -- --publisher <IP> --stream-id <ID> --inspect --seconds 30
```

The receiver CLI currently plays only when the stream channel count matches the
default output device. Run `cargo run -p receiver -- --help` for its options.

### Browser playback and publishing

Start the **Browser Sharing** gateway in the desktop app and open
`http://<computer-ip>:7100/` for playback or
`http://<computer-ip>:7100/publish` for browser microphone publishing. The
gateway's default HTTP port is `7100`; its WebSocket port is `7101`. Use the
access mode displayed in the app. The gateway is stopped by default.

Browser playback accepts Float32 PCM and resamples to the browser's output
sample rate. For multichannel streams it defaults to a selected stereo pair;
the optional alternate L/R mode averages even and odd channels and is not a
speaker-layout-aware surround downmix.

Browser microphone capture requires a secure browser context. Most browsers
allow microphone access on `localhost` or HTTPS, but not on an ordinary
HTTP LAN address. The gateway currently serves HTTP, not HTTPS, so mobile
publishing from another device may require a trusted HTTPS reverse proxy or a
browser/device configuration that grants microphone access to the LAN origin.

## Network and security

OpenAudio is designed for trusted local networks. Discovery uses IPv4 multicast
address `239.19.84.1` on UDP port `5450`; the native control service uses UDP
port `7000`. Audio uses UDP ports announced or selected during subscription.
The browser gateway uses TCP ports `7100` (HTTP) and `7101` (WebSocket) by
default. Host firewalls must allow the relevant traffic between participating
devices.

The browser gateway's password mode is an access control feature, not transport
encryption. Because the gateway does not provide TLS, do not expose it to the
public internet, forward its ports from a router, or use it on an untrusted
network. Prefer password protection even on a shared LAN.

## Audio format and interoperability

The v1 native and browser audio paths use interleaved little-endian Float32
PCM. Packet headers carry a sample-format identifier, but the Opus (`0x10`)
and FLAC (`0x11`) identifiers are reserved compatibility values only: OpenAudio
does not currently encode or decode those formats. The receiver CLI and browser
gateway report them as unsupported rather than interpreting compressed bytes
as PCM; other native playback paths discard non-Float32 packets.

Float32 is retained for v1 because it is already used throughout capture,
routing, browser playback, and receiver paths, and avoids codec delay and
implementation/dependency complexity while predictable LAN latency and
channel/sample integrity are the current priorities. Its uncompressed payload
rate is `channels × sample_rate × 32` bits/s: at 64 channels and 48 kHz this is
98.304 Mbit/s before packet/network overhead. Validate that load on target
hardware and networks; do not infer Wi-Fi capacity from the codec decision.
Any future codec requires a specified framing/timing contract, real encoder
and decoder implementations, malformed-input limits, and cross-implementation
interoperability tests before being advertised as supported.

## Current limitations

- Native receive and browser playback currently decode Float32 PCM only;
  Opus/FLAC format identifiers have no corresponding encoders or decoders.
- Browser sample-rate conversion uses linear interpolation. It handles source
  and output rate differences but is not a high-quality resampler or a complete
  end-to-end clock synchronization solution.
- The VST3 sender is an initial implementation, not yet certified across DAW
  hosts. It publishes mono/stereo tracks only, and its host-visible Publish
  parameter must be enabled before audio is sent. See
  [the plugin guide](./apps/plugin/README.md).
- Browser microphone publishing may be blocked by browsers that require HTTPS
  for microphone permission on non-localhost origins.
- Latency, audio quality, and reliability have not been certified across all
  listed operating systems, devices, and network conditions. Test the intended
  hardware and network before production use.

## Troubleshooting

- **No streams appear:** Check that devices are on a network that forwards IPv4
  multicast and that UDP `5450` is allowed through host firewalls. Guest Wi-Fi
  and client-isolation settings commonly block device discovery.
- **A stream appears but does not play:** Check UDP firewall rules, confirm the
  receiver output channel count matches the stream, and verify that the selected
  device is available to the operating system.
- **Browser playback is silent or distorted:** Confirm the stream is Float32,
  select the intended stereo pair, and check browser audio output/permissions.
  The browser player does not decode Opus or FLAC.
- **Browser microphone permission is denied:** Use localhost or serve the
  publishing page through a trusted HTTPS origin; ordinary HTTP LAN pages
  generally cannot access the microphone.
- **ASIO is unavailable:** Use a compatible Windows ASIO driver, enable the
  `asio` feature when building, and close any other session holding the driver.

## Project layout

- `crates/network-core`: packet protocol, discovery, and receiver transport.
- `crates/audio-core`: audio backends, routing, recording, and web gateways.
- `apps/sender`: desktop GUI.
- `apps/receiver`: headless receiver and inspection CLI.
- `apps/plugin`: experimental Windows VST3 track sender.
