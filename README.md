# OpenAudio

OpenAudio is an open-source, cross-platform audio-over-IP project written in
Rust. It includes a desktop sender/router, a headless receiver, and browser
playback and publishing gateways. The project is currently LAN-development
software, not a production-certified broadcast system. Publishing and playback
primarily use interleaved little-endian Float32 PCM over UDP, with Opus
(48 kHz, 10 ms frames, multichannel split into mono/stereo substreams) wired
through the current publish and receive paths. Opus interoperability and
hardware behavior remain release-validation items; see
[Audio formats and Opus status](#audio-formats-and-opus-status) before relying
on codec interoperability.

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
`http://<computer-ip>:7100/publish` for browser microphone publishing, where the
page's format selector chooses Float32 PCM or Opus. The
gateway's default HTTP port is `7100`; its WebSocket port is `7101`. Use the
access mode displayed in the app. The gateway is stopped by default.

The desktop browser gateway and the `receive_and_serve_web` relay both accept
Float32 and Opus streams and decode to PCM, resampling to the browser's output
sample rate. For
multichannel desktop-gateway streams, playback defaults to a selected stereo
pair; the optional alternate L/R mode averages even and odd channels and is not
a speaker-layout-aware surround downmix. Browser Opus interoperability still
needs end-to-end verification.

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

## Audio formats and Opus status

The wire packet format identifies Float32 PCM (`0x03`), Opus (`0x10`), and
FLAC (`0x11`). Current application support is uneven:

- **Float32 PCM** is the default format. Every publisher can emit either
  Float32 PCM or Opus: the desktop GUI (input, loopback, combined-source, and
  ASIO) through its format setting and saved presets, and the browser
  microphone page through its format selector.
- **Opus** is implemented in `network-core` (48 kHz, 10 ms frames, multichannel
  streams encoded as consecutive mono/stereo substreams) with encoder/decoder
  support and packet envelope helpers. The desktop GUI can publish via discovery
  (input or loopback) in Opus; combined-source publishing also supports Opus.
  Every receive path — simple live playback, WAV capture, ASIO playback, the
  ASIO subscriber, the mixed subscriber bus, the split-device receiver, the
  desktop browser gateway, the shared network receiver (`network_core::receiver`),
  the `receive_and_serve_web` relay, and the VST3 plugin — decodes through one
  shared packet decoder (`network_core::opus_receive::AudioPacketDecoder`). It
  assembles multichannel substreams, rejects duplicates, conceals a missing
  substream after one frame interval, and bounds pending frames. Receivers flush
  it when the socket goes quiet, so the tail of a stream — or a gap where packets
  stop arriving — is concealed and played rather than held indefinitely. Duplicate,
  malformed, and concealed traffic is counted and reported in the bus status line,
  the ASIO timing line, and the browser player's stats bar, so a silent or
  glitching stream can be told apart from a rejected one. The synthetic
  diagnostic publisher can emit Opus. Resampling from non-48 kHz sources to
  48 kHz is handled in the Opus publisher path. The wire framing,
  sequence/timestamp, and per-substream sequencing are implemented, but
  end-to-end interoperability still needs verification across all subscriber
  types.
- **FLAC** has a format identifier but no implemented encoder/decoder.

### Format negotiation

Publishers advertise the concrete wire format a stream sends in the discovery
advertisement (`sample_format`), and subscribers declare every format they can
decode in the subscribe request (`supported_formats`). A publisher only streams
to subscribers that declared support for its format, and reports the number of
skipped subscribers instead of sending them packets they cannot decode. A
subscribe request without `supported_formats` is treated as Float32-only, so
older clients keep working against a Float32 publisher and are safely skipped by
an Opus publisher.

Every subscriber (mixed, split, ASIO, browser gateway, network receiver, the
receiver CLI, and the VST3 plugin) declares PCM and Opus. The VST3 plugin also
publishes Opus when its "Publish Opus" parameter is enabled, which costs a little
CPU for a large bandwidth reduction on multichannel sessions.

The browser microphone page requests its format in the publish handshake and
runs the same registry and filtering as the native publishers, so a page
publishing Opus does not send packets to a receiver that declared PCM only.

The `Auto` publish policy resolves against both the channel count and the
subscribers already waiting on the stream: multichannel streams pick Opus, but if
every waiting subscriber declared PCM only, the publisher stays on PCM instead of
streaming into the void. Subscribers that connect later with a narrower format
are skipped and counted rather than silently dropping the stream. The resolved
format is fixed for the life of a publish session, so changing it means stopping
and restarting the publisher.

Opus is **integrated across every publish and receive path**: the wire framing and
sequence/timestamp contract should still be validated in target scenarios before
relying on Opus in production. Float32 remains important as the default because
it is already used across the regular capture, routing, and playback paths. Its
uncompressed payload rate is
`channels × sample_rate × 32` bits/s: at 64 channels and 48 kHz this is
98.304 Mbit/s before packet/network overhead. Opus may reduce bandwidth, but
its frame duration, per-substream sequencing, packet loss behavior, and added
codec work need validation on target hardware and networks; codec presence does
not prove lower latency or reliable Wi-Fi operation.

## Current limitations

- Opus is available on every publish and receive path, but it is only lightly
  exercised: no Opus stream has been validated on real hardware, across a live
  ASIO driver, or inside a DAW host. Publishers negotiate formats, so an Opus
  stream is never sent to a subscriber that cannot decode it.
- FLAC is not implemented despite its reserved format identifier.
- Browser sample-rate conversion uses linear interpolation. It handles source
  and output rate differences but is not a high-quality resampler or a complete
  end-to-end clock synchronization solution.
- The VST3 sender is an initial implementation, not yet certified across DAW
  hosts, including Studio One. It publishes host-supported mono, stereo, and
  multichannel layouts, and its host-visible Publish parameter must be enabled
  before audio is sent. See
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
  receiver output channel count matches the stream, verify that the selected
  device is available to the operating system, and confirm that both ends use
  a format supported by that complete receive path. Opus support varies by
  subscriber (e.g. mixed bus and desktop browser gateway support it; some
  native routes may not).
- **Browser playback is silent or distorted:** Check that the source format is
  supported by the selected gateway path, select the intended stereo pair, and
  check browser audio output/permissions. The desktop browser gateway supports
  Float32 and Opus (interoperability still needs verification). FLAC is
  unsupported.
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
