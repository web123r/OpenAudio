# OpenAudio Implementation Guide

This guide explains what OpenAudio is being built to become, what is already
implemented, what remains unverified, and how to continue without confusing a
successful build with a release-ready audio system. It is an engineering
handoff, not a claim of production certification.

For authoritative product requirements and the detailed audio/network test
runbook, see [the v1 release scope](./release-scope.md). For end-user
installation, see [the Windows installation guide](./windows-install.md).

**Status snapshot:** 2026-10-13

## 1. Product vision

OpenAudio aims to be a dependable, low-latency audio-over-IP tool for
production workflows on a trusted local network. The first release is
Windows-first: musicians and operators should be able to publish audio from a
computer or DAW, discover it on the LAN, route or receive it on another
computer, and monitor the result without needing a public service or a
complicated installer.

The intended foundation is a native desktop sender/router and receiver, with
optional browser access and a DAW sender. Audio must pass between capture,
network, and playback paths without channel/sample corruption or unbounded
queues. Time-critical audio callbacks must avoid network I/O and blocking work.
Clear diagnostics and repeatable measurements should make failures observable
instead of disguising them as successful playback.

The agreed v1 targets are:

- Up to 64 channels per native stream, with 48 kHz as the primary validation
  rate.
- Interleaved little-endian Float32 PCM over UDP.
- A measured end-to-end latency below 10 ms on a wired LAN. This remains a
  target, not a demonstrated result.
- Windows WASAPI and ASIO workflows.
- A Windows VST3 effect that passes mono/stereo DAW audio through unchanged and
  publishes it to OpenAudio when explicitly enabled.
- Trusted private LAN operation only. Native discovery, control, and audio are
  not encrypted or authenticated.

The project is not yet a production-certified replacement for established
broadcast audio systems. Do not expose its network services to the public
internet.

## 2. How the system fits together

At a high level, native audio follows this path:

```text
capture device or DAW
    -> bounded audio handoff
    -> Float32 packetization
    -> UDP audio on the LAN
    -> discovered stream and subscription control
    -> receive queue and playback conversion
    -> output device
```

The desktop app publishes and routes audio; the receiver CLI supports
discovery, playback, and packet inspection. Discovery and subscriptions tell
publishers which receivers are active. Browser gateways bridge selected
streams to browser playback or microphone publishing. The VST3 sender uses the
existing discovery and subscription protocol, but has its own shared,
ephemeral control listener so it does not conflict with the desktop app's fixed
control port.

The important implementation constraints are:

- Preserve interleaved frame/channel alignment through buffering, resampling,
  packetization, and output.
- Keep audio callbacks bounded and non-blocking. Queue overload must have an
  explicit drop policy and observable loss rather than unbounded latency.
- Keep UDP payloads within the conservative 1,200-byte packet budget used by
  the current publishers.
- Reject unsupported or malformed formats clearly. Opus and FLAC identifiers
  are reserved but have no v1 encoder or decoder.
- Treat Float32 bandwidth and packet rate as real capacity costs. At 64
  channels and 48 kHz, raw audio is 98.304 Mbit/s and the current packet size
  implies approximately 12,000 datagrams/s per stream.

These constraints describe current design intent and implementation choices;
they do not prove hardware timing, network capacity, or audio quality.

## 3. Repository map

| Location | Responsibility |
| --- | --- |
| `crates/network-core` | Wire protocol, Float32 packet handling, discovery, subscription control, and receiver transport |
| `crates/audio-core` | Audio backends, capture/publish paths, routing, playback, diagnostics, and browser gateways |
| `apps/sender` | Desktop GUI for publishing, receiving/routing, browser sharing, and diagnostics |
| `apps/receiver` | Headless receiver and inspection CLI |
| `apps/plugin` | Experimental mono/stereo Windows VST3 DAW sender |
| `.github/workflows/ci.yml` | Cross-platform checks/tests and Windows VST3 bundle build |
| `.github/workflows/release.yml` | Tag-triggered release packaging and checksum workflow |
| `docs/release-scope.md` | Requirements, current support boundary, release gates, and detailed test runbook |
| `docs/windows-install.md` | Portable Windows distribution and installation guidance |
| `apps/plugin/README.md` | VST3 behavior, build/install guidance, and REAPER smoke-test checklist |

## 4. Current implementation status

“Implemented” below means code and documentation exist. “Validated” means only
the specific checks named in the validation section have passed; it does not
imply real-hardware or release certification.

| Area | Implemented | Still open |
| --- | --- | --- |
| Audio reliability | Stateful resampling, frame-aligned queue trimming, bounded publishing handoffs, MTU-safe packetization, and software stress/regression tests | Wired end-to-end latency, sustained real UDP load, clock drift, and representative hardware behavior |
| Network controls | Validated and bounded discovery/subscriber state, subscriber leases, and expired-subscriber filtering | Wired/Wi-Fi loss and recovery thresholds, long-duration behavior, and security testing |
| Browser | Float32 playback, output-rate conversion, selected stereo-pair behavior, and interruption/recovery reporting | Browser/device interoperability and real-network recovery; this remains best-effort |
| Audio formats | Float32 PCM packet creation and clear unsupported-format handling | Opus/FLAC encoders and decoders are intentionally out of v1 scope |
| Windows releases | Portable Windows ZIP and tag-triggered cross-platform release workflow are implemented | Validate real release publication, installation/removal, firewall behavior, signing, and target Windows hardware |
| VST3 sender | Mono/stereo pass-through, opt-in publishing parameter, discovery/subscription integration, bounded callback queue, and Windows bundle generation | REAPER host scan and audio test, separate-receiver LAN test, sustained/overload behavior, third-party notices, and release packaging |

The VST3 is not currently included in the Windows portable ZIP. REAPER is
available on the development machine, but the host scan and audio path have
not yet been verified.

## 5. Work completed so far

The earlier phases addressed the initial report of distorted browser audio and
then strengthened the native and browser paths:

1. **Audio reliability:** Added stateful resampling and frame-aligned queue
   handling; bounded capture-to-publisher queues; moved publisher work away
   from key capture callbacks; and adopted a shared MTU-safe packet budget.
   Software tests exercise queue policies, sample ordering, and packet
   integrity.
2. **Network controls:** Added validation and resource bounds for discovery
   and subscriptions, along with expiring subscriber leases.
3. **Browser sharing:** Added interruption/recovery status, stale playback
   buffer clearing, and explicit handling when a stream format changes during
   playback.
4. **Interoperability:** Kept Float32 PCM as the only supported v1 format and
   made unsupported compressed formats fail clearly rather than being
   misinterpreted as PCM.
5. **Windows release packaging:** Implemented a portable x64 ZIP and
   tag-triggered release/checksum workflow. The Windows package does not
   install drivers or claim to be signed or hardware-certified.
6. **DAW integration:** Revised scope to include a VST3 sender. The first
   vertical slice builds for Windows x64 and is built by Windows CI.

The latest recorded automated checks passed:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets --offline`
- `cargo test --workspace --offline` — 50 Rust tests passed
- `node --test crates\audio-core\tests\web_player.test.js` — 4 tests passed
- `cargo xtask bundle openaudio-plugin --release --target x86_64-pc-windows-msvc --offline`

These results establish software/build coverage only. No actual DAW-host,
audio-interface, two-computer LAN, or calibrated latency test has been
completed.

## 6. Recommended continuation plan

Work through the gates in order. Record the machine, software versions,
topology, settings, measurements, and pass/fail result for each run in the
relevant runbook or a test-results document. Do not mark a gate complete based
only on a successful compile.

### Gate A — Validate the VST3 in REAPER

Follow the **REAPER host smoke test** in [the plugin guide](../apps/plugin/README.md).
First build the bundle and install it in a VST3 directory REAPER scans. Verify:

1. REAPER discovers and loads the plugin without errors.
2. Mono and stereo tracks remain audible and unchanged with publishing off and
   on.
3. The stream appears to an OpenAudio receiver; audio is sent only after
   enabling **Publish to OpenAudio** and subscribing.
4. Channel order and sample rate are correct at the 48 kHz reference rate.
   Repeat at other rates supported by the plugin when practical.
5. Turning publishing off and stopping/restarting playback do not replay stale
   queued audio. Check deactivation, reactivation, and sustained playback.
6. Observe dropped-frame reports and latency during a several-minute run.
   Repeat with a receiver on another computer over a trusted private LAN.

Keep receiver/channel configuration compatible with the plugin's mono/stereo
stream. Record any host-specific scan path or behavior in the plugin guide.
Do not add the VST3 to the release ZIP until this gate and the licensing gate
below are complete.

### Gate B — Close the native audio performance questions

Use the procedures and worksheet in [the v1 release scope](./release-scope.md),
especially its Phase 1 runbook. Re-run the diagnostic publisher after recent
changes and compare its observed packet rate with the expected rate. A
historical five-minute run reported 105,232 packets and 53 sequence-gap drops
(about 351 packets/s); that was far below the nominal 12,000 packets/s for
continuous 64-channel/48-kHz traffic and was not a pass.

Then measure on real systems:

- 64 channels at 48 kHz over UDP, including sustained packet rate, sequence
  gaps, sample integrity, CPU load, and queue/drop behavior.
- End-to-end latency on a wired two-host setup using a documented,
  calibrated method. Define the metric and statistic; the target is below
  10 ms.
- WASAPI and ASIO separately, using representative Windows devices and
  drivers.
- Reconnect and recovery behavior on wired LAN and managed Wi-Fi.
- Long-duration stability using an agreed duration and quantitative
  acceptance limits.

Do not infer these outcomes from synthetic packetizer or queue stress tests.
Those tests cover software behavior, not physical transport or hardware timing.

### Gate C — Review security and release readiness

- Keep unauthenticated native traffic and unencrypted browser traffic within
  the trusted-LAN boundary. Verify firewall guidance and never recommend
  router port forwarding or public exposure.
- Review the VST3 framework and all bundled dependency licenses; add
  appropriate third-party notices before distributing the plugin.
- Build and inspect the Windows portable ZIP, verify checksums, install and
  remove it as a standard user, and confirm it does not alter driver or service
  configuration.
- Exercise the release workflow on an appropriate version tag only when ready
  to publish. Ensure the tag matches the desktop package version and inspect
  the published assets.
- Publish a support matrix that distinguishes tested configurations from
  best-effort ones. Keep macOS/Linux and browser paths labeled accurately.

### Gate D — Decide on the next scope

Only after the preceding gates have evidence should maintainers decide whether
to ship the VST3 in the portable release, adjust v1 requirements, or start
additional product work. CLAP, AudioUnit, DAW receive plugins, multichannel
VST3 ports, a custom plugin editor, and compressed codecs are not part of the
current v1 commitment; each requires an explicit scope decision.

## 7. Build and test from a clean checkout

Run commands from the repository root. Keep the Rust test suite sequential:
UDP-based tests can interfere if multiple full suites run concurrently.

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
node --test crates\audio-core\tests\web_player.test.js
cargo build -p openaudio-gui --bin OpenAudio
cargo build -p receiver
cargo xtask bundle openaudio-plugin --release --target x86_64-pc-windows-msvc
```

For a Windows ASIO feature build, use the project's documented setup and
available SDK/driver prerequisites:

```powershell
cargo check -p openaudio-gui --features asio
```

If a command fails, fix the underlying issue and re-run the narrow failing
check, then repeat the full relevant sequence before reporting the gate as
passed. Some audio/network tests may need actual devices and multiple hosts;
state clearly when that coverage is unavailable.

## 8. Boundaries to preserve

- Do not claim the sub-10-ms latency target, 64-channel throughput, ASIO
  hardware support, Wi-Fi recovery, or VST3 host compatibility until measured
  and documented.
- Do not reinterpret Opus/FLAC or malformed packets as Float32.
- Do not move network I/O or blocking work onto audio callbacks.
- Do not silently hide drops, unsupported formats, mismatched channel layouts,
  or device errors.
- Do not expose discovery, control, audio, HTTP, or WebSocket services to
  untrusted networks.
- Do not expand plugin formats or codec scope without a concrete requirement
  and compatibility/test plan.

For open requirements, test thresholds, and the complete native audio test
procedure, use [the release-scope document](./release-scope.md) as the
maintained source of truth.
