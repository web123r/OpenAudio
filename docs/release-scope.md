# OpenAudio v1 Release Scope

This document records the agreed product target for the first release. It is a
scope baseline, not a claim that the criteria are already met.

## Product target

OpenAudio v1 targets professional audio production workflows on a trusted local
network. The first-class release platform is Windows. macOS and Linux remain
best-effort until they have equivalent hardware coverage and validation.

The release-critical product is the native desktop sender/router and native
receiver. Windows v1 must support both WASAPI and ASIO workflows. The v1 scope
now also includes a first Windows VST3 sender for mono and stereo DAW tracks.
Browser playback and publishing may remain best-effort and are not release
gates.

## Agreed requirements

- **Channel capacity:** Support streams of up to 64 channels.
- **Reference sample rate:** Use 48 kHz as the primary Phase 1 validation rate.
- **V1 audio representation:** Interleaved little-endian Float32 PCM. Opus and
  FLAC are not v1-supported formats.
- **Latency:** Target measured end-to-end latency below 10 ms on a wired LAN.
- **Network scope:** Support trusted wired LANs and managed Wi-Fi LANs. The
  under-10-ms latency target applies to wired LAN operation.
- **Trust boundary:** The v1 network model is a trusted LAN, not public-internet
  or untrusted-network use. Do not expose unauthenticated or unencrypted
  services to the public internet. Native discovery, subscription control, and
  audio UDP are unauthenticated and unencrypted; the browser gateway password
  adds access control but its HTTP/WebSocket traffic is unencrypted and can be
  observed on the LAN. It does not secure native streams or encrypt media.
- **Windows audio APIs:** WASAPI and ASIO are both release requirements.
- **DAW integration:** A VST3 effect plug-in sends mono/stereo host audio as a
  discoverable OpenAudio stream, passes audio through unchanged, and publishes
  only when its host-visible Publish parameter is enabled. Plugin processing
  must not perform network I/O or wait on a lock. CLAP, AudioUnit, DAW receive,
  multichannel plugin ports, and a custom editor are outside this first VST3
  milestone.
- **Non-goals for v1:** Release-critical browser playback/publishing and DAW
  plugin formats other than VST3.

## Release gates to define and verify

These are the measurable checks Phase 1 must turn into a repeatable validation
plan:

1. Define the latency test topology and method (including whether reported
   latency is one-way or round-trip, measurement instrumentation, and the
   statistic/threshold used). Then demonstrate the agreed sub-10-ms result on a
   reference Windows system over wired LAN.
2. Demonstrate sustained 64-channel publishing and receiving at 48 kHz without
   channel misalignment, corrupt samples, uncontrolled queue growth, or
   audio-thread failure. Specify the bit representation and test duration as
   part of the test plan.
3. Verify both WASAPI and ASIO end-to-end on representative Windows hardware.
4. Exercise discovery, subscription, loss/reorder recovery, reconnect, and
   multi-hour operation on wired LAN and managed Wi-Fi. Set quantitative packet
   loss, recovery-time, and soak-duration acceptance thresholds before calling
   the gate complete.
5. Confirm that unsupported formats and incompatible device/channel layouts
   fail clearly rather than producing misleading or malformed playback.
6. Publish a support matrix separating release-blocking Windows configurations
   from best-effort macOS/Linux configurations.

### Current support boundary

| Platform / path | Current status | V1 release role |
| --- | --- | --- |
| Windows desktop, WASAPI | Implemented; software-tested, hardware gate open | Release-critical |
| Windows ASIO | Optional feature builds and software-tests; representative hardware gate open | Release-critical |
| macOS / Linux native audio | Builds and behavior vary by available host backend; no equivalent hardware matrix | Best-effort |
| Browser playback / publishing | Available but not release-certified | Best-effort |
| Windows VST3 DAW sender | Initial implementation; host compatibility and hardware validation remain open | In-scope, pending validation |

The reference sample rate is set to 48 kHz. Remaining thresholds in items 1,
2, and 4 are intentionally not invented here: the reference hardware, latency
measurement definition, and acceptable loss/recovery limits still need to be
specified in the Phase 1 test plan before treating them as certification
criteria.

## Phase 1 test runbook

Run tests in increasing cost order. Use a trusted private LAN only, and do not
port-forward OpenAudio services to the internet. Record the date, app commit or
build, Windows version, audio device/driver and buffer settings, stream format,
network adapter/link rate, test duration, and raw console reports for every
run.

### 1. Local software and packet-path smoke test (one PC)

1. In PowerShell at the repository root, start the desktop app:

   ```powershell
   cargo run -p openaudio-gui --bin OpenAudio
   ```

2. Open **Diagnostics**, create a diagnostic stream, set **Channels** to 64
   and **Sample rate** to 48,000 Hz, and click **Start test**. Note the stream
   ID shown in the panel.
3. In a second PowerShell window, subscribe from the same PC and inspect
   packets (replace the ID):

   ```powershell
   cargo run -p receiver -- --publisher 127.0.0.1 --stream-id <ID> --inspect --seconds 30
   ```

4. Repeat with 2, 8, 16, 32, then 64 channels. Start with 30 seconds; if the
   machine remains responsive, run 5 minutes at 64 channels. A 64-channel
   48-kHz publisher uses 4 frames per datagram, so the nominal rate is about
   12,000 datagrams/s and 360,000 datagrams over 30 seconds. The receiver
   summary reports observed and nominal packet rates, received audio duration,
   nominal receive percentage, and sequence gaps. Compare these with the
   publisher's final report, especially `datagrams_sent`,
   `skipped_catch_up_frames`, and `send_errors`. Sequence-gap drops count
   missing packet sequence numbers, but cannot detect audio frames the
   publisher skipped before packet creation. The publisher also reports
   subscriber-active duration and active packet/datagram rates; compare these
   with wall-clock rates so time spent waiting for a subscriber is not mistaken
   for a pacing shortfall. Also note CPU load, GUI responsiveness, disconnects,
   and whether the stream remains discoverable.

This localhost test exercises packet generation, subscription, receiver
parsing/reordering, and local scheduling. It does not exercise Ethernet/Wi-Fi,
two-host discovery, physical audio devices, or the full socket/network path.
If no packets arrive, confirm the publisher is running, use the exact stream
ID, and allow the app through Windows Defender Firewall on a private network.

The user reported a five-minute 64-channel, 48-kHz inspection run with
105,232 received packets and 53 sequence-gap drops. That is about 351 received
packets/s, far below the continuous-stream nominal rate of 12,000 packets/s;
it is not a pass. One plausible sender-side contributor was coarse Windows
timing from the diagnostic publisher's former sub-millisecond sleep; deadline
waiting has now been improved, but the original result cannot be attributed
without sender metrics. Sequence-gap drops do not count sender-side skipped
audio frames. Repeat the run and collect the publisher's final `datagrams_sent`,
`skipped_catch_up_frames`, `send_errors`, subscriber-active duration, and
active packet/datagram rates alongside the receiver's observed/nominal-rate
summary before attributing a shortfall to network loss.

### 2. Two-host UDP throughput and recovery test

Use two Windows PCs on the same wired switch for the first network test. Avoid
VPNs, guest Wi-Fi, and client-isolated access points. On the sender, run the
GUI and start a 64-channel, 48-kHz diagnostic stream. Find its IPv4 address
with `ipconfig`. On the receiver, run:

```powershell
cargo run -p receiver -- --publisher <SENDER_IPV4> --stream-id <ID> --inspect --seconds 300
```

Begin with 2 channels, then 32 and 64. Run each format for 30 seconds, then
run the 64-channel case for 5 minutes and, after it is stable, an hour. Repeat
with the two PCs swapped if practical. Save the diagnostic publisher's final
report and receiver's `packets` / `dropped` result. Compare the receiver's
observed rate to the publisher's active datagram rate; the 64-channel nominal
is about 12,000 packets/s. Calculate receive rate as `packets / seconds`.
Treat any nonzero sender `send_errors` or `skipped_catch_up_frames`, receive
sequence gaps, format changes, unexpected disconnects, channel/sample errors,
or uncontrolled CPU/queue growth as a result to investigate, not as a pass.
The acceptable packet-loss/recovery threshold must be agreed before
certification; the current project has not established one.

For recovery, repeat while briefly disconnecting the network cable, then
restore it; separately stop/restart the diagnostic stream and reconnect the
receiver. Record time to resume packet reception, missing sequence count,
whether discovery/subscription recovers without restarting the app, and
whether audio resumes correctly. Do not combine cable-pull and publisher
restart into one run; isolate failure causes.

### 3. WASAPI and ASIO device tests

For WASAPI, use **Publish** with the actual input device and **Receive Audio**
with the actual output device. First run 2 channels at 48 kHz; verify meters
move, audio is clean, channel routing is correct, and optional recording
opens, grows, and finalizes correctly. Then test the highest channel count
your hardware exposes, increasing gradually. Record device names, shared or
exclusive mode where applicable, channel count, sample rate, buffer settings,
underruns, dropouts, and recording playback.

For ASIO, install and verify the interface vendor's driver, close other apps
that may own the driver, then launch the feature-enabled app:

```powershell
cargo run -p openaudio-gui --features asio --bin OpenAudio
```

Test ASIO Publish and ASIO Subscribe separately before testing them together.
Use a low-risk stereo test first, then the intended multichannel layout. Record
driver/version, selected input/output channel indices, sample rate, buffer
size, exclusive-use errors, audio dropouts, and whether channels remain in the
expected order. A successful compile or localhost synthetic test is not an
ASIO hardware pass.

### 4. Full end-to-end latency measurement

This gate needs at least two PCs, a wired LAN, audio interfaces, and calibrated
measurement equipment. A practical electrical method is to split a low-level
test pulse: feed one branch into the sender's capture input, and record a
reference branch on a spare channel of the same measurement recorder. Connect
the receiver's line output back to another recorder input. Record the
reference and returned pulse simultaneously on the same clock, then use
cross-correlation to find the delay from reference to returned signal. Use
proper line-level attenuation/isolation and suitable line inputs; never connect
a speaker output directly to a microphone input.

This measures the capture-to-receiver-output route plus the measurement
return/cabling path. Calibrate and subtract that return-path delay, or use
instrumentation that directly observes both endpoints; do not simply divide a
round-trip number by two and call it one-way latency. Repeat at least 100
pulses at 48 kHz under idle and representative load, and report median, p95,
p99, maximum, dropouts, and the raw captures. A proposed gate is p95 below
10 ms with no audio dropouts during the measurement, but the statistic,
instrument, test load, and acceptance rule must be agreed before this becomes
the formal pass criterion.

Without electrical loopback or equivalent calibrated instrumentation, speaker
to microphone measurements are useful only as rough demonstrations: room,
transducer, placement, and acoustic propagation contaminate the result.

### 5. Managed Wi-Fi and extended soak

After wired tests pass, repeat the same 2/32/64-channel receiver-inspect runs
on the intended managed Wi-Fi access point. Record band, channel, RSSI or
signal quality, link rate, distance, client count, and whether multicast and
peer-to-peer UDP are permitted. Verify discovery and subscription; guest
networks and client isolation are expected to prevent operation. Run the
64-channel soak for at least one hour initially, then multi-hour after issues
are resolved. Save sender/receiver reports and note drops, recovery time,
queue growth, CPU use, audio-thread errors, and whether the stream survives
brief Wi-Fi interruption and recovery.

### Results worksheet

Keep one row per run with: topology (localhost/wired/Wi-Fi), sender and
receiver, transport/device API, channels, sample rate, run duration, expected
and observed packets/s, subscriber-active duration, active sender packet and
datagram rates, sent datagrams, receiver packets, dropped packets, send errors,
skipped catch-up frames, CPU peak/average, queue/dropout evidence, recovery
time, and pass/fail/blocked reason. Mark a test **blocked** when the
required host, hardware, or measurement instrument is unavailable; do not
replace missing hardware evidence with the software simulations above.

## Phase 1 progress (2026-10-12)

The following software reliability improvements are in place:

- A stateful linear resampler now retains fractional phase and the boundary
  frame between network packets. It is used by standalone native playback,
  bus, split, ASIO receive, and the receiver CLI. A one-second 48 kHz to
  44.1 kHz packetized regression test confirms exact output frame count and
  channel pairing.
- Playback queue trimming now discards complete interleaved frames in the
  standalone and bus paths. Split queues are mono and remain sample aligned.
- Bus output remapping no longer allocates scratch buffers on every audio
  callback or allocates channel-count scratch storage for each remapped frame.
- Bus recordings use the rendered output channel count and sample rate when
  routed through a physical device.
- Receiver CLI tests cover output-channel mismatch rejection and
  frame-aligned queue trimming.
- Native, combined-source, and browser publishers now share a conservative
  1,200-byte UDP payload budget, including the 32-byte OpenAudio headers.
  This prevents the former 64-channel, 58-frame packets (14,880 bytes) from
  requiring IP fragmentation. ASIO and diagnostic publishers already enforce
  the same budget.
- Native fixed-destination and discovery-based WASAPI input and loopback
  publishers now hand timestamped audio blocks to a bounded 100-ms queue.
  Subscriber lookup, WAV writes, packet construction, and UDP sends run on a
  worker; overload drops stale complete blocks and reports dropped frames
  rather than waiting in the audio callback. Queue tests cover ordering,
  saturation, capacity, and shutdown.
- Combined-source capture now uses bounded nonblocking queue insertion;
  per-channel WAV writes are performed by the combine processing loop rather
  than the capture callbacks. Saturation drops stale channel frames and reports
  the loss.
- ASIO capture queue insertion now uses a nonblocking lock attempt and drops
  the current callback's frames, with the loss added to existing capture
  metrics, if the publisher worker holds the queue lock.
- A deterministic queue load test simulates five seconds of 64-channel,
  48-kHz capture in 1-ms callback blocks. It verifies exact sample ordering,
  no drops when producer and consumer keep pace, and that buffered samples
  remain within the configured 100-ms bound. This exercises queue behavior,
  not UDP throughput or hardware audio timing.
- A one-second 64-channel, 48-kHz packetizer/parser stress test constructs,
  parses, and validates 12,000 consecutive MTU-safe Float32 packets. It checks
  sequence continuity and exact interleaved sample preservation. It is a
  deterministic software integrity/CPU workload, not a socket-throughput or
  end-to-end network result.
- The synthetic diagnostic publisher now waits against the next sample-frame
  deadline and uses a short spin window for sub-millisecond intervals instead
  of relying on a 250-microsecond sleep that Windows may round up substantially.
  High-channel diagnostic runs can therefore consume noticeable CPU; record
  CPU usage alongside packet rate and skipped-frame counters.

These changes have passed `cargo test --workspace --offline`,
`cargo test -p audio-core --features asio --offline`, and
`cargo check --workspace --offline` on the current Windows development
environment. They do not establish hardware latency, UDP throughput, or ASIO
performance.

**Phase 1 status:** Software reliability work and repeatable software tests
are complete for the current environment. The phase is not hardware-certified
and must remain open for the wired two-host latency test, sustained real UDP
64-channel soak, representative WASAPI and ASIO tests, and wired/managed-Wi-Fi
recovery tests. Do not interpret the passing simulation tests as satisfying
those gates.

Remaining Phase 1 blockers/findings:

- The configured initial jitter buffer target is 20 ms and receive queues may
  grow to 200 ms. The current settings cannot substantiate the sub-10-ms
  end-to-end target; tuning them without hardware measurements risks trading
  latency for frequent underruns.
- The conservative packet budget avoids fragmentation, but has an important
  high-channel-count cost: at 64 channels, only 4 Float32 frames fit per
  packet, or about 12,000 packets per second per stream at 48 kHz. This
  packet-rate and CPU/network overhead must be measured under sustained load;
  the size fix is not evidence that 64-channel operation is certified.
- Realtime isolation is incomplete: combine capture callbacks still allocate
  a mono block and bounded queues use a short nonblocking mutex acquisition.
  ASIO conversion and interleaved queue insertion still occur in the callback.
  Native publishers copy callback buffers into owned blocks. Therefore,
  allocation-free/lock-free capture is not yet claimed.
- The new publisher handoff can buffer up to 100 ms, which alone exceeds the
  under-10-ms end-to-end goal when saturated. The queue is intended as a
  bounded overload safeguard, not a latency guarantee; its drop rate and
  practical capacity must be evaluated under representative load. Combined
  sources likewise allow up to 100 ms of capture buffering per input channel.
- No designated reference audio interface, wired LAN test pair, calibrated
  loopback measurement method, 64-channel soak result, managed-Wi-Fi result, or
  ASIO hardware result has been supplied. These remain release gates.
- Current available test setup is one Windows PC using WASAPI, with speakers
  or headphones and a microphone but no electrical output-to-input loopback.
  A second Windows PC may be arranged later. Until then, software/network
  stress tests can proceed, but a wired two-host end-to-end latency result and
  ASIO certification cannot be claimed.
- The selected latency metric is full end-to-end: capture, sender, LAN,
  receiver, and playback output. A calibrated measurement method and
  instrumentation still need to be selected; acoustic speaker-to-microphone
  measurements alone include room and transducer delay.

## Phase 2 progress (2026-10-13)

Network discovery and subscriber control now have explicit resource bounds:

- Discovery metadata is validated before it enters the live directory:
  required names and IDs, control port, supported channel range, and bounded
  label/name lengths are checked. The directory is capped at 256 live nodes.
- Subscription requests with receiver port zero are ignored. The control
  listener caps the registry at 1,024 stream IDs and 64 subscribers per stream.
- Subscribers renew their lease when they resubscribe and expire after
  15 seconds without a request. Publishers take a fresh active-subscriber
  snapshot, so abandoned receiver addresses stop receiving audio.

These checks limit accidental or hostile LAN resource growth; they are not
authentication. Discovery and subscription messages can still be spoofed by
devices on the LAN, and UDP audio remains unencrypted. Restrict inbound
OpenAudio ports to trusted private networks using the host firewall. Do not
port-forward discovery, control, audio, HTTP, or WebSocket ports to the public
internet. The browser gateway currently uses plain HTTP/WebSocket, so browser
passwords, session cookies, and media can be observed by a LAN attacker; its
password is only an access-control gate, not transport encryption. Browser
password protection does not change the native-protocol boundary. Hardware,
packet-loss, spoofing, and recovery behavior still need testing before network
reliability or security can be certified.

## Phase 3 progress (2026-10-13)

Browser playback now receives explicit stream interruption and recovery status
from the gateway. After one second without valid audio packets, the gateway
reports the interruption and discards any partial PCM batch. When packets
resume, it reports recovery and the browser clears stale playback-buffer
contents before accepting the new audio. This prevents stale queued audio from
being replayed after a network pause and gives the listener visible status
instead of leaving the player apparently live while silent.

If the selected stream changes channel count or sample rate during playback,
the gateway now reports the new-versus-current format and ends the browser
relay; restart playback to initialize the engine for the new format. Browser
recovery behavior has not yet been validated in supported browsers or under
real packet loss. The web gateway still uses unencrypted HTTP/WebSocket and is
best-effort rather than a v1 security or reliability guarantee.

## Phase 4 decision (2026-10-13)

**Decision: keep uncompressed interleaved little-endian Float32 PCM as the only
supported v1 audio representation.** All current capture, routing, browser,
and receiver paths already use Float32. On the target trusted LAN, this
avoids adding codec delay, CPU load, dependencies, framing ambiguity, and
cross-platform decoder variation before a measured bandwidth or performance
need has been demonstrated.

The cost is material and must be measured: 64 channels × 48,000 frames/s ×
32 bits/sample is **98.304 Mbit/s** of raw audio in each direction per stream,
before packet, UDP/IP, and link-layer overhead. At 64 channels the conservative
packet budget also yields approximately 12,000 datagrams/s. This decision does
not certify that throughput, especially on Wi-Fi; the Phase 1 sustained-load
and network tests remain release gates.

| Wire identifier | Format | V1 status |
| --- | --- | --- |
| `0x03` | Interleaved little-endian Float32 PCM | Implemented in current publishers and supported receivers |
| `0x10` | Opus | Reserved identifier; no encoder/decoder; rejected as unsupported |
| `0x11` | FLAC | Reserved identifier; no encoder/decoder; rejected as unsupported |

Receivers must not reinterpret compressed payloads as PCM. The receiver CLI and
browser gateway report Opus/FLAC as unsupported; other native playback paths
discard non-Float32 packets. A future codec proposal must define packet framing
and timestamps, maximum decode/resource cost, supported channel/sample-rate
combinations, malformed-input handling, and interoperability fixtures; it
must include working encoders/decoders and cross-platform tests before the
format can be listed as supported. Revisit codec support only if measured
target-network constraints justify the added complexity.

## Phase 5 progress (2026-10-13)

The tag-triggered release workflow now builds artifacts on all three
development platforms, packages the Windows x64 desktop app and receiver
together in a portable ZIP, and creates a SHA-256 checksum manifest in a
single publish job. The Windows package includes the README, changelog,
license, and focused installation guide. Release publishing requires
`contents: write` and uses the actual `openaudio-gui` Cargo package.

The Windows distribution is intentionally portable and unsigned; it does not
claim a code-signed installer, Windows Store packaging, ASIO SDK integration,
or hardware certification. Windows ASIO remains an optional custom build that
requires the user's compatible driver and licensed SDK setup.

The legacy `installer_staging` contains stale prebuilt artifacts and is not
used by the release workflow. Its old Inno Setup script attempted to install
and force-remove a kernel audio driver; those side effects have been removed.
The remaining per-user GUI installer template is not built or validated in CI.
Only the current workflow's release artifacts should be distributed.

## Phase 6 progress (2026-10-13)

The v1 scope was revised to include a Windows VST3 mono/stereo sender. The
initial plug-in implementation passes DAW audio through unchanged, offers a
host-visible Publish parameter that defaults off, advertises streams through
the existing discovery protocol, and moves packetization and UDP sending off
the audio callback using bounded preallocated blocks. Its subscription
listener uses an ephemeral port advertised with the stream, avoiding the
desktop app's fixed control-port conflict.

The plug-in bundle now builds locally and is included in the Windows CI build.
It is not yet a release asset: host scan, passthrough, discovery, sustained
playback, queue-overload behavior, and reference DAW compatibility still need
real-host validation.

## Phase sequence

1. **Audio reliability:** Validate receive paths, sample-rate/channel handling,
   bounded buffering, underrun behavior, and clock drift; establish repeatable
   latency and sustained-load measurements.
2. **Network reliability and security:** Define supported network behavior,
   recovery guarantees, service exposure, and the security boundary for trusted
   LAN deployments.
3. **Browser sharing:** Improve and validate browser behavior if it remains a
   desired best-effort feature; it is not a v1 release gate.
4. **Codec/interoperability:** V1 uses Float32 PCM only. Any future codec is
   gated on measured need, a specified wire contract, real encoders/decoders,
   and cross-implementation compatibility tests.
5. **Installation and release:** A reproducible Windows x64 portable package
   and checksums are generated on tags; validate installation, firewall, and
   hardware behavior on reference Windows systems. Document and validate
   macOS/Linux as best-effort.
6. **DAW integration:** Validate the Windows VST3 sender's passthrough, host
   parameter behavior, discovery, and streaming in supported reference DAWs.
   Package it for release only after those checks. Additional plugin formats
   and receive plugins require a separate scope decision.

## Current status

The repository already contains a Windows-first GUI, a receiver CLI, audio
routing paths, browser gateways, and optional ASIO build support. These are
implementation starting points, not proof that the requirements above have
been met. Hardware-in-the-loop latency, long-duration stability, and ASIO
validation require representative devices and a documented test setup.
