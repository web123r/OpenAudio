# OpenAudio VST3 sender

The first DAW integration is a mono/stereo VST3 effect that passes its input
through unchanged and can publish the track as an OpenAudio stream.

## Behavior

- Insert the plug-in on a mono or stereo audio track.
- The host-visible **Publish to OpenAudio** parameter defaults to off. Enable
  it to send audio; turning it off discards queued audio without changing the
  track's local audio. A block already being sent by the worker may finish.
- The stream is advertised as `DAW Track N` using OpenAudio discovery. Native
  receivers subscribe over the existing control protocol; audio is sent only
  to active subscribers.
- Audio processing uses a bounded, preallocated block pool. Discovery,
  packetization, and UDP sending run on worker threads, not the DAW audio
  callback. If the pool is exhausted, the oldest queued block is recycled for
  the newest audio to bound latency; dropped frames are reported by the worker
  rather than blocking the host.
- One ephemeral UDP control port is shared between active OpenAudio VST3
  instances in the DAW process and advertised with each stream. This avoids a
  port conflict with the desktop application's fixed control listener.

Native discovery, subscription control, and audio are unauthenticated and
unencrypted. Use this plug-in only on a trusted private LAN.

## Build status

Build the Windows VST3 bundle with:

```powershell
cargo xtask bundle openaudio-plugin --release
```

The bundle is created at `target\bundled\OpenAudio.vst3`. Copy that entire
folder to `%LOCALAPPDATA%\Programs\Common\VST3` or another VST3 folder scanned
by the DAW, then rescan plug-ins. The target folder is host-specific; verify
which VST3 locations your DAW scans.

The plug-in depends on the ISC-licensed
[nice-plug framework](https://codeberg.org/RustAudio/nice-plug), pinned to a
specific revision. A successful bundle build is not evidence of DAW-host
compatibility. Bundle installation, host scanning, transport behavior,
sample-rate changes, and sustained playback remain validation tasks before
release.

## REAPER host smoke test

1. Copy `target\bundled\OpenAudio.vst3` into a VST3 folder scanned by REAPER,
   then rescan VST3 plug-ins in REAPER's preferences.
2. Insert **OpenAudio VST3 sender** on a mono or stereo audio track. Confirm
   the track remains audible with **Publish to OpenAudio** off and after
   enabling it.
3. Use `receiver.exe --list` to confirm the `DAW Track N` stream is advertised.
   Subscribe to the listed publisher and stream ID with the receiver, then
   confirm the received audio is clean and has the expected channel order.
4. Repeat with mono and stereo tracks at 44.1 kHz and 48 kHz. Toggle publishing
   off and on, then stop and restart playback to check that stale audio is not
   replayed and discovery recovers.
5. Leave playback running for several minutes and check for distortion,
   excessive latency, or dropped-frame reports. Repeat with REAPER and the
   receiver on separate machines on a trusted private LAN.

## Current boundaries

- Mono and stereo only; it does not expose 64-channel VST3 ports.
- VST3 only; no CLAP, AudioUnit, or DAW receive plug-in.
- Track names are generated automatically, and there is no custom editor or
  endpoint configuration.
- The host's sample rate must be one supported by the OpenAudio wire protocol.
