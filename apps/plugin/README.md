# OpenAudio VST3 sender

The first DAW integration is a VST3 effect that passes its input through
unchanged and can publish the track as an OpenAudio stream. The plug-in
declares mono, stereo, and common multichannel layouts up to 64 channels;
host support for those layouts must be validated independently.

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

### Studio One

1. Copy `target\bundled\OpenAudio.vst3` to a VST3 folder that Studio One
   scans. The per-user folder
   `%LOCALAPPDATA%\Programs\Common\VST3` is recommended; the system-wide
   `%CommonProgramFiles%\VST3` folder is also commonly available.
2. Open **Studio One > View > Plug-in Manager**, choose **Reset Blocklist** if
   OpenAudio was previously rejected, and click **Update Plug-ins**.
3. Search for **OpenAudio DAW Sender** under the vendor **OpenAudio** and add
   it to a mono or stereo audio track. The plug-in passes the track through
   locally, so the track must remain audible before publishing is enabled.
4. Open the plug-in editor, enable **Publish to OpenAudio**, and use
   `receiver.exe --list` on the receiving machine to verify the advertised
   `DAW Track N` stream. Subscribe to it with the receiver and confirm clean
   audio and correct channel order.
5. Test Studio One at 44.1 kHz and 48 kHz, with different block sizes, and
   toggle publishing while playback is running. Confirm that stopping and
   restarting the transport does not replay stale audio.

If Studio One does not list the plug-in, close Studio One, confirm that the
copied folder is exactly named `OpenAudio.vst3`, then rescan from Plug-in
Manager. Do not copy only the files inside the bundle.

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

The Studio One procedure above is the equivalent host smoke test for Studio
One. Host certification still requires running it with the intended Windows
audio driver, sample rate, block sizes, and network topology.

## Current boundaries

- Mono and stereo only; it does not expose 64-channel VST3 ports.
- VST3 only; no CLAP, AudioUnit, or DAW receive plug-in.
- Track names are generated automatically, and there is no custom editor or
  endpoint configuration.
- The host's sample rate must be one supported by the OpenAudio wire protocol.
