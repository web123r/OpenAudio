# OpenAudio validation checklist

This checklist separates repeatable software checks from validation that requires
specific hardware, a second host, a DAW, or a managed network. A passing unit
test does not satisfy a hardware or release gate.

## Software and protocol

- [x] `cargo check --workspace`
- [x] `cargo test --workspace`
- [x] Browser playback regression tests
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Fuzz malformed packet parsing and encrypted-packet handling
- [ ] Property-test packet encode/decode across channel counts, rates, formats,
      truncation, and sequence rollover
- [ ] Verify Opus mono, stereo, multichannel, duplicate, missing-substream,
      malformed, and quiet-tail cases
- [ ] Verify Float32 and Opus format negotiation with legacy subscribers
- [ ] Verify encrypted and unencrypted streams cannot be mixed accidentally
- [ ] Verify clean shutdown of publishers, receivers, browser gateways, and
      plugin workers

## Audio real-time behavior

- [ ] Confirm no allocation, blocking mutex acquisition, codec work, logging,
      or network I/O occurs in audio callbacks
- [ ] Measure callback duration and worst-case callback duration
- [ ] Measure queue depth, underruns, drops, and recovery time
- [ ] Confirm queue bounds remain within the configured latency budget
- [ ] Verify channel ordering and interleaving with deterministic multichannel
      signals
- [ ] Verify sample-rate conversion at 44.1, 48, 96, and 192 kHz where supported
- [ ] Verify device removal, device restart, and output-device changes

## Windows release-critical validation

- [ ] WASAPI capture and playback on representative hardware
- [ ] ASIO capture and playback with a representative driver
- [ ] 48 kHz, 64-channel sustained publish/receive test
- [ ] Wired two-host end-to-end latency measurement below 10 ms
- [ ] Define latency instrumentation, direction, statistic, and acceptance rule
- [ ] Verify discovery, subscription, reconnect, and stream restart
- [ ] Verify Windows firewall setup and removal behavior
- [ ] Install and run the portable ZIP on a clean Windows machine
- [ ] Verify version metadata, included binaries, documentation, and checksums

## Network and reliability

- [ ] Two-host wired-LAN soak test with packet, CPU, queue, and underrun metrics
- [ ] Managed-Wi-Fi loss, reorder, burst-loss, and recovery test
- [ ] Multi-hour operation without unbounded memory or queue growth
- [ ] Multiple simultaneous publishers and subscribers
- [ ] Multiple streams with mixed Float32 and Opus subscribers
- [ ] Verify stale subscriber leases expire and do not receive packets
- [ ] Verify malformed discovery/control traffic is rejected and bounded
- [ ] Verify behavior when multicast discovery is unavailable

## Opus interoperability

- [ ] End-to-end Opus test for every native receiver path
- [ ] Opus test through ASIO capture and playback
- [ ] Opus test in the VST3 sender and a DAW host
- [ ] Browser microphone-to-native receiver test
- [ ] Browser gateway playback from native Opus publishers
- [ ] Measure codec CPU cost, latency, packet rate, and concealment behavior
- [ ] Document supported channel counts, rates, frame duration, and bitrate

## Security

- [ ] Confirm encrypted media authenticates headers and rejects tampering
- [ ] Review nonce uniqueness across stream restarts and sequence rollover
- [ ] Confirm incorrect passphrases fail clearly without downgrade
- [ ] Verify native discovery/control authentication expectations
- [ ] Verify browser HTTP/WebSocket exposure is clearly restricted to trusted
      networks or protected by TLS reverse proxy
- [ ] Fuzz unauthenticated discovery, control, and audio inputs
- [ ] Document the trusted-LAN threat model and supported deployment boundary

## VST3 and DAW

- [ ] Scan the plugin in REAPER
- [ ] Confirm audio pass-through when publishing is disabled
- [ ] Confirm mono and stereo publishing when enabled
- [ ] Confirm plugin processing performs no network I/O or blocking
- [ ] Verify plugin reconnect and shutdown
- [ ] Verify separate receiver playback on the LAN
- [ ] Verify overload behavior and bounded callback queues
- [ ] Decide whether the plugin is ready for the portable release package

## Release decision

- [ ] Publish a Windows support matrix
- [ ] Record hardware, driver, OS, network, commit, and test duration for each
      validation run
- [ ] Resolve all release-critical failures
- [ ] Update README, release scope, and changelog to match measured behavior
- [ ] Build release artifacts from a clean checkout
- [ ] Re-run the smoke tests against the exact release artifacts
