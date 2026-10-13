# Windows installation

The supported release package is currently a portable x64 ZIP. It contains the
desktop sender/router, the headless receiver, and this project's usage and
license documents. It does not install a virtual audio driver or modify
Windows driver configuration.

The experimental VST3 sender is not included in this ZIP yet. Build and install
it separately using the instructions in [the plugin guide](../apps/plugin/README.md);
DAW-host compatibility still needs validation.

## Install and launch

1. Download `OpenAudio-windows-x64.zip` from the GitHub release and verify its
   hash against `SHA256SUMS.txt`:

   ```powershell
   Get-FileHash .\OpenAudio-windows-x64.zip -Algorithm SHA256
   ```
2. Extract the ZIP to a user-writable folder, such as
   `%LOCALAPPDATA%\Programs\OpenAudio`. Administrator rights are not required.
3. Run `OpenAudio.exe`. Windows may show a SmartScreen warning because release
   binaries are not currently code-signed; verify the release source and
   checksum before choosing whether to continue.
4. If Windows Firewall prompts, allow OpenAudio only on trusted **Private**
   networks. Do not allow it on Public networks or configure router
   port-forwarding.

To remove the portable installation, close OpenAudio and delete the extracted
folder. The package does not install a driver, service, or startup task.

## Receiver CLI

Open PowerShell in the extracted folder and run:

```powershell
.\receiver.exe --list
.\receiver.exe --publisher <SENDER_IPV4> --stream-id <ID> --inspect --seconds 30
.\receiver.exe --publisher <SENDER_IPV4> --stream-id <ID>
```

The receiver plays only when the stream channel count matches the selected
default output layout. Check `.\receiver.exe --help` for available options.

## Audio and network prerequisites

- Windows 10 or 11 x64.
- WASAPI is available through the standard Windows audio stack.
- ASIO use requires a compatible, already-installed interface driver and an
  OpenAudio build compiled with its optional ASIO feature. The standard release
  ZIP is built without the ASIO SDK and does not bundle third-party drivers.
- OpenAudio discovery and native audio/control UDP are unauthenticated and
  unencrypted. Use a trusted private LAN and allow only the required private
  network traffic in the host firewall.
- Browser gateway traffic uses HTTP/WebSocket without TLS. Its password adds
  access control but does not encrypt credentials, session cookies, or media.
  Do not use it on an untrusted network or expose it to the public internet.

The ZIP is an unsigned portable distribution, not a Windows Store package or
an independently certified installer. Hardware, ASIO, firewall, and
uninstall/reinstall behavior still require validation on supported Windows
configurations.
