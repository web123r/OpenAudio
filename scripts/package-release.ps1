<#
.SYNOPSIS
    Builds and packages the complete standalone OpenAudio release with built-in ASIO support.
.DESCRIPTION
    1. Compiles OpenAudio.exe with --features asio (Release mode).
    2. Bundles OpenAudio.vst3 DAW plugin (Release mode).
    3. Stalls and packages setup-firewall and install-vst3 scripts.
    4. Generates dist/OpenAudio-v0.1.0-windows-x64.zip and SHA256 checksums.
#>

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " OpenAudio Release Packaging Automation                  " -ForegroundColor Cyan
Write-Host " (Built with Native ASIO, WASAPI, FEC & Visual Patchbay)  " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$version = "0.3.1"
$distDir = Join-Path $repoRoot "dist"
$packageDir = Join-Path $distDir "OpenAudio-v$version-windows-x64"
$zipFile = Join-Path $distDir "OpenAudio-v$version-windows-x64.zip"

Write-Host "[1/5] Building OpenAudio desktop app with ASIO enabled..." -ForegroundColor White
$buildApp = Start-Process cargo -ArgumentList "build", "--release", "--features", "asio", "--bin", "OpenAudio" -NoNewWindow -PassThru -Wait
if ($buildApp.ExitCode -ne 0) {
    Write-Host "Failed to build OpenAudio.exe with ASIO support." -ForegroundColor Red
    exit 1
}

Write-Host "Building OpenAudio headless CLI (receiver & publisher)..." -ForegroundColor White
$buildReceiver = Start-Process cargo -ArgumentList "build", "--release", "--bin", "receiver" -NoNewWindow -PassThru -Wait
if ($buildReceiver.ExitCode -ne 0) {
    Write-Host "Failed to build receiver.exe." -ForegroundColor Red
    exit 1
}

Write-Host "[2/5] Bundling OpenAudio.vst3 DAW plugin..." -ForegroundColor White
$buildPlugin = Start-Process cargo -ArgumentList "run", "--release", "-p", "openaudio-plugin-xtask", "--", "bundle", "openaudio-plugin", "--release" -NoNewWindow -PassThru -Wait
if ($buildPlugin.ExitCode -ne 0) {
    Write-Host "Failed to bundle OpenAudio.vst3 plugin." -ForegroundColor Red
    exit 1
}

Write-Host "[3/5] Assembling release directory structure..." -ForegroundColor White
if (Test-Path $packageDir) {
    Remove-Item -Path $packageDir -Recurse -Force
}
New-Item -ItemType Directory -Path $packageDir -Force | Out-Null

# Copy main binary
Copy-Item (Join-Path $repoRoot "target\release\OpenAudio.exe") $packageDir -Force
Write-Host " - Copied OpenAudio.exe (ASIO enabled)" -ForegroundColor Gray

# Copy headless CLI
Copy-Item (Join-Path $repoRoot "target\release\receiver.exe") $packageDir -Force
Write-Host " - Copied receiver.exe (Headless CLI Publisher & Receiver)" -ForegroundColor Gray

# Copy VST3 bundle
Copy-Item (Join-Path $repoRoot "target\bundled\OpenAudio.vst3") (Join-Path $packageDir "OpenAudio.vst3") -Recurse -Force
Write-Host " - Copied OpenAudio.vst3" -ForegroundColor Gray

# Copy helpers
Copy-Item (Join-Path $repoRoot "scripts\setup-firewall.bat") $packageDir -Force
Copy-Item (Join-Path $repoRoot "scripts\setup-firewall.ps1") $packageDir -Force
Copy-Item (Join-Path $repoRoot "scripts\install-vst3.bat") $packageDir -Force
Copy-Item (Join-Path $repoRoot "scripts\install-vst3.ps1") $packageDir -Force

# Generate Quickstart documentation
$quickstartContent = @"
# OpenAudio v$version (Windows x64)

Self-hosted, ultra-low latency Audio-over-IP for studios, DAWs, and live sound.

## Features Included in This Build
- **Native ASIO & WASAPI Engines:** Direct hardware input/output and loopback.
- **Pure-Rust Opus Compression:** 70x bandwidth reduction for multi-track streams (36–64 channels).
- **Lossless FLAC & WAV Archival Recording:** Direct per-stream recording on both publishers and subscribers.
- **Visual Audio Patchbay Matrix:** Real-time 2D crossbar routing between network streams and output hardware.
- **Live Latency & Network Quality Indicators:** Real-time RTT, 1-way network delay, jitter (RFC 3550), packet loss %, and buffer depth.
- **Autonomous Stream Reconnection:** Automatic discovery-based reconnect when publishers restart.
- **Headless CLI Tools (`receiver.exe`):** Headless publish and subscribe modes for audio servers and nodes.
- **RFC 5109 FEC & Packet Loss Concealment (PLC):** Bit-exact recovery and waveform smoothing over Wi-Fi/LAN.
- **Dynamic Jitter Buffer Controls:** Instant toggles for LAN (5ms), Wi-Fi (20ms), and Safe (50ms).
- **DAW VST3 Sender Plugin:** Low-latency track publishing straight from REAPER, Ableton, Cubase, etc.
- **Web Browser Gateway:** Live stream listening on phones, tablets, and remote PCs without installing software.

---

## Getting Started

### 1. Configure Windows Firewall (Recommended)
Double-click ``setup-firewall.bat`` to authorize UDP audio streaming and multicast discovery.
*(This prevents Windows from silently dropping audio packets on new network connections).*

### 2. Install DAW VST3 Plugin
Double-click ``install-vst3.bat`` to deploy the VST3 bundle to your DAW's plug-in directory.
In your DAW (REAPER, Ableton Live, Cubase, Studio One), rescan your VST3 plugins and look for **"OpenAudio DAW Sender"**.

### 3. Launch the OpenAudio Application
Double-click ``OpenAudio.exe`` to start the desktop routing interface.
- Visit the **Patchbay** tab for 1-click crossbar routing.
- Visit the **Subscribe** tab to adjust per-stream volume or configure jitter buffers.
- Visit the **Publish** tab to broadcast any microphone, interface, or loopback audio.
- Visit the **Browser** tab to activate the zero-install web player.
"@

Set-Content -Path (Join-Path $packageDir "QUICKSTART.md") -Value $quickstartContent -Encoding UTF8
Write-Host " - Generated QUICKSTART.md" -ForegroundColor Gray

Write-Host "[4/5] Creating standalone zip archive..." -ForegroundColor White
if (Test-Path $zipFile) {
    Remove-Item -Path $zipFile -Force
}
Compress-Archive -Path "$packageDir\*" -DestinationPath $zipFile -CompressionLevel Optimal
Write-Host " - Generated $zipFile" -ForegroundColor Gray

Write-Host "[5/5] Generating SHA256 checksums..." -ForegroundColor White
$checksumsFile = Join-Path $distDir "SHA256SUMS.txt"
$hashes = @()
$zipHash = Get-FileHash -Algorithm SHA256 -Path $zipFile
$hashes += "$($zipHash.Hash)  $([System.IO.Path]::GetFileName($zipFile))"

$exeHash = Get-FileHash -Algorithm SHA256 -Path (Join-Path $packageDir "OpenAudio.exe")
$hashes += "$($exeHash.Hash)  OpenAudio.exe"

$hashes | Set-Content -Path $checksumsFile -Encoding UTF8
Write-Host " - Generated SHA256SUMS.txt" -ForegroundColor Gray

$zipSizeMb = [math]::Round((Get-Item $zipFile).Length / 1MB, 2)
Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Package Successfully Built!                              " -ForegroundColor Green
Write-Host " Output: $zipFile ($zipSizeMb MB)" -ForegroundColor Cyan
Write-Host " SHA256: $($zipHash.Hash)" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Green
