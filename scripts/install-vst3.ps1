<#
.SYNOPSIS
    Installs the OpenAudio VST3 plugin bundle to Windows VST3 directories.
.DESCRIPTION
    Deploys OpenAudio.vst3 to:
    1. User VST3 folder: %LOCALAPPDATA%\Programs\Common\VST3\
    2. System VST3 folder: %CommonProgramFiles%\VST3\ (if run as Administrator)
#>

$ErrorActionPreference = "Continue"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " OpenAudio VST3 Plugin Installer                         " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $PSCommandPath
$vst3Source = Join-Path $scriptDir "OpenAudio.vst3"

if (-not (Test-Path $vst3Source)) {
    # Check if we're in repo root or target/bundled
    $altSource = Join-Path $scriptDir "..\target\bundled\OpenAudio.vst3"
    if (Test-Path $altSource) {
        $vst3Source = (Resolve-Path $altSource).Path
    } else {
        Write-Host "Error: Could not locate OpenAudio.vst3 bundle in $scriptDir" -ForegroundColor Red
        exit 1
    }
}

Write-Host "Found plugin bundle: $vst3Source" -ForegroundColor Gray

# Check for running DAWs
$daws = @("reaper", "reaper_x64", "Ableton Live", "Cubase", "Studio One", "FL64", "Bitwig Studio")
$runningDaws = Get-Process -ErrorAction SilentlyContinue | Where-Object { $daws -contains $_.ProcessName }
if ($runningDaws) {
    Write-Host "Warning: The following DAW(s) appear to be running:" -ForegroundColor Yellow
    foreach ($daw in $runningDaws) {
        Write-Host " - $($daw.ProcessName) (PID: $($daw.Id))" -ForegroundColor Yellow
    }
    Write-Host "Please close or restart your DAW after installation so it can scan the new plugin." -ForegroundColor Yellow
    Write-Host ""
}

$installedCount = 0

# 1. User VST3 folder (Standard Steinberg path for non-elevated user installs)
$userVst3Dir = Join-Path $env:LOCALAPPDATA "Programs\Common\VST3"
try {
    if (-not (Test-Path $userVst3Dir)) {
        New-Item -ItemType Directory -Path $userVst3Dir -Force | Out-Null
    }
    $userDest = Join-Path $userVst3Dir "OpenAudio.vst3"
    Copy-Item -Path $vst3Source -Destination $userDest -Recurse -Force
    Write-Host "[OK] Installed to user VST3 directory:" -ForegroundColor Green
    Write-Host "     $userDest" -ForegroundColor White
    $installedCount++
} catch {
    Write-Host "[WARN] Failed installing to user directory: $_" -ForegroundColor Yellow
}

# 2. System VST3 folder (Standard system-wide Steinberg path, requires Admin)
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($isAdmin) {
    $systemVst3Dir = Join-Path $env:CommonProgramFiles "VST3"
    try {
        if (-not (Test-Path $systemVst3Dir)) {
            New-Item -ItemType Directory -Path $systemVst3Dir -Force | Out-Null
        }
        $systemDest = Join-Path $systemVst3Dir "OpenAudio.vst3"
        Copy-Item -Path $vst3Source -Destination $systemDest -Recurse -Force
        Write-Host "[OK] Installed to system VST3 directory:" -ForegroundColor Green
        Write-Host "     $systemDest" -ForegroundColor White
        $installedCount++
    } catch {
        Write-Host "[WARN] Failed copying to system VST3 directory: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "[INFO] User-level installation completed. (System-wide folder requires Administrator elevation)." -ForegroundColor Gray
}

Write-Host ""
if ($installedCount -gt 0) {
    Write-Host "Installation Complete! OpenAudio VST3 is ready for your DAW." -ForegroundColor Green
    Write-Host "In REAPER, Ableton, or Cubase, re-scan your VST3 plug-ins or insert 'OpenAudio DAW Sender'." -ForegroundColor Cyan
} else {
    Write-Host "Installation failed. Please verify write permissions." -ForegroundColor Red
}
