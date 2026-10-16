<#
.SYNOPSIS
    Configures Windows Firewall rules for OpenAudio and DAW VST3 audio streaming.
.DESCRIPTION
    Creates inbound and outbound firewall rules for:
    - OpenAudio.exe application traffic (ASIO, WASAPI, WebRTC Gateway)
    - UDP Multicast Discovery (239.19.84.1:5450)
    - UDP Audio Transport & Control (ports 5000-7500)
    - Web Browser Gateway (HTTP/WebSocket port 8080)
#>

param(
    [switch]$Remove
)

$ErrorActionPreference = "Stop"

function Test-Admin {
    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]$currentIdentity
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Host "Elevating privileges to configure Windows Firewall..." -ForegroundColor Yellow
    $argsList = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    if ($Remove) { $argsList += " -Remove" }
    Start-Process powershell -Verb RunAs -ArgumentList $argsList
    exit 0
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " OpenAudio Windows Firewall Configuration Utility        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $PSCommandPath
$appExe = Join-Path $scriptDir "OpenAudio.exe"

$rulePrefix = "OpenAudio"

if ($Remove) {
    Write-Host "Removing existing OpenAudio firewall rules..." -ForegroundColor Yellow
    Get-NetFirewallRule -Name "$rulePrefix*" -ErrorAction SilentlyContinue | Remove-NetFirewallRule
    Write-Host "All OpenAudio firewall rules removed successfully." -ForegroundColor Green
    exit 0
}

# Remove any old rules first to prevent duplicate entries
Get-NetFirewallRule -Name "$rulePrefix*" -ErrorAction SilentlyContinue | Remove-NetFirewallRule

Write-Host "Adding Firewall Rules..." -ForegroundColor White

try {
    # 1. Program Rule for OpenAudio.exe (if present in same folder)
    if (Test-Path $appExe) {
        New-NetFirewallRule -Name "${rulePrefix}-App-In" `
            -DisplayName "OpenAudio Desktop Application (Inbound)" `
            -Description "Allows inbound UDP and TCP traffic for OpenAudio audio engine." `
            -Program $appExe `
            -Direction Inbound `
            -Action Allow `
            -Profile Private,Domain `
            -Enabled True | Out-Null

        New-NetFirewallRule -Name "${rulePrefix}-App-Out" `
            -DisplayName "OpenAudio Desktop Application (Outbound)" `
            -Description "Allows outbound UDP and TCP traffic for OpenAudio audio engine." `
            -Program $appExe `
            -Direction Outbound `
            -Action Allow `
            -Profile Private,Domain `
            -Enabled True | Out-Null

        Write-Host " [OK] Application rules configured for: $appExe" -ForegroundColor Green
    }

    # 2. Multicast Discovery (UDP 5450)
    New-NetFirewallRule -Name "${rulePrefix}-Discovery-In" `
        -DisplayName "OpenAudio Multicast Discovery (Inbound UDP 5450)" `
        -Description "Allows OpenAudio network discovery advertisements and queries." `
        -Protocol UDP `
        -LocalPort 5450 `
        -Direction Inbound `
        -Action Allow `
        -Profile Private,Domain `
        -Enabled True | Out-Null

    New-NetFirewallRule -Name "${rulePrefix}-Discovery-Out" `
        -DisplayName "OpenAudio Multicast Discovery (Outbound UDP 5450)" `
        -Description "Allows OpenAudio network discovery advertisements and queries." `
        -Protocol UDP `
        -LocalPort 5450 `
        -Direction Outbound `
        -Action Allow `
        -Profile Private,Domain `
        -Enabled True | Out-Null
    Write-Host " [OK] Discovery multicast rules (UDP 5450) configured." -ForegroundColor Green

    # 3. Audio Transport & Control (UDP 5000-7500 for standalone app, DAWs, and VST3)
    New-NetFirewallRule -Name "${rulePrefix}-Transport-In" `
        -DisplayName "OpenAudio Transport & Control (Inbound UDP 5000-7500)" `
        -Description "Allows low-latency Float32 PCM audio packets and subscription control for DAWs and OpenAudio." `
        -Protocol UDP `
        -LocalPort 5000-7500 `
        -Direction Inbound `
        -Action Allow `
        -Profile Private,Domain `
        -Enabled True | Out-Null

    New-NetFirewallRule -Name "${rulePrefix}-Transport-Out" `
        -DisplayName "OpenAudio Transport & Control (Outbound UDP 5000-7500)" `
        -Description "Allows low-latency Float32 PCM audio packets and subscription control for DAWs and OpenAudio." `
        -Protocol UDP `
        -LocalPort 5000-7500 `
        -Direction Outbound `
        -Action Allow `
        -Profile Private,Domain `
        -Enabled True | Out-Null
    Write-Host " [OK] Audio transport rules (UDP 5000-7500) configured." -ForegroundColor Green

    # 4. Web Browser Gateway (TCP 8080)
    New-NetFirewallRule -Name "${rulePrefix}-WebGateway-In" `
        -DisplayName "OpenAudio Web Browser Gateway (Inbound TCP 8080)" `
        -Description "Allows web browser player connections for OpenAudio HTTP/WebSocket relay." `
        -Protocol TCP `
        -LocalPort 8080 `
        -Direction Inbound `
        -Action Allow `
        -Profile Private,Domain `
        -Enabled True | Out-Null
    Write-Host " [OK] Browser Gateway rule (TCP 8080) configured." -ForegroundColor Green

    Write-Host ""
    Write-Host "Windows Firewall has been successfully configured for OpenAudio!" -ForegroundColor Green
    Write-Host "Your network audio streams, VST3 DAW plugins, and browser gateway are now fully authorized." -ForegroundColor Cyan
} catch {
    Write-Host "Error configuring firewall rules: $_" -ForegroundColor Red
    exit 1
}
