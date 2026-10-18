@echo off
title OpenAudio Firewall Setup
cd /d "%~dp0"
echo Requesting administrator privileges to configure Windows Firewall...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-firewall.ps1"
echo.
pause
