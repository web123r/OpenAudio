@echo off
title OpenAudio VST3 Installer
cd /d "%~dp0"
echo Installing OpenAudio VST3 plugin...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install-vst3.ps1"
echo.
pause
