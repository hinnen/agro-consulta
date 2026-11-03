@echo off
chcp 65001 >nul
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_interno\tunel_teste.ps1"
if errorlevel 1 pause
