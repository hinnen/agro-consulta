@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Atualizando PDV para https://zap.zapsistvale.online ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_interno\registrar_origin_pdv.ps1" -OriginUrl "https://zap.zapsistvale.online"
pause
