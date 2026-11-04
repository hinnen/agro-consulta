@echo off
chcp 65001 >nul
echo Reinicia so a ponte WhatsApp ^(nao mexe servidor nem tunel^).
start "Zap - ponte" cmd /k "%~dp0..\iniciar-local.bat"
echo Janela Zap - ponte aberta. Espere 10s e de F5 no Chrome.
pause
