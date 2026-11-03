@echo off
if /I not "%~1"=="--run" (
  start "Subir Zap" cmd /k "%~f0" --run
  exit /b 0
)
cd /d "%~dp0"
echo Abrindo servidor Django + ponte WhatsApp (duas janelas)...
start "Zap - servidor" cmd /k "%~dp0_interno\subir_django.bat"
timeout /t 5 /nobreak >nul
start "Zap - ponte" cmd /k "%~dp0..\iniciar-local.bat"
timeout /t 3 /nobreak >nul
start "Zap - internet tunel PDV" cmd /k "%~dp0\4 - Tunel teste - internet temporaria.bat"
echo.
echo Local: http://127.0.0.1:8000/atendimento-whatsapp/
echo Tunel: aguarda o servidor ficar pronto (nao feche as janelas).
pause
