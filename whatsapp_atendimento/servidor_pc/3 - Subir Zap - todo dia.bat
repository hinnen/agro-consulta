@echo off
cd /d "%~dp0"
echo Abrindo servidor Django + ponte WhatsApp (duas janelas)...
start "Zap — servidor" cmd /k "%~dp0_interno\subir_django.bat"
timeout /t 8 /nobreak >nul
start "Zap — ponte" cmd /k "%~dp0..\iniciar-local.bat"
echo.
echo Local: http://127.0.0.1:8000/atendimento-whatsapp/
echo Outra loja/celular: 4 - Tunel teste - internet temporaria.bat
pause
