@echo off
REM Abre servidor Django + ponte local (duas janelas). Tunel: rode tunel_rapido.bat a parte.
cd /d "%~dp0"
start "WA Servidor Django" cmd /k "%~dp0subir_servidor.bat"
timeout /t 8 /nobreak >nul
start "WA Ponte WhatsApp" cmd /k "%~dp0..\iniciar-local.bat"
echo Duas janelas abertas. QR em http://127.0.0.1:8000/atendimento-whatsapp/
