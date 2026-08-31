@echo off
cd /d "%~dp0"
if not defined AGRO_WA_DJANGO_URL set AGRO_WA_DJANGO_URL=http://127.0.0.1:8000
if not defined AGRO_WA_BRIDGE_TOKEN set AGRO_WA_BRIDGE_TOKEN=gm-agro-wa-ponte-local
echo Ponte WhatsApp Agro
echo Site: %AGRO_WA_DJANGO_URL%
echo Deixe esta janela aberta. QR aparece no Agro em /atendimento-whatsapp/
where node >nul 2>&1
if errorlevel 1 (
  echo Instale o Node.js em https://nodejs.org  (versao LTS)
  pause
  exit /b 1
)
if not exist node_modules call npm install
node index.js
pause
