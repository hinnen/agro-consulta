@echo off
chcp 65001 >nul
REM Tunel gratis para TESTE (URL muda se reiniciar). Producao: Cloudflare Tunnel fixo — ver docs.
cd /d "%~dp0"

where cloudflared >nul 2>&1
if errorlevel 1 (
  echo Instalando cloudflared via winget...
  winget install -e --id Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements
)

echo.
echo Apontando para http://127.0.0.1:8000
echo Copie a URL https://....trycloudflare.com que aparecer abaixo.
echo Cole em criar_env_servidor.bat ^(host^) ou ALLOWED_HOSTS_EXTRA no .env.wa-servidor
echo Abra o Zap nessa URL; ponte .env: AGRO_WA_DJANGO_URL= mesma URL
echo.
cloudflared tunnel --url http://127.0.0.1:8000
pause
