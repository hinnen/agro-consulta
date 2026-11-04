@echo off
chcp 65001 >nul
REM Um fluxo so: login CF (se precisar) + criar tunel + copiar CNAME + patch .env
if /I not "%~1"=="--run" (
  start "Dominio fixo Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo ==================================================
echo  7 - Dominio fixo zapsistvale.online ^(1 fluxo^)
echo  Voce so: autorizar Cloudflare no navegador ^(1x^)
echo           e colar CNAME no Hostinger ^(1x^)
echo ==================================================
echo.

where git >nul 2>&1
if not errorlevel 1 (
  set "REPO=%~dp0..\.."
  cd /d "%REPO%"
  git fetch origin whatsapp 2>nul
  git pull origin whatsapp 2>nul
  cd /d "%~dp0"
)

where cloudflared >nul 2>&1
if errorlevel 1 (
  echo Instalando cloudflared...
  winget install -e --id Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements --disable-interactivity
)

if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo.
  echo >>> Abrindo login Cloudflare - clique Authorize <<<
  cloudflared tunnel login
)
if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo ERRO: login Cloudflare nao concluido.
  goto fim
)

call "%~dp0tunel_fixo\tunel_fixo_criar_zapsistvale.bat"
if errorlevel 1 goto fim

for /f "delims=" %%C in ('powershell -NoProfile -Command "(Get-Content '%USERPROFILE%\.cloudflared\config-agro-wa.yml' -Raw) -match 'tunnel: (\S+)' | Out-Null; $matches[1]"') do set "UUID=%%C"
if defined UUID (
  set "CNAME=!UUID!.cfargotunnel.com"
  echo !CNAME!| clip
  echo.
  echo CNAME copiado ^(Ctrl+V no Hostinger^): !CNAME!
)

echo.
echo Hostinger: apague A @ 2.57.91.91 ^| CNAME nome zap ^| cole o CNAME
echo Depois: 3 - Subir Zap - todo dia.bat
echo.

:fim
pause
