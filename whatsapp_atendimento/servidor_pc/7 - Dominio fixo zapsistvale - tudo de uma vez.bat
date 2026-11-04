@echo off
chcp 65001 >nul
REM Login Cloudflare (1x) + criar tunel zapsistvale. Git: use 0 ou 3 antes.
if /I not "%~1"=="--run" (
  start "Dominio fixo Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo ==================================================
echo  7 - Dominio fixo zapsistvale.online
echo ==================================================
echo.

where cloudflared >nul 2>&1
if errorlevel 1 (
  echo Instalando cloudflared...
  winget install -e --id Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements --disable-interactivity
  if exist "%ProgramFiles(x86)%\cloudflared\cloudflared.exe" (
    set "PATH=%ProgramFiles(x86)%\cloudflared;%PATH%"
  )
)

if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo.
  echo Abrindo login Cloudflare - clique Authorize no navegador
  echo.
  cloudflared tunnel login
)

if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo ERRO: login Cloudflare nao concluido.
  goto fim
)

call "%~dp0tunel_fixo\tunel_fixo_criar_zapsistvale.bat"

:fim
pause
