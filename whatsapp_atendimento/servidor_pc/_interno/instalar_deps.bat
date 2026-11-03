@echo off
chcp 65001 >nul
setlocal

where winget >nul 2>&1
if errorlevel 1 (
  echo ERRO: Windows 10/11 com winget necessario ^(App Installer na Microsoft Store^).
  exit /b 1
)

where py >nul 2>&1
if errorlevel 1 (
  echo Instalando Python 3.12...
  winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements --disable-interactivity
)

where node >nul 2>&1
if errorlevel 1 (
  echo Instalando Node.js LTS...
  winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements --disable-interactivity
)

where cloudflared >nul 2>&1
if errorlevel 1 (
  echo Instalando cloudflared ^(tunel teste^)...
  winget install -e --id Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements --disable-interactivity
)

exit /b 0
