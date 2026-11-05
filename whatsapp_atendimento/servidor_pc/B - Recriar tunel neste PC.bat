@echo off
chcp 65001 >nul
REM Sem o .json do PC antigo: apaga o tunel na Cloudflare e cria outro neste PC.
if /I not "%~1"=="--run" (
  start "Recriar tunel Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal EnableDelayedExpansion
cd /d "%~dp0"

where cloudflared >nul 2>&1
if errorlevel 1 (
  echo ERRO: cloudflared nao encontrado.
  goto fim
)

if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo Login Cloudflare neste PC ^(navegador^)...
  call "%~dp0tunel_fixo\tunel_fixo_login.bat"
)
if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo ERRO: login nao concluido.
  goto fim
)

echo.
echo Apagando tunel antigo agro-wa-zapsistvale ^(se existir^)...
cloudflared tunnel delete -f agro-wa-zapsistvale
echo.
echo Criando tunel novo neste PC...
cloudflared tunnel create agro-wa-zapsistvale
if errorlevel 1 (
  echo ERRO ao criar tunel. Leia a mensagem acima.
  goto fim
)

set "UUID="
for /f "usebackq tokens=*" %%L in (`cloudflared tunnel list 2^>nul`) do (
  echo %%L | findstr /i "agro-wa-zapsistvale" >nul && (
    for /f "tokens=1" %%U in ("%%L") do set "UUID=%%U"
  )
)
if "!UUID!"=="" (
  echo ERRO: nao achei o ID do tunel novo.
  goto fim
)
if not exist "%USERPROFILE%\.cloudflared\!UUID!.json" (
  echo ERRO: credencial nao foi gravada: %USERPROFILE%\.cloudflared\!UUID!.json
  goto fim
)

set "HOST=zap.zapsistvale.online"
set "CFG=%USERPROFILE%\.cloudflared\config-agro-wa.yml"
(
echo tunnel: !UUID!
echo credentials-file: %USERPROFILE%\.cloudflared\!UUID!.json
echo ingress:
echo   - hostname: %HOST%
echo     service: http://127.0.0.1:8000
echo   - service: http_status:404
) > "%CFG%"

echo.
echo Atualizando DNS zap -^> tunel novo...
cloudflared tunnel route dns agro-wa-zapsistvale %HOST%
if errorlevel 1 echo AVISO: route dns falhou. Me avise o texto acima.

echo.
echo PRONTO. Tunel novo: !UUID!
echo Config: %CFG%
echo Agora rode: 3 - Subir Zap - todo dia.bat
echo.

:fim
pause
