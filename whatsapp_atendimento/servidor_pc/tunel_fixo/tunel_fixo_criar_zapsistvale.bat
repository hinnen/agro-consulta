@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
REM Tunel Cloudflare FIXO + DNS manual no Hostinger (zapsistvale.online).
where cloudflared >nul 2>&1
if errorlevel 1 (
  echo ERRO: cloudflared nao encontrado. Rode 3 ou instale Cloudflare.cloudflared.
  pause
  exit /b 1
)
if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo ERRO: rode primeiro tunel_fixo_login.bat
  pause
  exit /b 1
)

set "TNAME=agro-wa-zapsistvale"
set "HOST=zap.zapsistvale.online"
echo.
echo Dominio Hostinger: zapsistvale.online
echo Subdominio recomendado ^(CNAME^): [%HOST%]
set /p HOST_IN=Host publico HTTPS [%HOST%]: 
if not "%HOST_IN%"=="" set "HOST=%HOST_IN%"

echo.
echo Criando tunel %TNAME% ...
cloudflared tunnel create %TNAME% 2>nul
if errorlevel 1 echo ^(tunel pode ja existir - seguindo^)

set "UUID="
for /f "usebackq tokens=*" %%L in (`cloudflared tunnel list 2^>nul`) do (
  echo %%L | findstr /i "%TNAME%" >nul && (
    for /f "tokens=1" %%U in ("%%L") do set "UUID=%%U"
  )
)
if "!UUID!"=="" (
  echo.
  echo Nao achei UUID. Rode: cloudflared tunnel list
  echo Copie o ID do tunel %TNAME% e crie config manual ^(ver docs^).
  pause
  exit /b 1
)

set "CFG=%USERPROFILE%\.cloudflared\config-agro-wa.yml"
set "ORIGIN=https://%HOST%"
(
echo tunnel: !UUID!
echo credentials-file: %USERPROFILE%\.cloudflared\!UUID!.json
echo ingress:
echo   - hostname: %HOST%
echo     service: http://127.0.0.1:8000
echo   - service: http_status:404
) > "%CFG%"

set "REPO=%~dp0..\..\.."
set "ENVF=%REPO%\.env.wa-servidor"
set "CNAME=!UUID!.cfargotunnel.com"

echo Criando DNS na Cloudflare ^(zona zapsistvale.online^)...
cloudflared tunnel route dns %TNAME% %HOST%
if errorlevel 1 (
  echo AVISO: route dns falhou - crie CNAME manual no painel Cloudflare.
)

echo.
echo ========== CONFIG OK ==========
echo URL fixa: %ORIGIN%/atendimento-whatsapp/
echo Config: %CFG%
echo.
echo --- DNS ^(painel Cloudflare, NAO Hostinger^) ---
echo Registro CNAME:
echo    Nome: zap
echo    Alvo: %CNAME%
echo    ^(route dns acima pode ja ter criado^)
echo Teste: %ORIGIN%/healthz
echo ==============================
echo.

if exist "%ENVF%" (
  call "%~dp0..\_interno\patch_env_wa_host.bat" "%HOST%" "%ORIGIN%"
) else (
  echo AVISO: falta %ENVF% - rode 2 - Configurar
)

echo Proximo: 3 - Subir Zap - todo dia.bat ^(usa tunel fixo automatico^)
pause
