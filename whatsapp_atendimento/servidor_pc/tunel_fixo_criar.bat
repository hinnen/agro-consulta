@echo off
chcp 65001 >nul
setlocal
REM Cria tunel nomeado + DNS + config. Precisa: tunel_fixo_login.bat OK e dominio na Cloudflare.
where cloudflared >nul 2>&1
if errorlevel 1 (
  echo ERRO: cloudflared nao encontrado.
  pause
  exit /b 1
)
if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo ERRO: falta login. Rode tunel_fixo_login.bat primeiro.
  pause
  exit /b 1
)

set "TNAME=agro-wa"
set "HOST=zap.sistvale.com.br"
echo.
echo Nome do tunel [%TNAME%]:
set /p TNAME_IN=
if not "%TNAME_IN%"=="" set "TNAME=%TNAME_IN%"
echo Host publico HTTPS ^(subdominio no Cloudflare^) [%HOST%]:
set /p HOST_IN=
if not "%HOST_IN%"=="" set "HOST=%HOST_IN%"

echo.
echo Criando tunel %TNAME% ...
cloudflared tunnel create %TNAME%
if errorlevel 1 (
  echo Se ja existe, seguimos com o existente.
)

for /f "tokens=1" %%U in ('cloudflared tunnel list ^| findstr /i "%TNAME%"') do set "UUID=%%U"
if "%UUID%"=="" (
  echo ERRO: nao achei UUID do tunel %TNAME%. Rode: cloudflared tunnel list
  pause
  exit /b 1
)
echo UUID=%UUID%

echo Criando DNS %HOST% -^> tunel...
cloudflared tunnel route dns %TNAME% %HOST%
if errorlevel 1 (
  echo AVISO: DNS falhou. O dominio precisa estar na Cloudflare. Ajuste o host e tente de novo.
)

set "CFG=%USERPROFILE%\.cloudflared\config-agro-wa.yml"
(
echo tunnel: %UUID%
echo credentials-file: %USERPROFILE%\.cloudflared\%UUID%.json
echo ingress:
echo   - hostname: %HOST%
echo     service: http://127.0.0.1:8000
echo   - service: http_status:404
) > "%CFG%"
echo Config: %CFG%

set "REPO=%~dp0..\.."
set "ENVF=%REPO%\.env.wa-servidor"
if exist "%ENVF%" (
  echo.
  echo Atualize ALLOWED_HOSTS / CSRF no .env.wa-servidor com %HOST%
  echo ^(ou rode criar_env_servidor.bat com esse host^)
)

echo.
echo Proximo: com o Django rodando, duplo clique em tunel_fixo_rodar.bat
echo URL: https://%HOST%/atendimento-whatsapp/
echo.
pause
