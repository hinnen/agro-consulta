@echo off
chcp 65001 >nul
REM UNICO script do dia: git pull + Postgres + servidor + ponte + tunel + avisa PDV.
if /I not "%~1"=="--run" (
  start "Subir Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal EnableDelayedExpansion
cd /d "%~dp0"
set "REPO=%~dp0..\.."

echo.
echo ========================================
echo  3 - Subir Zap ^(tudo num lugar so^)
echo ========================================
echo.

if not exist "%REPO%\.env.wa-servidor" (
  echo ERRO: falta .env.wa-servidor. Rode 1 - Instalar ou 2 - Configurar.
  goto fim
)

where git >nul 2>&1
if not errorlevel 1 (
  echo Atualizando codigo ^(git pull whatsapp^)...
  cd /d "%REPO%"
  git fetch origin whatsapp 2>nul
  git checkout whatsapp 2>nul
  git pull origin whatsapp
  if errorlevel 1 echo AVISO: git pull falhou - continua com codigo local.
  cd /d "%~dp0"
) else (
  echo AVISO: Git nao encontrado - pulando pull.
)

call "%~dp0_interno\testar_postgres_agro_wa.bat"
if errorlevel 1 (
  echo.
  echo ERRO: Postgres agro_wa nao conecta.
  echo Rode: 5 - Corrigir Postgres - senha migrate.bat
  goto fim
)
echo Postgres OK.

if not exist "%USERPROFILE%\.cloudflared\config-agro-wa.yml" (
  echo.
  echo ERRO: tunel fixo nao configurado neste PC.
  echo Rode uma vez: A - Loja - primeira vez PC servidor.bat
  goto fim
)

echo.
echo Abrindo servidor + ponte + tunel ^(3 janelas^)...
start "Zap - servidor" cmd /k "%~dp0_interno\subir_django.bat"
timeout /t 5 /nobreak >nul
start "Zap - ponte" cmd /k "%~dp0..\iniciar-local.bat"
timeout /t 3 /nobreak >nul
if exist "%USERPROFILE%\.cloudflared\config-agro-wa.yml" (
  echo Tunel FIXO ^(dominio^)...
  start "Zap - tunel fixo PDV" cmd /k "%~dp0tunel_fixo\tunel_fixo_rodar.bat"
) else (
  echo Tunel teste ^(trycloudflare - URL muda^)...
  start "Zap - internet tunel PDV" cmd /k "%~dp0\4 - Tunel teste - internet temporaria.bat"
)

echo.
echo Local: http://127.0.0.1:8000/atendimento-whatsapp/
echo Tunel: espere "PDV atualizado" ^(botao verde na loja^).
echo Nao feche as 3 janelas pretas.
echo.

:fim
pause
