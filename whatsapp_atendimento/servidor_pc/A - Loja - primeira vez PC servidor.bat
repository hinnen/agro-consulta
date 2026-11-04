@echo off
chcp 65001 >nul
REM PC DA LOJA (servidor) - primeira vez. Depois: so 3 - Subir Zap - todo dia.bat
if /I not "%~1"=="--run" (
  start "Loja - 1a vez Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal EnableDelayedExpansion
cd /d "%~dp0"
set "REPO=%~dp0..\.."

echo.
echo ============================================================
echo  A - Loja - primeira vez no PC servidor
echo  Depois disso: TODO DIA so o "3 - Subir Zap - todo dia.bat"
echo ============================================================
echo.

if not exist "%REPO%\.venv\Scripts\python.exe" (
  echo Falta Python/venv. Rode primeiro:
  echo   1 - Instalar - primeira vez no PC.bat
  echo ^(espere terminar, depois volte aqui^)
  goto fim
)

if not exist "%REPO%\.env.wa-servidor" (
  echo Criando .env...
  call "%~dp0\2 - Configurar - primeira vez env e banco.bat" --run 2>nul
)
if not exist "%REPO%\.env.wa-servidor" (
  echo Rode: 2 - Configurar - primeira vez env e banco.bat
  goto fim
)

call "%~dp0_interno\testar_postgres_agro_wa.bat"
if errorlevel 1 (
  echo.
  echo Postgres nao OK neste PC.
  echo Rode: 5 - Corrigir Postgres - senha migrate.bat
  goto fim
)

if not exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo Login Cloudflare ^(Authorize no navegador^)...
  call "%~dp0\tunel_fixo\tunel_fixo_login.bat"
)

if not exist "%USERPROFILE%\.cloudflared\config-agro-wa.yml" (
  echo Criando tunel fixo zapsistvale.online ...
  call "%~dp0\tunel_fixo\tunel_fixo_criar_zapsistvale.bat"
)

echo.
echo ============================================================
echo  PRONTO - PC da loja configurado
echo  Todo dia ^(manha^): duplo clique em
echo    3 - Subir Zap - todo dia.bat
echo  Nao feche as janelas pretas durante o expediente.
echo  URL fixa: https://zap.zapsistvale.online/atendimento-whatsapp/
echo ============================================================
echo.
echo Teste agora? ^(S/N^)
set /p SOBE=
if /I "!SOBE!"=="S" call "%~dp0\3 - Subir Zap - todo dia.bat"

:fim
pause
