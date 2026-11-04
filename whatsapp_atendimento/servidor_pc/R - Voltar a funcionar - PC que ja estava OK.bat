@echo off
chcp 65001 >nul
REM PC que JA funcionou (Postgres OK) - git pull + teste + subir. Nao use no notebook com senha errada.
if /I not "%~1"=="--run" (
  start "Voltar Zap" cmd /k "%~f0" --run
  exit /b 0
)
setlocal
cd /d "%~dp0"
set "REPO=%~dp0..\.."

echo.
echo ========================================
echo  R - Voltar a funcionar ^(PC que ja estava OK^)
echo ========================================
echo.

where git >nul 2>&1
if errorlevel 1 (
  echo ERRO: Git nao encontrado.
  goto fim
)

cd /d "%REPO%"
git fetch origin whatsapp
git checkout whatsapp 2>nul
git pull origin whatsapp
if errorlevel 1 (
  echo AVISO: git pull falhou - resolva e rode de novo.
  goto fim
)

if not exist "%REPO%\.env.wa-servidor" (
  echo ERRO: falta .env.wa-servidor - neste PC rode 1 - Instalar ou 2 - Configurar.
  goto fim
)

call "%~dp0_interno\testar_postgres_agro_wa.bat"
if errorlevel 1 (
  echo.
  echo ERRO: Postgres agro_wa nao conecta neste PC.
  echo Se for o notebook com senha errada, NAO use este R - use 5 ou outro PC.
  goto fim
)

echo Postgres OK. Abrindo servidor + ponte + tunel...
cd /d "%~dp0"
call "%~dp0\3 - Subir Zap - todo dia.bat" --run

:fim
echo.
pause
