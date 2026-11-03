@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set "REPO=%~dp0..\.."
set "DADOS=C:\AgroWhatsApp\Dados"
set "MEDIA=%DADOS%\media"

echo.
echo === Instalar — primeira vez neste PC ===
echo.

where winget >nul 2>&1
if not errorlevel 1 (
  where py >nul 2>&1 || winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
  where node >nul 2>&1 || winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
  where psql >nul 2>&1 || echo Instale Postgres: winget install PostgreSQL.PostgreSQL.17
)

if not exist "%DADOS%" mkdir "%DADOS%"
if not exist "%MEDIA%" mkdir "%MEDIA%"

cd /d "%REPO%"
if not exist ".venv\Scripts\python.exe" (
  echo Criando .venv (2-5 min)...
  py -3.12 -m venv .venv 2>nul || python -m venv .venv
)
call ".venv\Scripts\activate.bat"
python -m pip install -U pip
pip install -r requirements.txt
if errorlevel 1 ( echo ERRO pip & pause & exit /b 1 )

if not exist ".env.wa-servidor" copy /Y "%~dp0env.exemplo" "%REPO%\.env.wa-servidor" >nul

cd /d "%~dp0..\"
if not exist node_modules call npm install

echo.
echo Pronto. Proximo: 2 - Configurar - primeira vez env e banco.bat
pause
