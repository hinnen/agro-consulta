@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set "REPO=%~dp0..\.."
set "DADOS=C:\AgroWhatsApp\Dados"
set "MEDIA=%DADOS%\media"

echo.
echo ========================================
echo  WhatsApp Agro — instalar servidor (PC)
echo  Repo: %REPO%
echo ========================================
echo.

where winget >nul 2>&1
if errorlevel 1 (
  echo AVISO: winget nao encontrado. Instale manualmente Python 3.12, Node LTS e Postgres.
  goto deps_manual
)

echo [1/4] Python 3.12 ^(se faltar^)...
where py >nul 2>&1
if errorlevel 1 (
  winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
)

echo [2/4] Node.js LTS ^(ponte^)...
where node >nul 2>&1
if errorlevel 1 (
  winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
)

echo [3/4] PostgreSQL ^(opcional; pode usar SQLite no laboratorio^)...
where psql >nul 2>&1
if errorlevel 1 (
  echo    Se quiser Postgres local, instale pelo instalador oficial ou:
  echo    winget install -e --id PostgreSQL.PostgreSQL.17
)

:deps_manual
echo [4/4] Pastas de dados e ambiente Python...
if not exist "%DADOS%" mkdir "%DADOS%"
if not exist "%MEDIA%" mkdir "%MEDIA%"

cd /d "%REPO%"
if not exist ".venv\Scripts\python.exe" (
  echo Criando .venv ^(primeira vez, 2-5 min^)...
  py -3.12 -m venv .venv 2>nul
  if errorlevel 1 python -m venv .venv
)
call ".venv\Scripts\activate.bat"
python -m pip install -U pip
pip install -r requirements.txt
if errorlevel 1 (
  echo ERRO: pip install falhou.
  pause
  exit /b 1
)

if not exist "%REPO%\.env.wa-servidor" (
  copy /Y "%~dp0env.servidor.exemplo" "%REPO%\.env.wa-servidor" >nul
  echo.
  echo Criado %REPO%\.env.wa-servidor
  echo Edite DATABASE_URL, SECRET_KEY e AGRO_WA_MEDIA_ROOT se precisar.
  echo Ou rode criar_env_servidor.bat para preencher com perguntas.
)

cd /d "%~dp0..\"
if not exist node_modules (
  echo Instalando dependencias da ponte...
  call npm install
)

echo.
echo Instalacao base OK.
echo Proximo: criar_env_servidor.bat ^(se ainda nao editou .env.wa-servidor^)
echo Depois: subir_servidor.bat + iniciar.bat ^(ou iniciar-local.bat^)
echo Doc: docs\WHATSAPP-SERVIDOR-PC.md
echo.
pause
