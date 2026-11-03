@echo off
chcp 65001 >nul
if /I not "%~1"=="--run" (
  start "Instalar Zap - 1a vez" cmd /k "%~f0" --run
  exit /b 0
)

setlocal
cd /d "%~dp0"
set "REPO=%~dp0..\.."
set "DADOS=C:\AgroWhatsApp\Dados"
set "MEDIA=%DADOS%\media"
set "OUT=%REPO%\.env.wa-servidor"

echo.
echo ========================================
echo  1 - Instalar TUDO ^(primeira vez^)
echo  Windows 10 ou 11 — pode demorar 20 min
echo ========================================
echo.

ver | find "6.1." >nul && (
  echo ERRO: Windows 7 nao suportado. Use Windows 10/11.
  goto fim
)

if not exist "%DADOS%" mkdir "%DADOS%"
if not exist "%MEDIA%" mkdir "%MEDIA%"

call "%~dp0_interno\instalar_deps.bat"
if errorlevel 1 goto fim

call "%~dp0_interno\instalar_postgres.bat"
if errorlevel 1 goto fim

call "%~dp0_interno\criar_banco.bat" auto
if errorlevel 1 goto fim

(
echo DATABASE_URL=postgres://agro_wa:agro_wa_lab_2026@127.0.0.1:5432/agro_wa
echo SECRET_KEY=wa-pc-%RANDOM%%RANDOM%-%RANDOM%
echo AGRO_WA_BRIDGE_TOKEN=gm-agro-wa-ponte-local
echo AGRO_WA_PDV_URL=https://www.sistvale.com.br
echo AGRO_WA_MEDIA_ROOT=%MEDIA%
echo AGRO_MONGO_ERP_DESLIGADO=true
echo AGRO_PDV_VENDA_SEM_MONGO_ERP=true
echo AGRO_FONTE_CATALOGO=agro_pg
echo AGRO_FONTE_FINANCEIRO=agro_pg
echo ALLOWED_HOSTS_EXTRA=127.0.0.1,localhost,.trycloudflare.com
echo CSRF_TRUSTED_ORIGINS_EXTRA=http://127.0.0.1:8000,https://127.0.0.1:8000
) > "%OUT%"

cd /d "%REPO%"
if not exist ".venv\Scripts\python.exe" (
  echo Criando ambiente Python...
  py -3.12 -m venv .venv 2>nul || python -m venv .venv
  if errorlevel 1 (
    echo ERRO: Python nao encontrado. Feche e abra o PowerShell e rode de novo.
    goto fim
  )
)
call ".venv\Scripts\activate.bat"
python -m pip install -U pip
pip install -r requirements.txt
if errorlevel 1 (
  echo ERRO: pip install
  goto fim
)

cd /d "%~dp0..\"
if not exist node_modules (
  echo Instalando ponte WhatsApp...
  call npm install
)

echo.
echo ========================================
echo  INSTALACAO OK
echo  Senhas: %DADOS%\SENHAS-LEIA.txt
echo  Proximo: 3 - Subir Zap - todo dia.bat
echo ========================================
echo.

:fim
pause
