@echo off
if /I not "%~1"=="--run" (
  start "Configurar Zap" cmd /k "%~f0" --run
  exit /b 0
)
chcp 65001 >nul
setlocal
set "REPO=%~dp0..\.."
set "OUT=%REPO%\.env.wa-servidor"
set "DADOS=C:\AgroWhatsApp\Dados\media"

echo.
echo === 2 - Configurar ^(opcional — so se quiser mudar algo^) ===
echo O passo 1 ja grava o .env. Enter = manter padrao.
echo.

set "DBURL=postgres://agro_wa:agro_wa_lab_2026@127.0.0.1:5432/agro_wa"
set /p DBURL_IN=DATABASE_URL [%DBURL%]: 
if not "%DBURL_IN%"=="" set "DBURL=%DBURL_IN%"

set "TOKEN=gm-agro-wa-ponte-local"
set /p TOKEN_IN=AGRO_WA_BRIDGE_TOKEN (MESMO da Sistvale Render) [%TOKEN%]: 
if not "%TOKEN_IN%"=="" set "TOKEN=%TOKEN_IN%"

set "PDV=https://www.sistvale.com.br"
set /p PDV_IN=AGRO_WA_PDV_URL (loja na internet) [%PDV%]: 
if not "%PDV_IN%"=="" set "PDV=%PDV_IN%"

set /p MEDIA_IN=Pasta midia [%DADOS%]: 
if not "%MEDIA_IN%"=="" set "DADOS=%MEDIA_IN%"

(
echo DATABASE_URL=%DBURL%
echo SECRET_KEY=wa-pc-%RANDOM%%RANDOM%-%RANDOM%
echo AGRO_WA_BRIDGE_TOKEN=%TOKEN%
echo AGRO_WA_PDV_URL=%PDV%
echo AGRO_WA_MEDIA_ROOT=%DADOS%
echo AGRO_MONGO_ERP_DESLIGADO=true
echo AGRO_PDV_VENDA_SEM_MONGO_ERP=true
echo AGRO_FONTE_CATALOGO=agro_pg
echo AGRO_FONTE_FINANCEIRO=agro_pg
echo ALLOWED_HOSTS_EXTRA=127.0.0.1,localhost,.trycloudflare.com
echo CSRF_TRUSTED_ORIGINS_EXTRA=http://127.0.0.1:8000,https://127.0.0.1:8000
) > "%OUT%"

set /p CRIAR=Recriar banco agro_wa? (S/N) [N]: 
if /I "%CRIAR%"=="S" call "%~dp0_interno\criar_banco.bat" auto

echo Gravado: %OUT%
pause
