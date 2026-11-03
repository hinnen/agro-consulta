@echo off
chcp 65001 >nul
setlocal
set "REPO=%~dp0..\.."
set "OUT=%REPO%\.env.wa-servidor"
set "DADOS=C:\AgroWhatsApp\Dados\media"

echo.
echo === Configurar — primeira vez ^(.env e banco^) ===
echo Enter = aceitar [padrao]
echo.

set "DBURL=postgres://agro_wa:agro_wa_lab_2026@127.0.0.1:5432/agro_wa"
set /p DBURL_IN=DATABASE_URL [%DBURL%]: 
if not "%DBURL_IN%"=="" set "DBURL=%DBURL_IN%"

set "TOKEN=gm-agro-wa-ponte-local"
set /p TOKEN_IN=AGRO_WA_BRIDGE_TOKEN [%TOKEN%]: 
if not "%TOKEN_IN%"=="" set "TOKEN=%TOKEN_IN%"

set /p MEDIA_IN=Pasta de fotos/audios [%DADOS%]: 
if not "%MEDIA_IN%"=="" set "DADOS=%MEDIA_IN%"

set "SECRET=wa-pc-%RANDOM%%RANDOM%-%RANDOM%"

(
echo DATABASE_URL=%DBURL%
echo SECRET_KEY=%SECRET%
echo AGRO_WA_BRIDGE_TOKEN=%TOKEN%
echo AGRO_WA_MEDIA_ROOT=%DADOS%
echo AGRO_MONGO_ERP_DESLIGADO=true
echo AGRO_PDV_VENDA_SEM_MONGO_ERP=true
echo AGRO_FONTE_CATALOGO=agro_pg
echo AGRO_FONTE_FINANCEIRO=agro_pg
echo ALLOWED_HOSTS_EXTRA=127.0.0.1,localhost,.trycloudflare.com
echo CSRF_TRUSTED_ORIGINS_EXTRA=http://127.0.0.1:8000,https://127.0.0.1:8000
) > "%OUT%"

if not exist "%DADOS%" mkdir "%DADOS%"

echo.
echo Gravado: %OUT%
echo.
set /p CRIAR=Criar banco Postgres agro_wa agora? (S/N) [S]: 
if /I "%CRIAR%"=="N" goto fim
call "%~dp0_interno\criar_banco.bat"

:fim
echo Proximo: 3 - Subir Zap - todo dia.bat
pause
