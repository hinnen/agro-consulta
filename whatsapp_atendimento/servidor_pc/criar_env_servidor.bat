@echo off
chcp 65001 >nul
setlocal
set "REPO=%~dp0..\.."
set "OUT=%REPO%\.env.wa-servidor"
set "DADOS=C:\AgroWhatsApp\Dados\media"

echo.
echo === Configurar .env.wa-servidor ===
echo Deixe em branco para usar o padrao entre [ ].
echo.

set /p DBURL=DATABASE_URL [postgres://agro_wa:senha@127.0.0.1:5432/agro_wa]: 
if "%DBURL%"=="" set "DBURL=postgres://agro_wa:senha@127.0.0.1:5432/agro_wa"

set /p TOKEN=AGRO_WA_BRIDGE_TOKEN [gm-agro-wa-ponte-local]: 
if "%TOKEN%"=="" set "TOKEN=gm-agro-wa-ponte-local"

set /p MEDIA=AGRO_WA_MEDIA_ROOT [%DADOS%]: 
if "%MEDIA%"=="" set "MEDIA=%DADOS%"

set /p TUNEL_HOST=Host publico do tunel ^(sem https^) [vazio=so local]: 

set "SECRET=wa-pc-%RANDOM%%RANDOM%-%RANDOM%"

set "ALLOWED=127.0.0.1,localhost"
set "CSRF=http://127.0.0.1:8000,https://127.0.0.1:8000"
if not "%TUNEL_HOST%"=="" (
  set "ALLOWED=127.0.0.1,localhost,%TUNEL_HOST%"
  set "CSRF=http://127.0.0.1:8000,https://127.0.0.1:8000,https://%TUNEL_HOST%"
)

(
echo DATABASE_URL=%DBURL%
echo SECRET_KEY=%SECRET%
echo AGRO_WA_BRIDGE_TOKEN=%TOKEN%
echo AGRO_WA_MEDIA_ROOT=%MEDIA%
echo AGRO_MONGO_ERP_DESLIGADO=true
echo AGRO_PDV_VENDA_SEM_MONGO_ERP=true
echo AGRO_FONTE_CATALOGO=agro_pg
echo AGRO_FONTE_FINANCEIRO=agro_pg
echo ALLOWED_HOSTS_EXTRA=%ALLOWED%
echo CSRF_TRUSTED_ORIGINS_EXTRA=%CSRF%
) > "%OUT%"

if not "%TUNEL_HOST%"=="" echo AGRO_CANONICAL_ORIGIN=https://%TUNEL_HOST%>> "%OUT%"

if not exist "%MEDIA%" mkdir "%MEDIA%"

echo.
echo Gravado: %OUT%
echo Proximo: subir_servidor.bat
echo Ponte: whatsapp_atendimento\iniciar-local.bat
echo.
pause
