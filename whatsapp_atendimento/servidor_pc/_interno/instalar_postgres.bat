@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
set "DADOS=C:\AgroWhatsApp\Dados"
set "PGSUPERPASS=AgroWaPgAdmin2026"
set "ENVFILE=%DADOS%\postgres-superuser.env"

if not exist "%DADOS%" mkdir "%DADOS%"

where psql >nul 2>&1
if not errorlevel 1 (
  echo Postgres ja instalado.
  if not exist "%ENVFILE%" (
    echo PGSUPERUSER=postgres> "%ENVFILE%"
    echo PGSUPERPASS=%PGSUPERPASS%>> "%ENVFILE%"
  )
  exit /b 0
)

where winget >nul 2>&1
if errorlevel 1 (
  echo ERRO: precisa Windows 10 ou 11 com winget ^(Loja: App Installer^).
  echo Win7 nao suportado. Instale Postgres manualmente e rode de novo.
  exit /b 1
)

echo.
echo Instalando PostgreSQL 17 ^(10-15 min, senha admin gravada em Dados\^)...
echo Senha do usuario postgres neste PC: %PGSUPERPASS%
echo.

winget install -e --id PostgreSQL.PostgreSQL.17 --accept-package-agreements --accept-source-agreements --disable-interactivity --override "--mode unattended --unattendedmodeui none --superpassword %PGSUPERPASS%"
if errorlevel 1 (
  echo AVISO: winget falhou — tente instalar PostgreSQL 17 pelo site postgresql.org
  exit /b 1
)

if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"

echo Aguardando servico Postgres...
set /a N=0
:waitpg
set /a N+=1
sc query postgresql-x64-17 | find "RUNNING" >nul 2>&1 && goto pgok
if !N! GEQ 60 (
  echo Servico ainda nao subiu — reinicie o PC e rode 1 - Instalar de novo.
  exit /b 1
)
timeout /t 5 /nobreak >nul
goto waitpg

:pgok
(
echo PGSUPERUSER=postgres
echo PGSUPERPASS=%PGSUPERPASS%
) > "%ENVFILE%"
(
echo Senhas deste PC ^(WhatsApp servidor^)
echo ================================
echo Postgres ADMIN ^(usuario postgres^): %PGSUPERPASS%
echo Banco Zap ^(usuario agro_wa^): agro_wa_lab_2026
echo Arquivo tecnico: %ENVFILE%
) > "%DADOS%\SENHAS-LEIA.txt"
exit /b 0
