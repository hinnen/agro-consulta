@echo off
chcp 65001 >nul
REM Alinha usuario agro_wa + banco agro_wa com .env.wa-servidor (senha padrao lab).
setlocal EnableDelayedExpansion
set "DADOS=C:\AgroWhatsApp\Dados"
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"
set "PGUSER=postgres"
set "MODO=%~1"

if exist "%DADOS%\postgres-superuser.env" (
  for /f "usebackq tokens=1,* delims==" %%A in ("%DADOS%\postgres-superuser.env") do (
    if /I "%%A"=="PGSUPERUSER" set "PGUSER=%%B"
    if /I "%%A"=="PGSUPERPASS" set "PGSUPERPASS=%%B"
  )
)

where psql >nul 2>&1
if errorlevel 1 (
  if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"
  ) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\16\bin;%PATH%"
  ) else (
    echo ERRO: Postgres nao encontrado. Rode 1 - Instalar.
    exit /b 1
  )
)

set "PGPASSWORD=%PGSUPERPASS%"
if not defined PGPASSWORD set "PGPASSWORD=AgroWaPgAdmin2026"

call :test_super
if errorlevel 1 (
  set "PGPASSWORD=AgroWaPgAdmin2026"
  call :test_super
)
if errorlevel 1 (
  if /I not "%MODO%"=="auto" goto ask_super
  echo.
  echo ERRO: senha admin do Postgres deste PC nao e a padrao AgroWaPgAdmin2026.
  echo Rode: 5 - Corrigir Postgres - senha migrate.bat
  echo ^(uma vez - digite a senha do usuario postgres deste PC^)
  exit /b 1
)
goto do_sync

:ask_super
echo.
echo Digite a senha do usuario ADMIN do Postgres neste PC ^(usuario %PGUSER%^).
echo Se nao lembrar: painel Windows ou reinstale Postgres pelo passo 1.
set /p PGPASSWORD=Senha admin postgres: 
call :test_super
if errorlevel 1 (
  echo ERRO: senha admin incorreta.
  exit /b 1
)
if not exist "%DADOS%" mkdir "%DADOS%"
(
echo PGSUPERUSER=%PGUSER%
echo PGSUPERPASS=%PGPASSWORD%
) > "%DADOS%\postgres-superuser.env"
echo Gravado em %DADOS%\postgres-superuser.env para proximas vezes.

:do_sync
echo Sincronizando banco %DBNAME% / usuario %DBUSER%...
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "ALTER USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE DATABASE %DBNAME% OWNER %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "GRANT ALL PRIVILEGES ON DATABASE %DBNAME% TO %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d %DBNAME% -v ON_ERROR_STOP=0 -c "GRANT ALL ON SCHEMA public TO %DBUSER%;"

set "PGPASSWORD=%DBPASS%"
psql -U %DBUSER% -h 127.0.0.1 -d %DBNAME% -v ON_ERROR_STOP=1 -c "SELECT 1 AS ok;" >nul 2>&1
if errorlevel 1 (
  echo ERRO: ainda nao conecta como %DBUSER%. Chame suporte / reinstale Postgres.
  exit /b 1
)
echo OK - banco %DBNAME% e senha %DBUSER% conferidos.
exit /b 0

:test_super
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=1 -c "SELECT 1;" >nul 2>&1
exit /b %ERRORLEVEL%
