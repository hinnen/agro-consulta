@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
set "DADOS=C:\AgroWhatsApp\Dados"
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"
set "PGUSER=postgres"
set "AUTO=%~1"

if exist "%DADOS%\postgres-superuser.env" (
  for /f "usebackq tokens=1,* delims==" %%A in ("%DADOS%\postgres-superuser.env") do (
    if /I "%%A"=="PGSUPERUSER" set "PGUSER=%%B"
    if /I "%%A"=="PGSUPERPASS" set "PGPASSWORD=%%B"
  )
)

if "%AUTO%"=="auto" goto skipask
set /p PGUSER_IN=Usuario superuser Postgres [%PGUSER%]: 
if not "%PGUSER_IN%"=="" set "PGUSER=%PGUSER_IN%"
if not defined PGPASSWORD set /p PGPASSWORD=Senha do superuser Postgres: 
goto run

:skipask
if not defined PGPASSWORD set "PGPASSWORD=AgroWaPgAdmin2026"

:run
where psql >nul 2>&1
if errorlevel 1 (
  if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"
  ) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\16\bin;%PATH%"
  ) else (
    echo ERRO: Postgres nao encontrado. Rode 1 - Instalar de novo.
    exit /b 1
  )
)

psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "ALTER USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE DATABASE %DBNAME% OWNER %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "GRANT ALL PRIVILEGES ON DATABASE %DBNAME% TO %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d %DBNAME% -v ON_ERROR_STOP=0 -c "GRANT ALL ON SCHEMA public TO %DBUSER%;"

echo Banco %DBNAME% pronto.
exit /b 0
