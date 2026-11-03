@echo off
chcp 65001 >nul
setlocal
set "PGUSER=postgres"
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"

echo.
echo === Criar banco Postgres agro_wa ===
set /p PGUSER_IN=Usuario superuser Postgres [%PGUSER%]: 
if not "%PGUSER_IN%"=="" set "PGUSER=%PGUSER_IN%"
set /p PGPASSWORD=Senha do superuser Postgres: 

where psql >nul 2>&1
if errorlevel 1 (
  if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"
  ) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\16\bin;%PATH%"
  ) else (
    echo ERRO: psql nao encontrado. Instale Postgres e tente de novo.
    pause
    exit /b 1
  )
)

psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "ALTER USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE DATABASE %DBNAME% OWNER %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "GRANT ALL PRIVILEGES ON DATABASE %DBNAME% TO %DBUSER%;"

echo.
echo OK. Use no 2 - Configurar - primeira vez env e banco.bat:
echo postgres://%DBUSER%:%DBPASS%@127.0.0.1:5432/%DBNAME%
echo.
pause
