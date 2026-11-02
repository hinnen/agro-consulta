@echo off
chcp 65001 >nul
setlocal
REM Cria usuario + banco agro_wa no Postgres LOCAL (so modulo WhatsApp).
REM Rode DEPOIS do instalador do Postgres (janela Setup).
REM Senha padrao lab: agro_wa_lab_2026  (igual .env.wa-servidor deste laboratorio)

set "PGUSER=postgres"
set "PGPASSWORD="
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"

echo.
echo === Criar banco Postgres agro_wa ===
echo Usuario do Postgres (superuser, padrao postgres):
set /p PGUSER_IN=PGUSER [%PGUSER%]: 
if not "%PGUSER_IN%"=="" set "PGUSER=%PGUSER_IN%"
set /p PGPASSWORD=Senha do superuser Postgres: 

where psql >nul 2>&1
if errorlevel 1 (
  rem tenta pasta padrao EDB 17
  if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"
  ) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\16\bin;%PATH%"
  ) else (
    echo ERRO: psql nao encontrado. Abra de novo o terminal apos instalar o Postgres.
    pause
    exit /b 1
  )
)

echo Criando role e banco (ignora erro se ja existir)...
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "ALTER USER %DBUSER% WITH PASSWORD '%DBPASS%';"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "CREATE DATABASE %DBNAME% OWNER %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d postgres -v ON_ERROR_STOP=0 -c "GRANT ALL PRIVILEGES ON DATABASE %DBNAME% TO %DBUSER%;"
psql -U %PGUSER% -h 127.0.0.1 -d %DBNAME% -v ON_ERROR_STOP=0 -c "GRANT ALL ON SCHEMA public TO %DBUSER%;"

if errorlevel 1 (
  echo Falhou. Confira se o servico PostgreSQL esta rodando e a senha do superuser.
  pause
  exit /b 1
)

echo.
echo OK. DATABASE_URL esperado:
echo postgres://%DBUSER%:%DBPASS%@127.0.0.1:5432/%DBNAME%
echo Proximo: subir_servidor.bat
echo.
pause
