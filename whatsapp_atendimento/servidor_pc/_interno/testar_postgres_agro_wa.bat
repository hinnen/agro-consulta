@echo off
chcp 65001 >nul
setlocal
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"

where psql >nul 2>&1
if errorlevel 1 (
  if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%"
  ) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PATH=C:\Program Files\PostgreSQL\16\bin;%PATH%"
  ) else (
    exit /b 1
  )
)

set "PGPASSWORD=%DBPASS%"
psql -U %DBUSER% -h 127.0.0.1 -d %DBNAME% -v ON_ERROR_STOP=1 -c "SELECT 1;" >nul 2>&1
exit /b %ERRORLEVEL%
