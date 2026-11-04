@echo off
REM Ponte falando com o Django DESTE PC (Chrome 127.0.0.1) — nao com a loja.
set "REPO=%~dp0..\.."
if exist "%REPO%\.env.wa-servidor" (
  for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%REPO%\.env.wa-servidor") do (
    if /I "%%A"=="AGRO_WA_BRIDGE_TOKEN" if not "%%B"=="" set "AGRO_WA_BRIDGE_TOKEN=%%B"
  )
)
set AGRO_WA_ALVO=local
set AGRO_WA_DJANGO_URL=http://127.0.0.1:8000
call "%~dp0iniciar.bat"
