@echo off
chcp 65001 >nul
REM Servidor ja rodando - so reenvia URL do tunel (clipboard) pro PDV.
cd /d "%~dp0"
set "URL=%~1"
if "%URL%"=="" (
  echo Cole a URL https do tunel e Enter ^(Ctrl+V^):
  set /p URL=
)
if "%URL%"=="" (
  echo ERRO: URL vazia.
  pause
  exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_interno\registrar_origin_pdv.ps1" -OriginUrl "%URL%"
pause
