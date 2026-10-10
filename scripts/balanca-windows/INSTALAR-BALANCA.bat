@echo off
title SisVale - driver da balanca Urano
cd /d "%~dp0"

net session >nul 2>&1
if errorlevel 1 (
  echo Pedindo permissao de Administrador...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)

echo.
echo  SisVale / Banana  -  driver da balanca (COM USB)
echo  ------------------------------------------------
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0instalar-driver-balanca.ps1" %*
set ERR=%ERRORLEVEL%
echo.
if not "%ERR%"=="0" (
  echo Algo falhou. Codigo %ERR%.
) else (
  echo Pronto. Pode fechar esta janela.
)
echo.
pause
exit /b %ERR%
