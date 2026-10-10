@echo off
cd /d "%~dp0"
title Remover inicio automatico
set "LNK=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\Agro Etiqueta Print.lnk"
if exist "%LNK%" (
  del /f /q "%LNK%"
  echo Removido: %LNK%
) else (
  echo Nao havia atalho de inicio automatico.
)
echo.
pause
