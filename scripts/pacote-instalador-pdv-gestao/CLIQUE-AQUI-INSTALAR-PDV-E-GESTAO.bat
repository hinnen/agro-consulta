@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title SisVale - Instalar PDV + Gestao
cd /d "%~dp0"

set "PS1=%~dp0instalar_sistvale_pdv_gestao.ps1"
if not exist "%PS1%" (
    echo.
    echo  ERRO: falta instalar_sistvale_pdv_gestao.ps1 nesta pasta.
    echo  Extraia o ZIP inteiro antes de clicar aqui.
    echo.
    pause
    exit /b 1
)

echo.
echo  SisVale - instalador PDV + Gestao
echo  Site: https://sistvale.com.br
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
  echo.
  echo  Instalacao nao concluida. Codigo: %EC%
  pause
  exit /b 1
)

echo.
echo  Concluido.
timeout /t 4 >nul
exit /b 0
