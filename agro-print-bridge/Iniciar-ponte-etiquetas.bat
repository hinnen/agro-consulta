@echo off
REM ASCII only - evita erro tle/cp no cmd.
cd /d "%~dp0"
title Agro Etiqueta Print
echo.
echo  Agro Etiqueta Print - ponte do SisVale
echo  Deixe esta janela aberta (ou minimize). Depois imprima no Chrome.
echo  Dica loja: use Instalar-inicio-Windows.bat ^(abre sozinho ao ligar o PC^).
echo.

call "%~dp0ensure-node.bat"
if errorlevel 1 (
  pause
  exit /b 1
)

if not exist "package.json" (
  echo ERRO: rode este arquivo de dentro da pasta agro-print-bridge.
  pause
  exit /b 1
)

if not exist "node_modules\electron\package.json" (
  echo Primeira vez: instalando pacotes... ^(pode demorar^)
  call npm install --ignore-scripts=false
  if errorlevel 1 (
    echo Falhou o npm install.
    pause
    exit /b 1
  )
)

echo Preparando Electron...
call node ensure-electron.js
if errorlevel 1 (
  echo.
  echo Falhou preparar o Electron.
  pause
  exit /b 1
)

set "ELECTRON_OVERRIDE_DIST_PATH=%LOCALAPPDATA%\AgroEtiquetaPrint\electron-dist"
if not exist "%ELECTRON_OVERRIDE_DIST_PATH%\electron.exe" (
  echo ERRO: electron.exe nao encontrado em
  echo   %ELECTRON_OVERRIDE_DIST_PATH%
  pause
  exit /b 1
)

echo Ligando ponte na porta 19192...
call npm start
if errorlevel 1 (
  echo.
  echo A ponte caiu. Feche outras janelas "Agro Etiqueta Print" e tente de novo.
)
pause
