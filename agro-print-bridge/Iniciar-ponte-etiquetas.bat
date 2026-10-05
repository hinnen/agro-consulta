@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Agro Etiqueta Print
echo.
echo  Agro Etiqueta Print — ponte do SisVale
echo  Deixe esta janela aberta (ou minimize). Depois imprima no Chrome.
echo  Dica loja: use Instalar-inicio-Windows.bat ^(abre sozinho ao ligar o PC^).
echo.

where node >nul 2>&1
if errorlevel 1 (
  echo ERRO: Node.js nao encontrado. Instale em https://nodejs.org ^(versao LTS 20 ou 22^).
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

REM Electron fora do OneDrive (LocalAppData) — extract na pasta do Git costuma falhar.
echo Preparando Electron...
call node ensure-electron.js
if errorlevel 1 (
  echo.
  echo Falhou preparar o Electron.
  echo Se o Node for v26, tente instalar o Node LTS 22 em https://nodejs.org
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
