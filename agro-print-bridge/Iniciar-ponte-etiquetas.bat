@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Agro Etiqueta Print
echo.
echo  Agro Etiqueta Print — ponte do SisVale
echo  Deixe esta janela aberta (ou minimize). Depois imprima no Chrome.
echo.
where node >nul 2>&1
if errorlevel 1 (
  echo ERRO: Node.js nao encontrado. Instale em https://nodejs.org
  pause
  exit /b 1
)
if not exist "node_modules\electron" (
  echo Primeira vez: instalando... (pode demorar)
  call npm install
  if errorlevel 1 (
    echo Falhou o npm install.
    pause
    exit /b 1
  )
)
echo Ligando ponte na porta 19192...
call npm start
pause
