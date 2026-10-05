@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Agro Etiqueta Print — inicio automatico
echo.
echo  Instala a ponte para ABRIR SOZINHA quando o Windows ligar.
echo  Rode UMA VEZ em cada PC da etiqueta (Centro / Vila).
echo.

where node >nul 2>&1
if errorlevel 1 (
  echo ERRO: Node.js nao encontrado. Instale LTS em https://nodejs.org
  pause
  exit /b 1
)

if not exist "node_modules\electron\package.json" (
  echo Instalando pacotes...
  call npm install --ignore-scripts=false
  if errorlevel 1 (
    echo Falhou npm install.
    pause
    exit /b 1
  )
)

echo Preparando Electron...
call node ensure-electron.js
if errorlevel 1 (
  echo Falhou Electron.
  pause
  exit /b 1
)

set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "LNK=%STARTUP%\Agro Etiqueta Print.lnk"
set "VBS=%~dp0iniciar-silencioso.vbs"

echo Criando atalho em Iniciar com o Windows...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$s=(New-Object -ComObject WScript.Shell).CreateShortcut('%LNK%'); $s.TargetPath='%VBS%'; $s.WorkingDirectory='%~dp0'; $s.WindowStyle=7; $s.Description='Agro Etiqueta Print - SisVale'; $s.Save()"

if not exist "%LNK%" (
  echo ERRO: nao criou o atalho em:
  echo   %LNK%
  pause
  exit /b 1
)

echo.
echo OK. Atalho criado:
echo   %LNK%
echo.
echo Ligando a ponte agora (segundo plano)...
wscript //nologo "%VBS%"
timeout /t 2 /nobreak >nul

echo.
echo Pronto.
echo  - Ao ligar o PC, a ponte sobe sozinha (bandeja do Windows).
echo  - Nao precisa mais abrir o .bat todo dia.
echo  - Chrome: Etiquetas deve mostrar "Ponte ligada" (verde).
echo.
pause
