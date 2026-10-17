@echo off
REM Prepara Node (sistema ou portatil em LocalAppData) e expoe no PATH desta janela.
set "AGRO_NODE_HOME=%LOCALAPPDATA%\AgroEtiquetaPrint\node"
set "AGRO_NODE_DIR="

where node >nul 2>&1
if not errorlevel 1 (
  exit /b 0
)

REM Ja tem portatil?
for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-x64") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)
for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-arm64") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)
for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-x86") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)

echo Baixando Node.js automaticamente ^(nao precisa instalar nada^)...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ensure-node.ps1"
if errorlevel 1 (
  echo Falhou baixar o Node automaticamente.
  echo Verifique internet neste PC e tente de novo.
  echo Ou instale Node LTS em https://nodejs.org e rode de novo.
  exit /b 1
)

for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-x64") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)
for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-arm64") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)
for /d %%D in ("%AGRO_NODE_HOME%\node-*-win-x86") do (
  if exist "%%D\node.exe" (
    set "AGRO_NODE_DIR=%%D"
    goto :have_portable
  )
)

echo ERRO: Node portatil nao apareceu apos o download.
exit /b 1

:have_portable
set "PATH=%AGRO_NODE_DIR%;%PATH%"
exit /b 0
