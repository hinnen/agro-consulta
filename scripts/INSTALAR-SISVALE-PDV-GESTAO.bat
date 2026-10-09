@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title SisVale — Instalar PDV + Gestao

set "DIR=%~dp0"
set "PS1=%DIR%instalar_sistvale_pdv_gestao.ps1"
set "BRANCH=teste"
set "RAW=https://raw.githubusercontent.com/hinnen/agro-consulta/%BRANCH%/scripts"

if not exist "%PS1%" (
    echo.
    echo  Baixando instalador do GitHub...
    echo.
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%RAW%/instalar_sistvale_pdv_gestao.ps1' -OutFile '%PS1%' -UseBasicParsing; Invoke-WebRequest -Uri '%RAW%/remover_apps_chrome_sistvale.ps1' -OutFile '%DIR%remover_apps_chrome_sistvale.ps1' -UseBasicParsing } catch { Write-Host 'Falha no download:' $_ -ForegroundColor Red; exit 1 }"
    if errorlevel 1 goto :fim_erro
)

echo.
echo  SisVale — instalador PDV + Gestao
echo  Site padrao: https://sistvale.com.br
echo  (staging: adicione na linha abaixo -BaseUrl "https://agro-consulta-staging.onrender.com")
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
  echo.
  echo  Instalacao nao concluida. Codigo: %EC%
  goto :fim_erro
)

echo.
echo  Concluido.
timeout /t 4 >nul
exit /b 0

:fim_erro
echo.
pause
exit /b 1
