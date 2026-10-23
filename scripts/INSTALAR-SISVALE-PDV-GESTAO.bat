@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title SisVale - Instalar PDV + Gestao

set "DIR=%~dp0"
set "PS1=%DIR%instalar_sistvale_pdv_gestao.ps1"
set "REM=%DIR%remover_apps_chrome_sistvale.ps1"
set "RAW=https://raw.githubusercontent.com/hinnen/agro-consulta/teste/scripts"

if not exist "%PS1%" (
    echo.
    echo  Baixando instalador do GitHub...
    echo.
    powershell -NoProfile -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; $b='%RAW%'; $d='%DIR%'; try { Invoke-WebRequest -Uri ($b+'/instalar_sistvale_pdv_gestao.ps1') -OutFile ($d+'instalar_sistvale_pdv_gestao.ps1') -UseBasicParsing; Invoke-WebRequest -Uri ($b+'/remover_apps_chrome_sistvale.ps1') -OutFile ($d+'remover_apps_chrome_sistvale.ps1') -UseBasicParsing; exit 0 } catch { Write-Host ('Falha no download: '+$_.Exception.Message); exit 1 }"
    if errorlevel 1 goto fim_erro
)

echo.
echo  SisVale - instalador PDV + Gestao
echo  Site padrao: https://sistvale.com.br
echo.
echo  Staging: arraste este BAT para o PowerShell e acrescente:
echo  -BaseUrl "https://agro-consulta-staging.onrender.com"
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
  echo.
  echo  Instalacao nao concluida. Codigo: %EC%
  goto fim_erro
)

echo.
echo  Concluido.
timeout /t 4 >nul
exit /b 0

:fim_erro
echo.
pause
exit /b 1
