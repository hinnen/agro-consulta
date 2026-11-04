@echo off
chcp 65001 >nul
cd /d "%~dp0"
set "REPO=%~dp0..\.."
if not exist "%REPO%\.env.wa-servidor" (
  echo ERRO: falta .env.wa-servidor
  pause
  exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%REPO%\.env.wa-servidor") do (
  if /I "%%A"=="AGRO_WA_BRIDGE_TOKEN" if not "%%B"=="" set "AGRO_WA_BRIDGE_TOKEN=%%B"
)
if not defined AGRO_WA_BRIDGE_TOKEN set "AGRO_WA_BRIDGE_TOKEN=gm-agro-wa-ponte-local"
echo Testando ponte em http://127.0.0.1:8000 ...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$h=@{'X-Agro-Wa-Token'='%AGRO_WA_BRIDGE_TOKEN%'}; try { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:8000/api/atendimento-whatsapp/bridge/saida/' -Headers $h -TimeoutSec 15; Write-Host 'OK' $r.StatusCode -ForegroundColor Green } catch { Write-Host 'FALHOU:' $_.Exception.Message -ForegroundColor Red; if ($_.Exception.Response.StatusCode.value__ -eq 403) { Write-Host 'Token diferente entre ponte e servidor. Feche Zap-servidor e Zap-ponte, rode 3 de novo.' } }"
pause
