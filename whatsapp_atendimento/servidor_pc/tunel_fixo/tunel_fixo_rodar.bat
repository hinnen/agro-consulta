@echo off
chcp 65001 >nul
REM Liga o tunel FIXO. Django precisa estar em :8000 ^(3 - Subir Zap - todo dia.bat^).
where cloudflared >nul 2>&1
if errorlevel 1 (
  echo ERRO: cloudflared nao encontrado.
  pause
  exit /b 1
)
set "CFG=%USERPROFILE%\.cloudflared\config-agro-wa.yml"
if not exist "%CFG%" (
  echo ERRO: falta %CFG%
  echo Rode antes: tunel_fixo_login.bat e tunel_fixo_criar.bat
  pause
  exit /b 1
)
echo.
echo Tunel fixo Agro WA - NAO FECHE esta janela.
echo Config: %CFG%
echo Origem: http://127.0.0.1:8000
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0..\_interno\tunel_fixo_aviso_pdv.ps1"
cloudflared tunnel --config "%CFG%" run
pause
