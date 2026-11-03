@echo off
if /I not "%~1"=="--run" (
  start "Corrigir Postgres Zap" cmd /k "%~f0" --run
  exit /b 0
)
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo ========================================
echo  5 - Corrigir Postgres ^(senha migrate^)
echo  Use quando Zap servidor der erro agro_wa
echo ========================================
echo.
call "%~dp0_interno\sincronizar_banco_agro_wa.bat"
if errorlevel 1 goto fim
echo.
echo Pronto. Agora rode de novo: 3 - Subir Zap - todo dia.bat
echo.
:fim
pause
