@echo off
chcp 65001 >nul
echo.
echo O passo R foi unificado no:
echo   3 - Subir Zap - todo dia.bat
echo.
echo ^(git pull + Postgres + servidor + ponte + tunel + PDV^)
echo.
pause
call "%~dp0\3 - Subir Zap - todo dia.bat"
