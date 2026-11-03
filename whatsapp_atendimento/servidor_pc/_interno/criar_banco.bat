@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
set "DADOS=C:\AgroWhatsApp\Dados"
set "DBUSER=agro_wa"
set "DBPASS=agro_wa_lab_2026"
set "DBNAME=agro_wa"
set "AUTO=%~1"

call "%~dp0sincronizar_banco_agro_wa.bat" %AUTO%
exit /b %ERRORLEVEL%
