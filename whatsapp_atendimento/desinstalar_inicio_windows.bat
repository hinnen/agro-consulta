@echo off
cd /d "%~dp0"
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
del /f /q "%STARTUP%\Agro WhatsApp Ponte.lnk" 2>nul
echo.
echo Pronto: a ponte NAO sobe mais sozinha ao ligar o Windows.
echo Para ligar de novo: rode instalar_inicio_windows.bat
pause
