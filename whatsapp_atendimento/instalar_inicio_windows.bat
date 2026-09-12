@echo off
cd /d "%~dp0"
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
powershell -NoProfile -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut($env:STARTUP + '\Agro WhatsApp Ponte.lnk'); $s.TargetPath = '%~dp0iniciar.bat'; $s.WorkingDirectory = '%~dp0'; $s.WindowStyle = 7; $s.Save()"
echo.
echo Pronto: ao ligar o Windows, a ponte sobe sozinha (janela preta).
echo Nao rode o iniciar.bat em outro PC ao mesmo tempo.
pause
