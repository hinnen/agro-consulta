@echo off
setlocal
cd /d "%~dp0"

if exist "%~dp0config.bat" (
  call "%~dp0config.bat"
) else (
  echo [Agro] Crie config.bat a partir de config-exemplo.bat
  pause
  exit /b 1
)

if not exist "%AGRO_CHROME%" (
  echo [Agro] Chrome nao encontrado em: %AGRO_CHROME%
  pause
  exit /b 1
)

if "%AGRO_DEFINIR_IMPRESSORA_PADRAO%"=="1" (
  echo [Agro] Impressora padrao: %AGRO_IMPRESSORA_ETIQUETAS%
  rundll32 printui.dll,PrintUIEntry /y /n "%AGRO_IMPRESSORA_ETIQUETAS%"
)

echo [Agro] Abrindo etiquetas (impressao silenciosa nesta janela)...
start "" "%AGRO_CHROME%" --kiosk-printing --app="%AGRO_URL_ETIQUETAS%"

endlocal
