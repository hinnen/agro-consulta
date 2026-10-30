@echo off
cd /d "%~dp0"
if exist "C:\Program Files\nodejs\node.exe" set "PATH=C:\Program Files\nodejs;%PATH%"
if exist "%LocalAppData%\Programs\node\node.exe" set "PATH=%LocalAppData%\Programs\node;%PATH%"
if exist "%~dp0.env" (
  for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%~dp0.env") do (
    if not "%%A"=="" set "%%A=%%B"
  )
)
if /I "%AGRO_WA_ALVO%"=="local" (
  set AGRO_WA_DJANGO_URL=http://127.0.0.1:8000
  set AGRO_WA_BRIDGE_TOKEN=gm-agro-wa-ponte-local
)
if not defined AGRO_WA_DJANGO_URL set AGRO_WA_DJANGO_URL=http://127.0.0.1:8000
if not defined AGRO_WA_BRIDGE_TOKEN set AGRO_WA_BRIDGE_TOKEN=gm-agro-wa-ponte-local
echo.
echo ========================================
echo  Ponte WhatsApp Agro
echo  Site: %AGRO_WA_DJANGO_URL%
echo  NAO FECHE esta janela preta.
echo  Abra so UMA janela (nao duplique o .bat).
echo  O QR aparece no Chrome em /atendimento-whatsapp/
echo  Producao: use o servico WA separado no Render (nao a URL do PDV).
echo  Veja docs/DEPLOY-WHATSAPP-RENDER.md e env.exemplo nesta pasta.
echo ========================================
echo.
where node >nul 2>&1
if errorlevel 1 (
  echo ERRO: Node.js nao encontrado.
  echo Instale a versao LTS em https://nodejs.org e rode este arquivo de novo.
  pause
  exit /b 1
)
if not exist node_modules (
  echo Primeira vez: instalando pecas. Pode demorar 1-2 minutos. Espere.
  echo.
  call npm install
  if errorlevel 1 (
    echo ERRO: npm install falhou. Confira internet e tente de novo.
    pause
    exit /b 1
  )
) else if not exist node_modules\ffmpeg-static (
  echo Instalando conversor de audio. Espere 1 minuto.
  call npm install
  if errorlevel 1 (
    echo ERRO: npm install falhou. Confira internet e tente de novo.
    pause
    exit /b 1
  )
)
if exist node_modules\ffmpeg-static\install.js (
  if not exist node_modules\ffmpeg-static\ffmpeg.exe (
    if not exist node_modules\ffmpeg-static\ffmpeg (
      echo Baixando o conversor de audio. Espere.
      node node_modules\ffmpeg-static\install.js
    )
  )
)
:loop
echo Ligando...
node index.js
if errorlevel 1 (
  echo.
  echo ERRO ao ligar a ponte (codigo acima). Janela aberta para ler.
  pause
)
echo.
echo A ponte parou. Religa em 5 segundos. Feche esta janela para parar de vez.
timeout /t 5 /nobreak >nul
goto loop
