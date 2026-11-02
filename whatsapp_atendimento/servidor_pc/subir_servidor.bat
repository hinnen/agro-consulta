@echo off
chcp 65001 >nul
setlocal
set "REPO=%~dp0..\.."
cd /d "%REPO%"

if not exist ".env.wa-servidor" (
  echo ERRO: falta .env.wa-servidor na raiz do repo.
  echo Rode instalar_servidor.bat ou copie env.servidor.exemplo
  pause
  exit /b 1
)

for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env.wa-servidor") do (
  if not "%%A"=="" set "%%A=%%B"
)

if not defined AGRO_WA_MEDIA_ROOT set "AGRO_WA_MEDIA_ROOT=C:\AgroWhatsApp\Dados\media"
if not exist "%AGRO_WA_MEDIA_ROOT%" mkdir "%AGRO_WA_MEDIA_ROOT%"

if not exist ".venv\Scripts\python.exe" (
  echo ERRO: rode instalar_servidor.bat primeiro.
  pause
  exit /b 1
)
call ".venv\Scripts\activate.bat"

echo Migrando banco...
python manage.py migrate --noinput
if errorlevel 1 (
  echo ERRO migrate — confira DATABASE_URL em .env.wa-servidor
  pause
  exit /b 1
)

echo Coletando arquivos estaticos ^(JS do chat^)...
python manage.py collectstatic --noinput
if errorlevel 1 (
  echo AVISO: collectstatic falhou — chat pode ficar sem JS.
)

echo.
echo Servidor em http://127.0.0.1:8000/
echo Atendimento: http://127.0.0.1:8000/atendimento-whatsapp/
echo Feche esta janela para parar.
echo Para internet ^(duas lojas^): outra janela com tunel_rapido.bat
echo.
python manage.py runserver 0.0.0.0:8000
