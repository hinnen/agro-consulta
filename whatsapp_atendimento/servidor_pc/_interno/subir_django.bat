@echo off
chcp 65001 >nul
setlocal
set "REPO=%~dp0..\..\.."
cd /d "%REPO%"

if not exist ".env.wa-servidor" (
  echo ERRO: falta .env.wa-servidor na raiz do repo.
  echo Rode: 2 - Configurar - primeira vez env e banco.bat
  pause
  exit /b 1
)

for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env.wa-servidor") do (
  if not "%%A"=="" set "%%A=%%B"
)

if not defined AGRO_WA_MEDIA_ROOT set "AGRO_WA_MEDIA_ROOT=C:\AgroWhatsApp\Dados\media"
if not exist "%AGRO_WA_MEDIA_ROOT%" mkdir "%AGRO_WA_MEDIA_ROOT%"

if not exist ".venv\Scripts\python.exe" (
  echo ERRO: rode 1 - Instalar - primeira vez no PC.bat primeiro.
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

echo Coletando JS/CSS do chat...
python manage.py collectstatic --noinput

echo.
echo Local: http://127.0.0.1:8000/atendimento-whatsapp/
echo Internet: outra janela — 4 - Tunel teste - internet temporaria.bat
echo Feche esta janela para parar o servidor.
echo.
python manage.py runserver 0.0.0.0:8000
