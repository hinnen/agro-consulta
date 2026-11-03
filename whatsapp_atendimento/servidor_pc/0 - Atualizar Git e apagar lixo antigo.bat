@echo off
chcp 65001 >nul
cd /d "%~dp0"
set "REPO=%~dp0..\.."

echo.
echo === Atualizar Git + apagar arquivos ANTIGOS desta pasta ===
echo Repo: %REPO%
echo.

cd /d "%REPO%"
where git >nul 2>&1
if errorlevel 1 (
  echo ERRO: Git nao encontrado. Instale Git for Windows.
  pause
  exit /b 1
)

echo Baixando branch whatsapp...
git fetch origin whatsapp
git checkout whatsapp 2>nul
git pull origin whatsapp
if errorlevel 1 (
  echo AVISO: git pull falhou — resolva conflito e rode de novo.
  pause
  exit /b 1
)

cd /d "%~dp0"
echo.
echo Apagando nomes velhos ^(OneDrive as vezes deixa copia^)...

for %%F in (
  instalar_servidor.bat
  criar_env_servidor.bat
  subir_servidor.bat
  subir_tudo.bat
  tunel_rapido.bat
  tunel_rapido.ps1
  criar_banco_agro_wa.bat
  env.servidor.exemplo
  tunel_fixo_criar.bat
  tunel_fixo_login.bat
  tunel_fixo_rodar.bat
  1-instalar.bat
  2-configurar.bat
  3-subir.bat
  4-tunel-teste.bat
) do if exist "%%F" del /Q "%%F" 2>nul

echo.
echo Pronto. Use SOMENTE os arquivos:
echo   1 - Instalar - primeira vez no PC.bat
echo   2 - Configurar - primeira vez env e banco.bat
echo   3 - Subir Zap - todo dia.bat
echo   4 - Tunel teste - internet temporaria.bat
echo.
pause
