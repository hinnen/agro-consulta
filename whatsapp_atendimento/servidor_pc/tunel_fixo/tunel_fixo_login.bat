@echo off
chcp 65001 >nul
REM Abre o navegador para autorizar a conta Cloudflare (uma vez por PC).
where cloudflared >nul 2>&1
if errorlevel 1 (
  echo ERRO: cloudflared nao instalado. Instale ou rode 4-tunel-teste.bat uma vez.
  pause
  exit /b 1
)
echo.
echo Vai abrir o navegador Cloudflare.
echo 1^) Entre na conta
echo 2^) Escolha o dominio/zona na lista
echo 3^) Autorize
echo.
echo Se a lista vier VAZIA: falta adicionar um site na Cloudflare.
echo Lab sem dominio: use ..\4-tunel-teste.bat ^(URL muda^).
echo Nao aponte o dominio da loja PDV sem OK do Renan.
echo.
cloudflared tunnel login
echo.
if exist "%USERPROFILE%\.cloudflared\cert.pem" (
  echo OK — login salvo. Proximo: tunel_fixo_criar.bat
) else (
  echo Login incompleto — cert.pem nao apareceu.
  echo Causa comum: conta sem dominio. Veja docs\WHATSAPP-SERVIDOR-PC.md ^(opcoes A/B/C^).
)
pause
