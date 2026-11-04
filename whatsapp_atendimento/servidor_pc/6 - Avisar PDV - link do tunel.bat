@echo off
chcp 65001 >nul
echo.
echo O passo 6 foi unificado no 3 ^(tunel avisa o PDV sozinho^).
echo Use 6 so se o tunel ja estiver aberto e faltou "PDV atualizado":
echo.
pause
powershell -NoProfile -ExecutionPolicy Bypass -Command "$u=(Get-Clipboard -Raw).Trim(); if(-not $u){Write-Host 'Cole a URL https no clipboard (Ctrl+C na janela tunel) e Enter'; Read-Host}; & '%~dp0_interno\registrar_origin_pdv.ps1' -OriginUrl $u"
pause
