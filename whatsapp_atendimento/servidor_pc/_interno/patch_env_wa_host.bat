@echo off
chcp 65001 >nul
REM Acrescenta host do dominio fixo no .env.wa-servidor
setlocal
set "HOST=%~1"
set "ORIGIN=%~2"
set "REPO=%~dp0..\..\.."
set "ENVF=%REPO%\.env.wa-servidor"
if not exist "%ENVF%" exit /b 0

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$f='%ENVF%'; $host='%~1'; $origin='%~2';" ^
  "$lines=Get-Content $f -Encoding UTF8;" ^
  "$out=@(); $seen=@{};" ^
  "foreach($k in @('AGRO_WA_TUNEL_FIXO_ORIGIN','ALLOWED_HOSTS_EXTRA','CSRF_TRUSTED_ORIGINS_EXTRA')){ $seen[$k]=$false };" ^
  "foreach($line in $lines){" ^
  "  if($line -match '^AGRO_WA_TUNEL_FIXO_ORIGIN='){ $out+=('AGRO_WA_TUNEL_FIXO_ORIGIN='+$origin); $seen['AGRO_WA_TUNEL_FIXO_ORIGIN']=$true; continue }" ^
  "  if($line -match '^ALLOWED_HOSTS_EXTRA='){ $v=$line.Substring(22); if($v -notmatch [regex]::Escape($host)){ $v+=','+$host }; $out+=('ALLOWED_HOSTS_EXTRA='+$v); $seen['ALLOWED_HOSTS_EXTRA']=$true; continue }" ^
  "  if($line -match '^CSRF_TRUSTED_ORIGINS_EXTRA='){ $u=$origin; if($line -notmatch [regex]::Escape($u)){ $out+=($line+','+$u); $seen['CSRF_TRUSTED_ORIGINS_EXTRA']=$true; continue } }" ^
  "  $out+=$line }" ^
  "if(-not $seen['AGRO_WA_TUNEL_FIXO_ORIGIN']){ $out+=('AGRO_WA_TUNEL_FIXO_ORIGIN='+$origin) }" ^
  "if(-not $seen['ALLOWED_HOSTS_EXTRA']){ $out+=('ALLOWED_HOSTS_EXTRA=127.0.0.1,localhost,.trycloudflare.com,'+$host) }" ^
  "if(-not $seen['CSRF_TRUSTED_ORIGINS_EXTRA']){ $out+=('CSRF_TRUSTED_ORIGINS_EXTRA=http://127.0.0.1:8000,https://127.0.0.1:8000,'+$origin) }" ^
  "$out | Set-Content $f -Encoding UTF8"
exit /b 0
