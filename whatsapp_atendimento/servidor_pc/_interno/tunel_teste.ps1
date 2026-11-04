# Tunel de TESTE (URL muda se fechar a janela). Requer 3-subir.bat rodando.
$ErrorActionPreference = "Stop"
if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
  $cf = "${env:ProgramFiles(x86)}\cloudflared\cloudflared.exe"
  if (Test-Path $cf) { $env:Path = "$(Split-Path $cf);$env:Path" } else { throw "cloudflared nao encontrado. Rode: winget install Cloudflare.cloudflared" }
}

Write-Host ""
Write-Host "Aguardando Django na porta 8000 (migrate pode levar 1-3 min)..."
$ready = $false
for ($i = 0; $i -lt 120; $i++) {
  try {
    $null = Invoke-WebRequest -Uri "http://127.0.0.1:8000/healthz" -UseBasicParsing -TimeoutSec 5
    $ready = $true
    break
  } catch {
    if ($i -eq 0 -or ($i % 15 -eq 0)) {
      Write-Host "  ... ainda subindo (${i}s) - veja janela Zap servidor"
    }
    Start-Sleep -Seconds 2
  }
}
if (-not $ready) {
  Write-Host ""
  Write-Host "ERRO: servidor nao respondeu em ~4 min."
  Write-Host "Olhe a janela ZAP SERVIDOR: migrate, Postgres ou .env.wa-servidor."
  Write-Host "Quando aparecer 'Starting development server', rode de novo:"
  Write-Host "  4 - Tunel teste - internet temporaria.bat"
  Read-Host "Enter para sair"
  exit 1
}

Write-Host "Servidor OK."
Write-Host "Tunel TESTE - link copiado (Ctrl+V). Nao feche esta janela."
Write-Host ""

$log = Join-Path $env:TEMP "agro-wa-tunel-rapido.log"
Remove-Item $log -ErrorAction SilentlyContinue
$p = Start-Process -FilePath "cloudflared" `
  -ArgumentList @("tunnel","--url","http://127.0.0.1:8000") `
  -RedirectStandardError $log `
  -PassThru `
  -WindowStyle Minimized

$url = $null
for ($i = 0; $i -lt 60 -and -not $url; $i++) {
  Start-Sleep -Seconds 1
  if (Test-Path $log) {
    $t = Get-Content $log -Raw -ErrorAction SilentlyContinue
    if ($t -match 'https://[a-z0-9-]+\.trycloudflare\.com') { $url = $Matches[0] }
  }
  if ($p.HasExited) { break }
}

if (-not $url) {
  Write-Host "ERRO: URL nao apareceu. Log:"
  if (Test-Path $log) { Get-Content $log }
  Read-Host "Enter"
  exit 1
}

Set-Clipboard -Value $url
Write-Host "URL: $url"
Write-Host "Abra: $url/atendimento-whatsapp/"
& (Join-Path $PSScriptRoot "registrar_origin_pdv.ps1") -OriginUrl $url
Write-Host "Use local: http://127.0.0.1:8000/atendimento-whatsapp/"
Wait-Process -Id $p.Id
