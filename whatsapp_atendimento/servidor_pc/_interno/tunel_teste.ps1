# Tunel de TESTE (URL muda se fechar a janela). Requer 3-subir.bat rodando.
$ErrorActionPreference = "Stop"
if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
  $cf = "${env:ProgramFiles(x86)}\cloudflared\cloudflared.exe"
  if (Test-Path $cf) { $env:Path = "$(Split-Path $cf);$env:Path" } else { throw "cloudflared nao encontrado — winget install Cloudflare.cloudflared" }
}

try {
  $null = Invoke-WebRequest -Uri "http://127.0.0.1:8000/healthz" -UseBasicParsing -TimeoutSec 3
} catch {
  Write-Host "ERRO: rode antes 3 - Subir Zap - todo dia.bat (servidor na porta 8000)"
  Read-Host "Enter para sair"
  exit 1
}

Write-Host ""
Write-Host "Tunel TESTE — link copiado (Ctrl+V). Nao feche esta janela."
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
try { Start-Process "$url/entrar/" } catch {}
Wait-Process -Id $p.Id
