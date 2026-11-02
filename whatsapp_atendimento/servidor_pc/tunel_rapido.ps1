# Tunel rapido Cloudflare — copia URL pro clipboard e abre o login.
# Requer Django em http://127.0.0.1:8000 e ALLOWED_HOSTS com .trycloudflare.com
$ErrorActionPreference = "Stop"
if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
  $cf = "${env:ProgramFiles(x86)}\cloudflared\cloudflared.exe"
  if (Test-Path $cf) { $env:Path = "$(Split-Path $cf);$env:Path" } else { throw "cloudflared nao encontrado" }
}

try {
  $null = Invoke-WebRequest -Uri "http://127.0.0.1:8000/healthz" -UseBasicParsing -TimeoutSec 3
} catch {
  Write-Host "ERRO: servidor Django nao responde em http://127.0.0.1:8000"
  Write-Host "Rode antes: subir_servidor.bat  (ou subir_tudo.bat)"
  Read-Host "Enter para sair"
  exit 1
}

Write-Host ""
Write-Host "========================================"
Write-Host " Tunel RAPIDO WhatsApp Agro"
Write-Host " URL muda se fechar esta janela."
Write-Host " Link sera copiado (Ctrl+V)."
Write-Host "========================================"
Write-Host ""

$log = Join-Path $env:TEMP "agro-wa-tunel-rapido.log"
Remove-Item $log -ErrorAction SilentlyContinue
$p = Start-Process -FilePath "cloudflared" `
  -ArgumentList @("tunnel","--url","http://127.0.0.1:8000") `
  -RedirectStandardError $log `
  -PassThru `
  -WindowStyle Minimized `
  -NoNewWindow:$false

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
  Write-Host "ERRO: nao achei URL trycloudflare. Log:"
  if (Test-Path $log) { Get-Content $log }
  Read-Host "Enter para sair"
  exit 1
}

Set-Clipboard -Value $url
Write-Host "URL: $url"
Write-Host "Copiado pro Ctrl+V."
Write-Host ""
Write-Host "Chrome: $url/entrar/   (renan / 9973)"
Write-Host "Depois:  $url/atendimento-whatsapp/"
Write-Host ""
Write-Host "NAO FECHE esta janela (o tunel vive nela)."
try { Start-Process "$url/entrar/" } catch {}

Wait-Process -Id $p.Id
