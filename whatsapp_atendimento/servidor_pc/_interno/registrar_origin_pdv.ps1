# Avisa o PDV (Sistvale) qual e a URL publica do tunel - botao WhatsApp atualiza sozinho.
param(
  [Parameter(Mandatory = $true)]
  [string]$OriginUrl
)

$ErrorActionPreference = "Stop"

function Get-AgroWaServidorEnv {
  $repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
  $f = Join-Path $repo ".env.wa-servidor"
  $h = @{}
  if (Test-Path $f) {
    Get-Content $f -Encoding UTF8 | ForEach-Object {
      $line = $_.Trim()
      if (-not $line -or $line.StartsWith("#")) { return }
      $i = $line.IndexOf("=")
      if ($i -lt 1) { return }
      $k = $line.Substring(0, $i).Trim()
      $v = $line.Substring($i + 1).Trim()
      $h[$k] = $v
    }
  }
  return $h
}

$url = ($OriginUrl.Trim().TrimEnd("/"))
if ($url -notmatch "^https://") {
  Write-Host "AVISO: URL invalida para PDV - $url"
  return
}

$envMap = Get-AgroWaServidorEnv
$pdv = ($envMap["AGRO_WA_PDV_URL"] -or "https://www.sistvale.com.br").Trim().TrimEnd("/")
$token = ($envMap["AGRO_WA_BRIDGE_TOKEN"] -or "").Trim()
if (-not $token -or $token -eq "gm-agro-wa-ponte-local") {
  Write-Host ""
  Write-Host "AVISO: AGRO_WA_BRIDGE_TOKEN ainda e o padrao de laboratorio."
  Write-Host "       Rode 2 - Configurar e cole o MESMO token da Sistvale (Render Producao)."
  Write-Host "       Sem isso o PDV nao aceita atualizar o link."
  Write-Host ""
  return
}

$api = "$pdv/api/atendimento-whatsapp/registrar-origin-publica/"
$body = @{ origin = $url } | ConvertTo-Json -Compress
$headers = @{ "X-Agro-Wa-Token" = $token; "Content-Type" = "application/json" }

try {
  $resp = Invoke-RestMethod -Uri $api -Method POST -Body $body -Headers $headers -TimeoutSec 45
  if ($resp.ok) {
    Write-Host ""
    Write-Host "PDV atualizado: botao WhatsApp aponta para $url"
    Write-Host "(Servico: $pdv)"
    Write-Host ""
  } else {
    Write-Host "PDV respondeu erro: $($resp | ConvertTo-Json -Compress)"
  }
} catch {
  Write-Host ""
  Write-Host "NAO foi possivel avisar o PDV ($pdv)."
  Write-Host "Motivo: $($_.Exception.Message)"
  Write-Host "Confira: internet, token igual ao Render, e deploy da loja com migrate 0155."
  Write-Host ""
}
