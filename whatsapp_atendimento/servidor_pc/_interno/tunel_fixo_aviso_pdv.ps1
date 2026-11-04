# Antes de cloudflared run: avisa PDV da URL fixa (479).
$ErrorActionPreference = "SilentlyContinue"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$envFile = Join-Path $repo ".env.wa-servidor"
$origin = $null
if (Test-Path $envFile) {
  Get-Content $envFile -Encoding UTF8 | ForEach-Object {
    if ($_ -match '^AGRO_WA_TUNEL_FIXO_ORIGIN=(.+)$') { $origin = $matches[1].Trim() }
  }
}
if (-not $origin) { exit 0 }
& (Join-Path $PSScriptRoot "registrar_origin_pdv.ps1") -OriginUrl $origin
