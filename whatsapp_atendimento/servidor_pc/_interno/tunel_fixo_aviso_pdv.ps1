# Antes de cloudflared run: avisa PDV da URL fixa (479).
$ErrorActionPreference = "SilentlyContinue"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$envFile = Join-Path $repo ".env.wa-servidor"
$origin = "https://zap.zapsistvale.online"
if (Test-Path $envFile) {
  Get-Content $envFile -Encoding UTF8 | ForEach-Object {
    if ($_ -match '^AGRO_WA_TUNEL_FIXO_ORIGIN=(.+)$') {
      $v = $matches[1].Trim()
      if ($v) { $origin = $v.TrimEnd("/") }
    }
  }
}
& (Join-Path $PSScriptRoot "registrar_origin_pdv.ps1") -OriginUrl $origin
