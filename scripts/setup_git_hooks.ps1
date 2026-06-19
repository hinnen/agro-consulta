# Uma vez por clone: aponta o Git para .githooks (pre-commit sobe VERSION).
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
git config core.hooksPath .githooks
Write-Host "Git hooks: core.hooksPath = .githooks"
Write-Host "Cada commit em teste/producao sobe VERSION (1.01 -> 1.02 -> ...)."
