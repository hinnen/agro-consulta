# Alinha o repo local com origin/teste (Cursor PC ↔ nuvem).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "== Agro Consulta — alinhar branch teste =="

git fetch origin teste

git rev-parse --verify teste 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    git checkout -b teste origin/teste
} else {
    git checkout teste
}

$dirty = git status --porcelain
if ($dirty) {
    Write-Host ""
    Write-Host "AVISO: há alterações locais não commitadas."
    Write-Host "  Commit, stash ou descarte antes do pull se quiser evitar conflito."
    Write-Host ""
}

git pull --ff-only origin teste
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Pull falhou (provável divergência). Opções:"
    Write-Host "  git status"
    Write-Host "  git stash push -m wip; git pull --ff-only origin teste; git stash pop"
    exit 1
}

$head = git rev-parse --short HEAD
Write-Host ""
Write-Host "Branch: teste @ $head"
Write-Host "Últimos commits:"
git log -3 --oneline
Write-Host ""
Write-Host "Dica: abra banana.md → ## CHECKPOINT DE ATUALIZAÇÃO"
