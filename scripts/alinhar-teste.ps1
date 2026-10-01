# Alinha a branch teste local com origin/teste (Cursor PC x agente na nuvem).
# Uso: na raiz do repo → .\scripts\alinhar-teste.ps1

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

function Read-VersionFile {
    if (Test-Path "VERSION") {
        return (Get-Content "VERSION" -Raw).Trim()
    }
    return "(sem VERSION)"
}

Write-Host "== Agro Consulta — alinhar branch teste ==" -ForegroundColor Cyan

$branch = git rev-parse --abbrev-ref HEAD 2>$null
if ($branch -ne "teste") {
    Write-Host "Branch atual: $branch (esperado: teste)" -ForegroundColor Yellow
    $ok = Read-Host "Trocar para teste? (s/N)"
    if ($ok -match '^[sS]') {
        git checkout teste
    } else {
        Write-Host "Abortado. Rode na branch teste ou confirme a troca." -ForegroundColor Red
        exit 1
    }
}

Write-Host "Fetching origin/teste..." -ForegroundColor DarkGray
git fetch origin teste

$porcelain = git status --porcelain
if ($porcelain) {
    Write-Host ""
    Write-Host "Ha alteracoes locais nao commitadas. Commit ou git stash antes do pull." -ForegroundColor Yellow
    git status -sb
    exit 2
}

$behind = [int](git rev-list --count HEAD..origin/teste 2>$null)
$ahead = [int](git rev-list --count origin/teste..HEAD 2>$null)

Write-Host ""
Write-Host "VERSION local:  $(Read-VersionFile)"
Write-Host "Commits: atrás de origin/teste = $behind | à frente = $ahead"

if ($ahead -gt 0 -and $behind -gt 0) {
    Write-Host ""
    Write-Host "Divergiu (atrás e à frente). Resolva manualmente (rebase ou merge) — nao auto-pull." -ForegroundColor Red
    exit 3
}

if ($behind -eq 0) {
    Write-Host ""
    Write-Host "Ja alinhado com origin/teste." -ForegroundColor Green
    exit 0
}

Write-Host ""
Write-Host "Puxando origin/teste (rebase)..." -ForegroundColor Cyan
git pull --rebase origin teste

Write-Host ""
Write-Host "OK. VERSION agora: $(Read-VersionFile)" -ForegroundColor Green
Write-Host "Dica: Ctrl+F5 no Chrome; reinicie runserver se estiver aberto."
