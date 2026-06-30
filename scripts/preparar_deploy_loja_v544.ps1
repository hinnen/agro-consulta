# Pacote loja v5.44 - Relacionamento F8 + extras Postgres (migration 0046)
# Uso:
#   .\scripts\preparar_deploy_loja_v544.ps1
#   .\scripts\preparar_deploy_loja_v544.ps1 -ExecutarPush

param(
    [switch]$ExecutarPush
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))

function Test-VersionUtf8 {
    param([string]$Ref)
    $bytes = git show "${Ref}:VERSION" 2>$null
    if (-not $bytes) { throw "VERSION ausente em $Ref" }
    if ($bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
        throw "VERSION em UTF-16 (BOM) em $Ref - corrigir antes do deploy"
    }
    $text = [System.Text.Encoding]::UTF8.GetString($bytes).Trim()
    Write-Host "  VERSION $Ref = $text (UTF-8 OK)"
    return $text
}

Write-Host "=== Pacote loja v5.44 - Relacionamento F8 ===" -ForegroundColor Cyan
Write-Host ""

git fetch origin teste producao 2>$null | Out-Null

$prodHash = (git rev-parse --short origin/producao)
$testHash = (git rev-parse --short origin/teste)
Write-Host "Branch producao: $prodHash"
Write-Host "Branch teste:    $testHash"
Write-Host ""

Write-Host "Validando VERSION..."
$verProd = Test-VersionUtf8 "origin/producao"
$verTest = Test-VersionUtf8 "origin/teste"
Write-Host ""

Write-Host "Arquivos que mudam (producao -> teste):" -ForegroundColor Yellow
git diff --stat origin/producao origin/teste
Write-Host ""

Write-Host "Migration nova: produtos/migrations/0046_clienteagro_relacionamento_extras.py"
Write-Host ""

if (-not $ExecutarPush) {
    Write-Host "DRY-RUN - nada foi enviado para producao." -ForegroundColor Green
    Write-Host ""
    Write-Host "Quando Renan autorizar (pode subir para producao + senha 99738595):"
    Write-Host "  1. Zap loja: nao finalize venda ~2 min"
    Write-Host "  2. .\scripts\preparar_deploy_loja_v544.ps1 -ExecutarPush"
    Write-Host "  3. Render SistVale ate Live"
    Write-Host "  4. Ctrl+F5 PDVs - badge v5.44"
    exit 0
}

Write-Host "EXECUTANDO merge teste -> producao..." -ForegroundColor Magenta
git checkout producao
git pull origin producao
git merge origin/teste -m "deploy loja v5.44: relacionamento F8, pets Postgres, fiado link PDV"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Merge falhou - resolver conflitos manualmente." -ForegroundColor Red
    exit 1
}

Test-VersionUtf8 "HEAD" | Out-Null
git push origin producao

Write-Host ""
Write-Host "Push producao feito. Acompanhar Render SistVale ate Live." -ForegroundColor Green
Write-Host "Registrar hash final no banana.md CHECKPOINT."
