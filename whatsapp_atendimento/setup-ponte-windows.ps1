# Ponte WhatsApp — Windows (PowerShell)
# 1) Instala Node.js LTS (winget)
# 2) Cria .env na mesma pasta deste script
#
# Como rodar (PowerShell):
#   cd "C:\caminho\do\agro-consulta\whatsapp_atendimento"
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\setup-ponte-windows.ps1

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $here

Write-Host ""
Write-Host "=== 1/2 Node.js LTS ===" -ForegroundColor Cyan
if (Get-Command node -ErrorAction SilentlyContinue) {
    Write-Host "Node ja instalado:" (node -v)
} else {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Host "winget nao encontrado. Instale Node manualmente: https://nodejs.org (LTS)" -ForegroundColor Red
        exit 1
    }
    Write-Host "Instalando OpenJS.NodeJS.LTS via winget (pode pedir confirmacao)..."
    winget install -e --id OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
    Write-Host "Feche este PowerShell, abra OUTRO novo, e rode iniciar.bat de novo." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== 2/2 Arquivo .env ===" -ForegroundColor Cyan
Write-Host "Cole o AGRO_WA_BRIDGE_TOKEN (mesmo valor da Sistvale - Producao no Render)."
$token = Read-Host "AGRO_WA_BRIDGE_TOKEN"
$token = ($token -replace '^\s+|\s+$', '')
if ($token.Length -lt 8) {
    Write-Host "Token curto demais — confira no Render." -ForegroundColor Red
    exit 1
}

$envPath = Join-Path $here ".env"
$content = @"
AGRO_WA_DJANGO_URL=https://agro-consulta-wa.onrender.com
AGRO_WA_BRIDGE_TOKEN=$token
"@

# UTF-8 sem BOM (o .bat le mal com BOM)
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($envPath, $content.TrimEnd() + "`n", $utf8NoBom)

Write-Host "Criado: $envPath" -ForegroundColor Green
Write-Host ""
Write-Host "Proximo passo: de um duplo-clique em iniciar.bat (uma janela so)." -ForegroundColor Green
Write-Host "A janela deve mostrar Site: https://agro-consulta-wa.onrender.com" -ForegroundColor Green
Write-Host ""
