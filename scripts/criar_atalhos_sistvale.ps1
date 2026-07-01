# Atalhos Windows - SisVale PDV + Gestao (Chrome modo app)
#
# ONDE RODAR (PC da loja, uma vez):
#   1) Tecla Windows -> digite PowerShell -> Enter
#   2) cd "C:\Users\RenanHinnen\OneDrive\Documentos\GitHub\agro-consulta"
#   3) powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1
#
# Loja: -BaseUrl "https://SEU-DOMINIO"
# Teste: omita -BaseUrl (staging padrao)

param(
    [string]$BaseUrl = "https://agro-consulta-staging.onrender.com",
    [string]$ChromePath = "",
    [string]$Desktop = [Environment]::GetFolderPath("Desktop")
)

$ErrorActionPreference = "Stop"

$BaseUrl = $BaseUrl.Trim().TrimEnd("/")
if ($BaseUrl -notmatch "^https?://") {
    Write-Error "BaseUrl invalida. Exemplo: https://agro-consulta-staging.onrender.com"
    exit 1
}

if (-not $ChromePath) {
    $candidates = @(
        "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) {
            $ChromePath = $c
            break
        }
    }
}

if (-not $ChromePath -or -not (Test-Path -LiteralPath $ChromePath)) {
    Write-Host ""
    Write-Host "ERRO: Google Chrome nao encontrado neste PC." -ForegroundColor Red
    Write-Host "Instale o Chrome ou use -ChromePath com o caminho do chrome.exe" -ForegroundColor Yellow
    exit 1
}

$pdvUrl = "$BaseUrl/pdv/?agro_dual=1&agro_app_role=pdv"
$gestaoUrl = "$BaseUrl/dashboard/gerencial/?agro_dual=1&agro_app_role=gestao"

function New-ChromeAppShortcut {
    param(
        [string]$Name,
        [string]$Url,
        [string]$IconHint
    )
    $lnk = Join-Path $Desktop "$Name.lnk"
    $wsh = New-Object -ComObject WScript.Shell
    $sc = $wsh.CreateShortcut($lnk)
    $sc.TargetPath = $ChromePath
    $sc.Arguments = "--app=`"$Url`""
    $sc.WorkingDirectory = Split-Path $ChromePath
    $sc.WindowStyle = 1
    $sc.Description = "SisVale - $IconHint"
    $sc.Save()
    Write-Host "OK: $lnk" -ForegroundColor Green
}

Write-Host ""
Write-Host "Chrome: $ChromePath"
Write-Host "Site:   $BaseUrl"
Write-Host "Desktop: $Desktop"
Write-Host ""

try {
    New-ChromeAppShortcut -Name "SisVale PDV" -Url $pdvUrl -IconHint "Balcao"
    New-ChromeAppShortcut -Name "SisVale Gestao" -Url $gestaoUrl -IconHint "Gestao"
}
catch {
    Write-Host ""
    Write-Host "ERRO ao criar atalho: $_" -ForegroundColor Red
    Write-Host "Use: powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "Pronto! Na area de trabalho: SisVale PDV.lnk e SisVale Gestao.lnk" -ForegroundColor Cyan
Write-Host "Abra os DOIS atalhos. Opcional: fixar cada um na barra de tarefas."
