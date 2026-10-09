# SisVale — instalador único: PDV + Gestão (Chrome PWA) · Windows 10/11
#
# Uso (recomendado): dois cliques em INSTALAR-SISVALE-PDV-GESTAO.bat
# Ou:
#   powershell -ExecutionPolicy Bypass -File .\instalar_sistvale_pdv_gestao.ps1
#   powershell -ExecutionPolicy Bypass -File .\instalar_sistvale_pdv_gestao.ps1 -BaseUrl "https://agro-consulta-staging.onrender.com"
#
# O que faz sozinho:
#   · Verifica / baixa / instala Google Chrome se não existir
#   · Remove apps Chrome antigos (SistVale único) que impedem instalar PDV+Gestão
#   · Abre as 2 páginas certas e ESPERA você clicar "Instalar página como app"
#   · Detecta app-id, cria atalhos chrome_proxy, área de trabalho e barra de tarefas
#
# Limitação (Chrome): instalar PWA ainda exige 2 cliques no menu — não há API pública 100% silenciosa.

param(
    [string]$BaseUrl = "https://sistvale.com.br",
    [string]$ChromePath = "",
    [string]$ChromeProxyPath = "",
    [string]$ProfileDirectory = "Default",
    [switch]$PularChrome,
    [switch]$PularLimpeza,
    [int]$AguardarMinutos = 20,
    [string]$PdvAppId = "",
    [string]$GestaoAppId = ""
)

$ErrorActionPreference = "Stop"

$AtalhosDir = Join-Path $env:LOCALAPPDATA "SisVale\Atalhos"
$StartMenuDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\SisVale"
$TaskbarDir = Join-Path $env:APPDATA "Microsoft\Internet Explorer\Quick Launch\User Pinned\TaskBar"
$WebAppsDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data\$ProfileDirectory\Web Applications"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$BaseUrl = $BaseUrl.Trim().TrimEnd("/")
if ($BaseUrl -notmatch "^https?://") {
    Write-Error "BaseUrl invalida. Exemplo: https://sistvale.com.br"
    exit 1
}

$pdvUrl = "$BaseUrl/pdv/?agro_dual=1&agro_app_role=pdv"
$gestaoUrl = "$BaseUrl/dashboard/gerencial/?agro_dual=1&agro_app_role=gestao"

function Write-Step {
    param([string]$Text)
    Write-Host ""
    Write-Host "==> $Text" -ForegroundColor Cyan
}

function Show-InfoBox {
    param([string]$Title, [string]$Message)
    try {
        Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
        [System.Windows.Forms.MessageBox]::Show($Message, $Title, [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
    } catch {
        Write-Host $Message -ForegroundColor Yellow
    }
}

function Ensure-Tls12 {
    try {
        if ([Net.ServicePointManager]::SecurityProtocol -band [Net.SecurityProtocolType]::Tls12) { return }
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    } catch {}
}

function Resolve-ChromePaths {
    if (-not $ChromePath) {
        $candidates = @(
            "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
            "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
            "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
        )
        foreach ($c in $candidates) {
            if (Test-Path -LiteralPath $c) { $script:ChromePath = $c; break }
        }
    }
    if ($ChromePath -and -not $ChromeProxyPath) {
        $script:ChromeProxyPath = Join-Path (Split-Path $ChromePath) "chrome_proxy.exe"
    }
}

function Install-GoogleChromeIfMissing {
    if ($PularChrome) { return }
    Resolve-ChromePaths
    if (Test-Path -LiteralPath $ChromePath) {
        Write-Host "Chrome OK: $ChromePath" -ForegroundColor Green
        return
    }

    Write-Step "Google Chrome nao encontrado — baixando instalador oficial..."
    Ensure-Tls12
    $dest = Join-Path $env:TEMP "ChromeSetup-SisVale.exe"
    $uri = "https://dl.google.com/chrome/install/latest/chrome_installer.exe"
    try {
        Invoke-WebRequest -Uri $uri -OutFile $dest -UseBasicParsing
    } catch {
        Write-Host "Falha no download. Instale Chrome manualmente: https://www.google.com/chrome/" -ForegroundColor Red
        throw
    }

    Write-Host "Instalando Chrome (pode pedir permissao UAC)..."
    $proc = Start-Process -FilePath $dest -ArgumentList "/silent", "/install" -Wait -PassThru
    Start-Sleep -Seconds 3
    Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue

    $script:ChromePath = ""
    $script:ChromeProxyPath = ""
    Resolve-ChromePaths
    if (-not (Test-Path -LiteralPath $ChromePath)) {
        Write-Host "Chrome ainda nao apareceu. Abra o Chrome uma vez e rode o instalador de novo." -ForegroundColor Red
        exit 1
    }
    Write-Host "Chrome instalado: $ChromePath" -ForegroundColor Green
}

function Get-ChromeWebAppIdByLabel {
    param([string[]]$LabelPatterns)
    if (-not (Test-Path -LiteralPath $WebAppsDir)) { return $null }
    foreach ($folder in Get-ChildItem -LiteralPath $WebAppsDir -Directory -Filter "_crx_*" -ErrorAction SilentlyContinue) {
        $appId = $folder.Name -replace '^_crx_', ''
        $icon = Get-ChildItem -LiteralPath $folder.FullName -Filter "*.ico.md5" -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $icon) { continue }
        $label = $icon.Name -replace '\.ico\.md5$', ''
        foreach ($pat in $LabelPatterns) {
            if ($label -like $pat) {
                return @{ Id = $appId; Label = $label }
            }
        }
    }
    return $null
}

function Get-DetectedPdvGestaoIds {
    param([string]$ForcePdv = "", [string]$ForceGestao = "")
    $pdv = $ForcePdv
    $gest = $ForceGestao
    if (-not $pdv) {
        $found = Get-ChromeWebAppIdByLabel -LabelPatterns @("SisVale PDV", "SisVale PDV *", "*SisVale*PDV*")
        if ($found) { $pdv = $found.Id }
    }
    if (-not $gest) {
        $found = Get-ChromeWebAppIdByLabel -LabelPatterns @(
            "SisVale Gestao", "SisVale Gest*o", "*SisVale*Gest*", "*SisVale*Intelig*", "*Intelig*Neg*"
        )
        if ($found) { $gest = $found.Id }
    }
    return @{ Pdv = $pdv; Gestao = $gest }
}

function Test-HasLegacySistValeApps {
    $ids = Get-DetectedPdvGestaoIds
    if ($ids.Pdv -and $ids.Gestao) { return $false }
    if (-not (Test-Path -LiteralPath $WebAppsDir)) { return $false }
    foreach ($folder in Get-ChildItem -LiteralPath $WebAppsDir -Directory -Filter "_crx_*" -ErrorAction SilentlyContinue) {
        $icon = Get-ChildItem -LiteralPath $folder.FullName -Filter "*.ico.md5" -ErrorAction SilentlyContinue | Select-Object -First 1
        $label = if ($icon) { $icon.Name -replace '\.ico\.md5$', '' } else { "" }
        if ($label -match 'SistVale|SisVale|Consulta|Intelig|Cadastro') {
            if ($label -notlike "*SisVale PDV*" -and $label -notlike "*SisVale Gest*") {
                return $true
            }
        }
    }
    return $false
}

function Invoke-LegacyCleanup {
    $remover = Join-Path $ScriptDir "remover_apps_chrome_sistvale.ps1"
    if (-not (Test-Path -LiteralPath $remover)) {
        Write-Host "Aviso: remover_apps_chrome_sistvale.ps1 nao encontrado — limpe manualmente em chrome://apps" -ForegroundColor Yellow
        return
    }
    Write-Step "Removendo apps Chrome antigos (SistVale unico)..."
    & powershell -NoProfile -ExecutionPolicy Bypass -File $remover -FecharChrome
}

function Remove-OldTaskbarPins {
    if (-not (Test-Path -LiteralPath $TaskbarDir)) { return }
    $patterns = @("SisVale*", "SistVale*", "SisVale   Intelig*")
    foreach ($pat in $patterns) {
        Get-ChildItem -LiteralPath $TaskbarDir -Filter $pat -ErrorAction SilentlyContinue | ForEach-Object {
            try {
                $target = (New-Object -ComObject WScript.Shell).CreateShortcut($_.FullName).TargetPath
                $name = Split-Path $target -Leaf
                if ($name -ieq "chrome.exe") {
                    Remove-Item -LiteralPath $_.FullName -Force
                    Write-Host "Barra: removido pin chrome.exe ($($_.Name))" -ForegroundColor DarkYellow
                }
                elseif ($_.Name -match '\(\d+\)\.lnk$') {
                    Remove-Item -LiteralPath $_.FullName -Force
                    Write-Host "Barra: removido duplicata $($_.Name)" -ForegroundColor DarkYellow
                }
            } catch {}
        }
    }
}

function New-ChromeProxyShortcut {
    param(
        [string]$TargetPath,
        [string]$AppId,
        [string]$Description
    )
    Resolve-ChromePaths
    $wsh = New-Object -ComObject WScript.Shell
    $sc = $wsh.CreateShortcut($TargetPath)
    $sc.TargetPath = $ChromeProxyPath
    $sc.Arguments = "--profile-directory=$ProfileDirectory --app-id=$AppId"
    $sc.WorkingDirectory = Split-Path $ChromeProxyPath
    $sc.WindowStyle = 1
    $sc.Description = $Description
    try { $sc.IconLocation = "$ChromeProxyPath,0" } catch {}
    $sc.Save()
    Write-Host "OK: $TargetPath  (app-id $AppId)" -ForegroundColor Green
}

function Publish-Shortcut {
    param(
        [string]$Name,
        [string]$AppId,
        [string]$Hint
    )
    foreach ($dir in @($AtalhosDir, $StartMenuDir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    $canonical = Join-Path $AtalhosDir "$Name.lnk"
    New-ChromeProxyShortcut -TargetPath $canonical -AppId $AppId -Description "SisVale - $Hint"
    New-ChromeProxyShortcut -TargetPath (Join-Path $StartMenuDir "$Name.lnk") -AppId $AppId -Description "SisVale - $Hint"

    $desktop = [Environment]::GetFolderPath("Desktop")
    Copy-Item -LiteralPath $canonical -Destination (Join-Path $desktop "$Name.lnk") -Force

    if (-not (Test-Path -LiteralPath $TaskbarDir)) {
        New-Item -ItemType Directory -Force -Path $TaskbarDir | Out-Null
    }
    Copy-Item -LiteralPath $canonical -Destination (Join-Path $TaskbarDir "$Name.lnk") -Force
    Write-Host "Barra + area de trabalho: $Name" -ForegroundColor Cyan
}

function Open-InstallWindows {
    Resolve-ChromePaths
    Write-Step "Abrindo PDV e Gestao no Chrome..."
    Start-Process -FilePath $ChromePath -ArgumentList "--new-window `"$pdvUrl`""
    Start-Sleep -Seconds 2
    Start-Process -FilePath $ChromePath -ArgumentList "--new-window `"$gestaoUrl`""
}

function Wait-ForPwaInstall {
    param([int]$Minutes)
    $deadline = (Get-Date).AddMinutes($Minutes)
    Write-Host ""
    Write-Host "Aguardando voce instalar os 2 apps no Chrome (ate $Minutes min)..." -ForegroundColor Yellow
    Write-Host "  Janela 1: menu (...) -> Instalar pagina como app -> nome: SisVale PDV"
    Write-Host "  Janela 2: menu (...) -> Instalar pagina como app -> nome: SisVale Gestao"
    Write-Host "  Se so aparecer 'Criar atalho': marque Abrir como janela -> Criar (2x)"
    Write-Host ""

    while ((Get-Date) -lt $deadline) {
        $ids = Get-DetectedPdvGestaoIds -ForcePdv $PdvAppId -ForceGestao $GestaoAppId
        if ($ids.Pdv -and $ids.Gestao) {
            $script:PdvAppId = $ids.Pdv
            $script:GestaoAppId = $ids.Gestao
            Write-Host ""
            Write-Host "Detectado PDV + Gestao!" -ForegroundColor Green
            return $true
        }
        $missing = @()
        if (-not $ids.Pdv) { $missing += "PDV" }
        if (-not $ids.Gestao) { $missing += "Gestao" }
        Write-Host ("  ... falta: {0}  ({1})" -f ($missing -join ", "), (Get-Date -Format "HH:mm:ss")) -ForegroundColor DarkGray
        Start-Sleep -Seconds 4
    }
    return $false
}

# --- main ---

Write-Host ""
Write-Host "========================================" -ForegroundColor White
Write-Host "  SisVale — Instalador PDV + Gestao" -ForegroundColor White
Write-Host "  Site: $BaseUrl" -ForegroundColor DarkGray
Write-Host "========================================" -ForegroundColor White

if ($env:OS -notmatch "Windows") {
    Write-Host "Este script e somente para Windows." -ForegroundColor Red
    exit 1
}

Install-GoogleChromeIfMissing
Resolve-ChromePaths
if (-not (Test-Path -LiteralPath $ChromePath) -or -not (Test-Path -LiteralPath $ChromeProxyPath)) {
    Write-Host "ERRO: chrome.exe ou chrome_proxy.exe nao encontrado." -ForegroundColor Red
    exit 1
}

if (-not $PularLimpeza -and (Test-HasLegacySistValeApps)) {
    Invoke-LegacyCleanup
    Start-Sleep -Seconds 2
}

$detected = Get-DetectedPdvGestaoIds -ForcePdv $PdvAppId -ForceGestao $GestaoAppId
if (-not $detected.Pdv -or -not $detected.Gestao) {
    Show-InfoBox -Title "SisVale — instalar 2 apps" -Message @"
Vao abrir 2 janelas do Chrome.

Em CADA uma:
  Menu (3 pontos) -> Salvar e compartilhar
  -> Instalar pagina como app

Nomes EXATOS:
  • SisVale PDV
  • SisVale Gestao

Este instalador continua sozinho quando detectar os dois.
"@
    Open-InstallWindows
    if (-not (Wait-ForPwaInstall -Minutes $AguardarMinutos)) {
        Write-Host ""
        Write-Host "ERRO: tempo esgotado. Instale os 2 apps e rode o script de novo." -ForegroundColor Red
        Write-Host "Se ja instalou, feche o Chrome e execute novamente." -ForegroundColor Yellow
        exit 1
    }
} else {
    $script:PdvAppId = $detected.Pdv
    $script:GestaoAppId = $detected.Gestao
    Write-Host "Apps ja instalados — PDV $($PdvAppId) · Gestao $($GestaoAppId)" -ForegroundColor Green
}

Write-Step "Criando atalhos e fixando na barra..."
Remove-OldTaskbarPins
try {
    Publish-Shortcut -Name "SisVale PDV" -AppId $PdvAppId -Hint "Balcao"
    Publish-Shortcut -Name "SisVale Gestao" -AppId $GestaoAppId -Hint "Gestao"
} catch {
    Write-Host "ERRO ao criar atalhos: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Pronto! Use SisVale PDV e SisVale Gestao na barra." -ForegroundColor Green
Write-Host "  Nao use icone globo / Chrome generico." -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

Show-InfoBox -Title "SisVale" -Message "Instalacao concluida.`n`nBarra de tarefas: SisVale PDV + SisVale Gestao.`nDesfixe pins antigos (SistVale, Inteligencia) se ainda existirem."

exit 0
