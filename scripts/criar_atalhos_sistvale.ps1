# Atalhos Windows - SisVale PDV + Gestao (Chrome app instalado)
#
# IMPORTANTE: use apps INSTALADOS do Chrome (chrome_proxy), nao chrome.exe --app=.
# Misturar perfis/ tipos faz abrir aba no Chrome generico (icone globo).
#
# Passo a passo loja:
#   1) powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1 `
#        -BaseUrl "https://sistvale.com.br" -AbrirParaInstalar
#   2) Em cada janela: Chrome menu (3 pontos) -> Salvar e compartilhar ->
#      Instalar pagina como app -> nome "SisVale PDV" e "SisVale Gestao"
#   3) Rode de novo com -FixarBarra (detecta app-id automatico)
#
#   -LimparBarra remove pins antigos duplicados (SistVale.lnk, chrome.exe, etc.)

param(
    [string]$BaseUrl = "https://agro-consulta-staging.onrender.com",
    [string]$ChromePath = "",
    [string]$ChromeProxyPath = "",
    [string]$ProfileDirectory = "Default",
    [switch]$Desktop,
    [switch]$FixarBarra,
    [switch]$LimparBarra,
    [switch]$AbrirParaInstalar,
    [string]$PdvAppId = "",
    [string]$GestaoAppId = ""
)

$ErrorActionPreference = "Stop"

$AtalhosDir = Join-Path $env:LOCALAPPDATA "SisVale\Atalhos"
$StartMenuDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\SisVale"
$TaskbarDir = Join-Path $env:APPDATA "Microsoft\Internet Explorer\Quick Launch\User Pinned\TaskBar"
$WebAppsDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data\$ProfileDirectory\Web Applications"

$BaseUrl = $BaseUrl.Trim().TrimEnd("/")
if ($BaseUrl -notmatch "^https?://") {
    Write-Error "BaseUrl invalida. Exemplo: https://sistvale.com.br"
    exit 1
}

foreach ($dir in @($AtalhosDir, $StartMenuDir)) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

if (-not $ChromePath) {
    $candidates = @(
        "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { $ChromePath = $c; break }
    }
}

if (-not $ChromeProxyPath) {
    $ChromeProxyPath = Join-Path (Split-Path $ChromePath) "chrome_proxy.exe"
}

if (-not (Test-Path -LiteralPath $ChromePath)) {
    Write-Host "ERRO: chrome.exe nao encontrado." -ForegroundColor Red
    exit 1
}
if (-not (Test-Path -LiteralPath $ChromeProxyPath)) {
    Write-Host "ERRO: chrome_proxy.exe nao encontrado." -ForegroundColor Red
    exit 1
}

$pdvUrl = "$BaseUrl/pdv/?agro_dual=1&agro_app_role=pdv"
$gestaoUrl = "$BaseUrl/dashboard/gerencial/?agro_dual=1&agro_app_role=gestao"

function Remove-OldTaskbarPins {
    if (-not (Test-Path -LiteralPath $TaskbarDir)) { return }
    $patterns = @(
        "SisVale*",
        "SistVale*",
        "SisVale   Intelig*"
    )
    foreach ($pat in $patterns) {
        Get-ChildItem -LiteralPath $TaskbarDir -Filter $pat -ErrorAction SilentlyContinue | ForEach-Object {
            $target = (New-Object -ComObject WScript.Shell).CreateShortcut($_.FullName).TargetPath
            $name = Split-Path $target -Leaf
            # Remove chrome.exe --app pins (geram icone globo). Mantemos so se for chrome_proxy.
            if ($name -ieq "chrome.exe") {
                Remove-Item -LiteralPath $_.FullName -Force
                Write-Host "Removido da barra (chrome.exe): $($_.Name)" -ForegroundColor DarkYellow
            }
            elseif ($_.Name -match '\(\d+\)\.lnk$') {
                Remove-Item -LiteralPath $_.FullName -Force
                Write-Host "Removido duplicata: $($_.Name)" -ForegroundColor DarkYellow
            }
        }
    }
}

function Get-ChromeWebAppIdByLabel {
    param([string[]]$LabelPatterns)
    if (-not (Test-Path -LiteralPath $WebAppsDir)) { return $null }
    foreach ($folder in Get-ChildItem -LiteralPath $WebAppsDir -Directory -Filter "_crx_*") {
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

function New-ChromeProxyShortcut {
    param(
        [string]$TargetPath,
        [string]$AppId,
        [string]$Description
    )
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
    $canonical = Join-Path $AtalhosDir "$Name.lnk"
    New-ChromeProxyShortcut -TargetPath $canonical -AppId $AppId -Description "SisVale - $Hint"
    New-ChromeProxyShortcut -TargetPath (Join-Path $StartMenuDir "$Name.lnk") -AppId $AppId -Description "SisVale - $Hint"

    if ($Desktop) {
        Copy-Item -LiteralPath $canonical -Destination (Join-Path ([Environment]::GetFolderPath("Desktop")) "$Name.lnk") -Force
    }

    if ($FixarBarra) {
        if (-not (Test-Path -LiteralPath $TaskbarDir)) {
            New-Item -ItemType Directory -Force -Path $TaskbarDir | Out-Null
        }
        Copy-Item -LiteralPath $canonical -Destination (Join-Path $TaskbarDir "$Name.lnk") -Force
        Write-Host "Barra: $Name.lnk" -ForegroundColor Cyan
    }
}

if ($LimparBarra) {
    Remove-OldTaskbarPins
    if (-not $FixarBarra -and -not $AbrirParaInstalar -and -not $PdvAppId -and -not $GestaoAppId) {
        Write-Host "Barra limpa (pins chrome.exe removidos)." -ForegroundColor Green
        Write-Host "Proximo: -AbrirParaInstalar -> instalar 2 apps -> -FixarBarra -Desktop" -ForegroundColor Cyan
        exit 0
    }
}

if ($AbrirParaInstalar) {
    Write-Host ""
    Write-Host "=== PASSO 0 (se o menu so mostra 'Abrir no app SistVale') ===" -ForegroundColor Yellow
    Write-Host "Ja existe app antigo instalado. Remova ANTES:"
    Write-Host "  1) Chrome: digite na barra  chrome://apps"
    Write-Host "  2) Botao direito em SistVale / Inteligencia / Consulta -> Remover"
    Write-Host "  3) Volte aqui e rode -AbrirParaInstalar de novo"
    Write-Host ""
    Write-Host "=== PASSO 1 - abrindo 2 janelas ===" -ForegroundColor Yellow
    Write-Host "Em CADA janela: menu (3 pontos) -> Salvar e compartilhar ->"
    Write-Host "  Se aparecer 'Instalar pagina como app' -> use e nomeie:"
    Write-Host "    Janela 1: SisVale PDV"
    Write-Host "    Janela 2: SisVale Gestao"
    Write-Host "  Se SO aparecer 'Criar atalho...' -> marque Abrir como janela -> Criar"
    Write-Host "    (depois fixe o atalho da area de trabalho na barra)"
    Write-Host ""
    Start-Process -FilePath $ChromePath -ArgumentList "--new-window `"$pdvUrl`""
    Start-Sleep -Seconds 2
    Start-Process -FilePath $ChromePath -ArgumentList "--new-window `"$gestaoUrl`""
    Write-Host "Depois: ... -FixarBarra -Desktop -LimparBarra" -ForegroundColor Cyan
    exit 0
}

# Detectar app-id pelos nomes dados na instalacao
if (-not $PdvAppId) {
    $found = Get-ChromeWebAppIdByLabel -LabelPatterns @("*SisVale*PDV*", "SisVale PDV", "SisVale PDV *", "*PDV*")
    if ($found) { $PdvAppId = $found.Id; Write-Host "Detectado PDV: $($found.Label) -> $PdvAppId" -ForegroundColor DarkCyan }
}
if (-not $GestaoAppId) {
    $found = Get-ChromeWebAppIdByLabel -LabelPatterns @(
        "*SisVale*Gest*",
        "*SisVale*Intelig*",
        "*Intelig*Neg*",
        "SisVale Gestao",
        "SisVale Gest*o",
        "*Gestao*"
    )
    if ($found) { $GestaoAppId = $found.Id; Write-Host "Detectado Gestao: $($found.Label) -> $GestaoAppId" -ForegroundColor DarkCyan }
}

if (-not $PdvAppId -or -not $GestaoAppId) {
    Write-Host ""
    Write-Host "ERRO: app-id PDV/Gestao nao encontrado." -ForegroundColor Red
    Write-Host "Rode primeiro: ... -AbrirParaInstalar"
    Write-Host "Instale as 2 paginas como app (nomes SisVale PDV / SisVale Gestao)."
    Write-Host "Ou passe -PdvAppId / -GestaoAppId manualmente."
    exit 1
}

Write-Host ""
Write-Host "Chrome proxy: $ChromeProxyPath"
Write-Host "Perfil:       $ProfileDirectory (Chrome normal da loja)"
Write-Host "Site:         $BaseUrl"
Write-Host ""

try {
    Publish-Shortcut -Name "SisVale PDV" -AppId $PdvAppId -Hint "Balcao"
    Publish-Shortcut -Name "SisVale Gestao" -AppId $GestaoAppId -Hint "Gestao"
}
catch {
    Write-Host "ERRO: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Pronto!" -ForegroundColor Green
Write-Host ""
Write-Host "Fixar na barra: botao direito no .lnk -> Fixar  OU  ja usou -FixarBarra." -ForegroundColor Yellow
Write-Host "Nao use o icone globo/Chrome generico - use SisVale PDV e SisVale Gestao." -ForegroundColor Yellow
Write-Host "Desfixe pins antigos: SistVale, Inteligencia, Consulta (se ainda estiverem)." -ForegroundColor DarkYellow
