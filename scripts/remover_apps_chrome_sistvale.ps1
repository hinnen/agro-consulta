# Remove apps Chrome/SistVale/SisVale presos (desinstalar pelo Chrome fecha o browser).
#
# Uso (feche o Chrome antes, ou use -FecharChrome):
#   powershell -ExecutionPolicy Bypass -File .\scripts\remover_apps_chrome_sistvale.ps1 -FecharChrome
#
# Depois rode criar_atalhos_sistvale.ps1 -AbrirParaInstalar

param(
    [switch]$FecharChrome,
    [switch]$ListarSomente
)

$ErrorActionPreference = "Stop"

$ProfileDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data\Default"
$WebAppsDir = Join-Path $ProfileDir "Web Applications"
$PrefsPath = Join-Path $ProfileDir "Preferences"
$SecurePrefsPath = Join-Path $ProfileDir "Secure Preferences"
$TaskbarDir = Join-Path $env:APPDATA "Microsoft\Internet Explorer\Quick Launch\User Pinned\TaskBar"

$LabelPatterns = @("*SistVale*", "*SisVale*", "*Consulta*SisVale*", "*Intelig*Negocio*", "*Cadastro*ERP*")

function Get-SistValeWebApps {
    if (-not (Test-Path -LiteralPath $WebAppsDir)) { return @() }
    $list = @()
    foreach ($folder in Get-ChildItem -LiteralPath $WebAppsDir -Directory -Filter "_crx_*") {
        $appId = $folder.Name -replace '^_crx_', ''
        $icon = Get-ChildItem -LiteralPath $folder.FullName -Filter "*.ico.md5" -ErrorAction SilentlyContinue | Select-Object -First 1
        $label = if ($icon) { $icon.Name -replace '\.ico\.md5$', '' } else { $folder.Name }
        $match = $false
        foreach ($pat in $LabelPatterns) {
            if ($label -like $pat) { $match = $true; break }
        }
        if ($match) {
            $list += [PSCustomObject]@{ AppId = $appId; Label = $label; Folder = $folder.FullName }
        }
    }
    return $list
}

function Remove-ShortcutIfSistVale {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return }
    try {
        $s = (New-Object -ComObject WScript.Shell).CreateShortcut($Path)
        $args = $s.Arguments
        $desc = $s.Description
        $name = [IO.Path]::GetFileNameWithoutExtension($Path)
        $hit = $false
        foreach ($pat in $LabelPatterns) {
            if ($name -like $pat -or $desc -like $pat) { $hit = $true; break }
        }
        if ($args -match 'chgnfdn|mdpdael|mlhnmam|pbbiaoj|dodhgdb|sistvale|sistvale\.com') { $hit = $true }
        if ($hit) {
            Remove-Item -LiteralPath $Path -Force
            Write-Host "Atalho removido: $Path" -ForegroundColor Yellow
        }
    } catch {}
}

$apps = Get-SistValeWebApps
Write-Host ""
Write-Host "Apps SisVale/SistVale encontrados:" -ForegroundColor Cyan
if ($apps.Count -eq 0) {
    Write-Host "  (nenhum na pasta Web Applications - pode restar lixo no Preferences)" -ForegroundColor DarkGray
} else {
    $apps | ForEach-Object { Write-Host "  - $($_.Label)  [$($_.AppId)]" }
}

if ($ListarSomente) { exit 0 }

if ($FecharChrome) {
    Write-Host ""
    Write-Host "Fechando Chrome..." -ForegroundColor Yellow
    Get-Process chrome, chrome_proxy -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

if (-not $FecharChrome) {
    $running = Get-Process chrome -ErrorAction SilentlyContinue
    if ($running) {
        Write-Host ""
        Write-Host "ERRO: Chrome ainda aberto. Feche TODAS as janelas ou use -FecharChrome" -ForegroundColor Red
        exit 1
    }
}

# 1) Pastas _crx_
foreach ($app in $apps) {
    if (Test-Path -LiteralPath $app.Folder) {
        Remove-Item -LiteralPath $app.Folder -Recurse -Force
        Write-Host "Pasta removida: $($app.Folder)" -ForegroundColor Green
    }
}

# 2) Atalhos barra / desktop / menu
foreach ($dir in @(
        $TaskbarDir,
        [Environment]::GetFolderPath("Desktop"),
        (Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\SisVale"),
        (Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Chrome Apps")
    )) {
    if (Test-Path -LiteralPath $dir) {
        Get-ChildItem -LiteralPath $dir -Filter "*.lnk" -ErrorAction SilentlyContinue | ForEach-Object {
            Remove-ShortcutIfSistVale -Path $_.FullName
        }
    }
}

# 3) Preferences / Secure Preferences (Python)
$appIds = @($apps | ForEach-Object { $_.AppId })
$pyScript = Join-Path $PSScriptRoot "_remover_web_apps_prefs.py"
@'
import json
import pathlib
import sys

profile = pathlib.Path(sys.argv[1])
app_ids = set(sys.argv[2:])

def scrub(obj, path=""):
    if isinstance(obj, dict):
        keys = list(obj.keys())
        for k in keys:
            full = f"{path}.{k}" if path else k
            drop = False
            if k.startswith("_crx_"):
                for aid in app_ids:
                    if aid in k:
                        drop = True
                        break
            if not drop:
                for aid in app_ids:
                    if k == aid or (isinstance(obj.get(k), str) and aid in str(obj[k])):
                        drop = True
                        break
            if drop:
                del obj[k]
            else:
                scrub(obj[k], full)
    elif isinstance(obj, list):
        for i in range(len(obj) - 1, -1, -1):
            if isinstance(obj[i], str) and any(aid in obj[i] for aid in app_ids):
                del obj[i]
            else:
                scrub(obj[i], path)

for name in ("Preferences", "Secure Preferences"):
    p = profile / name
    if not p.exists():
        continue
    raw = p.read_text(encoding="utf-8")
    data = json.loads(raw)
    scrub(data)
    bak = p.with_suffix(p.suffix + ".bak-sistvale")
    bak.write_text(raw, encoding="utf-8")
    p.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print("OK prefs", name)
'@ | Set-Content -Path $pyScript -Encoding UTF8

if ($appIds.Count -gt 0 -and (Test-Path -LiteralPath $PrefsPath)) {
    & python $pyScript $ProfileDir @appIds
    Remove-Item -LiteralPath $pyScript -Force -ErrorAction SilentlyContinue
} elseif (Test-Path -LiteralPath $PrefsPath) {
    # fallback: remove chgnfdn conhecido
    & python $pyScript $ProfileDir "chgnfdnhdadneofjflhceelponcohfgc"
    Remove-Item -LiteralPath $pyScript -Force -ErrorAction SilentlyContinue
}

# 4) Registro Windows -> Apps instalados (Configuracoes) — causa do fantasma
$uninstallRoot = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall"
if (Test-Path $uninstallRoot) {
    Get-ChildItem -Path $uninstallRoot -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            $p = Get-ItemProperty -LiteralPath $_.PSPath -ErrorAction SilentlyContinue
            if (-not $p) { return }
            $name = [string]$p.DisplayName
            $uninst = [string]$p.UninstallString
            $hit = $false
            foreach ($pat in $LabelPatterns) {
                if ($name -like $pat) { $hit = $true; break }
            }
            if ($uninst -match 'uninstall-app-id=(chgnfdn|mdpdael|mlhnmam|pbbiaoj|dodhgdb)') { $hit = $true }
            if ($hit) {
                Remove-Item -LiteralPath $_.PSPath -Recurse -Force
                Write-Host "Apps Windows removido: $name ($($_.PSChildName))" -ForegroundColor Green
            }
        } catch {}
    }
}

# 5) Tiles Start / CloudStore
$regPaths = @(
    "HKCU:\Software\Microsoft\Windows\CurrentVersion\Start\TileProperties",
    "HKCU:\Software\Microsoft\Windows\CurrentVersion\CloudStore\Store\DefaultAccount\Current"
)
foreach ($base in $regPaths) {
    if (-not (Test-Path $base)) { continue }
    Get-ChildItem -Path $base -Recurse -ErrorAction SilentlyContinue | Where-Object {
        $_.PSChildName -match 'SistVale|SisVale|chgnfdn|mdpdael|mlhnmam'
    } | ForEach-Object {
        try {
            Remove-Item -LiteralPath $_.PSPath -Recurse -Force -ErrorAction SilentlyContinue
            Write-Host "Registro removido: $($_.PSChildName)" -ForegroundColor DarkYellow
        } catch {}
    }
}

Write-Host ""
Write-Host "Pronto. Abra o Chrome de novo." -ForegroundColor Green
Write-Host "Configuracoes Windows -> Apps: SistVale deve sumir AGORA (feche e reabra Configuracoes)." -ForegroundColor DarkCyan
Write-Host "Proximo: criar_atalhos_sistvale.ps1 -BaseUrl https://sistvale.com.br -AbrirParaInstalar" -ForegroundColor Cyan
