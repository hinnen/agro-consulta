# Atalhos Windows — SisVale PDV + Gestão (Chrome modo app)
# Uso: .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://SEU-SITE.onrender.com"

param(
    [Parameter(Mandatory = $true)]
    [string]$BaseUrl,
    [string]$ChromePath = "",
    [string]$Desktop = [Environment]::GetFolderPath("Desktop")
)

$BaseUrl = $BaseUrl.TrimEnd("/")

if (-not $ChromePath) {
    $candidates = @(
        "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { $ChromePath = $c; break }
    }
}

if (-not (Test-Path $ChromePath)) {
    Write-Error "Chrome não encontrado. Passe -ChromePath."
    exit 1
}

$pdvUrl = "$BaseUrl/pdv/?agro_dual=1"
$gestaoUrl = "$BaseUrl/dashboard/gerencial/?agro_dual=1"

function New-ChromeAppShortcut($Name, $Url, $IconHint) {
    $lnk = Join-Path $Desktop "$Name.lnk"
    $wsh = New-Object -ComObject WScript.Shell
    $sc = $wsh.CreateShortcut($lnk)
    $sc.TargetPath = $ChromePath
    $sc.Arguments = "--app=`"$Url`""
    $sc.WorkingDirectory = Split-Path $ChromePath
    $sc.WindowStyle = 1
    $sc.Description = "SisVale — $IconHint"
    $sc.Save()
    Write-Host "OK: $lnk"
}

New-ChromeAppShortcut "SisVale PDV" $pdvUrl "Balcão"
New-ChromeAppShortcut "SisVale Gestão" $gestaoUrl "Gestão e abas"

Write-Host ""
Write-Host "Abra os dois atalhos na área de trabalho. PDV e Gestão ficam em janelas separadas."
