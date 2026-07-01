# Atalhos Windows — SisVale PDV + Gestão (Chrome modo app)
# Uso: .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://SEU-SITE.onrender.com"
#
# Ícones separados na barra de tarefas:
#   1) Fixe cada atalho .lnk na barra (botão direito → Fixar)
#   2) Ou no Chrome: menu ⋮ → «Instalar SisVale…» em cada janela (manifest por role)

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

$pdvUrl = "$BaseUrl/pdv/?agro_dual=1&agro_app_role=pdv"
$gestaoUrl = "$BaseUrl/dashboard/gerencial/?agro_dual=1&agro_app_role=gestao"

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

New-ChromeAppShortcut "SisVale PDV" $pdvUrl "Balcão (não abrir gestão neste atalho)"
New-ChromeAppShortcut "SisVale Gestão" $gestaoUrl "Gestão e abas (não abrir PDV neste atalho)"

Write-Host ""
Write-Host "Abra os DOIS atalhos na area de trabalho (nao use aba normal do Chrome para operar)."
Write-Host "Fixe cada icone na barra de tarefas para ficarem separados."
Write-Host "Gestao: links internos ficam nesta janela (SistValeGestao). PDV: consultas abrem em painel FECHAR."
