# Garante Node.js portatil em %LOCALAPPDATA%\AgroEtiquetaPrint\node (sem admin).
# Usa o Node do sistema se ja existir.
# Baixa com fallbacks de versao/espelho (evita 404 em PCs velhos / proxy).
$ErrorActionPreference = 'Stop'

$base = Join-Path $env:LOCALAPPDATA 'AgroEtiquetaPrint'
$nodeHome = Join-Path $base 'node'

function Get-NodeArchFolder {
  $arch = ($env:PROCESSOR_ARCHITECTURE + '').ToUpperInvariant()
  if ($arch -eq 'ARM64') { return 'win-arm64' }
  if ($arch -eq 'X86') { return 'win-x86' }
  # AMD64 e demais -> x64
  return 'win-x64'
}

function Has-WorkingNode {
  try {
    $null = Get-Command node -ErrorAction Stop
    $v = & node -v 2>$null
    if ($LASTEXITCODE -eq 0 -and $v) { return $true }
  } catch {}
  return $false
}

function Test-PortableNode {
  param([string]$Dir)
  $exe = Join-Path $Dir 'node.exe'
  return (Test-Path $exe)
}

if (Has-WorkingNode) {
  Write-Host "Node do sistema OK: $(node -v)"
  exit 0
}

$archFolder = Get-NodeArchFolder
Write-Host "Arquitetura: $archFolder"

# Ja tem portatil?
$existing = Get-ChildItem -Path $nodeHome -Directory -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -like "node-*-$archFolder" -or $_.Name -like 'node-*-win-*' }
foreach ($d in $existing) {
  if (Test-PortableNode $d.FullName) {
    Write-Host "Node portatil OK: $($d.FullName)"
    $env:Path = "$($d.FullName);$env:Path"
    [Environment]::SetEnvironmentVariable('AGRO_NODE_DIR', $d.FullName, 'Process')
    exit 0
  }
}

function Get-LtsVersionFromIndex {
  try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $idx = Invoke-RestMethod -Uri 'https://nodejs.org/dist/index.json' -UseBasicParsing
    foreach ($row in $idx) {
      if ($row.lts -and $row.lts -ne $false -and $row.files -contains $archFolder) {
        return [string]$row.version
      }
    }
  } catch {
    Write-Host "Aviso: nao li index.json ($($_.Exception.Message))"
  }
  return $null
}

$lts = Get-LtsVersionFromIndex
# Ordem: LTS atual (se achou) + pins conhecidos que ainda existem no dist
$versions = @()
if ($lts) { $versions += $lts }
$versions += @('v22.22.0', 'v22.14.0', 'v20.19.0', 'v20.18.0')
$versions = $versions | Select-Object -Unique

$mirrors = @(
  'https://nodejs.org/dist/{0}/{1}.zip',
  'https://npmmirror.com/mirrors/node/{0}/{1}.zip'
)

New-Item -ItemType Directory -Force -Path $nodeHome | Out-Null
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ok = $false
$lastErr = ''
foreach ($ver in $versions) {
  $folderName = "node-$($ver.TrimStart('v'))-$archFolder"
  $zipPath = Join-Path $nodeHome "$folderName.zip"
  $nodeDir = Join-Path $nodeHome $folderName
  $nodeExe = Join-Path $nodeDir 'node.exe'

  foreach ($tpl in $mirrors) {
    $zipUrl = ($tpl -f $ver, $folderName)
    Write-Host "Tentando baixar Node $ver ..."
    Write-Host "  URL: $zipUrl"
    try {
      if (Test-Path $zipPath) { Remove-Item -Force $zipPath -ErrorAction SilentlyContinue }
      Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath -UseBasicParsing
      if (-not (Test-Path $zipPath) -or ((Get-Item $zipPath).Length -lt 1000000)) {
        throw "zip pequeno ou ausente"
      }
      Write-Host "Extraindo..."
      if (Test-Path $nodeDir) { Remove-Item -Recurse -Force $nodeDir }
      Expand-Archive -Path $zipPath -DestinationPath $nodeHome -Force
      Remove-Item -Force $zipPath -ErrorAction SilentlyContinue
      if (-not (Test-Path $nodeExe)) {
        throw "node.exe nao encontrado apos extrair em $nodeDir"
      }
      $env:Path = "$nodeDir;$env:Path"
      [Environment]::SetEnvironmentVariable('AGRO_NODE_DIR', $nodeDir, 'Process')
      Write-Host "Node portatil pronto: $(& $nodeExe -v)"
      $ok = $true
      break
    } catch {
      $lastErr = $_.Exception.Message
      Write-Host "  Falhou: $lastErr"
      Remove-Item -Force $zipPath -ErrorAction SilentlyContinue
    }
  }
  if ($ok) { break }
}

if (-not $ok) {
  Write-Host "ERRO ao baixar Node: $lastErr"
  Write-Host "Dica: instale Node LTS em https://nodejs.org e rode 1-INSTALAR.bat de novo."
  exit 1
}
exit 0
