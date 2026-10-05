# Garante Node.js portatil em %LOCALAPPDATA%\AgroEtiquetaPrint\node (sem admin).
# Usa o Node do sistema se ja existir.
$ErrorActionPreference = 'Stop'
$ver = 'v22.14.0'
$base = Join-Path $env:LOCALAPPDATA 'AgroEtiquetaPrint'
$nodeHome = Join-Path $base 'node'
$folderName = "node-$($ver.TrimStart('v'))-win-x64"
$nodeDir = Join-Path $nodeHome $folderName
$nodeExe = Join-Path $nodeDir 'node.exe'
$npmCmd = Join-Path $nodeDir 'npm.cmd'

function Has-WorkingNode {
  try {
    $p = Get-Command node -ErrorAction Stop
    $v = & node -v 2>$null
    if ($LASTEXITCODE -eq 0 -and $v) { return $true }
  } catch {}
  return $false
}

if (Has-WorkingNode) {
  Write-Host "Node do sistema OK: $(node -v)"
  exit 0
}

if (Test-Path $nodeExe) {
  Write-Host "Node portatil OK: $nodeExe"
  $env:Path = "$nodeDir;$env:Path"
  [Environment]::SetEnvironmentVariable('AGRO_NODE_DIR', $nodeDir, 'Process')
  exit 0
}

Write-Host "Baixando Node.js $ver (uma vez, pode demorar)..."
New-Item -ItemType Directory -Force -Path $nodeHome | Out-Null
$zipUrl = "https://nodejs.org/dist/$ver/$folderName.zip"
$zipPath = Join-Path $nodeHome "$folderName.zip"

try {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath -UseBasicParsing
} catch {
  Write-Host "ERRO ao baixar Node: $($_.Exception.Message)"
  exit 1
}

if (-not (Test-Path $zipPath)) {
  Write-Host "ERRO: zip do Node nao baixou."
  exit 1
}

Write-Host "Extraindo Node..."
if (Test-Path $nodeDir) { Remove-Item -Recurse -Force $nodeDir }
Expand-Archive -Path $zipPath -DestinationPath $nodeHome -Force
Remove-Item -Force $zipPath -ErrorAction SilentlyContinue

if (-not (Test-Path $nodeExe)) {
  Write-Host "ERRO: node.exe nao encontrado apos extrair."
  exit 1
}

$env:Path = "$nodeDir;$env:Path"
[Environment]::SetEnvironmentVariable('AGRO_NODE_DIR', $nodeDir, 'Process')
Write-Host "Node portatil pronto: $(& $nodeExe -v)"
exit 0
