#Requires -Version 5.1
<#
.SYNOPSIS
  Instala os drivers USB-serial da balanca Urano neste Windows.

.DESCRIPTION
  O PDV (Chrome) fala com a Urano via porta COM. Sem driver, o Windows
  nao cria "USB Serial Port (COMx)" e o Pesar nao conecta.

  Instala, nesta ordem:
    1. Silicon Labs CP210x  (adaptador oficial Urano TTL/USB)
    2. Pacote USB da Urano  (mesmo chip, zip do site urano.com.br)
    3. WCH CH340/CH341      (cabo USB-serial generico)
    4. FTDI VCP             (aparece como "USB Serial Port") se o site deixar baixar

  Primeira vez: precisa de internet. Os arquivos ficam em .\cache\
  Depois da para copiar esta pasta inteira no pendrive e instalar offline.

.PARAMETER Offline
  Nao baixa nada; so instala o que ja estiver em .\cache\

.EXAMPLE
  Clique com o direito em INSTALAR-BALANCA.bat -> Executar como administrador
#>
[CmdletBinding()]
param(
  [switch]$Offline
)

$ErrorActionPreference = 'Continue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
try { chcp 65001 | Out-Null } catch {}

function Write-Step([string]$msg) {
  Write-Host ""
  Write-Host ">> $msg" -ForegroundColor Cyan
}

function Write-Ok([string]$msg) { Write-Host "   OK  $msg" -ForegroundColor Green }
function Write-Warn([string]$msg) { Write-Host "   !!  $msg" -ForegroundColor Yellow }
function Write-Fail([string]$msg) { Write-Host "   XX  $msg" -ForegroundColor Red }

function Test-Admin {
  $id = [Security.Principal.WindowsIdentity]::GetCurrent()
  $p = New-Object Security.Principal.WindowsPrincipal($id)
  return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
  Write-Fail "Abra INSTALAR-BALANCA.bat como Administrador (botao direito)."
  exit 1
}

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Cache = Join-Path $Root 'cache'
New-Item -ItemType Directory -Force -Path $Cache | Out-Null

$Downloads = @(
  @{
    Name = 'Silicon Labs CP210x (oficial Urano)'
    File = 'CP210x_Universal_Windows_Driver.zip'
    Url  = 'https://www.silabs.com/documents/public/software/CP210x_Universal_Windows_Driver.zip'
  },
  @{
    Name = 'Silicon Labs CP210x (Windows completo)'
    File = 'CP210x_Windows_Drivers.zip'
    Url  = 'https://www.silabs.com/documents/public/software/CP210x_Windows_Drivers.zip'
  },
  @{
    Name = 'Driver USB Urano (site urano.com.br)'
    File = 'Driver-USB-Urano.zip'
    Url  = 'https://www.urano.com.br/media/wysiwyg/softwares/Driver%20USB%20da%20Bal%20Analitica-Urano%20Lab%20.zip'
  },
  @{
    Name = 'CH340 / CH341 (cabo generico)'
    File = 'CH341SER.EXE'
    Url  = 'https://cdn.sparkfun.com/assets/learn_tutorials/8/4/4/CH341SER.EXE'
  },
  @{
    Name = 'FTDI VCP (USB Serial Port)'
    File = 'CDM2123620_Setup.zip'
    Url  = 'https://ftdichip.com/wp-content/uploads/2025/03/CDM2123620_Setup.zip'
  }
)

function Get-DriverFile($item) {
  $dest = Join-Path $Cache $item.File
  if (Test-Path $dest) {
    $len = (Get-Item $dest).Length
    if ($len -gt 20000) {
      Write-Ok ("Ja tinha {0} ({1:N0} KB)" -f $item.File, ($len / 1KB))
      return $dest
    }
  }
  if ($Offline) {
    Write-Warn ("Offline: falta {0}" -f $item.File)
    return $null
  }
  Write-Host ("   Baixando {0} ..." -f $item.Name)
  try {
    Invoke-WebRequest -Uri $item.Url -OutFile $dest -UseBasicParsing -TimeoutSec 90
  } catch {
    Write-Warn ("Nao baixou {0}: {1}" -f $item.Name, $_.Exception.Message)
    return $null
  }
  if (-not (Test-Path $dest) -or (Get-Item $dest).Length -lt 20000) {
    Write-Warn ("Arquivo pequeno ou vazio: {0}" -f $item.File)
    return $null
  }
  Write-Ok ("Baixou {0} ({1:N0} KB)" -f $item.File, ((Get-Item $dest).Length / 1KB))
  return $dest
}

function Expand-IfZip([string]$path) {
  if ($path -notmatch '\.zip$') { return $path }
  $name = [IO.Path]::GetFileNameWithoutExtension($path)
  $out = Join-Path $Cache $name
  if (-not (Test-Path $out)) {
    New-Item -ItemType Directory -Force -Path $out | Out-Null
    try {
      Expand-Archive -LiteralPath $path -DestinationPath $out -Force
    } catch {
      Write-Warn ("Nao descompactou {0}: {1}" -f $path, $_.Exception.Message)
      return $null
    }
  }
  return $out
}

function Install-InfTree([string]$dir) {
  if (-not $dir -or -not (Test-Path $dir)) { return 0 }
  $infs = Get-ChildItem -LiteralPath $dir -Recurse -Filter *.inf -ErrorAction SilentlyContinue
  if (-not $infs) { return 0 }
  $n = 0
  foreach ($inf in $infs) {
    Write-Host ("   pnputil  {0}" -f $inf.Name)
    $p = Start-Process -FilePath 'pnputil.exe' -ArgumentList @('/add-driver', $inf.FullName, '/install') -Wait -PassThru -WindowStyle Hidden
    if ($p.ExitCode -eq 0 -or $p.ExitCode -eq 259 -or $p.ExitCode -eq 3010) {
      Write-Ok ("Instalou {0}" -f $inf.Name)
      $n++
    } else {
      Write-Warn ("{0} codigo {1} (as vezes o inf nao e deste Windows — segue)" -f $inf.Name, $p.ExitCode)
    }
  }
  return $n
}

function Install-Exe([string]$path) {
  if (-not $path -or -not (Test-Path $path)) { return $false }
  Write-Host ("   Executando {0} ..." -f (Split-Path $path -Leaf))
  $p = Start-Process -FilePath $path -ArgumentList @('/SILENT', '/VERYSILENT', '/NORESTART', '/S') -Wait -PassThru
  if ($p.ExitCode -ne 0) {
    $p = Start-Process -FilePath $path -Wait -PassThru
  }
  if ($p.ExitCode -eq 0 -or $p.ExitCode -eq 3010) {
    Write-Ok ("Instalador {0} terminou" -f (Split-Path $path -Leaf))
    return $true
  }
  Write-Warn ("Instalador {0} codigo {1}" -f (Split-Path $path -Leaf), $p.ExitCode)
  return $false
}

Write-Host ""
Write-Host "  SisVale  |  driver da balanca Urano (USE-P2 / COM USB)" -ForegroundColor White
Write-Host "  PC: $env:COMPUTERNAME   Windows $([Environment]::OSVersion.VersionString)"
Write-Host "  Pasta: $Root"
if ($Offline) { Write-Host "  Modo: OFFLINE (so cache)" -ForegroundColor Yellow }

Write-Step "Baixar / reusar pacotes"
$installed = 0
foreach ($item in $Downloads) {
  $file = Get-DriverFile $item
  if (-not $file) { continue }
  if ($file -match '\.exe$') {
    if (Install-Exe $file) { $installed++ }
    continue
  }
  $dir = Expand-IfZip $file
  $n = Install-InfTree $dir
  $installed += $n
  $setup = Get-ChildItem -LiteralPath $dir -Recurse -Include 'setup.exe', '*Setup.exe', 'CDM*Setup.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($setup) { if (Install-Exe $setup.FullName) { $installed++ } }
}

Write-Step "Pedir ao Windows para reconhecer o USB"
try {
  Start-Process -FilePath 'pnputil.exe' -ArgumentList @('/scan-devices') -Wait -WindowStyle Hidden | Out-Null
  Write-Ok "pnputil /scan-devices"
} catch {
  Write-Warn $_.Exception.Message
}

Write-Step "Portas COM neste PC"
$ports = @()
try {
  $ports = Get-CimInstance Win32_PnPEntity -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '\(COM\d+\)' } |
    Select-Object -ExpandProperty Name
} catch {}
if (-not $ports) {
  $ports = Get-CimInstance Win32_SerialPort -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty DeviceID
}
if ($ports) {
  foreach ($p in $ports) { Write-Ok $p }
} else {
  Write-Warn "Nenhuma porta COM visivel. Conecte o cabo da balanca e rode o script de novo."
}

Write-Step "Chrome / Edge (o PDV usa Web Serial, nao o LePeso)"
$chrome = @(
  "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
  "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
  "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1
$edge = @(
  "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
  "${env:ProgramFiles}\Microsoft\Edge\Application\msedge.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1
if ($chrome) { Write-Ok "Chrome encontrado" } else { Write-Warn "Chrome nao encontrado. Instale o Chrome (ou use o Edge)." }
if ($edge) { Write-Ok "Edge encontrado" }

Write-Host ""
Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray
Write-Host " No outro PC, depois do driver:" -ForegroundColor White
Write-Host "  1. Plugue o cabo USB da balanca." -ForegroundColor White
Write-Host "  2. Na Urano: FUNCAO -> 3 -> IMPRIME ate USE-P2 -> ENTRA." -ForegroundColor White
Write-Host "  3. Abra o PDV no Chrome (https), F10 Pesar, Conectar se pedir." -ForegroundColor White
Write-Host "  4. No popup, marque USB Serial Port (COMx). Nao precisa ser COM4." -ForegroundColor White
Write-Host "  5. Ctrl+F5 se o overlay Pesar estiver antigo." -ForegroundColor White
Write-Host ""
Write-Host " Pendrive: copie a pasta inteira (com cache\) para instalar offline." -ForegroundColor DarkGray
Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray

if ($installed -le 0 -and -not $ports) { exit 2 }
exit 0
