param(
  [string]$RemoteHost = "",
  [string]$RemoteUser = "",
  [int]$Port = 22,
  [string]$AppDir = "",
  [string]$KeyPath = "",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$StageDir = Join-Path $RepoRoot "output\snapshot-worker-rpi-aarch64"
$ArchivePath = Join-Path $RepoRoot "output\snapshot-worker-rpi-aarch64.tar"

if (-not $RemoteHost) { $RemoteHost = $env:RPI_HOST }
if (-not $RemoteUser) { $RemoteUser = $env:RPI_USER }
if (-not $AppDir) { $AppDir = $env:RPI_SNAPSHOT_WORKER_APP_DIR }
if (-not $KeyPath) { $KeyPath = $env:RPI_SSH_KEY }
if (-not $AppDir -and $RemoteUser) { $AppDir = "/home/$RemoteUser/app/rpf-listings-snapshot-worker" }

if (-not $RemoteHost -or -not $RemoteUser) {
  throw "RemoteHost and RemoteUser are required. Use -RemoteHost/-RemoteUser or set RPI_HOST/RPI_USER."
}

if (-not $SkipBuild) {
  & (Join-Path $RepoRoot "scripts\build-snapshot-worker-rpi-aarch64.ps1")
  if ($LASTEXITCODE -ne 0) {
    throw "Raspberry Pi snapshot worker cross-build failed."
  }
}

if (-not (Test-Path $StageDir)) {
  throw "Missing staged bundle at: $StageDir"
}

$Target = "$RemoteUser@$RemoteHost"
$SshArgs = @("-p", "$Port")
$ScpArgs = @("-P", "$Port")
if ($KeyPath) {
  $SshArgs += @("-i", $KeyPath)
  $ScpArgs += @("-i", $KeyPath)
}

if (Test-Path $ArchivePath) {
  Remove-Item $ArchivePath -Force
}

& ssh @SshArgs $Target "mkdir -p '$AppDir'"
if ($LASTEXITCODE -ne 0) {
  throw "Failed to prepare remote app directory: $AppDir"
}

Push-Location $StageDir
try {
  & tar.exe -cf $ArchivePath .
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create deployment archive."
  }
}
finally {
  Pop-Location
}

Push-Location $RepoRoot
try {
  & scp @ScpArgs "output/snapshot-worker-rpi-aarch64.tar" "${Target}:${AppDir}.deploy.tar"
  if ($LASTEXITCODE -ne 0) { throw "Failed to upload deployment archive." }

  & scp @ScpArgs "scripts/remote-rpi-snapshot-worker-postdeploy.sh" "${Target}:${AppDir}.postdeploy.sh"
  if ($LASTEXITCODE -ne 0) { throw "Failed to upload remote postdeploy script." }
}
finally {
  Pop-Location
}

& ssh @SshArgs $Target "chmod +x '$AppDir.postdeploy.sh' && bash '$AppDir.postdeploy.sh' '$AppDir' '${AppDir}.deploy.tar'"
if ($LASTEXITCODE -ne 0) {
  throw "Remote deploy/restart sequence failed."
}

if (Test-Path $ArchivePath) {
  Remove-Item $ArchivePath -Force
}

Write-Host "Snapshot worker deployment completed: ${Target}:$AppDir"
