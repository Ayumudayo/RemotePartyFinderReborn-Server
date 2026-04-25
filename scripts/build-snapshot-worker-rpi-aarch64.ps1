param()

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$TargetTriple = "aarch64-unknown-linux-gnu"
$StageDir = Join-Path $RepoRoot "output\snapshot-worker-rpi-aarch64"
$ReleaseDir = Join-Path $RepoRoot "target\$TargetTriple\release"
$BinaryName = "listings_snapshot_worker"

function Require-Command {
  param([string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $Name"
  }
}

function Invoke-NativeQuiet {
  param(
    [string]$Command,
    [string[]]$Arguments = @()
  )

  $stdoutPath = Join-Path $env:TEMP ("snapshot-worker-native-" + [guid]::NewGuid().ToString() + ".stdout.log")
  $stderrPath = Join-Path $env:TEMP ("snapshot-worker-native-" + [guid]::NewGuid().ToString() + ".stderr.log")
  try {
    $process = Start-Process -FilePath $Command `
      -ArgumentList $Arguments `
      -NoNewWindow `
      -Wait `
      -PassThru `
      -RedirectStandardOutput $stdoutPath `
      -RedirectStandardError $stderrPath
    return $process.ExitCode
  }
  finally {
    if (Test-Path $stdoutPath) {
      Remove-Item $stdoutPath -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $stderrPath) {
      Remove-Item $stderrPath -Force -ErrorAction SilentlyContinue
    }
  }
}

Require-Command rustup
Require-Command cargo
Require-Command zig
Require-Command cargo-zigbuild

if ((Invoke-NativeQuiet "cargo-zigbuild" @("--version")) -ne 0) {
  throw "cargo-zigbuild is not installed. Run `cargo install cargo-zigbuild` first."
}

if ((Invoke-NativeQuiet "rustup" @("target", "add", $TargetTriple)) -ne 0) {
  throw "Failed to install Rust target $TargetTriple."
}

Push-Location $RepoRoot
try {
  & cargo zigbuild --release --target $TargetTriple --bin $BinaryName
  if ($LASTEXITCODE -ne 0) {
    throw "cargo zigbuild failed."
  }
}
finally {
  Pop-Location
}

if (Test-Path $StageDir) {
  Remove-Item $StageDir -Recurse -Force
}

New-Item -ItemType Directory -Force -Path $StageDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $StageDir "scripts") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $StageDir "logs") | Out-Null

Copy-Item (Join-Path $ReleaseDir $BinaryName) (Join-Path $StageDir $BinaryName) -Force
Copy-Item (Join-Path $RepoRoot "ecosystem.snapshot-worker.config.js") (Join-Path $StageDir "ecosystem.snapshot-worker.config.js") -Force
Copy-Item (Join-Path $RepoRoot "config.example.toml") (Join-Path $StageDir "config.example.toml") -Force
Copy-Item (Join-Path $RepoRoot "scripts\remote-rpi-snapshot-worker-postdeploy.sh") (Join-Path $StageDir "scripts\remote-rpi-snapshot-worker-postdeploy.sh") -Force

Write-Host "Prepared Raspberry Pi snapshot worker deployment bundle: $StageDir"
