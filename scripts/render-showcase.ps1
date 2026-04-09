$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$showcaseDir = Join-Path $repoRoot "output/showcase"
$port = 50421
$url = "http://127.0.0.1:$port/listings.html"

Push-Location $repoRoot
try {
    cargo run --bin render_showcase
    if ($LASTEXITCODE -ne 0) {
        throw "cargo run --bin render_showcase failed with exit code $LASTEXITCODE"
    }

    $existingServer = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match '^python(?:\.exe)?$' -and
            $_.CommandLine -match "http\.server\s+$port\b" -and
            $_.CommandLine -match [regex]::Escape($showcaseDir)
        }

    if (-not $existingServer) {
        Start-Process python -ArgumentList "-m", "http.server", "$port" -WorkingDirectory $showcaseDir | Out-Null
        Start-Sleep -Seconds 1
    }

    Start-Process $url
    Write-Host "Showcase opened at $url"
}
finally {
    Pop-Location
}
