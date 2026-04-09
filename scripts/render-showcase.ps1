$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    cargo run --bin render_showcase
    if ($LASTEXITCODE -ne 0) {
        throw "cargo run --bin render_showcase failed with exit code $LASTEXITCODE"
    }
    Start-Process (Join-Path $repoRoot "output/showcase/listings.html")
}
finally {
    Pop-Location
}
