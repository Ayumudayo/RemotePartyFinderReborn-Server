@echo off
echo Starting Server in Release Mode...
cargo run --release
if errorlevel 1 (
    echo.
    echo Error: Server validation failed or crashed.
    pause
    exit /b %errorlevel%
)
pause