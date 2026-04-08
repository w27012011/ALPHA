@echo off
setlocal enabledelayedexpansion

:: AGAM-ALPHA | MISSION COMMAND LAUNCHER
:: ──────────────────────────────────────────────────────────
title AGAM-ALPHA // MISSION_CONTROL
cd /d "%~dp0"

echo [*] INITIALIZING MISSION_COMMAND...
echo [*] Checking Host Environment...
if not exist "%~dp0.env" (
    echo [!] WARNING: API Credentials (.env) not found on pendrive.
    echo [!] Real-time Copernicus/Sentinel data will be disabled.
    echo [!] Please copy .env.template to .env and fill in your keys.
    echo [!] Note: Public FFWC and USGS feeds will still work with internet.
    echo.
) else (
    echo [*] API Credentials (.env) detected. Real-time enabled.
)

:: Check for Python or py launcher
set "PY_CMD=python"
%PY_CMD% --version >nul 2>&1
if !errorlevel! neq 0 (
    set "PY_CMD=py"
    !PY_CMD! --version >nul 2>&1
    if !errorlevel! neq 0 (
        echo [!] ERROR: Python 3 was not found.
        echo [!] Ensure Python is installed and in PATH.
        pause
        exit /b
    )
)

echo [*] Verified Python: !PY_CMD!
echo [*] Installing Tactical Dependencies (Silent)...
!PY_CMD! -m pip install -r "%~dp0requirements.txt" --quiet
if !errorlevel! neq 0 (
    echo [!] WARNING: Dependency sync incomplete. Retrying...
    !PY_CMD! -m pip install flask flask-cors requests python-dotenv --quiet
)

echo [*] Launching AGAM-ADL Orchestrator...
start /b "" !PY_CMD! "%~dp0data_systemd.py" --mode LIVE

echo [*] Launching Command Theater...
start /b "" !PY_CMD! "%~dp0dashboard_server.py"

echo [*] Synchronizing National Monitor...
:: Wait for servers to bind
timeout /t 5 >nul
start "" "http://localhost:8080/strat_master.html"

echo [*] SYSTEM ONLINE. GOOD LUCK AT THE SCIENCE FAIR.
echo [*] Keep this window open during the presentation.
pause
