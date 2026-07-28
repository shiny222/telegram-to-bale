@echo off
title Bale Bridge 2
setlocal
cd /d "%~dp0"

:: Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo [INFO] No virtual environment found. Creating one now...
    python -m venv venv
    call venv\Scripts\activate.bat
    echo [INFO] Installing required libraries...
    pip install httpx python-telegram-bot
    echo [INFO] Installation complete!
) else (
    call venv\Scripts\activate.bat
)

:loop
echo [%date% %time%] Starting Bale Bridge... >> restarts_bale.txt
echo Starting Bridge2...

python bridge2.py

echo [%date% %time%] Bot stopped/crashed. Restarting in 10 seconds... >> restarts_bale.txt
echo Bot stopped. Restarting in 10 seconds...
timeout /t 10
goto loop