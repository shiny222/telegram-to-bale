#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# Telegram -> Bale Bridge (Linux version)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "[INFO] No virtual environment found. Creating one now..."
    python3 -m venv venv
    source venv/bin/activate
    echo "[INFO] Installing required libraries..."
    pip install httpx python-telegram-bot
    echo "[INFO] Installation complete!"
else
    source venv/bin/activate
fi

while true; do
    echo "[$(date)] Starting Bale Bridge..." >> restarts_bale.txt
    echo "Starting Bridge2..."

    python bridge2.py

    echo "[$(date)] Bot stopped/crashed. Restarting in 10 seconds..." >> restarts_bale.txt
    echo "Bot stopped. Restarting in 10 seconds..."
    sleep 10
done
