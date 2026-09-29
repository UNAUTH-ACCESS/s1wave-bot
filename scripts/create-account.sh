#!/bin/bash
# scripts/create-account.sh
# ===========================
# Thin CLI wrapper over engine/provisioning.py (2026-09-29) — the exact
# same logic the control panel's "+ New Account" web button uses, so the
# two can never drift apart. Runs with S1WAVE_ENV_FILE=.env.control so
# the new account gets registered in the SAME control_accounts table the
# homepage reads from.
#
# Usage: scripts/create-account.sh <name>

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <account-name>" >&2
    exit 1
fi

REPO_DIR="/home/solana/s1wave-bot/solanabot"
cd "$REPO_DIR"
source .venv/bin/activate
S1WAVE_ENV_FILE=.env.control PYTHONPATH="$REPO_DIR" python3 -m control_panel.cli "$1"
