#!/bin/bash
# scripts/create-account.sh
# ===========================
# Provisions a new, fully isolated S1Wave live-trading account from this
# SAME codebase checkout (2026-09-29, "a way to create multiple s1wave
# accounts") — own wallet, own database, own port, own systemd service.
# Runs identical code to every other account (same source files, just a
# different .env selected via S1WAVE_ENV_FILE — see config/settings.py's
# comment on that variable), so a bug fix or filter-calibration update
# made once applies to every account automatically; no per-account
# checkout to keep in sync.
#
# Usage:
#   scripts/create-account.sh <name>
#   e.g. scripts/create-account.sh trading2
#
# What it does:
#   1. Creates a new Postgres database (s1wave_<name>) in the SAME
#      s1wave_postgres container — fully isolated trade history, zero
#      risk of cross-contaminating the base account's data.
#   2. Generates a brand-new Solana keypair for the account's wallet.
#      The private key goes straight into the new .env file; only the
#      PUBLIC address is ever printed, same handling discipline as every
#      other real key in this project.
#   3. Writes .env.<name> from the base .env as a template — new DB name,
#      new port, new wallet key, CONFLUENCE_LIVE_ENABLED=False (a fresh,
#      unfunded account must never come up armed), and BLANK SolanaTracker
#      keys (deliberately -- see the printed reminder at the end: sharing
#      the base account's already-strained keys would double the load on
#      an already-scarce shared resource).
#   4. Writes and enables a new systemd --user unit
#      (s1wave-bot-<name>.service) pointed at this same WorkingDirectory,
#      with Environment=S1WAVE_ENV_FILE=.env.<name>.
#
# Does NOT touch: the base account's .env, database, or service. Does NOT
# set up a public dashboard subdomain/nginx vhost -- that's a separate,
# lower-urgency step (needs a DuckDNS subdomain + nginx vhost + TLS cert,
# see NOTEBOOK.md's nginx section) since the account is fully functional
# and reachable on its own port without one.

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <account-name>" >&2
    echo "  account-name: lowercase letters, digits, hyphens only (e.g. 'trading2')" >&2
    exit 1
fi

NAME="$1"
if ! [[ "$NAME" =~ ^[a-z0-9-]+$ ]]; then
    echo "ERROR: account name must be lowercase letters, digits, hyphens only (got: $NAME)" >&2
    exit 1
fi

REPO_DIR="/home/solana/s1wave-bot/solanabot"
ENV_FILE="$REPO_DIR/.env.$NAME"
BASE_ENV="$REPO_DIR/.env"
UNIT_FILE="$HOME/.config/systemd/user/s1wave-bot-$NAME.service"
DB_NAME="s1wave_$NAME"

if [ -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE already exists — an account named '$NAME' may already exist. Aborting." >&2
    exit 1
fi
if [ -f "$UNIT_FILE" ]; then
    echo "ERROR: $UNIT_FILE already exists. Aborting." >&2
    exit 1
fi

cd "$REPO_DIR"

# ── 1. Pick the next free port (scan existing .env* files, default 8000) ──
NEXT_PORT=8001
for f in "$REPO_DIR"/.env "$REPO_DIR"/.env.*; do
    [ -f "$f" ] || continue
    PORT=$(grep -E '^API_PORT=' "$f" 2>/dev/null | head -1 | cut -d'=' -f2)
    if [ -n "${PORT:-}" ] && [ "$PORT" -ge "$NEXT_PORT" ] 2>/dev/null; then
        NEXT_PORT=$((PORT + 1))
    fi
done
echo "Assigning port: $NEXT_PORT"

# ── 2. Create the database ─────────────────────────────────────────────
echo "Creating database $DB_NAME..."
docker exec s1wave_postgres psql -U s1wave -d postgres -c "CREATE DATABASE $DB_NAME;"

# ── 3. Generate a new wallet keypair ───────────────────────────────────
echo "Generating a new wallet keypair..."
KEYPAIR_JSON=$(source "$REPO_DIR/.venv/bin/activate" && PYTHONPATH="$REPO_DIR" python3 -c "
from solders.keypair import Keypair
kp = Keypair()
print(str(kp))
print(str(kp.pubkey()))
")
WALLET_SECRET=$(echo "$KEYPAIR_JSON" | sed -n '1p')
WALLET_PUBKEY=$(echo "$KEYPAIR_JSON" | sed -n '2p')

# ── 4. Write the new .env from the base .env as a template ────────────
NEW_DATABASE_URL=$(grep -E '^DATABASE_URL=' "$BASE_ENV" | sed "s#/s1wave\$#/$DB_NAME#")

sed \
    -e "s|^DATABASE_URL=.*|$NEW_DATABASE_URL|" \
    -e "s|^API_PORT=.*|API_PORT=$NEXT_PORT|" \
    -e "s|^CONFLUENCE_LIVE_WALLET_PRIVATE_KEY=.*|CONFLUENCE_LIVE_WALLET_PRIVATE_KEY=$WALLET_SECRET|" \
    -e "s|^CONFLUENCE_LIVE_ENABLED=.*|CONFLUENCE_LIVE_ENABLED=False|" \
    -e "s|^SOLANA_TRACKER_API_KEY=.*|SOLANA_TRACKER_API_KEY=|" \
    -e "s|^SOLANA_TRACKER_API_KEY_DISCOVERY=.*|SOLANA_TRACKER_API_KEY_DISCOVERY=|" \
    "$BASE_ENV" > "$ENV_FILE"

{
    echo ""
    echo "# ── Account identity (added by create-account.sh) ────────────────────────"
    echo "S1WAVE_ACCOUNT_NAME=S1Wave-$NAME"
    echo "# Public wallet address: $WALLET_PUBKEY — fund this address to activate."
} >> "$ENV_FILE"

chmod 600 "$ENV_FILE"

# ── 5. Write and enable the systemd unit ───────────────────────────────
mkdir -p "$HOME/.config/systemd/user"
cat > "$UNIT_FILE" <<EOF
[Unit]
Description=S1Wave account '$NAME' (discovery/sampling/shadow/live workers + API, port $NEXT_PORT)
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$REPO_DIR
Environment=S1WAVE_ENV_FILE=.env.$NAME
ExecStart=$REPO_DIR/.venv/bin/python main.py
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now "s1wave-bot-$NAME.service"

echo ""
echo "======================================================================"
echo "Account '$NAME' created and running."
echo "  Database:        $DB_NAME"
echo "  Port:             $NEXT_PORT (http://127.0.0.1:$NEXT_PORT — not yet public)"
echo "  Wallet address:   $WALLET_PUBKEY"
echo "  Systemd service:  s1wave-bot-$NAME.service"
echo ""
echo "STILL NEEDED before this account can actually trade:"
echo "  1. Fund $WALLET_PUBKEY with SOL."
echo "  2. Add real SOLANA_TRACKER_API_KEY / SOLANA_TRACKER_API_KEY_DISCOVERY"
echo "     values to $ENV_FILE (left blank on purpose — sharing the base"
echo "     account's already-strained keys would double the load on an"
echo "     already-scarce shared resource)."
echo "  3. Set CONFLUENCE_LIVE_ENABLED=True in $ENV_FILE once funded and ready,"
echo "     then: systemctl --user restart s1wave-bot-$NAME.service"
echo "  4. No public dashboard subdomain yet — reachable on localhost:$NEXT_PORT"
echo "     for now; ask to set up a public subdomain when ready."
echo "======================================================================"
