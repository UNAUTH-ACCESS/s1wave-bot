#!/bin/bash
# collect-keys.sh — copy every account's wallet private key into ONE private
# file for you to store offline. Run it yourself in your own terminal:
#
#   /home/solana/s1wave-bot/solanabot/scripts/collect-keys.sh
#
# Keys are written only to the file (mode 600), never printed. Do NOT run this
# through an AI session or pipe its output anywhere. After saving the file to a
# password manager / offline storage, destroy it:  shred -u <file>
set -euo pipefail
umask 077
ROOT="${S1WAVE_ROOT:-/home/solana/s1wave-bot/solanabot}"
OUT="$HOME/s1wave-wallet-keys-$(date +%Y%m%d-%H%M%S).txt"
ACCOUNTS=("base:.env" "second:.env.second" "efetobo:.env.efetobo")

{
  echo "S1Wave wallet keys, collected $(date -u +%FT%TZ)"
  echo "Anyone holding a key controls that wallet. Store offline, never email or paste."
  echo
} > "$OUT"

for entry in "${ACCOUNTS[@]}"; do
  tag="${entry%%:*}"; envfile="$ROOT/${entry#*:}"
  [ -f "$envfile" ] || { echo "== $tag: env file missing" >> "$OUT"; continue; }
  for var in CONFLUENCE_LIVE_WALLET_PRIVATE_KEY WALLET_PRIVATE_KEY; do
    val=$(grep -E "^${var}=" "$envfile" | head -1 | cut -d= -f2- | tr -d '"'"'" || true)
    [ -n "$val" ] || continue
    pub=$(KEYVAL="$val" "$ROOT/.venv/bin/python" -c '
import os
from solders.keypair import Keypair
print(Keypair.from_base58_string(os.environ["KEYVAL"]).pubkey())' 2>/dev/null || echo "unknown")
    { echo "== account: $tag   var: $var"; echo "   public address: $pub"; echo "   private key:   $val"; echo; } >> "$OUT"
  done
done

echo "Saved to: $OUT (permissions 600)."
echo "Copy it to offline storage, verify you can read it, then run:  shred -u $OUT"
echo "Import: in Phantom/Solflare use 'Import private key' with the base58 string."
