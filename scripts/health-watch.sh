#!/bin/bash
# health-watch.sh — S1Wave watchdog, run from cron every 5 minutes.
#
#   */5 * * * * /home/solana/s1wave-bot/solanabot/scripts/health-watch.sh
#
# Emails ONLY when the set of problems changes (new problem, or all clear), so
# a sustained outage is one message. Same Resend mechanism as the other
# watchdogs on this box (no local MTA). Does not touch trading.
#
# Checks: the three bot services (active, no new crash-restarts), fresh
# market-data snapshots (a silent sampling outage = no signals, no data),
# newest DB backup age, disk, and memory/swap pressure (the box is 2 GB).
#
# CHANGE THE ALERT ADDRESS HERE:
ALERT_EMAIL="hoelineben@gmail.com"

set -uo pipefail
export PATH="/usr/bin:/bin:$PATH"

FROM="S1Wave Watchdog <noreply@anchorledger.space>"
RESEND_ENV="/home/solana/quantedge/.env"
ROOT="/home/solana/s1wave-bot/solanabot"
STATE_DIR="/home/solana/s1wave-ops"
STATE_FILE="$STATE_DIR/health-watch.state"
LOG_FILE="$STATE_DIR/health-watch.log"
SERVICES=(s1wave-bot s1wave-bot-second s1wave-bot-efetobo)
SNAPSHOT_MAX_AGE_MIN=30
BACKUP_MAX_AGE_H=36
DISK_MAX_PCT=85
MEM_AVAIL_MIN_MB=120
SWAP_MAX_PCT=95

mkdir -p "$STATE_DIR"
exec 200>/tmp/s1wave-health-watch.lock
flock -n 200 || exit 0
log() { echo "$(date '+%F %T') $*" >> "$LOG_FILE"; }
export XDG_RUNTIME_DIR="/run/user/$(id -u)"

problems=()

# --- services -------------------------------------------------------------
declare -A restarts
for s in "${SERVICES[@]}"; do
  a=$(systemctl --user is-active "$s" 2>/dev/null || true)
  [ "$a" = "active" ] || problems+=("service $s is '$a'")
  n=$(systemctl --user show "$s" -p NRestarts --value 2>/dev/null || echo 0)
  [[ "$n" =~ ^[0-9]+$ ]] || n=0
  restarts[$s]=$n
done

# --- market-data freshness (base account DB) -------------------------------
DBURL=$(grep -E '^DATABASE_URL=' "$ROOT/.env" | head -1 | cut -d= -f2-)
creds=$(echo "$DBURL" | sed -E 's#postgresql\+asyncpg://##')
user=$(echo "$creds" | sed -E 's#:.*##'); pass=$(echo "$creds" | sed -E 's#^[^:]+:([^@]+)@.*#\1#'); db=$(echo "$creds" | sed -E 's#.*/##')
age=$(docker exec -e PGPASSWORD="$pass" s1wave_postgres psql -U "$user" -d "$db" -tAc \
  "select coalesce(round(extract(epoch from (now()-max(sampled_at)))/60),99999) from token_snapshots" 2>/dev/null | tr -d '[:space:]')
if [[ "$age" =~ ^[0-9]+$ ]]; then
  [ "$age" -le "$SNAPSHOT_MAX_AGE_MIN" ] || problems+=("no new market-data snapshot for ${age} min (sampling keys exhausted or worker stuck: no signals while this lasts)")
else
  problems+=("cannot query the S1Wave database")
fi

# --- backups ---------------------------------------------------------------
newest=$(find /home/solana/backups -name 's1wave-*.sql.gz' -printf '%T@\n' 2>/dev/null | sort -n | tail -1)
if [ -z "$newest" ]; then problems+=("no S1Wave backup exists")
else
  hrs=$(( ( $(date +%s) - ${newest%.*} ) / 3600 ))
  [ "$hrs" -le "$BACKUP_MAX_AGE_H" ] || problems+=("newest S1Wave backup is ${hrs}h old")
fi

# --- disk / memory ---------------------------------------------------------
dpct=$(df --output=pcent / | tail -1 | tr -dc 0-9)
[ "$dpct" -lt "$DISK_MAX_PCT" ] || problems+=("disk ${dpct}% full")
avail=$(free -m | awk '/^Mem:/{print $7}')
swt=$(free -m | awk '/^Swap:/{print $2}'); swu=$(free -m | awk '/^Swap:/{print $3}')
spct=0; [ "${swt:-0}" -gt 0 ] && spct=$(( swu * 100 / swt ))
if [ "$avail" -lt "$MEM_AVAIL_MIN_MB" ] && [ "$spct" -ge "$SWAP_MAX_PCT" ]; then
  problems+=("memory exhausted: ${avail} MB available, swap ${spct}% used (processes will be killed)")
fi

# --- state compare ---------------------------------------------------------
prev_sig=""; declare -A prev_restarts
if [ -f "$STATE_FILE" ]; then
  prev_sig=$(sed -n '1p' "$STATE_FILE")
  for s in "${SERVICES[@]}"; do prev_restarts[$s]=$(grep "^R:$s=" "$STATE_FILE" | cut -d= -f2); done
fi
crashed=()
for s in "${SERVICES[@]}"; do
  p=${prev_restarts[$s]:-${restarts[$s]}}; [[ "$p" =~ ^[0-9]+$ ]] || p=${restarts[$s]}
  [ "${restarts[$s]}" -gt "$p" ] && crashed+=("$s restarted $p -> ${restarts[$s]}")
done

sig=$(printf '%s;' "${problems[@]:-}" | md5sum | cut -c1-12)
[ ${#problems[@]} -eq 0 ] && sig="OK"

send_alert() {
  local subject="$1" body="$2" key payload code
  key=$(grep -E '^RESEND_API_KEY=' "$RESEND_ENV" 2>/dev/null | head -1 | cut -d= -f2-)
  [ -n "$key" ] || { log "ERROR: no RESEND_API_KEY, cannot send: $subject"; return 1; }
  payload=$(jq -n --arg from "$FROM" --arg to "$ALERT_EMAIL" --arg s "$subject" --arg t "$body" \
    '{from:$from,to:[$to],subject:$s,text:$t}')
  code=$(curl -s -o /tmp/s1wave-watch-resp.json -w '%{http_code}' -X POST https://api.resend.com/emails \
    -H "Authorization: Bearer $key" -H 'Content-Type: application/json' -d "$payload")
  [ "$code" = "200" ] && log "alert sent: $subject" || log "ERROR: Resend HTTP $code for '$subject'"
}

host=$(hostname)
if [ "$sig" != "$prev_sig" ]; then
  if [ "$sig" = "OK" ]; then
    [ -n "$prev_sig" ] && send_alert "S1Wave all clear ($host)" "All checks pass again at $(date '+%F %T %Z')."
    log "all clear"
  else
    send_alert "S1Wave problem ($host): ${problems[0]}" \
"S1Wave health check on $host at $(date '+%F %T %Z'):

$(printf ' - %s\n' "${problems[@]}")

Trading itself is unaffected unless a service above is not active.
Runbook: CLAUDE.md 'Resilience' in /home/solana/s1wave-bot/solanabot."
    log "problems: ${problems[*]}"
  fi
elif [ ${#crashed[@]} -gt 0 ]; then
  send_alert "S1Wave service crashed and restarted ($host)" \
"$(printf ' - %s\n' "${crashed[@]}")

Recent log: journalctl --user -u <service> -n 60"
  log "restart: ${crashed[*]}"
fi

{ echo "$sig"; for s in "${SERVICES[@]}"; do echo "R:$s=${restarts[$s]}"; done; } > "$STATE_FILE"
