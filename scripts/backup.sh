#!/bin/bash
# backup.sh — nightly Postgres dump of ALL S1Wave account databases
# (base, second, efetobo; one s1wave_postgres container, one DB per account).
#
# Crontab (log must live somewhere the solana user can write; the old
# /var/log target silently stopped this job from ever running):
#   0 4 * * * /home/solana/s1wave-bot/solanabot/scripts/backup.sh >> /home/solana/backups/s1wave-backup-cron.log 2>&1
#
# Dumps are local unless an rclone remote named 'b2' exists. A local-only copy
# does NOT survive losing this machine; see CLAUDE.md "Resilience".
# Wallet private keys are in the .env files, NOT in these dumps: keep your own
# offline copy of them.

set -uo pipefail

BACKUP_DIR="/home/solana/backups"
RETENTION_DAYS=14
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
RCLONE_REMOTE="b2:${B2_BUCKET_NAME:-anchorledger-backups}"
ROOT="/home/solana/s1wave-bot/solanabot"
# tag:env-file
ACCOUNTS=("base:.env" "second:.env.second" "efetobo:.env.efetobo")

mkdir -p "$BACKUP_DIR"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1"; }
failed=0

for entry in "${ACCOUNTS[@]}"; do
  tag="${entry%%:*}"; envfile="$ROOT/${entry#*:}"
  DATABASE_URL=$(grep -E '^DATABASE_URL=' "$envfile" 2>/dev/null | head -1 | cut -d'=' -f2-)
  if [ -z "$DATABASE_URL" ]; then log "ERROR[$tag]: no DATABASE_URL in $envfile"; failed=1; continue; fi
  creds=$(echo "$DATABASE_URL" | sed -E 's#postgresql\+asyncpg://##')
  user=$(echo "$creds" | sed -E 's#:.*##')
  pass=$(echo "$creds" | sed -E 's#^[^:]+:([^@]+)@.*#\1#')
  db=$(echo "$creds" | sed -E 's#.*/##')
  if [ -z "$user" ] || [ -z "$pass" ] || [ -z "$db" ]; then log "ERROR[$tag]: cannot parse DATABASE_URL"; failed=1; continue; fi

  dump="$BACKUP_DIR/s1wave-${tag}-${TIMESTAMP}.sql.gz"
  docker exec -e PGPASSWORD="$pass" s1wave_postgres pg_dump -U "$user" "$db" | gzip > "$dump"
  rc=("${PIPESTATUS[@]}")
  if [ "${rc[0]}" -ne 0 ] || [ "${rc[1]}" -ne 0 ] || [ ! -s "$dump" ]; then
    log "ERROR[$tag]: pg_dump failed (rc=${rc[0]}/${rc[1]}); removing partial file"
    rm -f "$dump"; failed=1; continue
  fi
  log "[$tag] dump created: $dump ($(du -h "$dump" | cut -f1))"

  if rclone listremotes 2>/dev/null | grep -q "^b2:"; then
    if rclone copy "$dump" "$RCLONE_REMOTE" --quiet; then log "[$tag] uploaded to $RCLONE_REMOTE"
    else log "ERROR[$tag]: rclone upload failed (dump kept locally)"; failed=1; fi
  fi
done

if ! rclone listremotes 2>/dev/null | grep -q "^b2:"; then
  log "WARNING: no 'b2' rclone remote configured: dumps are on this machine only."
fi

# Both the old single-DB names (s1wave-2026...) and the new per-account names match.
find "$BACKUP_DIR" -name "s1wave-*.sql.gz" -mtime +$RETENTION_DAYS -delete

[ "$failed" -eq 0 ] && log "Backup complete." || { log "Backup finished WITH ERRORS."; exit 1; }
