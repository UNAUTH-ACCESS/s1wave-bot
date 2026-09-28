#!/bin/bash
# backup.sh
# Nightly Postgres dump of s1wave_postgres (real trading history + audit
# trail + halt-override baseline — single-copy in the docker volume before
# this existed, unlike quantedge/contentpipe which already had backups).
# Mirrors quantedge/scripts/backup.sh's pattern exactly (local dump,
# rotation, best-effort off-server copy via rclone if a 'b2' remote is
# configured) so this machine has one consistent backup convention.
#
# Add to crontab: 0 4 * * * /home/solana/s1wave-bot/solanabot/scripts/backup.sh >> /var/log/s1wave-backup-cron.log 2>&1
# (0 4, not 0 3 like quantedge's, so the two nightly dumps don't compete
# for disk/CPU at the exact same minute.)

set -uo pipefail

BACKUP_DIR="/home/solana/backups"
RETENTION_DAYS=14
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
DUMP_FILE="$BACKUP_DIR/s1wave-${TIMESTAMP}.sql.gz"
RCLONE_REMOTE="b2:${B2_BUCKET_NAME:-anchorledger-backups}"
ENV_FILE="/home/solana/s1wave-bot/solanabot/.env"

mkdir -p "$BACKUP_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1"; }

log "Starting s1wave backup..."

# s1wave's .env keeps creds embedded in DATABASE_URL
# (postgresql+asyncpg://USER:PASSWORD@host:port/DB), not separate
# POSTGRES_* vars like quantedge's .env — parse it out instead.
DATABASE_URL=$(grep -E '^DATABASE_URL=' "$ENV_FILE" | head -1 | cut -d'=' -f2-)
if [ -z "$DATABASE_URL" ]; then
  log "ERROR: could not read DATABASE_URL from $ENV_FILE - aborting"
  exit 1
fi
# Strip the asyncpg driver suffix and parse user:password@host:port/db
CREDS_HOST_DB=$(echo "$DATABASE_URL" | sed -E 's#postgresql\+asyncpg://##')
POSTGRES_USER=$(echo "$CREDS_HOST_DB" | sed -E 's#:.*##')
POSTGRES_PASSWORD=$(echo "$CREDS_HOST_DB" | sed -E 's#^[^:]+:([^@]+)@.*#\1#')
POSTGRES_DB=$(echo "$CREDS_HOST_DB" | sed -E 's#.*/##')

if [ -z "$POSTGRES_PASSWORD" ] || [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_DB" ]; then
  log "ERROR: could not parse Postgres credentials out of DATABASE_URL - aborting"
  exit 1
fi

docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" s1wave_postgres \
  pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" | gzip > "$DUMP_FILE"

if [ ! -s "$DUMP_FILE" ]; then
  log "ERROR: dump file is empty - not uploading, not rotating, leaving it for inspection"
  exit 1
fi
log "Dump created: $DUMP_FILE ($(du -h "$DUMP_FILE" | cut -f1))"

if ! rclone listremotes 2>/dev/null | grep -q "^b2:"; then
  log "WARNING: no 'b2' rclone remote configured yet - dump saved locally only, NOT copied off-server. Run 'rclone config' to add B2 credentials."
else
  if rclone copy "$DUMP_FILE" "$RCLONE_REMOTE" --quiet; then
    log "Uploaded to $RCLONE_REMOTE"
  else
    log "ERROR: rclone upload to $RCLONE_REMOTE failed - dump still retained locally"
  fi
fi

# Local retention only - off-server (B2) retention should be set as a
# lifecycle rule on the bucket itself once it exists, not duplicated here.
find "$BACKUP_DIR" -name "s1wave-*.sql.gz" -mtime +$RETENTION_DAYS -delete

log "Backup complete."
