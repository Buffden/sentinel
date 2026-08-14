#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/../migrations"

# Load .env from repo root
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-sentinel}"
POSTGRES_DB="${POSTGRES_DB:-sentinel}"
CONTAINER="sentinel-timescaledb"

echo "Container: $CONTAINER"
echo "Database: $POSTGRES_DB"
echo "User: $POSTGRES_USER"
echo ""

# Wait for PostgreSQL to be ready inside the container before running migrations.
# docker compose up -d returns as soon as containers start, not when services are ready.
MAX_RETRIES=20
RETRY_INTERVAL=3
echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
  if docker exec "$CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q; then
    echo "PostgreSQL is ready."
    echo ""
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "PostgreSQL did not become ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Aborting."
    exit 1
  fi
  echo "  attempt $i/$MAX_RETRIES -- not ready yet, retrying in ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
done

for migration in "$MIGRATIONS_DIR"/[0-9]*.sql; do
  [ -e "$migration" ] || { echo "No migration files found in $MIGRATIONS_DIR"; exit 1; }
  filename="$(basename "$migration")"
  echo "--> $filename"
  docker exec -i "$CONTAINER" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    < "$migration"
  echo "    ok"
  echo ""
done

echo "All migrations applied."
