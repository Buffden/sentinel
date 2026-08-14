#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/schema.cypher"

# Load .env from repo root
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

# NEO4J_AUTH format is username/password (set in .env and passed to docker-compose.yml).
# Split on the first / only -- passwords may contain slashes in theory.
NEO4J_AUTH="${NEO4J_AUTH:-neo4j/neo4j}"
NEO4J_USER="${NEO4J_AUTH%%/*}"
NEO4J_PASSWORD="${NEO4J_AUTH#*/}"

CONTAINER="sentinel-neo4j"

echo "Container: $CONTAINER"
echo "Neo4j user: $NEO4J_USER"
echo ""

# Bounded readiness check.
# The docker-compose healthcheck tests the HTTP port, but cypher-shell needs the
# Bolt port to be ready. Neo4j can pass the HTTP check before Bolt accepts connections.
MAX_RETRIES=20
RETRY_INTERVAL=5
echo "Waiting for Neo4j Bolt to accept connections..."
for i in $(seq 1 $MAX_RETRIES); do
  if docker exec "$CONTAINER" cypher-shell \
       -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
       "RETURN 1 AS ready" > /dev/null 2>&1; then
    echo "Neo4j is ready."
    echo ""
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "Neo4j did not become ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Aborting."
    exit 1
  fi
  echo "  attempt $i/$MAX_RETRIES -- not ready, retrying in ${RETRY_INTERVAL}s..."
  sleep "$RETRY_INTERVAL"
done

# Apply schema.
# cypher-shell reads from stdin when invoked without a --file argument.
# Errors in schema.cypher propagate as non-zero exit codes through docker exec.
echo "--> applying schema.cypher"
docker exec -i "$CONTAINER" cypher-shell \
  -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" \
  < "$SCHEMA_FILE"
echo "    ok"
echo ""
echo "Neo4j schema applied."
