#!/usr/bin/env bash
set -euo pipefail

CONTAINER="sentinel-redpanda"

# Canonical Sentinel topics -- defined in docs/ARCHITECTURE.md and docs/DATA_MODEL.md
TOPICS=(
    "adsb.raw"
    "ais.raw"
    "position.normalized"
    "deviation.candidates"
    "proximity.candidates"
    "alerts"
    "adsb.dlq"
    "ais.dlq"
)

# Local Checkpoint 3 settings -- implementation choices, not production MSK sizing
# Partitions: 1 per topic; single broker, no application consumers yet, ordering simple
# Replication: 1; forced by single-broker dev setup -- not a production decision
PARTITIONS=1
REPLICATION_FACTOR=1

echo "Broker: $CONTAINER"
echo "Partitions: $PARTITIONS  (local dev -- not production sizing)"
echo "Replication: $REPLICATION_FACTOR  (single broker -- not production)"
echo ""

# Bounded readiness check -- docker compose up returns before Redpanda is accepting connections
MAX_RETRIES=15
RETRY_INTERVAL=3
echo "Waiting for Redpanda to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
    if docker exec "$CONTAINER" rpk cluster info > /dev/null 2>&1; then
        echo "Redpanda is ready."
        echo ""
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "Redpanda did not become ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Aborting."
        exit 1
    fi
    echo "  attempt $i/$MAX_RETRIES -- not ready, retrying in ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
done

# Fetch existing topics once.
# Using an explicit list rather than swallowing errors lets us distinguish:
#   - topic already exists  -> skip (not an error)
#   - broker unreachable    -> rpk topic list exits non-zero; set -e aborts
#   - provisioning failure  -> rpk topic create exits non-zero; set -e aborts
EXISTING=$(docker exec "$CONTAINER" rpk topic list | tail -n +2 | awk '{print $1}')

for topic in "${TOPICS[@]}"; do
    if echo "$EXISTING" | grep -qx "$topic"; then
        echo "  exists   $topic"
    else
        docker exec "$CONTAINER" rpk topic create "$topic" \
            --partitions "$PARTITIONS" \
            --replicas "$REPLICATION_FACTOR" > /dev/null
        echo "  created  $topic"
    fi
done

echo ""
echo "All canonical topics provisioned."
