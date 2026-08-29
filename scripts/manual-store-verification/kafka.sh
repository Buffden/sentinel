#!/usr/bin/env bash
# Kafka / Redpanda manual store verification
# Run from the project root after: make up && make topics

set -euo pipefail

# List all canonical topics
docker exec sentinel-redpanda rpk topic list

# Inspect adsb.raw partition metadata
docker exec sentinel-redpanda rpk topic describe adsb.raw

# Produce the manual store verification synthetic record
echo '{"checkpoint":5,"source":"manual-store-verification"}' | \
	docker exec -i sentinel-redpanda rpk topic produce adsb.raw

# Consume at explicit offset 1 (one message only)
docker exec sentinel-redpanda rpk topic consume adsb.raw \
	--offset 1 --num 1

# Consume with named group from the start
docker exec sentinel-redpanda rpk topic consume adsb.raw \
	--group manual-store-verification-test --offset start --num 1

# Inspect group committed offset and lag
docker exec sentinel-redpanda rpk group describe manual-store-verification-test

# Verify downstream topics have no records yet
docker exec sentinel-redpanda rpk topic describe position.normalized
docker exec sentinel-redpanda rpk topic describe alerts

# Failure test
docker exec sentinel-redpanda rpk topic consume definitely-does-not-exist || true
