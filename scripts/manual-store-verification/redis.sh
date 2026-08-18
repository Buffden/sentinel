#!/usr/bin/env bash
# CP-05 Redis manual verification
# Run from the project root after: make up

set -euo pipefail

REDIS="docker exec sentinel-redis redis-cli"

# Create entity live hash (canonical field shape from DATA_MODEL)
$REDIS HSET entity:live:cp5-test-entity \
  lat 40.7128 \
  lon -74.0060 \
  entity_type aircraft \
  last_seen_ms 1700000000000 \
  live_geo_cell 8928308280fffff

# Read all fields back
$REDIS HGETALL entity:live:cp5-test-entity

# Check TTL: -1 means exists with no expiry
$REDIS TTL entity:live:cp5-test-entity

# Set a 30-second TTL (millisecond precision)
$REDIS PEXPIRE entity:live:cp5-test-entity 30000

# Check remaining TTL in milliseconds
$REDIS PTTL entity:live:cp5-test-entity

# Create geo-cell sorted set: score = last_seen_ms
$REDIS ZADD geo-cell:cp5-test-cell 1700000000000 cp5-test-entity-a
$REDIS ZADD geo-cell:cp5-test-cell 1700000005000 cp5-test-entity-b

# Freshness query: members with score above 1700000003000
$REDIS ZRANGEBYSCORE geo-cell:cp5-test-cell 1700000003000 +inf WITHSCORES

# Failure test: non-numeric score
$REDIS ZADD geo-cell:cp5-test-cell not-a-number cp5-test-entity-c || true

# Cleanup
$REDIS DEL entity:live:cp5-test-entity geo-cell:cp5-test-cell

# Verify both keys gone (expects 0)
$REDIS EXISTS entity:live:cp5-test-entity geo-cell:cp5-test-cell
