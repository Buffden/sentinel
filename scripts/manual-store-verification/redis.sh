#!/usr/bin/env bash
# Redis manual store verification
# Run from the project root after: make up

set -euo pipefail

REDIS="docker exec sentinel-redis redis-cli"

# Create entity live hash (canonical field shape from DATA_MODEL)
$REDIS HSET entity:live:test-entity \
  lat 40.7128 \
  lon -74.0060 \
  entity_type aircraft \
  last_seen_ms 1700000000000 \
  live_geo_cell 8928308280fffff

# Read all fields back
$REDIS HGETALL entity:live:test-entity

# Check TTL: -1 means exists with no expiry
$REDIS TTL entity:live:test-entity

# Set a 30-second TTL (millisecond precision)
$REDIS PEXPIRE entity:live:test-entity 30000

# Check remaining TTL in milliseconds
$REDIS PTTL entity:live:test-entity

# Create geo-cell sorted set: score = last_seen_ms
$REDIS ZADD geo-cell:test-cell 1700000000000 test-entity-a
$REDIS ZADD geo-cell:test-cell 1700000005000 test-entity-b

# Freshness query: members with score above 1700000003000
$REDIS ZRANGEBYSCORE geo-cell:test-cell 1700000003000 +inf WITHSCORES

# Failure test: non-numeric score
$REDIS ZADD geo-cell:test-cell not-a-number test-entity-c || true

# Cleanup
$REDIS DEL entity:live:test-entity geo-cell:test-cell

# Verify both keys gone (expects 0)
$REDIS EXISTS entity:live:test-entity geo-cell:test-cell
