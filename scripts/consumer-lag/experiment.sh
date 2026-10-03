#!/usr/bin/env bash
# Reproduce backlog growth and recovery on a fresh topic/group.
# Leaves experiment metadata for inspection; records expire after one day.
set -euo pipefail

cd "$(dirname "$0")/../.."

experiment="cp6-lag-$(date +%s)-$$"
docker compose exec -T redpanda rpk topic create "$experiment" \
  --partitions 1 --replicas 1 -c retention.ms=86400000
printf 'baseline\n' | docker compose exec -T redpanda rpk topic produce \
  "$experiment" --compression none
docker compose exec -T redpanda rpk topic consume "$experiment" \
  --group "$experiment" --num 1 --format '%o %v\n'
bash scripts/consumer-lag/inspect.sh "$experiment"

# Consumer has exited: committed next offset stays at 1.
seq 1 10 | docker compose exec -T redpanda rpk topic produce \
  "$experiment" --compression none
bash scripts/consumer-lag/inspect.sh "$experiment"
seq 11 30 | docker compose exec -T redpanda rpk topic produce \
  "$experiment" --compression none
bash scripts/consumer-lag/inspect.sh "$experiment"

# Restart the same group in two passes.
docker compose exec -T redpanda rpk topic consume "$experiment" \
  --group "$experiment" --num 10 --format '%o %v\n'
bash scripts/consumer-lag/inspect.sh "$experiment"
docker compose exec -T redpanda rpk topic consume "$experiment" \
  --group "$experiment" --num 20 --format '%o %v\n'
bash scripts/consumer-lag/inspect.sh "$experiment"
