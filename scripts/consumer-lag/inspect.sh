#!/usr/bin/env bash
# Read broker offsets only; never join a group, consume records, or reset offsets.
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
	cat <<'USAGE'
Usage: bash scripts/consumer-lag/inspect.sh [GROUP ...]
Without arguments, inspect position-consumer, correlation-worker, alert-evaluator, api.
Requires the Compose redpanda service to be running. Run from the repository root.
USAGE
	exit 0
fi

groups=("$@")
if [[ ${#groups[@]} -eq 0 ]]; then
	groups=(position-consumer correlation-worker alert-evaluator api)
fi

for group in "${groups[@]}"; do
	if [[ -z "$group" || "$group" == -* ]]; then
		printf 'Invalid consumer group: %s\n' "$group" >&2
		exit 2
	fi
done

for group in "${groups[@]}"; do
	printf '\nConsumer group: %s\n' "$group"
	docker compose exec -T redpanda rpk group describe "$group"
done
