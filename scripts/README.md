# Scripts

Manual verification and one-off scripts for Sentinel's infrastructure stores. These are not application code and are not run automatically.

All scripts assume the stack is up (`make up`) and provisioned (`make migrate && make topics && make neo4j-schema`).

## Folders

| Folder | Purpose |
| --- | --- |
| [`timescaledb-schema/`](timescaledb-schema/) | TimescaleDB schema verification and transaction rollback experiment |
| [`manual-store-verification/`](manual-store-verification/) | Direct CLI verification of all four stores: Redpanda, TimescaleDB, Redis, Neo4j |

## timescaledb-schema

| Script | What it does |
| --- | --- |
| [`verify-schema.sql`](timescaledb-schema/verify-schema.sql) | Confirms all tables, hypertable metadata, chunk interval, retention policy, and indexes are present |
| [`failure-experiment.sql`](timescaledb-schema/failure-experiment.sql) | Runs a deliberate `SELECT 1/0` inside a transaction to prove DDL rollback and `ON_ERROR_STOP` behavior |

Run inside psql:

```bash
docker exec -it sentinel-timescaledb psql -U sentinel -d sentinel -v ON_ERROR_STOP=1
```

## manual-store-verification

| Script | What it does |
| --- | --- |
| [`kafka.sh`](manual-store-verification/kafka.sh) | Produce and consume a synthetic record, inspect partition metadata and consumer group lag |
| [`timescaledb.sql`](manual-store-verification/timescaledb.sql) | Insert a test row, verify idempotency, inspect chunk, delete and confirm chunk persists |
| [`redis.sh`](manual-store-verification/redis.sh) | Create a live entity hash, exercise TTL commands, create a geo-cell sorted set, run freshness query |
| [`neo4j.cypher`](manual-store-verification/neo4j.cypher) | Create Entity nodes and relationships, test MERGE vs CREATE under the uniqueness constraint |

Run each:

```bash
bash scripts/manual-store-verification/kafka.sh
bash scripts/manual-store-verification/redis.sh
docker exec -it sentinel-timescaledb psql -U sentinel -d sentinel < scripts/manual-store-verification/timescaledb.sql
# Neo4j password from NEO4J_AUTH in .env
docker exec -i sentinel-neo4j cypher-shell -u neo4j -p <password> < scripts/manual-store-verification/neo4j.cypher
```
