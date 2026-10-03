# Consumer lag inspection

Run from the repository root with the Compose Redpanda service running:

```bash
make lag
make lag GROUP=position-consumer
bash scripts/consumer-lag/inspect.sh correlation-worker alert-evaluator
```

Defaults: `position-consumer`, `correlation-worker`, `alert-evaluator`, `api`.
The script runs `rpk group describe`; it only reads metadata and offsets.

Sample every two seconds (Ctrl-C to stop):

```bash
while make lag GROUP=position-consumer; do sleep 2; done
```

## Reading the report

`LAG = LOG-END-OFFSET - CURRENT-OFFSET` per partition. `CURRENT-OFFSET` is
the committed **next** offset: processing offset 40 commits 41. A log end of
51 therefore gives lag 10.

An empty group can have lag. Growing lag means the consumer is stopped or
committing more slowly than records arrive. Check its logs; datastore health
checks do not measure Kafka progress. Missing commits and report errors do not
mean zero lag. Zero lag does not confirm downstream delivery. Restart with the
same group to catch up; seeking to the end skips the backlog.

## Stop/restart experiment

The script creates a fresh topic/group each run, outside the live pipeline. Records have one-day retention; topic and group metadata remain.
`--compression none` avoids the known unsupported-codec issue.

```bash
bash scripts/consumer-lag/experiment.sh
```

Expected `(current offset, log end, lag)`:
`(1, 1, 0)` → `(1, 11, 10)` → `(1, 31, 30)` → `(11, 31, 20)` → `(31, 31, 0)`.
Restarts consume offsets 1–10, then 11–30. This checks Kafka commits and restart
behavior with `rpk`; application persistence and replay remain for the failure lab.
