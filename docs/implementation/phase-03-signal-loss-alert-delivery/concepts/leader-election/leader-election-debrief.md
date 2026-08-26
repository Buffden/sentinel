# Leader Election Debrief

---

## Setup

```bash
make up
cd services/alert-evaluator
npm install
```

---

## Experiment 1: single instance acquires leader lease

```bash
npm run evaluator
```

Expected log:

```
{ instanceId: '...' } alert evaluator starting
{ instanceId: '...' } acquired leader lease — starting scan loop
{ instanceId: '...' } leader scan tick
```

Verify the Redis key while the evaluator is running:

```bash
docker exec sentinel-redis redis-cli GET alert-evaluator:leader
docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader
```

| Check | Expected | Observed |
| --- | --- | --- |
| Key value | instance UUID | |
| PTTL | between 0 and 15000 ms | |
| Key renewed (check PTTL twice, 6s apart) | TTL resets above 10000 ms | |

---

## Experiment 2: follower stands by

Run two terminals simultaneously:

```bash
# Terminal A
npm run evaluator

# Terminal B (1 second later)
npm run evaluator
```

Expected:

| Instance | Expected log |
| --- | --- |
| First (leader) | `acquired leader lease — starting scan loop` |
| Second (follower) | `running as follower — waiting for leader lease` |

Only one `leader scan tick` should appear. Two would indicate both instances are scanning.

| Check | Expected | Observed |
| --- | --- | --- |
| Exactly one leader log | yes | |
| Follower standby log | yes | |
| One scan tick per interval | yes | |

---

## Experiment 3: follower takeover after leader crash

```bash
# Terminal A — start leader
npm run evaluator

# Terminal B — start follower
npm run evaluator

# Kill the leader (Ctrl+C in Terminal A or kill the process)
```

Expected: follower acquires the lease within `LEADER_LEASE_TTL_MS` (15s) and begins scanning.

| Check | Expected | Observed |
| --- | --- | --- |
| Follower logs `acquired leader lease` after leader dies | yes | |
| Takeover within 15s | yes | |
| Scan tick appears on new leader | yes | |

---

## Experiment 4: graceful shutdown releases key immediately

```bash
npm run evaluator
# after one scan tick, Ctrl+C
docker exec sentinel-redis redis-cli EXISTS alert-evaluator:leader
```

| Check | Expected | Observed |
| --- | --- | --- |
| Key absent immediately after shutdown | `0` | |

---

## Observations

| Concept | Observed |
| --- | --- |
| SET NX PX is atomic — only one instance acquires | |
| Lua renewal guards ownership — no foreign key extension | |
| Lua release guards ownership — no foreign key deletion | |
| Lease lost mid-interval causes evaluator to stop scanning | |
| Follower takeover within one TTL window | |
| Graceful shutdown releases lease immediately | |
