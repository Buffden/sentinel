# Leader Election

Concept notes and debrief for the Alert Evaluator leader election implementation.

| File | What's inside |
| --- | --- |
| [leader-election-design.md](leader-election-design.md) | Why only one evaluator should scan at a time, the Redis lease approach, ownership-safe acquire/renew/release, Lua guard scripts, and failure modes |
| [leader-election-debrief.md](leader-election-debrief.md) | Observed outputs from running the leader election and follower takeover experiments |
