# Downstream Publishing

Concept notes and debrief for the Position Consumer's two downstream publish operations: `position.normalized` to Kafka and `position-updates` to Redis pub/sub.

| File | What's inside |
| --- | --- |
| [normalized-publish.md](normalized-publish.md) | `position.normalized` schema, why it blocks offset commit, keying strategy, relationship to upstream consumers |
| [position-updates-pubsub.md](position-updates-pubsub.md) | Redis pub/sub design, fire-and-forget semantics, payload shape, why failure must not block the offset |
| [downstream-publishing-debrief.md](downstream-publishing-debrief.md) | Observed outputs from Kafka consume and live Redis pub/sub subscription |
