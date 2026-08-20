# Producer Mechanics

## What actually happens when you call `producer.send()`

There are three steps, and it helps to picture them as a short conversation between your process and the broker.

First, `producer.connect()` opens a TCP connection and does a version handshake — your client asks the broker "what Kafka protocol versions do you support?" and they agree on one. This is the moment the producer becomes aware of the cluster topology.

Then `producer.send()` fires off a ProduceRequest. The broker receives it, appends the record to the partition log, assigns it an offset, and sends back a RecordMetadata response telling you exactly where the record landed. From your code's perspective it's one async call, but underneath there's a round trip happening.

Finally `producer.disconnect()` closes the TCP session cleanly. In the CP1 experiment the producer does its job and exits — it's not a long-running process yet.

---

## Why we set a key on every message

```typescript
messages: [{ key: event.entity_id, value: JSON.stringify(event) }]
```

Right now `adsb.raw` has one partition, so the key doesn't affect where the record lands — there's only one place it can go. But the convention matters.

When we scale to multiple partitions later, kafkajs will hash the key (murmur2 mod partition count) and route all events for the same `entity_id` to the same partition. That means a single consumer always sees events for a given aircraft in the order they were produced. Without a key, records get round-robined across partitions and order is gone.

The key is also stored in the partition log and delivered to consumers alongside the value, so downstream services can use it for routing or deduplication without having to parse the payload.

---

## Reading the RecordMetadata response

After `producer.send()` resolves, you get back one `RecordMetadata` object per topic-partition in the batch. The important fields:

- `partition` — which partition the record landed in
- `baseOffset` — the offset of the first record in the batch (as a string, see below)
- `timestamp` — broker-assigned timestamp in milliseconds

`baseOffset` comes back as a string, not a number. Kafka offsets are 64-bit integers and can grow large enough to exceed JavaScript's safe integer range, so kafkajs keeps them as strings to avoid silent precision loss. To verify, run `rpk topic describe -p adsb.raw` after producing — the HIGH-WATERMARK should be `baseOffset + 1`.

---

## Acknowledgement and what it means for duplicates

kafkajs defaults to `acks: -1`, which means all in-sync replicas must confirm the write before the producer call resolves. In our dev setup there's one replica, so the broker acknowledges as soon as it writes. In a three-replica production setup, all three would need to confirm.

The catch: if the broker writes the record but the network drops before the acknowledgement reaches your producer, kafkajs will retry and potentially write the same record twice. This is not a bug — it's the documented at-least-once behaviour. The downstream services (TimescaleDB's `ON CONFLICT DO NOTHING`, Redis's monotonic timestamp guard) are what absorb those duplicates. The producer's job is to get the record there; correctness under redelivery is the consumer's responsibility.
