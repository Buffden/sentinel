# Producer Mechanics

## Produce sequence

1. `producer.connect()` — TCP connection + Kafka API version handshake.
2. `producer.send()` — ProduceRequest with topic, key, and encoded value. Broker writes to partition log, assigns offset, responds with RecordMetadata.
3. `producer.disconnect()` — closes the TCP session cleanly.

---

## Message key

```typescript
messages: [{ key: event.entity_id, value: JSON.stringify(event) }]
```

`entity_id` is the key. In multi-partition topics kafkajs hashes the key (murmur2 mod partition count) so all events for the same entity land in the same partition — preserving per-entity order for one consumer. With one partition the key has no routing effect but the convention is correct from the start.

The key is stored in the partition log and delivered to consumers alongside the value.

---

## RecordMetadata

`producer.send()` returns one `RecordMetadata` per topic-partition in the batch.

| Field | Type | Meaning |
| --- | --- | --- |
| `topicName` | string | Topic written to |
| `partition` | number | Partition the record landed in |
| `baseOffset` | string | Offset of the first record in the batch |
| `timestamp` | string | Broker-assigned timestamp in ms |

`baseOffset` is a string — Kafka offsets are 64-bit integers and can exceed JS safe integer range. Cross-verify with `rpk topic describe -p adsb.raw`: HIGH-WATERMARK after the produce should be `baseOffset + 1`.

---

## Acknowledgement

kafkajs default: `acks: -1` — all in-sync replicas must acknowledge. With replication factor 1 (dev setup) there is one replica; the broker acknowledges immediately after writing. With replication factor 3 (production), all three must confirm.

Producer retries on timeout can produce duplicate records. Durable idempotency (`ON CONFLICT DO NOTHING`, monotonic Redis guard) absorbs those duplicates downstream.
