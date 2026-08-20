# Kafka Experiment: Node.js/TypeScript Producer and Consumer

CP1 is about getting your hands on Kafka before building anything real. You produce one event, consume it, watch the offsets move, crash and replay — all with `rpk` open in another terminal so you can see what the broker actually thinks is happening.

Read these in order:

| File | What it covers |
| --- | --- |
| [nodejs-kafka-client.md](nodejs-kafka-client.md) | Why kafkajs, how the two services are structured, tsx runtime, broker address |
| [producer-mechanics.md](producer-mechanics.md) | What `producer.send()` actually does, why we key by entity_id, RecordMetadata, duplicates |
| [consumer-group-offset-model.md](consumer-group-offset-model.md) | The three offset concepts, autoCommit gap, fromBeginning, crash replay, group independence |
| [kafka-node-experiment-debrief.md](kafka-node-experiment-debrief.md) | What we actually observed when we ran it — offsets, restart experiments, second group |

## Diagrams

| File | Shows |
| --- | --- |
| [cp1-sequence.puml](cp1-sequence.puml) | Time-ordered interaction: consumer startup, producer run, delivery, autoCommit, crash replay |
| [cp1-partition-state.excalidraw](cp1-partition-state.excalidraw) | Partition log with all four records and two independent consumer groups both caught up |
| [data-flow.puml](data-flow.puml) | Component view: producer to broker to consumer, offset commit cycle |
| [offset-model.puml](offset-model.puml) | Partition log with two independent consumer groups at different positions |
