# Kafka Experiment: Node.js/TypeScript Producer and Consumer

| File | Topic |
| --- | --- |
| [nodejs-kafka-client.md](nodejs-kafka-client.md) | Library choice, project structure, tsx runtime, broker address |
| [producer-mechanics.md](producer-mechanics.md) | kafkajs producer, message key, RecordMetadata, acknowledgement |
| [consumer-group-offset-model.md](consumer-group-offset-model.md) | Three offset concepts, autoCommit, fromBeginning, restart behavior, group independence |
| [kafka-node-experiment-debrief.md](kafka-node-experiment-debrief.md) | Observed runtime results, rpk inspection, offset/restart experiment, second-group experiment |

## Diagrams

| File | Shows |
| --- | --- |
| [data-flow.puml](data-flow.puml) | Node.js producer → adsb.raw → Node.js consumer group; offset commit cycle |
| [offset-model.puml](offset-model.puml) | Partition log, record offsets, two independent consumer-group positions |
