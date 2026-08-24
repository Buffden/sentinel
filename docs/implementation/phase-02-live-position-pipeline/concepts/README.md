# Phase 02 Concepts

Concept notes and debrief records for Phase 02 checkpoints, in the order you'd read them while working through the phase.

| Folder / File | What's inside |
| --- | --- |
| [kafka-experiment/](kafka-experiment/) | Producer, consumer, offset model, group behavior — the Kafka experiment |
| [opensky-ingestion/](opensky-ingestion/) | OpenSky API shape, polling loop design, provider-fidelity boundary, observed debrief |
| [normalization-schema-plan.md](normalization-schema-plan.md) | Full canonical schema design: NormalizedPosition fields, source vs provider taxonomy, NavigationStatus enum, checkpoint boundaries CP3-CP8 |
| [cp4-dlq-routing/](cp4-dlq-routing/) | Rejection classification (parse_error vs missing_entity_id vs no_position), DLQ envelope design, unconditional offset commit contract, observed debrief |
