# Redpanda and Kafka: Basics

## What Kafka is

Kafka is a **distributed event log**. Services write events to it; other services read those events at their own pace.

The key idea: when something publishes an event, it does not call the downstream service directly. It writes to Kafka and forgets. The downstream service reads from Kafka whenever it is ready. The two sides never talk to each other:Kafka sits between them.

This is different from a direct HTTP call, where the caller must wait for the receiver to respond, and the receiver must be available right now.

## Broker, producer, consumer

**Broker**:the Kafka server. It receives events, stores them, and serves them to readers. In Sentinel there is one broker running locally (Redpanda).

**Producer**:a service that writes events to the broker. Example: the Ingestion Poller writes raw ADS-B telemetry to the broker.

**Consumer**:a service that reads events from the broker. Example: the Position Consumer reads from the broker and writes normalized positions to TimescaleDB.

A service can be both a producer and a consumer at the same time. The Position Consumer reads raw telemetry and writes normalized events back to the broker.

## Topic

A **topic** is a named stream of events. Producers write to a topic by name; consumers read from a topic by name.

Sentinel's topics map directly to data flow boundaries:

```
adsb.raw            :raw ADS-B telemetry from the Ingestion Poller
ais.raw             :raw AIS telemetry from the Ingestion Poller
position.normalized :cleaned positions from the Position Consumer
deviation.candidates:per-ping route classifications from the Deviation Detector
proximity.candidates:new proximity episodes from the Correlation Worker
alerts              :alert events from the Alert Evaluator
adsb.dlq / ais.dlq  :rejected records that could not be parsed
```

Each topic is an independent stream. A consumer reading `position.normalized` has no effect on a consumer reading `alerts`.

## Why Sentinel uses an event broker

Without a broker, services would call each other directly over HTTP. That creates tight coupling:

- the Position Consumer would need to know the address of the Deviation Detector and the Correlation Worker
- if either downstream service is slow or unavailable, the Position Consumer would stall or fail
- adding a new consumer of position events would require changing the Position Consumer

With Kafka, the Position Consumer writes to `position.normalized` and stops. It does not know or care how many services are reading from that topic. The Deviation Detector and Correlation Worker each read independently, at their own pace, without affecting each other or the producer.

This also means a service can restart and catch up on events it missed while it was down:without the producer needing to retry or buffer anything. That property is used heavily in Sentinel for crash recovery and replay.

## What Redpanda is

Redpanda is a **Kafka-compatible broker** rewritten from scratch in C++. It speaks the same Kafka wire protocol, so any Kafka client library connects to Redpanda without changes.

The practical differences for local development:
- no ZooKeeper required:simpler to run in Docker
- ships as a single binary
- comes with `rpk`, a clean CLI for topic management and inspection

In production Sentinel targets **Amazon MSK**, which is managed Kafka on AWS. The application code does not change between environments:it uses the Kafka protocol in both cases. Redpanda is the local stand-in.

## What runs in Checkpoint 1

At this point, Redpanda is running and healthy but has no topics and no producers or consumers connected to it. The cluster is empty. Checkpoint 3 will create the canonical topics and exercise the producer/consumer cycle manually.
