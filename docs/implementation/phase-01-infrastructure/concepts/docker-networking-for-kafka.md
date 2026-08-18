# Docker Networking for Kafka

`localhost` always means "this machine." From your Mac it is your Mac. From inside a container it is that container:not your Mac, not another container.

Docker Compose puts all services on a private virtual network where each container is reachable by its **service name**. So `redpanda:29092` works from another container, but not from your Mac. Your Mac can only reach what is published via `ports:`.

Kafka makes this harder: when a client connects, the broker responds with **metadata** telling the client which address to use for follow-up requests. If that advertised address is wrong for the caller, the connection fails after the handshake.

The fix is two listeners:

| Listener | Advertised as | Used by |
|---|---|---|
| `PLAINTEXT` | `localhost:9092` | Your Mac (`rpk`, manual exercises) |
| `PLAINTEXT_INTERNAL` | `redpanda:29092` | Future services inside Docker |

In production, MSK provides a single endpoint:no dual-listener needed. The only difference between local and prod is the value of `KAFKA_BROKERS`.
