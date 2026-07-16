# INGSOL Messaging

Real-time messaging service. Spring Boot + Kafka + STOMP over WebSocket.

## Run

```bash
# 1. Kafka (single-broker dev cluster)
docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0

# 2. App
mvn spring-boot:run   # or ./mvnw after `mvn wrapper:wrapper`

# 3. Open the test client
open http://localhost:8080/
```

## API

| Transport | Endpoint | Purpose |
|-----------|----------|---------|
| WebSocket | connect `ws://host/ws` (SockJS) | browser clients |
| WebSocket | SEND `/app/chat.send` | send a message |
| WebSocket | SUBSCRIBE `/topic/rooms/{roomId}` | receive a room's messages |
| REST | `POST /api/messages` | send from bots/integrations (202 Accepted) |

Message body:
```json
{ "roomId": "room-42", "sender": "alice", "content": "hello" }
```

## Why this architecture

- **WebSocket** gives clients a live push channel. But a WebSocket broker only knows
  the clients connected to *its own* JVM. With more than one instance behind a load
  balancer, a message sent on instance A never reaches a subscriber on instance B.
- **Kafka** is the fan-out bus that fixes that. Every message goes to Kafka; every
  instance consumes it and pushes to *its* local subscribers. Add instances freely.
- **Unique consumer group per instance** (`group-id` uses `random.uuid`): a shared
  group would load-balance messages so only one instance saw each one. Unique groups
  mean every instance sees every message. This is the crux of the design.
- **roomId as the Kafka key**: same key -> same partition -> ordered per room, while
  different rooms still spread across partitions for throughput.
- **Idempotent producer + acks=all**: safe retries, no silent loss on broker failover.

## Deliberately skipped (add when needed)

- **Persistence / history** — messages are transport-only; add Postgres or a historian
  and a `GET /api/rooms/{id}/messages` when you need replay.
- **Auth** — no identity yet; put a WebSocket handshake interceptor + JWT on the REST
  API before exposing it. `sender` is currently client-asserted and trusted.
- **Dead-letter topic** — relying on Spring's default retry; add a DLT for poison messages.
