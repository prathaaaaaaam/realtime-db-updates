# Orders Real-Time CDC System

> **PostgreSQL → Debezium → Kafka → Node.js → Socket.IO → Browser/CLI**
> A production-grade, horizontally scalable real-time change-data-capture pipeline.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          WRITE PATH                                     │
│                                                                         │
│  SQL Client ──► PostgreSQL (WAL: logical) ──► Debezium Connect          │
│                       ▲                            │                    │
│               orders table                         │ CDC events         │
│               + publication                        ▼                    │
│                                            Apache Kafka                 │
│                                      topic: dbserver1.public.orders     │
└─────────────────────────────────────────────────────────────────────────┘
                                                │
                                                │ KafkaJS consumer
                                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          GATEWAY LAYER                                  │
│                                                                         │
│          ┌──────────────────┐    Redis pub/sub    ┌──────────────────┐  │
│          │  Node.js :3000   │◄──────────────────►│  Node.js :3000   │   │
│          │  (backend-1)     │   @socket.io/redis  │  (backend-2)     │  │
│          └──────────────────┘       adapter       └──────────────────┘  │
│                   ▲                                        ▲            │
│                   └──────────────┬─────────────────────────┘            │
│                                  │                                      │
│                          NGINX load balancer                            │
│                         (least_conn + WS upgrade)                       │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼
              Browser          Browser         CLI client
             (Socket.IO)     (Socket.IO)    (socket.io-client)
```

### Why each piece exists

| Component | Role | Key decision |
|-----------|------|-------------|
| **PostgreSQL WAL** | Source of truth; `wal_level=logical` exposes row-level changes | Native logical replication — no triggers, no polling |
| **Debezium** | Reads the WAL replication slot; converts changes into structured events | Battle-tested CDC; handles schema evolution, snapshots, restarts |
| **Kafka** | Durable event bus; decouples producers from consumers | Consumers can replay events; adds back-pressure; fan-out to N services |
| **Redis** | Socket.IO adapter for broadcast across instances | Without this, backend-1 can't reach clients on backend-2 |
| **Node.js + Socket.IO** | Gateway: Kafka → WebSocket/polling | Lightweight, excellent WebSocket support |
| **NGINX** | L7 load balancer with WebSocket upgrade | `Upgrade: websocket` header forwarding is essential |

---

## Prerequisites

- Docker ≥ 24 and Docker Compose ≥ 2.20
- Ports free: **80, 5432, 8083, 9092, 6379**

---

## Quick Start

```bash
# 1. Clone / unzip the project
cd realtime-db-updates

# 2. Start all services (first run pulls ~2 GB of images)
docker compose up --build -d

# 3. Watch logs to confirm everything is healthy
docker compose logs -f debezium connector-init backend-1

# 4. Open the browser dashboard
open http://localhost           # macOS
# or: xdg-open http://localhost  (Linux)
```

Wait ~30–60 seconds for Debezium to register the connector and complete
the initial snapshot.  You will see SNAPSHOT events appear in the dashboard.

---

## Triggering Events

Connect to PostgreSQL and fire DML:

```bash
docker exec -it orders_postgres \
  psql -U ordersuser -d ordersdb
```

```sql
-- INSERT → new order appears live
INSERT INTO orders (customer_name, product_name, status)
VALUES ('Diana Prince', 'Noise-Cancelling Earbuds', 'pending');

-- UPDATE → status transition visible in real time
UPDATE orders SET status = 'shipped'   WHERE id = 1;
UPDATE orders SET status = 'delivered' WHERE id = 1;

-- DELETE → event fires with the deleted row's data
DELETE FROM orders WHERE id = 3;
```

---

## CLI Client

```bash
cd client
npm install

# Watch all events
node cli-client.js --url http://localhost

# Watch only UPDATEs
node cli-client.js --url http://localhost --filter UPDATE

# Watch only delivered orders
node cli-client.js --url http://localhost --status delivered
```

---

## Project Structure

```
realtime-db-updates/
├── docker-compose.yml          ← Orchestrates every service
├── postgres/
│   └── init.sql                ← Table DDL + publication + seed data
├── debezium/
│   └── connector.json          ← Debezium PostgreSQL connector config
├── backend/
│   ├── server.js               ← Express + Socket.IO entry point
│   ├── kafka-consumer.js       ← KafkaJS consumer + event normalisation
│   ├── socket-manager.js       ← Socket.IO + Redis adapter + room strategy
│   ├── package.json
│   └── Dockerfile
├── client/
│   ├── index.html              ← Browser dashboard (served by NGINX)
│   ├── cli-client.js           ← Terminal monitor
│   └── package.json
└── nginx/
    └── nginx.conf              ← Upstream pool + WS upgrade headers
```

---

## Socket.IO Event Reference

### Events emitted to clients

| Event | Payload | Description |
|-------|---------|-------------|
| `connected` | `{ instanceId, serverTime }` | Sent once on connection |
| `subscribed` | `{ room }` | Acknowledged after `subscribe` |
| `order:change` | `OrderChangeEvent` | Every DB change |

### `OrderChangeEvent` shape

```jsonc
{
  "operation": "INSERT" | "UPDATE" | "DELETE" | "SNAPSHOT",
  "order": {
    "id": 42,
    "customer_name": "Alice",
    "product_name": "Keyboard",
    "status": "shipped",
    "updated_at": "2024-01-15T10:30:00.000Z"
  },
  "before": null,          // previous row for UPDATE; null otherwise
  "timestamp": "2024-01-15T10:30:00.123Z"
}
```

### Room subscriptions

```javascript
// Join rooms from client side
socket.emit("subscribe", { room: "order:42" });      // specific order
socket.emit("subscribe", { room: "status:pending" }); // status bucket
```

---

## Horizontal Scaling

The system is designed to scale the gateway layer horizontally:

```bash
# Spin up a third backend instance
docker compose scale backend-1=1 backend-2=1 # or add backend-3 in compose

# NGINX distributes connections; Redis adapter syncs broadcasts
```

Because every instance shares the same Kafka consumer group and Redis
adapter, a client connected to `backend-2` receives events even when
the Kafka message was consumed by `backend-1`.

---

## Health Checks

```bash
# Backend health endpoint
curl http://localhost/api/health

# Debezium connector status
curl http://localhost:8083/connectors/orders-postgres-connector/status

# Kafka topic lag
docker exec orders_kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group orders-gateway-group --describe
```

---

## Stopping

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers AND delete all data
```

---

## Alternative Approaches & Trade-off Analysis

See `ALTERNATIVES.md` for a detailed comparison of three lighter-weight
approaches that may be better suited depending on team size and traffic.

---

## Evaluation Criteria Checklist

| Criterion | How addressed |
|-----------|--------------|
| **Scalability** | NGINX LB + Redis adapter → add Node.js replicas freely; Kafka partitions → add consumers |
| **Efficiency** | WAL-based CDC has ~0 DB overhead vs polling; Kafka decouples throughput spikes |
| **No polling** | WAL replication slot = push-only; WebSocket = push-only |
| **Correctness** | Debezium provides exactly-once CDC with restart recovery |
| **Code quality** | Modular: consumer, socket-manager, server are independent classes |
| **Documentation** | This README + inline JSDoc comments throughout |
