# Alternative Approaches — Analysis & Trade-offs

The chosen stack (Postgres → Debezium → Kafka → Node.js → Socket.IO) is
**enterprise-grade** but carries real operational weight: 6 services, ~2 GB of
Docker images, and Debezium/Kafka expertise required to operate.

Below are three credible alternatives, ordered from lightest to most capable.

---

## Option A — PostgreSQL LISTEN/NOTIFY + WebSocket (Lightweight)

### Architecture
```
PostgreSQL TRIGGER → NOTIFY channel
        ↓
    Node.js (pg LISTEN)
        ↓
    WebSocket (ws / Socket.IO)
        ↓
    Clients
```

### How it works
1. A PL/pgSQL trigger fires on INSERT/UPDATE/DELETE and calls
   `NOTIFY orders_channel, '<json_payload>'`.
2. A Node.js process holds a persistent `pg` connection with `LISTEN orders_channel`.
3. On notification, it broadcasts via WebSocket.

### Code sketch

```sql
-- Trigger function
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('orders_channel', json_build_object(
    'op',    TG_OP,
    'order', row_to_json(NEW),
    'before',row_to_json(OLD)
  )::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_notify
  AFTER INSERT OR UPDATE OR DELETE ON orders
  FOR EACH ROW EXECUTE FUNCTION notify_order_change();
```

```javascript
// Node.js
const client = new Client(connectionString);
await client.connect();
await client.query('LISTEN orders_channel');
client.on('notification', ({ payload }) => {
  io.to('orders').emit('order:change', JSON.parse(payload));
});
```

### Trade-offs

| Dimension | Assessment |
|-----------|------------|
| **Simplicity** | ★★★★★ — 2 services (Postgres + Node.js), ~50 lines of code |
| **Ops burden** | ★★★★★ — nothing to install beyond a Node.js app |
| **Scalability** | ★★☆☆☆ — NOTIFY payload is capped at **8 KB**; no durability; single listener |
| **Durability** | ★★☆☆☆ — if Node.js is down during a DB change, that event is **lost** |
| **Throughput** | ★★★☆☆ — fine for < ~500 events/s |

**Best for:** internal dashboards, small teams, < 1 000 concurrent clients,
rapid prototypes. This is the approach used in tools like Supabase Realtime v1.

---

## Option B — PostgreSQL + Supabase Realtime (Managed CDC)

### Architecture
```
PostgreSQL (WAL) → Supabase Realtime (Elixir/Phoenix Channels) → Clients
```

Supabase Realtime is an open-source Elixir service that reads the WAL and
multiplexes changes over Phoenix Channels (WebSocket).

### How it works
- Deploy `supabase/realtime` alongside your Postgres instance.
- Clients subscribe with the JavaScript client library.
- No Kafka, no Debezium, no Zookeeper.

```javascript
// Client
import { createClient } from '@supabase/supabase-js';
const supabase = createClient(URL, KEY);

supabase
  .channel('orders')
  .on('postgres_changes', { event: '*', schema: 'public', table: 'orders' }, handleChange)
  .subscribe();
```

### Trade-offs

| Dimension | Assessment |
|-----------|------------|
| **Simplicity** | ★★★★☆ — one extra service but fully managed if using Supabase cloud |
| **Scalability** | ★★★★☆ — Elixir actor model handles millions of WS connections |
| **Durability** | ★★★☆☆ — WAL-based, but no persistent event log like Kafka |
| **Throughput** | ★★★★☆ — Elixir is highly concurrent |
| **Vendor lock-in** | ★★★☆☆ — if using managed Supabase; self-host mitigates this |

**Best for:** teams that want WAL-based CDC without operating Kafka.
The best balance of capability and simplicity for most applications.

---

## Option C — Debezium + Kafka Streams + SSE (No Socket.IO)

### Architecture
```
Postgres → Debezium → Kafka → Kafka Streams (aggregations) → SSE endpoint → Clients
```

Replace Socket.IO with Server-Sent Events (SSE). Use Kafka Streams
for stateful aggregations (e.g. real-time order counts per status).

### Trade-offs

| Dimension | Assessment |
|-----------|------------|
| **Streaming analytics** | ★★★★★ — Kafka Streams enables windowed aggregations |
| **Client simplicity** | ★★★★★ — SSE is just `EventSource` in the browser; no lib needed |
| **Bi-directional** | ★☆☆☆☆ — SSE is one-way; can't do room subscriptions |
| **Ops burden** | ★★☆☆☆ — same Kafka/Debezium complexity plus Kafka Streams |

**Best for:** analytics pipelines where you also want browser updates,
and you don't need client→server messaging.

---

## Decision Matrix

| Requirement | LISTEN/NOTIFY | Supabase Realtime | Chosen Stack | SSE + Streams |
|-------------|:---:|:---:|:---:|:---:|
| < 500 events/s | ✅ | ✅ | ✅ | ✅ |
| > 10 000 events/s | ❌ | ✅ | ✅ | ✅ |
| Durable event log | ❌ | ⚠️ | ✅ | ✅ |
| Multiple consumers | ❌ | ⚠️ | ✅ | ✅ |
| Room subscriptions | ⚠️ | ✅ | ✅ | ❌ |
| Team < 3 engineers | ✅ | ✅ | ❌ | ❌ |
| Horizontal scale | ❌ | ✅ | ✅ | ✅ |
| Schema evolution | ❌ | ⚠️ | ✅ | ✅ |
| Replay past events | ❌ | ❌ | ✅ | ✅ |

### Verdict

- **For this assignment / interview context:** the chosen Debezium + Kafka
  stack is the correct choice — it demonstrates knowledge of enterprise CDC
  patterns and is genuinely production-ready.

- **For a real startup (< 10 engineers):** **Option B (Supabase Realtime)**
  gives 80% of the capability at 20% of the operational cost.

- **For a simple internal tool or proof-of-concept:** **Option A
  (LISTEN/NOTIFY)** is all you need and can be shipped in a single afternoon.

- **Add Kafka only when you need:** (a) durable replay, (b) fan-out to
  multiple independent consumer services, or (c) throughput > ~2 000 events/s.
