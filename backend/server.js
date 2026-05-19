"use strict";

const http          = require("http");
const express       = require("express");
const KafkaConsumer = require("./kafka-consumer");
const SocketManager = require("./socket-manager");

const PORT        = parseInt(process.env.PORT || "3000", 10);
const INSTANCE_ID = process.env.INSTANCE_ID || "local";

// ─── App bootstrap ───────────────────────────────────────────

const app    = express();
const server = http.createServer(app);

// ─── REST endpoints ──────────────────────────────────────────

app.get("/health", (_req, res) => {
  res.json({
    status:     "ok",
    instanceId: INSTANCE_ID,
    uptime:     process.uptime(),
    timestamp:  new Date().toISOString(),
  });
});

app.get("/", (_req, res) => {
  res.json({
    service:    "Orders Real-Time Gateway",
    instanceId: INSTANCE_ID,
    docs:       "Connect via Socket.IO to receive real-time order updates.",
    rooms: {
      "orders":          "All order changes",
      "order:<id>":      "Changes for a specific order",
      "status:<status>": "Changes for orders in a given status bucket",
    },
  });
});

// ─── Main ────────────────────────────────────────────────────

async function main() {
  console.log(`\n🚀  Orders Gateway [${INSTANCE_ID}] starting…\n`);

  // 1. Set up Socket.IO with Redis adapter
  const socketManager = new SocketManager(server);
  await socketManager.attachRedisAdapter();
  socketManager.listen();

  // 2. Wire Kafka consumer → Socket.IO broadcast
  const kafkaConsumer = new KafkaConsumer();
  kafkaConsumer.onOrderChange((event) => {
    socketManager.broadcast(event);
  });

  await kafkaConsumer.connect();

  // 3. Start HTTP + WS server
  server.listen(PORT, () => {
    console.log(`✅  Listening on port ${PORT}  [instance: ${INSTANCE_ID}]`);
  });

  // ─── Graceful shutdown ──────────────────────────────────────
  const shutdown = async (signal) => {
    console.log(`\n⚠️   Received ${signal} — shutting down gracefully…`);
    server.close(async () => {
      await kafkaConsumer.disconnect();
      console.log("✅  Clean shutdown complete");
      process.exit(0);
    });
    // Force exit after 10 s
    setTimeout(() => process.exit(1), 10_000);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT",  () => shutdown("SIGINT"));
}

main().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
