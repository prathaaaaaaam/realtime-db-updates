"use strict";

const { Server }       = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");
const { createClient }  = require("ioredis");

const REDIS_URL   = process.env.REDIS_URL   || "redis://localhost:6379";
const INSTANCE_ID = process.env.INSTANCE_ID || "local";

/**
 * SocketManager
 * ─────────────
 * Wraps Socket.IO and the Redis pub/sub adapter that synchronises
 * events across every backend instance behind NGINX.
 *
 * Room strategy
 * ─────────────
 * • "orders"            – broadcast to ALL connected clients
 * • "order:<id>"        – subscribe to a specific order's changes
 * • "status:<status>"   – subscribe to a specific status bucket
 *
 * Clients can join rooms by emitting: socket.emit("subscribe", { room })
 */
class SocketManager {
  constructor(httpServer) {
    this.io = new Server(httpServer, {
      cors: { origin: "*", methods: ["GET", "POST"] },
      transports: ["websocket", "polling"],
    });

    this._connectedCount = 0;
  }

  async attachRedisAdapter() {
    const pubClient = createClient(REDIS_URL);
    const subClient = pubClient.duplicate();

    this.io.adapter(createAdapter(pubClient, subClient));
    console.log(`[Socket.IO] Redis adapter attached — ${REDIS_URL}`);
  }

  listen() {
    this.io.on("connection", (socket) => {
      this._connectedCount++;
      console.log(
        `[Socket.IO][${INSTANCE_ID}] Client connected: ${socket.id} ` +
        `(total on this instance: ${this._connectedCount})`
      );

      // Always join the global "orders" room
      socket.join("orders");

      // Allow fine-grained room subscriptions
      socket.on("subscribe", ({ room }) => {
        if (typeof room === "string" && room.match(/^[\w:]+$/)) {
          socket.join(room);
          socket.emit("subscribed", { room });
          console.log(`[Socket.IO] ${socket.id} joined room: ${room}`);
        }
      });

      socket.on("unsubscribe", ({ room }) => {
        socket.leave(room);
        socket.emit("unsubscribed", { room });
      });

      socket.on("disconnect", (reason) => {
        this._connectedCount = Math.max(0, this._connectedCount - 1);
        console.log(
          `[Socket.IO][${INSTANCE_ID}] Client disconnected: ${socket.id} — ${reason}`
        );
      });

      // Send current instance info on connect
      socket.emit("connected", {
        instanceId: INSTANCE_ID,
        serverTime: new Date().toISOString(),
      });
    });
  }

  /**
   * Broadcast an OrderChangeEvent to relevant rooms.
   *
   * Every event goes to the global "orders" room.
   * Specific rooms (order:<id>, status:<status>) are also notified
   * so clients can subscribe only to what they care about.
   */
  broadcast(event) {
    const { operation, order } = event;

    // Global room — all clients
    this.io.to("orders").emit("order:change", event);

    // Per-order room
    if (order?.id) {
      this.io.to(`order:${order.id}`).emit("order:change", event);
    }

    // Per-status rooms (both before and after status)
    if (order?.status) {
      this.io.to(`status:${order.status}`).emit("order:change", event);
    }
    if (event.before?.status && event.before.status !== order?.status) {
      this.io.to(`status:${event.before.status}`).emit("order:change", event);
    }

    console.log(
      `[Socket.IO][${INSTANCE_ID}] Broadcasted ${operation} — ` +
      `order #${order?.id ?? "?"} — ${order?.status ?? ""}`
    );
  }
}

module.exports = SocketManager;
