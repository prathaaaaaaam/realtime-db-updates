"use strict";

const { Kafka, logLevel } = require("kafkajs");

const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const TOPIC   = process.env.KAFKA_TOPIC    || "dbserver1.public.orders";
const GROUP   = process.env.KAFKA_GROUP_ID || "orders-gateway-group";

/**
 * KafkaConsumer wraps KafkaJS and emits normalised order-change events.
 *
 * Debezium message anatomy
 * ─────────────────────────
 * {
 *   op: "c" | "u" | "d" | "r"   (create / update / delete / snapshot-read)
 *   before: { ...row } | null
 *   after:  { ...row } | null
 *   source: { ts_ms, … }
 * }
 */
class KafkaConsumer {
  constructor() {
    this.kafka = new Kafka({
      clientId: `orders-gateway-${process.env.INSTANCE_ID || "local"}`,
      brokers: BROKERS,
      logLevel: logLevel.WARN,
      retry: {
        initialRetryTime: 300,
        retries: 10,
      },
    });

    this.consumer = this.kafka.consumer({
      groupId: GROUP,
      sessionTimeout: 30_000,
      heartbeatInterval: 5_000,
    });

    // Subscribers: Array<(event: OrderChangeEvent) => void>
    this._subscribers = [];
  }

  /** Register a callback invoked for every order change event */
  onOrderChange(fn) {
    this._subscribers.push(fn);
    return () => {
      this._subscribers = this._subscribers.filter((s) => s !== fn);
    };
  }

  async connect() {
    await this.consumer.connect();
    console.log(`[Kafka] Connected — group: ${GROUP}`);

    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: false });
    console.log(`[Kafka] Subscribed to topic: ${TOPIC}`);

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const raw = message.value?.toString();
          if (!raw) return; // tombstone / null payload

          const payload = JSON.parse(raw);
          const event   = this._normalise(payload);
          if (!event) return;

          this._subscribers.forEach((fn) => fn(event));
        } catch (err) {
          console.error("[Kafka] Failed to process message:", err.message);
        }
      },
    });
  }

  async disconnect() {
    await this.consumer.disconnect();
    console.log("[Kafka] Disconnected");
  }

  // ── Private ──────────────────────────────────────────────────

  /**
   * Convert a raw Debezium envelope to a clean OrderChangeEvent.
   *
   * @returns {OrderChangeEvent|null}
   *
   * OrderChangeEvent = {
   *   operation : "INSERT" | "UPDATE" | "DELETE" | "SNAPSHOT"
   *   order     : { id, customer_name, product_name, status, updated_at }
   *   before    : <previous row for UPDATE, null otherwise>
   *   timestamp : ISO string
   * }
   */
  _normalise(payload) {
    const opMap = { c: "INSERT", u: "UPDATE", d: "DELETE", r: "SNAPSHOT" };
    const operation = opMap[payload.op];
    if (!operation) return null;

    const row    = payload.after  ?? payload.before; // DELETE has only before
    const before = payload.before ?? null;

    if (!row) return null;

    return {
      operation,
      order: this._formatRow(row),
      before: before ? this._formatRow(before) : null,
      timestamp: new Date(payload.source?.ts_ms ?? Date.now()).toISOString(),
    };
  }

  _formatRow(row) {
    if (!row) return null;
    return {
      id:            row.id,
      customer_name: row.customer_name,
      product_name:  row.product_name,
      status:        row.status,
      updated_at:    row.updated_at,
    };
  }
}

module.exports = KafkaConsumer;
