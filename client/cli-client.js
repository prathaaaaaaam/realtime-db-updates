#!/usr/bin/env node
/**
 * cli-client.js — Terminal-based real-time orders monitor
 *
 * Usage:
 *   node cli-client.js [--url http://localhost:80] [--filter UPDATE]
 *
 * Connects to the Socket.IO gateway and pretty-prints every
 * order:change event to stdout.
 */

const { io } = require("socket.io-client");

const args    = process.argv.slice(2);
const getArg  = (flag, def) => {
  const i = args.indexOf(flag);
  return i !== -1 && args[i + 1] ? args[i + 1] : def;
};

const SERVER_URL    = getArg("--url",    "http://localhost:80");
const FILTER_OP     = getArg("--filter", "ALL").toUpperCase();
const FILTER_STATUS = getArg("--status", "ALL").toLowerCase();

// ── ANSI colours ──────────────────────────────────────────────
const C = {
  reset:  "\x1b[0m",
  dim:    "\x1b[2m",
  bold:   "\x1b[1m",
  cyan:   "\x1b[36m",
  green:  "\x1b[32m",
  amber:  "\x1b[33m",
  red:    "\x1b[31m",
  purple: "\x1b[35m",
  grey:   "\x1b[90m",
};

const opColour = { INSERT: C.green, UPDATE: C.amber, DELETE: C.red, SNAPSHOT: C.purple };

function pad(s, n)  { return String(s).padEnd(n); }
function rpad(s, n) { return String(s).padStart(n); }
function ts()       { return new Date().toTimeString().slice(0, 8); }

function printBanner() {
  console.log(`\n${C.cyan}${C.bold}`);
  console.log("  ╔═══════════════════════════════════╗");
  console.log("  ║   Orders Real-Time CLI Monitor    ║");
  console.log("  ╚═══════════════════════════════════╝");
  console.log(`${C.reset}`);
  console.log(`  ${C.dim}Server : ${C.reset}${SERVER_URL}`);
  console.log(`  ${C.dim}Filter : ${C.reset}op=${FILTER_OP}  status=${FILTER_STATUS}`);
  console.log(`  ${C.dim}Press Ctrl-C to exit${C.reset}\n`);
  console.log(`  ${C.grey}${"─".repeat(70)}${C.reset}`);
  console.log(
    `  ${C.grey}${pad("TIME", 10)}${pad("OP", 10)}${pad("ID", 6)}` +
    `${pad("CUSTOMER", 22)}${pad("PRODUCT", 20)}STATUS${C.reset}`
  );
  console.log(`  ${C.grey}${"─".repeat(70)}${C.reset}`);
}

function printEvent(ev) {
  const { operation, order, before, timestamp } = ev;

  // Apply filters
  if (FILTER_OP !== "ALL" && operation !== FILTER_OP) return;
  const row = order || before;
  if (FILTER_STATUS !== "all" && row?.status !== FILTER_STATUS) return;

  const col    = opColour[operation] || C.reset;
  const status = order?.status || before?.status || "—";
  const statusLabel =
    status === "pending"   ? `${C.amber}pending${C.reset}` :
    status === "shipped"   ? `${C.purple}shipped${C.reset}` :
    status === "delivered" ? `${C.green}delivered${C.reset}` :
    status;

  // Show status diff for UPDATEs
  const statusDisplay =
    operation === "UPDATE" && before?.status && before.status !== order?.status
      ? `${C.grey}${before.status}${C.reset} → ${statusLabel}`
      : statusLabel;

  const time   = new Date(timestamp).toTimeString().slice(0, 8);
  const opStr  = `${col}${pad(operation, 10)}${C.reset}`;
  const idStr  = `${C.cyan}${rpad(row?.id ?? "?", 4)}${C.reset}  `;
  const custStr = pad((row?.customer_name ?? "—").slice(0, 20), 22);
  const prodStr = pad((row?.product_name ?? "—").slice(0, 18), 20);

  console.log(`  ${C.dim}${time}${C.reset}  ${opStr}${idStr}${custStr}${prodStr}${statusDisplay}`);
}

// ── Connect ───────────────────────────────────────────────────
printBanner();

const socket = io(SERVER_URL, {
  transports: ["websocket", "polling"],
  reconnectionDelay: 2000,
});

let eventCount = 0;

socket.on("connect", () => {
  console.log(`\n  ${C.green}✓${C.reset} Connected — socket: ${socket.id.slice(0, 12)}…\n`);
});

socket.on("connected", ({ instanceId }) => {
  console.log(`  ${C.dim}Backend instance: ${C.reset}${instanceId}\n`);
  console.log(`  ${C.grey}${"─".repeat(70)}${C.reset}`);
});

socket.on("order:change", (event) => {
  eventCount++;
  printEvent(event);
});

socket.on("disconnect", (reason) => {
  console.log(`\n  ${C.red}✗${C.reset} Disconnected: ${reason}\n`);
});

socket.on("connect_error", (err) => {
  console.log(`\n  ${C.red}✗${C.reset} Connection error: ${err.message}`);
  console.log(`  ${C.dim}Retrying…${C.reset}\n`);
});

process.on("SIGINT", () => {
  console.log(`\n\n  ${C.dim}Total events received: ${eventCount}${C.reset}`);
  console.log(`  Goodbye!\n`);
  socket.disconnect();
  process.exit(0);
});
