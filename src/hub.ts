/**
 * hub.ts — HUB component for chanbus.
 *
 * Exports: Conn, StateStore, PersistedState, MemoryStore, FileStore, Hub, serveHub
 */

import {
  type ClientFrame,
  type ServerFrame,
  type AgentInfo,
  type AgentState,
  type Message,
  type RoutingEvent,
  uniqueName,
  DEFAULT_PORT,
  DEFAULT_HOST,
  DEFAULT_STATE_PATH,
  WS_PATH,
  HTTP,
  HEARTBEAT_MS,
  HEARTBEAT_GRACE_MS,
  MAILBOX_MAX,
  MAILBOX_TTL_MS,
  HISTORY_MAX,
} from "./protocol.ts";

import { existsSync, mkdirSync, renameSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { homedir } from "node:os";

// ─────────────────────────────── interfaces ───────────────────────────────

/** Transport-agnostic connection the hub talks through. */
export interface Conn {
  send(frame: ServerFrame): void;
  close(): void;
}

export interface PersistedState {
  agents: Record<string, { name: string; cwd?: string; host?: string; lastSeen: number; workspace?: string }>;
  mailbox: Record<string, Message[]>;
  history: RoutingEvent[];
}

export interface StateStore {
  load(): PersistedState;
  save(s: PersistedState): void;
}

// ─────────────────────────────── MemoryStore ───────────────────────────────

export class MemoryStore implements StateStore {
  private state: PersistedState = { agents: {}, mailbox: {}, history: [] };

  load(): PersistedState {
    return JSON.parse(JSON.stringify(this.state)) as PersistedState;
  }

  save(s: PersistedState): void {
    this.state = JSON.parse(JSON.stringify(s)) as PersistedState;
  }
}

// ─────────────────────────────── FileStore ───────────────────────────────

export class FileStore implements StateStore {
  constructor(private readonly path: string) {}

  load(): PersistedState {
    if (!existsSync(this.path)) {
      return { agents: {}, mailbox: {}, history: [] };
    }
    try {
      return JSON.parse(readFileSync(this.path, "utf8")) as PersistedState;
    } catch {
      return { agents: {}, mailbox: {}, history: [] };
    }
  }

  save(s: PersistedState): void {
    const dir = dirname(this.path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    const tmp = `${this.path}.tmp.${process.pid}`;
    writeFileSync(tmp, JSON.stringify(s, null, 2), "utf8");
    renameSync(tmp, this.path);
  }
}

// ─────────────────────────────── internal types ───────────────────────────────

interface AgentRecord {
  id: string;
  name: string;
  state: AgentState;
  lastSeen: number;
  workspace: string; // isolation partition; "default" when unset
  cwd?: string;
  host?: string;
  conn?: Conn; // present when online
}

// ─────────────────────────────── Hub ───────────────────────────────

export class Hub {
  private agents = new Map<string, AgentRecord>(); // id → record
  private connToId = new Map<Conn, string>(); // conn → id
  private mailbox = new Map<string, Message[]>(); // id → queued messages
  private history: RoutingEvent[] = [];
  private subscribers = new Set<(e: RoutingEvent) => void>();
  // Track messageId → fromId for ack routing (bounded map)
  private msgToSender = new Map<string, string>(); // messageId → fromId
  private readonly now: () => number;
  private readonly store: StateStore;

  constructor(opts?: { store?: StateStore; now?: () => number }) {
    this.now = opts?.now ?? (() => Date.now());
    this.store = opts?.store ?? new MemoryStore();
    this._loadState();
  }

  private _loadState(): void {
    const state = this.store.load();
    for (const [id, rec] of Object.entries(state.agents)) {
      this.agents.set(id, {
        id,
        name: rec.name,
        state: "offline",
        lastSeen: rec.lastSeen,
        workspace: rec.workspace ?? "default",
        cwd: rec.cwd,
        host: rec.host,
      });
    }
    for (const [id, msgs] of Object.entries(state.mailbox)) {
      this.mailbox.set(id, msgs);
    }
    this.history = state.history ?? [];
  }

  private _saveState(): void {
    const agents: PersistedState["agents"] = {};
    for (const [id, rec] of this.agents) {
      agents[id] = {
        name: rec.name,
        lastSeen: rec.lastSeen,
        workspace: rec.workspace,
        ...(rec.cwd && { cwd: rec.cwd }),
        ...(rec.host && { host: rec.host }),
      };
    }
    const mailbox: PersistedState["mailbox"] = {};
    for (const [id, msgs] of this.mailbox) {
      if (msgs.length > 0) mailbox[id] = msgs;
    }
    this.store.save({ agents, mailbox, history: this.history });
  }

  private _emit(event: RoutingEvent): void {
    this.history.push(event);
    // Cap history
    if (this.history.length > HISTORY_MAX) {
      this.history = this.history.slice(this.history.length - HISTORY_MAX);
    }
    for (const fn of this.subscribers) {
      try { fn(event); } catch { /* ignore subscriber errors */ }
    }
  }

  // ─── public API ───

  connect(conn: Conn): void {
    // Just registers the conn. Identity comes with register frame.
  }

  handle(conn: Conn, frame: ClientFrame): void {
    switch (frame.type) {
      case "register":
        this._handleRegister(conn, frame);
        break;
      case "send":
        this._handleSend(conn, frame);
        break;
      case "broadcast":
        this._handleBroadcast(conn, frame);
        break;
      case "list":
        this._handleList(conn, frame);
        break;
      case "setname":
        this._handleSetname(conn, frame);
        break;
      case "ack":
        this._handleAck(conn, frame);
        break;
      case "pong":
        this._handlePong(conn);
        break;
    }
  }

  disconnect(conn: Conn): void {
    const id = this.connToId.get(conn);
    if (!id) return;
    const rec = this.agents.get(id);
    if (rec) {
      rec.state = "offline";
      rec.conn = undefined;
      this._emit({ ts: this.now(), kind: "deregister", from: rec.name });
    }
    this.connToId.delete(conn);
    this._saveState();
  }

  onPong(conn: Conn): void {
    const id = this.connToId.get(conn);
    if (!id) return;
    const rec = this.agents.get(id);
    if (rec) rec.lastSeen = this.now();
  }

  sweep(): void {
    const cutoff = this.now() - HEARTBEAT_GRACE_MS;
    for (const rec of this.agents.values()) {
      if (rec.state === "online" && rec.lastSeen <= cutoff) {
        // Fix #1b: remove old conn from connToId before clearing it
        if (rec.conn) this.connToId.delete(rec.conn);
        rec.state = "offline";
        rec.conn = undefined;
        this._emit({ ts: this.now(), kind: "deregister", from: rec.name, detail: "sweep" });
      }
    }
    this._saveState();
  }

  roster(): AgentInfo[] {
    return Array.from(this.agents.values()).map((r) => ({
      id: r.id,
      name: r.name,
      state: r.state,
      lastSeen: r.lastSeen,
      workspace: r.workspace,
      ...(r.cwd && { cwd: r.cwd }),
      ...(r.host && { host: r.host }),
    }));
  }

  events(): RoutingEvent[] {
    return [...this.history];
  }

  subscribe(fn: (e: RoutingEvent) => void): () => void {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  /** Test-only: number of active event subscribers. */
  subscriberCount(): number {
    return this.subscribers.size;
  }

  // Human-plane helpers

  injectSay(toName: string, text: string): boolean {
    // Human DMs use the same routing as agents: deliver if online, else mailbox.
    // The operator plane is GLOBAL: resolve the target across ALL workspaces, then
    // route within that target's workspace. Unknown target -> false (HTTP -> 404).
    const target = this._resolveByName(toName) ?? this.agents.get(toName);
    if (!target) return false;
    return this._route("human", "human", toName, text, target.workspace).ok;
  }

  injectBroadcast(text: string): number {
    const msg: Message = {
      messageId: crypto.randomUUID(),
      fromId: "human",
      fromName: "human",
      text,
      ts: this.now(),
      broadcast: true,
    };
    let count = 0;
    for (const rec of this.agents.values()) {
      if (rec.state === "online" && rec.conn) {
        rec.conn.send({ type: "message", message: msg });
        count++;
      }
    }
    this._trackMsg(msg.messageId, "human");
    this._emit({ ts: this.now(), kind: "broadcast", from: "human", text });
    return count;
  }

  kick(name: string): boolean {
    const rec = this._resolveByName(name);
    if (!rec) return false;
    if (rec.conn) rec.conn.close();
    rec.state = "offline";
    rec.conn = undefined;
    const id = rec.id;
    this.connToId.forEach((cid, conn) => {
      if (cid === id) this.connToId.delete(conn);
    });
    this._emit({ ts: this.now(), kind: "deregister", from: name, detail: "kick" });
    this._saveState();
    return true;
  }

  // ─── private handlers ───

  private _handleRegister(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "register" }>
  ): void {
    const { id, name } = frame;
    const workspace = frame.workspace?.trim() || "default";
    const now = this.now();

    // Check if this id was seen before (reconnect)
    const existing = this.agents.get(id);

    if (existing) {
      // Reconnect: reclaim prior name
      // The name to reclaim is the one stored; ignore collision with own offline entry
      const priorName = existing.name;

      // Build taken set from LIVE agents (excluding this id)
      const taken = new Set<string>();
      for (const rec of this.agents.values()) {
        if (rec.id !== id && rec.state === "online") {
          taken.add(rec.name);
        }
      }

      // If priorName is taken by another LIVE agent, suffix it
      const finalName = taken.has(priorName)
        ? uniqueName(priorName, taken)
        : priorName;

      // Fix #1a: close old conn and remove it from connToId before reassigning
      if (existing.state === "online" && existing.conn && existing.conn !== conn) {
        this.connToId.delete(existing.conn);
        try { existing.conn.close(); } catch { /* ignore */ }
      }

      existing.name = finalName;
      existing.state = "online";
      existing.lastSeen = now;
      existing.workspace = workspace;
      existing.conn = conn;
      if (frame.meta?.cwd) existing.cwd = String(frame.meta.cwd);
      if (frame.meta?.host) existing.host = String(frame.meta.host);
      this.connToId.set(conn, id);

      conn.send({ type: "registered", id, name: finalName });
      this._emit({ ts: now, kind: "register", from: finalName });

      // Drain mailbox
      this._flushMailbox(id, conn, now);
    } else {
      // New agent: uniqueness enforced among ALL known agents (online + offline)
      // so that offline agents can reclaim their names on reconnect.
      const taken = new Set<string>();
      for (const rec of this.agents.values()) {
        taken.add(rec.name);
      }
      const finalName = uniqueName(name, taken);

      const rec: AgentRecord = {
        id,
        name: finalName,
        state: "online",
        lastSeen: now,
        workspace,
        conn,
      };
      if (frame.meta?.cwd) rec.cwd = String(frame.meta.cwd);
      if (frame.meta?.host) rec.host = String(frame.meta.host);

      this.agents.set(id, rec);
      this.connToId.set(conn, id);

      conn.send({ type: "registered", id, name: finalName });
      this._emit({ ts: now, kind: "register", from: finalName });
    }

    this._saveState();
  }

  private _flushMailbox(id: string, conn: Conn, now: number): void {
    const queue = this.mailbox.get(id) ?? [];
    // Filter out expired messages
    const fresh = queue.filter((m) => now - m.ts <= MAILBOX_TTL_MS);
    if (fresh.length > 0) {
      for (const msg of fresh) {
        conn.send({ type: "message", message: msg });
      }
      this._emit({ ts: now, kind: "flush", to: id, detail: `${fresh.length} messages` });
    }
    this.mailbox.delete(id);
    this._saveState();
  }

  /**
   * Shared DM routing for both the agent plane and the human plane:
   * resolve the target, deliver if online, otherwise mailbox it.
   * Returns ok:false (no delivery) for unknown or ambiguous targets.
   */
  private _route(
    fromId: string,
    fromName: string,
    to: string,
    text: string,
    workspace: string,
    replyTo?: string
  ): { ok: boolean; to?: string; error?: string } {
    const target = this._resolveTarget(to, workspace);
    if (target === "ambiguous") return { ok: false, error: "ambiguous: use id" };
    if (!target) return { ok: false, error: `no such agent: ${to}` };

    // only propagate replyTo if it looks like a UUID (untrusted field)
    const uuidShape = /^[0-9a-f-]{36}$/i;
    const validReplyTo = replyTo && uuidShape.test(replyTo) ? replyTo : undefined;

    const msg: Message = {
      messageId: crypto.randomUUID(),
      fromId,
      fromName,
      text,
      ts: this.now(),
      ...(validReplyTo && { replyTo: validReplyTo }),
    };
    this._trackMsg(msg.messageId, fromId);

    if (target.state === "online" && target.conn) {
      target.conn.send({ type: "message", message: msg });
      this._emit({ ts: this.now(), kind: "dm", from: fromName, to: target.name, text });
    } else {
      // Offline — mailbox (cap, drop oldest)
      let queue = this.mailbox.get(target.id) ?? [];
      queue.push(msg);
      if (queue.length > MAILBOX_MAX) queue = queue.slice(queue.length - MAILBOX_MAX);
      this.mailbox.set(target.id, queue);
      this._emit({ ts: this.now(), kind: "mailbox", from: fromName, to: target.name, text });
      this._saveState();
    }
    return { ok: true, to: target.name };
  }

  private _handleSend(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "send" }>
  ): void {
    const senderId = this.connToId.get(conn);
    if (!senderId) return;
    const sender = this.agents.get(senderId)!;
    const { to, text, reqId, replyTo } = frame;
    const res = this._route(senderId, sender.name, to, text, sender.workspace, replyTo);
    conn.send({
      type: "sent",
      reqId,
      ok: res.ok,
      ...(res.to !== undefined && { to: res.to }),
      ...(res.error !== undefined && { error: res.error }),
    });
  }

  private _handleBroadcast(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "broadcast" }>
  ): void {
    const senderId = this.connToId.get(conn);
    if (!senderId) return;
    const sender = this.agents.get(senderId)!;
    const { text, reqId } = frame;

    const msg: Message = {
      messageId: crypto.randomUUID(),
      fromId: senderId,
      fromName: sender.name,
      text,
      ts: this.now(),
      broadcast: true,
    };

    this._trackMsg(msg.messageId, senderId);

    let count = 0;
    for (const rec of this.agents.values()) {
      if (
        rec.id !== senderId &&
        rec.workspace === sender.workspace &&
        rec.state === "online" &&
        rec.conn
      ) {
        rec.conn.send({ type: "message", message: msg });
        count++;
      }
    }

    conn.send({ type: "sent", reqId, ok: true, count });
    this._emit({ ts: this.now(), kind: "broadcast", from: sender.name, text });
  }

  private _handleList(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "list" }>
  ): void {
    const id = this.connToId.get(conn);
    const workspace = id ? this.agents.get(id)?.workspace : undefined;
    const agents =
      workspace === undefined
        ? this.roster()
        : this.roster().filter((a) => a.workspace === workspace);
    conn.send({ type: "agents", reqId: frame.reqId, agents });
  }

  private _handleSetname(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "setname" }>
  ): void {
    const id = this.connToId.get(conn);
    if (!id) return;
    const rec = this.agents.get(id)!;
    const { name, reqId } = frame;

    if (!name.trim()) {
      conn.send({ type: "renamed", reqId, ok: false, error: "name cannot be empty" });
      return;
    }

    // Fix #3: Build taken set from ALL OTHER known agents (online AND offline),
    // so setname cannot steal the identity of an offline agent.
    const taken = new Set<string>();
    for (const r of this.agents.values()) {
      if (r.id !== id) taken.add(r.name);
    }

    const oldName = rec.name;
    const finalName = uniqueName(name, taken);
    rec.name = finalName;

    conn.send({ type: "renamed", reqId, ok: true, name: finalName });
    this._emit({ ts: this.now(), kind: "rename", from: oldName, to: finalName });
    this._saveState();
  }

  private _handleAck(
    conn: Conn,
    frame: Extract<ClientFrame, { type: "ack" }>
  ): void {
    const ackerId = this.connToId.get(conn);
    if (!ackerId) return;
    const acker = this.agents.get(ackerId)!;
    const { messageId } = frame;

    const senderId = this.msgToSender.get(messageId);
    if (!senderId) return;

    const senderRec = this.agents.get(senderId);
    if (senderRec?.state === "online" && senderRec.conn) {
      senderRec.conn.send({
        type: "receipt",
        messageId,
        byId: ackerId,
        byName: acker.name,
      });
      this._emit({ ts: this.now(), kind: "receipt", from: acker.name, detail: messageId });
    }
  }

  private _handlePong(conn: Conn): void {
    const id = this.connToId.get(conn);
    if (!id) return;
    const rec = this.agents.get(id);
    if (rec) rec.lastSeen = this.now();
  }

  private _resolveTarget(to: string, workspace: string): AgentRecord | "ambiguous" | undefined {
    // 1. Exact id match (scoped to workspace — no cross-workspace probing)
    const byId = this.agents.get(to);
    if (byId && byId.workspace === workspace) return byId;

    // 2. Exact name match within the workspace
    const byName: AgentRecord[] = [];
    for (const rec of this.agents.values()) {
      if (rec.name === to && rec.workspace === workspace) byName.push(rec);
    }
    if (byName.length === 1) return byName[0];
    if (byName.length > 1) return "ambiguous";
    return undefined;
  }

  private _resolveByName(name: string): AgentRecord | undefined {
    for (const rec of this.agents.values()) {
      if (rec.name === name) return rec;
    }
    return undefined;
  }

  private _trackMsg(messageId: string, fromId: string): void {
    this.msgToSender.set(messageId, fromId);
    // Bound the map to avoid unbounded growth (keep last 10000)
    if (this.msgToSender.size > 10_000) {
      const firstKey = this.msgToSender.keys().next().value;
      if (firstKey !== undefined) this.msgToSender.delete(firstKey);
    }
  }
}

// ─────────────────────────────── serveHub ───────────────────────────────

export function serveHub(opts?: {
  port?: number;
  host?: string;
  store?: StateStore;
}): { port: number; stop(): Promise<void>; hub: Hub } {
  const hub = new Hub({ store: opts?.store });
  const host = opts?.host ?? DEFAULT_HOST;
  const requestedPort = opts?.port ?? DEFAULT_PORT;

  // Fix #6: warn when binding on a non-localhost address
  if (host !== "127.0.0.1" && host !== "localhost") {
    process.stderr.write(
      `chanbus: WARNING binding ${host} exposes an UNAUTHENTICATED prompt-injection surface\n`
    );
  }

  let stopped = false;
  let pingInterval: ReturnType<typeof setInterval> | null = null;
  let sweepInterval: ReturnType<typeof setInterval> | null = null;
  let stopFn: (() => Promise<void>) | null = null;

  // We use Bun.serve with websocket support
  // Bun WebSocket server context holds conn per socket
  const connMap = new WeakMap<
    import("bun").ServerWebSocket<unknown>,
    Conn
  >();

  const server = Bun.serve({
    port: requestedPort,
    hostname: host,

    async fetch(req, srv) {
      // Upgrade WS
      if (new URL(req.url).pathname === WS_PATH) {
        const ok = srv.upgrade(req);
        if (ok) return undefined as unknown as Response;
        return new Response("WebSocket upgrade failed", { status: 500 });
      }
      // Pass stopFn for shutdown endpoint
      return handleHttp(req, hub, srv, () => { if (stopFn) void stopFn(); });
    },

    websocket: {
      open(ws) {
        const conn: Conn = {
          send(frame: ServerFrame) {
            ws.send(JSON.stringify(frame));
          },
          close() {
            ws.close();
          },
        };
        connMap.set(ws, conn);
        hub.connect(conn);
      },

      message(ws, raw) {
        const conn = connMap.get(ws);
        if (!conn) return;
        let frame: ClientFrame;
        try {
          frame = JSON.parse(typeof raw === "string" ? raw : raw.toString("utf8")) as ClientFrame;
        } catch {
          return;
        }
        hub.handle(conn, frame);
      },

      close(ws) {
        const conn = connMap.get(ws);
        if (!conn) return;
        hub.disconnect(conn);
        connMap.delete(ws);
      },

      ping(ws) {
        // Bun's ping/pong: when we send pings, browser/bun auto-pongs
        // We rely on explicit pong frames from connectors, but also update lastSeen on ws ping
        const conn = connMap.get(ws);
        if (conn) hub.onPong(conn);
      },
    },
  });

  const actualPort = server.port as number;

  // Start heartbeat ping + sweep intervals
  pingInterval = setInterval(() => {
    if (stopped) return;
    // Send ping to all connected sockets
    // We can't iterate Bun WS sockets easily without tracking them;
    // use hub.roster() to just do sweep. The real WS ping is via publish
    // Use server.publish for ping — but we have individual socket references via connMap.
    // Instead, track live sockets separately.
    for (const rec of (hub as unknown as { agents: Map<string, { conn?: Conn }> }).agents.values()) {
      try {
        if (rec.conn) rec.conn.send({ type: "ping" });
      } catch { /* ignore */ }
    }
  }, HEARTBEAT_MS);

  sweepInterval = setInterval(() => {
    if (stopped) return;
    hub.sweep();
  }, HEARTBEAT_MS);

  async function stop(): Promise<void> {
    if (stopped) return;
    stopped = true;
    if (pingInterval) clearInterval(pingInterval);
    if (sweepInterval) clearInterval(sweepInterval);
    await server.stop(true);
  }

  stopFn = stop;

  return { port: actualPort, stop, hub };
}

// ─────────────────────────────── HTTP handler ───────────────────────────────

function handleHttp(
  req: Request,
  hub: Hub,
  _srv: import("bun").Server<unknown>,
  onShutdown?: () => void
): Response | Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname;
  const method = req.method;

  if (path === HTTP.healthz && method === "GET") {
    return new Response("ok", { headers: { "content-type": "text/plain" } });
  }

  if (path === HTTP.status && method === "GET") {
    const agents = hub.roster();
    const online = agents.filter((a) => a.state === "online").length;
    const banner = `chanbus hub — ${online} online / ${agents.length} known`;
    return new Response(banner, { headers: { "content-type": "text/plain" } });
  }

  if (path === HTTP.agents && method === "GET") {
    return new Response(JSON.stringify(hub.roster()), {
      headers: { "content-type": "application/json" },
    });
  }

  if (path === HTTP.log && method === "GET") {
    const lines = hub
      .events()
      .map((e) => `[${new Date(e.ts).toISOString()}] ${e.kind} ${e.from ?? ""} ${e.to ?? ""} ${e.text ?? ""} ${e.detail ?? ""}`.trimEnd())
      .join("\n");
    return new Response(lines + "\n", { headers: { "content-type": "text/plain" } });
  }

  if (path === HTTP.events && method === "GET") {
    // SSE stream
    const agentFilter = url.searchParams.get("agent");
    const encoder = new TextEncoder();
    // Fix #2: capture unsub in outer scope and add cancel() so the subscriber
    // is removed when the client disconnects (the return value of start() is
    // ignored by the ReadableStream spec).
    let unsub: (() => void) | undefined;
    const stream = new ReadableStream({
      start(controller) {
        unsub = hub.subscribe((e) => {
          if (agentFilter && e.from !== agentFilter && e.to !== agentFilter) return;
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(e)}\n\n`));
        });
      },
      cancel() {
        if (unsub) { unsub(); unsub = undefined; }
      },
    });
    return new Response(stream, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      },
    });
  }

  if (path === HTTP.say && method === "POST") {
    const toName = url.searchParams.get("to");
    if (!toName) return new Response("missing ?to=", { status: 400 });
    return req.text().then((text) => {
      const ok = hub.injectSay(toName, text);
      if (!ok) return new Response(`unknown agent: ${toName}`, { status: 404 });
      return new Response("ok");
    });
  }

  if (path === HTTP.broadcast && method === "POST") {
    return req.text().then((text) => {
      const count = hub.injectBroadcast(text);
      return new Response(JSON.stringify({ count }), {
        headers: { "content-type": "application/json" },
      });
    });
  }

  if (path === HTTP.kick && method === "POST") {
    const name = url.searchParams.get("name");
    if (!name) return new Response("missing ?name=", { status: 400 });
    const ok = hub.kick(name);
    if (!ok) return new Response(`unknown agent: ${name}`, { status: 404 });
    return new Response("ok");
  }

  if (path === HTTP.shutdown && method === "POST") {
    // Trigger async shutdown after sending the response
    if (onShutdown) setTimeout(onShutdown, 50);
    return new Response("shutting down");
  }

  return new Response("not found", { status: 404 });
}

// ─────────────────────────────── main ───────────────────────────────

if (import.meta.main) {
  const statePath = process.env.CHANBUS_STATE
    ? process.env.CHANBUS_STATE
    : join(homedir(), DEFAULT_STATE_PATH);

  const store = new FileStore(statePath);
  const bindHost = process.env.CHANBUS_HOST ?? DEFAULT_HOST;
  const { port, hub, stop } = serveHub({ port: DEFAULT_PORT, host: bindHost, store });

  if (bindHost !== "127.0.0.1" && bindHost !== "localhost") {
    process.stderr.write(
      `chanbus: WARNING binding ${bindHost} exposes an UNAUTHENTICATED prompt-injection surface\n`
    );
  }
  console.log(`chanbus hub listening on ${bindHost}:${port}`);

  hub.subscribe((e) => {
    const online = hub.roster().filter((a) => a.state === "online").length;
    switch (e.kind) {
      case "register":
        console.log(`+ ${e.from} [${online} online]`);
        break;
      case "deregister":
        console.log(`- ${e.from} bye [${online} online]`);
        break;
      case "dm":
        console.log(`${e.from} → ${e.to}   dm   "${e.text}"`);
        break;
      case "broadcast":
        console.log(`${e.from} → (broadcast)   "${e.text}"`);
        break;
      case "mailbox":
        console.log(`${e.from} → ${e.to}   (mailbox)   "${e.text}"`);
        break;
      case "flush":
        console.log(`flush → ${e.to}   ${e.detail}`);
        break;
      case "rename":
        console.log(`rename ${e.from} → ${e.to}`);
        break;
      case "receipt":
        console.log(`receipt ${e.from} ack ${e.detail}`);
        break;
    }
  });

  process.on("SIGINT", async () => {
    console.log("\nchanbus hub stopping…");
    await stop();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    await stop();
    process.exit(0);
  });
}
