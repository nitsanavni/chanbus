/**
 * Tests for the chanbus Connector.
 *
 * All tests use:
 *  - FakeSocket  — in-process WireSocket that captures outbound frames and
 *                  lets tests push inbound frames.
 *  - InMemoryTransport — MCP SDK linked pair; no stdio needed.
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { tmpdir } from "node:os";
import { join, basename } from "node:path";
import { mkdtempSync, rmSync, readdirSync, readFileSync } from "node:fs";

import {
  Connector,
  resolveIdentity,
  type WireSocket,
} from "./connector.js";
import {
  CHANNEL_NOTIFICATION,
  channelMeta,
} from "./protocol.js";
import type { ServerFrame, Message } from "./protocol.js";

// ──────────────────────────────── FakeSocket ─────────────────────────────────

class FakeSocket implements WireSocket {
  sent: string[] = [];
  onopen?: () => void;
  onclose?: () => void;
  onmessage?: (data: string) => void;

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.onclose?.();
  }

  /** Helper: push a frame from the hub into the connector. */
  push(frame: ServerFrame): void {
    this.onmessage?.(JSON.stringify(frame));
  }

  /** Decode the last sent frame. */
  lastFrame<T = unknown>(): T {
    return JSON.parse(this.sent[this.sent.length - 1]) as T;
  }

  /** Decode all sent frames. */
  frames<T = unknown>(): T[] {
    return this.sent.map((s) => JSON.parse(s) as T);
  }

  /** Trigger the open event. */
  open(): void {
    this.onopen?.();
  }
}

// ──────────────────────────────── Test helpers ───────────────────────────────

async function makeConnector(opts: {
  id?: string;
  name?: string;
  hubUrl?: string;
  meta?: Record<string, unknown>;
  requestTimeoutMs?: number;
} = {}): Promise<{ connector: Connector; client: Client; socket: FakeSocket }> {
  const socket = new FakeSocket();
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();

  const connector = new Connector({
    id: opts.id ?? "test-id",
    name: opts.name ?? "test-agent",
    hubUrl: opts.hubUrl ?? "ws://fake",
    meta: opts.meta ?? {},
    mcpTransport: serverTransport,
    wsFactory: () => socket,
    requestTimeoutMs: opts.requestTimeoutMs ?? 100, // fast timeouts in tests
  });

  const client = new Client(
    { name: "test-client", version: "0.0.1" },
    { capabilities: {} }
  );

  // Start connector (connects MCP transport + opens WS)
  await connector.start();
  // Connect client — triggers MCP handshake
  await client.connect(clientTransport);

  return { connector, client, socket };
}

// ────────────────────────────────── Tests ────────────────────────────────────

describe("resolveIdentity", () => {
  let tmpDir: string;
  const origEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "chanbus-test-"));
    // Save env
    origEnv.CHANHUB_ID = process.env.CHANHUB_ID;
    origEnv.CHANHUB_NAME = process.env.CHANHUB_NAME;
    origEnv.CHANBUS_ID_DIR = process.env.CHANBUS_ID_DIR;
    delete process.env.CHANHUB_ID;
    delete process.env.CHANHUB_NAME;
    process.env.CHANBUS_ID_DIR = tmpDir;
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
    // Restore env
    for (const [k, v] of Object.entries(origEnv)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  });

  it("uses CHANHUB_NAME when set", () => {
    process.env.CHANHUB_NAME = "my-agent";
    const { name } = resolveIdentity();
    expect(name).toBe("my-agent");
  });

  it("falls back to cwd basename for name", () => {
    const { name } = resolveIdentity();
    const expected = basename(process.cwd());
    expect(name).toBe(expected);
  });

  it("uses CHANHUB_ID when set", () => {
    process.env.CHANHUB_ID = "explicit-id";
    const { id } = resolveIdentity();
    expect(id).toBe("explicit-id");
  });

  it("persists id to disk and reuses on second call", () => {
    const first = resolveIdentity();
    const second = resolveIdentity();
    expect(first.id).toBe(second.id);
  });

  it("env CHANHUB_ID overrides persisted id", () => {
    // First call persists a uuid
    resolveIdentity();
    // Now set env override
    process.env.CHANHUB_ID = "override-id";
    const { id } = resolveIdentity();
    expect(id).toBe("override-id");
  });

  it("persists id file under CHANBUS_ID_DIR", () => {
    const { id } = resolveIdentity();
    // Should be a valid uuid
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
    // File should exist in tmpDir
    const files = readdirSync(tmpDir);
    expect(files.length).toBe(1);
    expect(readFileSync(join(tmpDir, files[0]), "utf8").trim()).toBe(id);
  });
});

describe("Connector — register frame", () => {
  it("sends register frame when socket opens", async () => {
    const { socket } = await makeConnector({ id: "reg-id", name: "reg-name", meta: { cwd: "/x" } });
    // Socket is created synchronously; trigger open
    socket.open();
    // There should be a register frame
    const frames = socket.frames<{ type: string; id: string; name: string; meta?: unknown }>();
    const reg = frames.find((f) => f.type === "register");
    expect(reg).toBeDefined();
    expect(reg!.id).toBe("reg-id");
    expect(reg!.name).toBe("reg-name");
    expect((reg!.meta as Record<string, unknown>)?.cwd).toBe("/x");
  });
});

describe("Connector — registered reply updates whoami", () => {
  it("updates id and name from registered frame", async () => {
    const { connector, socket } = await makeConnector({ id: "orig-id", name: "orig-name" });
    socket.open();
    socket.push({ type: "registered", id: "srv-id", name: "srv-name" });
    expect(connector.whoami()).toEqual({ id: "srv-id", name: "srv-name" });
  });
});

describe("Connector — send tool", () => {
  it("emits send frame and returns success on sent reply", async () => {
    const { client, socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    // Intercept hub reply after tool call
    let toolResultPromise: Promise<unknown>;
    toolResultPromise = client.callTool({ name: "send", arguments: { to: "bob", text: "hello" } });

    // Wait a tick for the frame to be sent
    await new Promise((r) => setTimeout(r, 0));

    // Find the send frame
    const frames = socket.frames<{ type: string; to?: string; text?: string; reqId?: string }>();
    const sendFrame = frames.find((f) => f.type === "send");
    expect(sendFrame).toBeDefined();
    expect(sendFrame!.to).toBe("bob");
    expect(sendFrame!.text).toBe("hello");

    // Reply from hub
    socket.push({ type: "sent", reqId: sendFrame!.reqId!, ok: true, to: "bob" });

    const result = await toolResultPromise as { content: Array<{ type: string; text: string }>; isError?: boolean };
    expect(result.content[0].text).toContain("bob");
    expect(result.isError).toBeFalsy();
  });
});

describe("Connector — broadcast tool", () => {
  it("emits broadcast frame and returns success", async () => {
    const { client, socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    const toolResultPromise = client.callTool({ name: "broadcast", arguments: { text: "hello everyone" } });

    await new Promise((r) => setTimeout(r, 0));

    const frames = socket.frames<{ type: string; text?: string; reqId?: string }>();
    const bcFrame = frames.find((f) => f.type === "broadcast");
    expect(bcFrame).toBeDefined();
    expect(bcFrame!.text).toBe("hello everyone");

    socket.push({ type: "sent", reqId: bcFrame!.reqId!, ok: true, count: 3 });

    const result = await toolResultPromise as { content: Array<{ type: string; text: string }> };
    expect(result.content[0].text).toContain("3");
  });
});

describe("Connector — list_agents tool", () => {
  it("emits list frame and formats agent list", async () => {
    const { client, socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    const toolResultPromise = client.callTool({ name: "list_agents", arguments: {} });

    await new Promise((r) => setTimeout(r, 0));

    const frames = socket.frames<{ type: string; reqId?: string }>();
    const listFrame = frames.find((f) => f.type === "list");
    expect(listFrame).toBeDefined();

    socket.push({
      type: "agents",
      reqId: listFrame!.reqId!,
      agents: [
        { id: "a1", name: "alice", state: "online", lastSeen: Date.now() },
        { id: "b1", name: "bob", state: "offline", lastSeen: Date.now() },
      ],
    });

    const result = await toolResultPromise as { content: Array<{ type: string; text: string }> };
    expect(result.content[0].text).toContain("alice");
    expect(result.content[0].text).toContain("bob");
  });
});

describe("Connector — set_name tool", () => {
  it("emits setname frame, updates whoami on success", async () => {
    const { connector, client, socket } = await makeConnector({ id: "id1", name: "old-name" });
    socket.open();
    socket.push({ type: "registered", id: "id1", name: "old-name" });

    const toolResultPromise = client.callTool({ name: "set_name", arguments: { name: "new-name" } });

    await new Promise((r) => setTimeout(r, 0));

    const frames = socket.frames<{ type: string; name?: string; reqId?: string }>();
    const setNameFrame = frames.find((f) => f.type === "setname");
    expect(setNameFrame).toBeDefined();
    expect(setNameFrame!.name).toBe("new-name");

    socket.push({ type: "renamed", reqId: setNameFrame!.reqId!, ok: true, name: "new-name" });

    const result = await toolResultPromise as { content: Array<{ type: string; text: string }> };
    expect(result.content[0].text).toContain("new-name");
    expect(connector.whoami().name).toBe("new-name");
  });
});

describe("Connector — inbound message notification", () => {
  it("delivers hub message as CHANNEL_NOTIFICATION with channelMeta", async () => {
    const { socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    // Set up notification handler BEFORE the client is connected (already connected above)
    // We need to capture notifications via the client
    const notifications: Array<{ method: string; params: unknown }> = [];

    // Use the client's fallbackNotificationHandler
    const { client } = await makeConnector(); // fresh pair

    // Re-create a clean pair
    const socket2 = new FakeSocket();
    const [ct, st] = InMemoryTransport.createLinkedPair();
    const connector2 = new Connector({
      id: "test-id-2",
      name: "test-agent-2",
      hubUrl: "ws://fake",
      mcpTransport: st,
      wsFactory: () => socket2,
      requestTimeoutMs: 100,
    });

    const client2 = new Client({ name: "c2", version: "0.0.1" }, { capabilities: {} });
    client2.fallbackNotificationHandler = async (notif) => {
      notifications.push({ method: notif.method, params: notif.params });
    };

    await connector2.start();
    await client2.connect(ct);

    socket2.open();
    socket2.push({ type: "registered", id: "test-id-2", name: "test-agent-2" });

    const msg: Message = {
      messageId: "msg-1",
      fromId: "sender-id",
      fromName: "sender",
      text: "hello world",
      ts: Date.now(),
    };
    socket2.push({ type: "message", message: msg });

    // Give async delivery a tick
    await new Promise((r) => setTimeout(r, 10));

    expect(notifications.length).toBeGreaterThan(0);
    const notif = notifications.find((n) => n.method === CHANNEL_NOTIFICATION);
    expect(notif).toBeDefined();
    const params = notif!.params as { content: string; meta: Record<string, string> };
    expect(params.content).toBe("hello world");
    expect(params.meta).toEqual(channelMeta(msg));
  });

  it("delivers broadcast message with broadcast=true in meta", async () => {
    const socket2 = new FakeSocket();
    const [ct, st] = InMemoryTransport.createLinkedPair();
    const notifications: Array<{ method: string; params: unknown }> = [];

    const connector2 = new Connector({
      id: "test-id-bc",
      name: "test-bc",
      hubUrl: "ws://fake",
      mcpTransport: st,
      wsFactory: () => socket2,
      requestTimeoutMs: 100,
    });

    const client2 = new Client({ name: "c-bc", version: "0.0.1" }, { capabilities: {} });
    client2.fallbackNotificationHandler = async (notif) => {
      notifications.push({ method: notif.method, params: notif.params });
    };

    await connector2.start();
    await client2.connect(ct);

    socket2.open();
    socket2.push({ type: "registered", id: "test-id-bc", name: "test-bc" });

    const msg: Message = {
      messageId: "msg-bc",
      fromId: "sender-id",
      fromName: "sender",
      text: "broadcast message",
      ts: Date.now(),
      broadcast: true,
    };
    socket2.push({ type: "message", message: msg });

    await new Promise((r) => setTimeout(r, 10));

    const notif = notifications.find((n) => n.method === CHANNEL_NOTIFICATION);
    expect(notif).toBeDefined();
    const params = notif!.params as { content: string; meta: Record<string, string> };
    expect(params.meta.broadcast).toBe("true");
  });
});

describe("Connector — pre-handshake buffering", () => {
  it("buffers messages arriving before initialized, delivers after", async () => {
    const socket2 = new FakeSocket();
    const [ct, st] = InMemoryTransport.createLinkedPair();
    const notifications: Array<{ method: string; params: unknown }> = [];

    const connector2 = new Connector({
      id: "buf-id",
      name: "buf-agent",
      hubUrl: "ws://fake",
      mcpTransport: st,
      wsFactory: () => socket2,
      requestTimeoutMs: 100,
    });

    // Push a message BEFORE the client connects (before initialized)
    await connector2.start(); // server transport connected, but client hasn't done MCP handshake

    socket2.open();
    socket2.push({ type: "registered", id: "buf-id", name: "buf-agent" });

    // Feed message before MCP client connects
    const earlyMsg: Message = {
      messageId: "early-1",
      fromId: "s1",
      fromName: "sender1",
      text: "early message",
      ts: Date.now(),
    };
    socket2.push({ type: "message", message: earlyMsg });

    // Now connect client — triggers initialized
    const client2 = new Client({ name: "c-buf", version: "0.0.1" }, { capabilities: {} });
    client2.fallbackNotificationHandler = async (notif) => {
      notifications.push({ method: notif.method, params: notif.params });
    };
    await client2.connect(ct);

    // Give async flush a few ticks
    await new Promise((r) => setTimeout(r, 20));

    const notif = notifications.find((n) => n.method === CHANNEL_NOTIFICATION);
    expect(notif).toBeDefined();
    const params = notif!.params as { content: string };
    expect(params.content).toBe("early message");
  });
});

describe("Connector — request timeout", () => {
  it("returns error when hub never replies within timeout", async () => {
    const { client, socket } = await makeConnector({ requestTimeoutMs: 50 });
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    // Call tool but never push a reply — should timeout
    const result = await client.callTool({ name: "send", arguments: { to: "nobody", text: "hi" } }) as {
      content: Array<{ type: string; text: string }>;
      isError?: boolean;
    };

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain("timed out");
  }, 2000);
});

describe("Connector — pong on ping", () => {
  it("sends pong when hub sends ping", async () => {
    const { socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    socket.push({ type: "ping" });

    // Give a tick for the response
    await new Promise((r) => setTimeout(r, 0));

    const frames = socket.frames<{ type: string }>();
    const pong = frames.find((f) => f.type === "pong");
    expect(pong).toBeDefined();
  });
});

describe("Connector — whoami tool", () => {
  it("returns id and name", async () => {
    const { client, socket } = await makeConnector({ id: "my-id", name: "my-name" });
    socket.open();
    socket.push({ type: "registered", id: "my-id", name: "my-name" });

    const result = await client.callTool({ name: "whoami", arguments: {} }) as {
      content: Array<{ type: string; text: string }>;
    };
    expect(result.content[0].text).toContain("my-id");
    expect(result.content[0].text).toContain("my-name");
  });
});

// ────────────────────── Regression tests for bug fixes ───────────────────────

describe("Fix 1 — in-flight requests rejected on unexpected socket close", () => {
  it("pending request rejects immediately with 'connection lost' when socket closes", async () => {
    const { client, socket } = await makeConnector({ requestTimeoutMs: 5000 });
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    // Issue a tool call that will never get a hub reply
    const toolPromise = client.callTool({
      name: "send",
      arguments: { to: "nobody", text: "hello" },
    }) as Promise<{ content: Array<{ type: string; text: string }>; isError?: boolean }>;

    // Wait a tick so the request is registered in _pending
    await new Promise((r) => setTimeout(r, 0));

    const start = Date.now();
    // Trigger unexpected close (not via stop())
    socket.onclose?.();

    const result = await toolPromise;
    const elapsed = Date.now() - start;

    // Should resolve quickly (well under the 5000ms timeout)
    expect(elapsed).toBeLessThan(500);
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain("connection lost");
  });
});

describe("Fix 2 — _hubRequest rejects immediately when not connected", () => {
  it("tool call before socket opens returns error promptly without timeout wait", async () => {
    // Create connector but do NOT open the socket
    const socket = new FakeSocket();
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    const connector = new Connector({
      id: "fix2-id",
      name: "fix2-agent",
      hubUrl: "ws://fake",
      mcpTransport: serverTransport,
      wsFactory: () => socket,
      requestTimeoutMs: 5000, // large timeout to prove we don't wait
    });
    const client = new Client({ name: "fix2-client", version: "0.0.1" }, { capabilities: {} });
    await connector.start();
    await client.connect(clientTransport);
    // socket is created but onopen has NOT been called → _ws is set by _openWs
    // However _ws is assigned before onopen fires; to test "not connected" we
    // need _ws === null. Force that by triggering close immediately.
    socket.onclose?.(); // now _ws = null

    const start = Date.now();
    const result = await client.callTool({
      name: "send",
      arguments: { to: "anyone", text: "hello" },
    }) as { content: Array<{ type: string; text: string }>; isError?: boolean };
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(500);
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain("not connected to hub");

    connector.stop();
  });
});

describe("Fix 3 — stop() closes the MCP server", () => {
  it("stop() does not throw and rejects pending requests, server.close is invoked", async () => {
    const { connector, client, socket } = await makeConnector({ requestTimeoutMs: 5000 });
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    // Issue a pending request
    const toolPromise = client.callTool({
      name: "send",
      arguments: { to: "nobody", text: "hi" },
    });

    await new Promise((r) => setTimeout(r, 0));

    // stop() should not throw
    expect(() => connector.stop()).not.toThrow();

    // After stop(), pending requests are rejected; the client transport is also
    // closed (server.close), so callTool may reject with McpError or return
    // isError. Either outcome confirms stop() completed its work.
    let sawStoppedOrClosed = false;
    try {
      const result = await toolPromise as { content: Array<{ type: string; text: string }>; isError?: boolean };
      if (result.isError && result.content[0].text.includes("stopped")) {
        sawStoppedOrClosed = true;
      }
    } catch (e) {
      // MCP transport was closed — that is the expected server.close() effect
      sawStoppedOrClosed = true;
    }
    expect(sawStoppedOrClosed).toBe(true);
  });
});

describe("Fix 4 — stable id when only name is provided", () => {
  let tmpDir: string;
  const origEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "chanbus-fix4-"));
    origEnv.CHANHUB_ID = process.env.CHANHUB_ID;
    origEnv.CHANHUB_NAME = process.env.CHANHUB_NAME;
    origEnv.CHANBUS_ID_DIR = process.env.CHANBUS_ID_DIR;
    delete process.env.CHANHUB_ID;
    delete process.env.CHANHUB_NAME;
    process.env.CHANBUS_ID_DIR = tmpDir;
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
    for (const [k, v] of Object.entries(origEnv)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  });

  it("new Connector({ name }) twice yields the same persisted id and keeps provided name", () => {
    const socket1 = new FakeSocket();
    const [, st1] = InMemoryTransport.createLinkedPair();
    const c1 = new Connector({
      name: "foo",
      hubUrl: "ws://fake",
      mcpTransport: st1,
      wsFactory: () => socket1,
    });
    const id1 = c1.whoami().id;
    const name1 = c1.whoami().name;

    const socket2 = new FakeSocket();
    const [, st2] = InMemoryTransport.createLinkedPair();
    const c2 = new Connector({
      name: "foo",
      hubUrl: "ws://fake",
      mcpTransport: st2,
      wsFactory: () => socket2,
    });
    const id2 = c2.whoami().id;
    const name2 = c2.whoami().name;

    // Both should have the same persisted id
    expect(id1).toBe(id2);
    // Both should have the name "foo" (opts.name wins over resolved name)
    expect(name1).toBe("foo");
    expect(name2).toBe("foo");
    // id should be a valid UUID (not empty / "agent")
    expect(id1).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe("Fix 5 — capped exponential reconnect backoff", () => {
  it("reconnect delay grows exponentially and is capped at 5000ms", async () => {
    const scheduledDelays: number[] = [];
    const sockets: FakeSocket[] = [];

    const [, serverTransport] = InMemoryTransport.createLinkedPair();
    const connector = new Connector({
      id: "backoff-id",
      name: "backoff-agent",
      hubUrl: "ws://fake",
      mcpTransport: serverTransport,
      wsFactory: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
      requestTimeoutMs: 100,
      scheduler: (fn, ms) => {
        scheduledDelays.push(ms);
        // Run immediately so reconnect happens synchronously for testing
        fn();
      },
    });

    await connector.start();

    // Trigger 7 consecutive close events (no open between them)
    // Each onclose on the current socket triggers a reconnect which creates a
    // new socket; we close that too, etc.
    for (let i = 0; i < 7; i++) {
      const current = sockets[sockets.length - 1];
      current.onclose?.();
    }

    // First delay: 100ms, then doubles: 200, 400, 800, 1600, 3200, 5000 (capped)
    expect(scheduledDelays.length).toBeGreaterThanOrEqual(7);
    expect(scheduledDelays[0]).toBe(100);
    expect(scheduledDelays[1]).toBe(200);
    expect(scheduledDelays[2]).toBe(400);
    expect(scheduledDelays[3]).toBe(800);
    expect(scheduledDelays[4]).toBe(1600);
    expect(scheduledDelays[5]).toBe(3200);
    // 7th and beyond should be capped at 5000
    expect(scheduledDelays[6]).toBe(5000);

    connector.stop();
  });

  it("backoff resets to 100ms after a successful open", async () => {
    const scheduledDelays: number[] = [];
    const sockets: FakeSocket[] = [];

    const [, serverTransport] = InMemoryTransport.createLinkedPair();
    const connector = new Connector({
      id: "backoff-reset-id",
      name: "backoff-reset",
      hubUrl: "ws://fake",
      mcpTransport: serverTransport,
      wsFactory: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
      requestTimeoutMs: 100,
      scheduler: (fn, ms) => {
        scheduledDelays.push(ms);
        fn();
      },
    });

    await connector.start();

    // Two closes without open — delays should be 100, 200
    sockets[sockets.length - 1].onclose?.();
    sockets[sockets.length - 1].onclose?.();

    expect(scheduledDelays[0]).toBe(100);
    expect(scheduledDelays[1]).toBe(200);

    // Now simulate a successful open (resets backoff)
    sockets[sockets.length - 1].open();

    // Next close should restart from 100
    const beforeLen = scheduledDelays.length;
    sockets[sockets.length - 1].onclose?.();
    expect(scheduledDelays[beforeLen]).toBe(100);

    connector.stop();
  });
});

describe("Fix 6 — hub-wide error frames surfaced to stderr", () => {
  it("error frame with reqId routes to pending request, not stderr", async () => {
    const { client, socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    const toolPromise = client.callTool({
      name: "send",
      arguments: { to: "nobody", text: "hi" },
    }) as Promise<{ content: Array<{ type: string; text: string }>; isError?: boolean }>;

    await new Promise((r) => setTimeout(r, 0));

    const frames = socket.frames<{ type: string; reqId?: string }>();
    const sendFrame = frames.find((f) => f.type === "send");
    expect(sendFrame?.reqId).toBeDefined();

    // Push error frame WITH reqId — should route to pending, not stderr
    const errorSpy: string[] = [];
    const origError = console.error;
    console.error = (...args: unknown[]) => { errorSpy.push(args.join(" ")); };
    try {
      socket.push({ type: "error", reqId: sendFrame!.reqId!, error: "no such agent" });
      const result = await toolPromise;
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain("no such agent");
      // No stderr for a reqId-bearing error
      expect(errorSpy.length).toBe(0);
    } finally {
      console.error = origError;
    }
  });

  it("error frame without reqId is written to stderr", async () => {
    const { socket } = await makeConnector();
    socket.open();
    socket.push({ type: "registered", id: "test-id", name: "test-agent" });

    const stderrLines: string[] = [];
    const origError = console.error;
    console.error = (...args: unknown[]) => { stderrLines.push(args.join(" ")); };
    try {
      socket.push({ type: "error", error: "hub overload" } as ServerFrame);
      await new Promise((r) => setTimeout(r, 0));
      expect(stderrLines.some((l) => l.includes("hub overload"))).toBe(true);
    } finally {
      console.error = origError;
    }
  });
});
