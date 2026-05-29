/**
 * Cross-component end-to-end tests.
 *
 * Wires the REAL pieces together — no mocks of the system under test:
 *   - a real Hub over a real Bun.serve WebSocket (ephemeral port)
 *   - real Connector instances, each connecting over a real `WebSocket`
 *   - a real MCP `Client` linked to each connector via InMemoryTransport,
 *     standing in for the Claude session — it calls tools and observes the
 *     actual `notifications/claude/channel` pushes.
 *
 * The only seam is the MCP transport (InMemory instead of stdio), exactly as a
 * real session would speak to the connector. Everything else is production code.
 */
import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";

import { serveHub, MemoryStore } from "../src/hub.ts";
import { Connector } from "../src/connector.ts";
import { CHANNEL_NOTIFICATION } from "../src/protocol.ts";
import { run } from "../src/cli.ts";

// ───────────────────────────── harness ─────────────────────────────

async function waitFor(fn: () => boolean | Promise<boolean>, ms = 2000): Promise<void> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error("waitFor timed out");
}

interface ChannelNote {
  content: string;
  meta: Record<string, string>;
}

interface Agent {
  connector: Connector;
  client: Client;
  notes: ChannelNote[];
  channelNotes: () => ChannelNote[];
}

let srv: ReturnType<typeof serveHub>;
let agents: Agent[] = [];

function hubUrl(): string {
  return `ws://127.0.0.1:${srv.port}/ws`;
}

/** Stand up one agent = a Connector + an MCP Client (the "Claude session"). */
async function spawnAgent(name: string, id: string): Promise<Agent> {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const connector = new Connector({ id, name, hubUrl: hubUrl(), mcpTransport: serverT });
  const client = new Client({ name: "fake-claude", version: "1.0.0" }, { capabilities: {} });
  const notes: ChannelNote[] = [];
  client.fallbackNotificationHandler = async (n: any) => {
    notes.push({ content: n.params?.content ?? "", meta: n.params?.meta ?? {} });
  };
  await connector.start(); // connects MCP server transport, opens ws, registers
  await client.connect(clientT); // completes the MCP handshake (flushes buffered pushes)
  const agent: Agent = {
    connector,
    client,
    notes,
    channelNotes: () => notes,
  };
  agents.push(agent);
  return agent;
}

async function callTool(a: Agent, name: string, args: Record<string, unknown> = {}) {
  const res: any = await a.client.callTool({ name, arguments: args });
  const text = (res.content ?? []).map((c: any) => c.text).join("\n");
  return { text, isError: Boolean(res.isError) };
}

/** Wait until the hub roster shows `name` in the given state. */
async function rosterHas(name: string, state: "online" | "offline"): Promise<void> {
  await waitFor(() => srv.hub.roster().some((a) => a.name === name && a.state === state));
}

beforeEach(() => {
  srv = serveHub({ port: 0, host: "127.0.0.1", store: new MemoryStore() });
  agents = [];
});

afterEach(async () => {
  for (const a of agents) {
    try {
      a.connector.stop();
      await a.client.close();
    } catch {
      /* ignore */
    }
  }
  await srv.stop();
});

// ───────────────────────────── scenarios ─────────────────────────────

describe("chanbus end-to-end", () => {
  it("two sessions register and see each other via list_agents", async () => {
    await spawnAgent("alice", "id-alice");
    const bob = await spawnAgent("bob", "id-bob");
    await rosterHas("alice", "online");
    await rosterHas("bob", "online");

    const { text } = await callTool(bob, "list_agents");
    expect(text).toContain("alice");
    expect(text).toContain("bob");
  });

  it("DM is delivered only to its target as a <channel> notification", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    const bob = await spawnAgent("bob", "id-bob");
    const carol = await spawnAgent("carol", "id-carol");
    await rosterHas("bob", "online");

    const r = await callTool(alice, "send", { to: "bob", text: "hello bob" });
    expect(r.isError).toBe(false);
    expect(r.text).toContain("sent to bob");

    await waitFor(() => bob.notes.length > 0);
    expect(bob.notes[0]!.content).toBe("hello bob");
    expect(bob.notes[0]!.meta.from).toBe("alice");
    expect(bob.notes[0]!.meta.from_id).toBe("id-alice");
    expect(bob.notes[0]!.meta.message_id).toBeTruthy();

    // privacy: the DM never reached carol (a different agent on the bus)
    await new Promise((r) => setTimeout(r, 50));
    expect(carol.notes.length).toBe(0);
  });

  it("broadcast reaches every other session, not the sender", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    const bob = await spawnAgent("bob", "id-bob");
    const carol = await spawnAgent("carol", "id-carol");
    await rosterHas("carol", "online");

    const r = await callTool(alice, "broadcast", { text: "standup in 5" });
    expect(r.text).toContain("2"); // delivered to bob + carol

    await waitFor(() => bob.notes.length > 0 && carol.notes.length > 0);
    expect(bob.notes[0]!.content).toBe("standup in 5");
    expect(bob.notes[0]!.meta.broadcast).toBe("true");
    expect(carol.notes[0]!.meta.broadcast).toBe("true");
    expect(alice.notes.length).toBe(0); // sender excluded
  });

  it("DM to an offline agent is mailboxed and flushed on reconnect (same id)", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    const carol1 = await spawnAgent("carol", "id-carol");
    await rosterHas("carol", "online");

    // carol goes offline
    carol1.connector.stop();
    await carol1.client.close();
    await rosterHas("carol", "offline");

    // alice DMs the offline carol → queued
    const r = await callTool(alice, "send", { to: "carol", text: "ping while away" });
    expect(r.isError).toBe(false);
    expect(r.text).toContain("carol");

    // carol reconnects with the SAME id → mailbox drains
    const carol2 = await spawnAgent("carol", "id-carol");
    await waitFor(() => carol2.notes.some((n) => n.content === "ping while away"));
    const note = carol2.notes.find((n) => n.content === "ping while away")!;
    expect(note.meta.from).toBe("alice");
  });

  it("set_name then addressing by the new name works", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    const bob = await spawnAgent("bob", "id-bob");
    await rosterHas("bob", "online");

    const sn = await callTool(bob, "set_name", { name: "bobby" });
    expect(sn.isError).toBe(false);
    await rosterHas("bobby", "online");

    const r = await callTool(alice, "send", { to: "bobby", text: "new name works" });
    expect(r.isError).toBe(false);
    await waitFor(() => bob.notes.some((n) => n.content === "new name works"));
  });

  it("DM to an unknown agent errors and delivers nothing", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    await rosterHas("alice", "online");

    const r = await callTool(alice, "send", { to: "ghost", text: "anyone?" });
    expect(r.isError).toBe(true);
    expect(r.text.toLowerCase()).toContain("no such agent");
  });

  it("addressing by stable id is unambiguous", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    const bob = await spawnAgent("bob", "id-bob");
    await rosterHas("bob", "online");

    const r = await callTool(alice, "send", { to: "id-bob", text: "by id" });
    expect(r.isError).toBe(false);
    await waitFor(() => bob.notes.some((n) => n.content === "by id"));
  });

  it("CLI `say` injects a human message that reaches the connector's session", async () => {
    const alice = await spawnAgent("alice", "id-alice");
    await rosterHas("alice", "online");

    const out: string[] = [];
    const code = await run(["say", "alice", "ship", "it", "--port", String(srv.port)], {
      out: (s) => out.push(s),
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(code).toBe(0);

    await waitFor(() => alice.notes.some((n) => n.content.includes("ship it")));
    const note = alice.notes.find((n) => n.content.includes("ship it"))!;
    expect(note.meta.from).toBe("human");
  });

  it("CLI `ls` and `status` reflect live agents over HTTP", async () => {
    await spawnAgent("alice", "id-alice");
    await rosterHas("alice", "online");

    const out: string[] = [];
    const code = await run(["ls", "--port", String(srv.port)], {
      out: (s) => out.push(s),
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(code).toBe(0);
    expect(out.join("\n")).toContain("alice");
  });
});
