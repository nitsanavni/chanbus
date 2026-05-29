#!/usr/bin/env bun
/**
 * Manual end-to-end driver — exercises the REAL processes, not in-memory shims:
 *   - the hub started via the real CLI:  `bun src/cli.ts up --port N`
 *   - connectors as real subprocesses over real WebSockets, spawned by a real
 *     MCP StdioClientTransport (exactly how Claude Code spawns them)
 *   - every human action via the real CLI binary (`status`, `ls`, `say`, `log`, `down`)
 *   - an MCP stdio Client standing in for the Claude session: it calls the
 *     connector's tools and observes the actual `<channel>` notifications.
 *
 * Run:  bun scripts/manual-e2e.ts
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { dirname, join } from "node:path";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";

const ROOT = join(dirname(new URL(import.meta.url).pathname), "..");
const PORT = 49321;
const HUB_URL = `ws://127.0.0.1:${PORT}/ws`;
const tmp = mkdtempSync(join(tmpdir(), "chanbus-e2e-"));
const ENV = {
  ...process.env,
  CHANBUS_STATE: join(tmp, "state.json"),
  CHANBUS_ID_DIR: join(tmp, "ids"),
  CHANBUS_HOST: "127.0.0.1",
} as Record<string, string>;

let pass = 0,
  fail = 0;
const ok = (name: string, cond: boolean, extra = "") => {
  (cond ? pass++ : fail++);
  console.log(`  ${cond ? "✅" : "❌"} ${name}${extra ? "  — " + extra : ""}`);
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn: () => boolean | Promise<boolean>, ms = 4000, label = "condition") {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn()) return true;
    await sleep(40);
  }
  throw new Error(`waitFor timed out: ${label}`);
}

// real CLI invocation as a child process
async function cli(...args: string[]): Promise<{ code: number; out: string; err: string }> {
  const p = Bun.spawn(["bun", "src/cli.ts", ...args, "--port", String(PORT)], {
    cwd: ROOT,
    env: ENV,
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = await new Response(p.stdout).text();
  const err = await new Response(p.stderr).text();
  const code = await p.exited;
  return { code, out, err };
}

async function agentsJson(): Promise<any[]> {
  try {
    const r = await fetch(`http://127.0.0.1:${PORT}/agents`);
    return (await r.json()) as any[];
  } catch {
    return [];
  }
}

interface Sess {
  name: string;
  id: string;
  client: Client;
  transport: StdioClientTransport;
  notes: { content: string; meta: Record<string, string> }[];
}
const sessions: Sess[] = [];

async function startSession(name: string, id: string): Promise<Sess> {
  const transport = new StdioClientTransport({
    command: "bun",
    args: ["src/connector.ts"],
    cwd: ROOT,
    env: { ...ENV, CHANHUB_URL: HUB_URL, CHANHUB_NAME: name, CHANHUB_ID: id },
  });
  const client = new Client({ name: "fake-claude", version: "1.0.0" }, { capabilities: {} });
  const notes: Sess["notes"] = [];
  client.fallbackNotificationHandler = async (n: any) => {
    if (n.method === "notifications/claude/channel")
      notes.push({ content: n.params?.content ?? "", meta: n.params?.meta ?? {} });
  };
  await client.connect(transport);
  const s: Sess = { name, id, client, transport, notes };
  sessions.push(s);
  return s;
}

async function tool(s: Sess, name: string, args: Record<string, unknown> = {}) {
  const res: any = await s.client.callTool({ name, arguments: args });
  return { text: (res.content ?? []).map((c: any) => c.text).join("\n"), isError: Boolean(res.isError) };
}

// ─────────────────────────────────────────────────────────────────────────
let hub: ReturnType<typeof Bun.spawn> | null = null;
const hubLog: string[] = [];

async function main() {
  console.log(`\n▶ manual E2E — port ${PORT}, state ${tmp}\n`);

  // 1. start the real hub via the real CLI
  hub = Bun.spawn(["bun", "src/cli.ts", "up", "--port", String(PORT)], {
    cwd: ROOT,
    env: ENV,
    stdout: "pipe",
    stderr: "pipe",
  });
  (async () => {
    for await (const chunk of hub!.stdout as any) hubLog.push(new TextDecoder().decode(chunk));
  })();
  await waitFor(async () => {
    try {
      return (await fetch(`http://127.0.0.1:${PORT}/healthz`)).ok;
    } catch {
      return false;
    }
  }, 5000, "hub healthz");
  console.log("§ hub up");

  // 2. status before any agents
  const s0 = await cli("status");
  ok("status reports hub up, 0 agents", s0.code === 0 && /0 agent/i.test(s0.out), s0.out.trim());

  // 3. two sessions register (real subprocess connectors over real WS)
  const alice = await startSession("alice", "id-alice");
  const bob = await startSession("bob", "id-bob");
  await waitFor(async () => (await agentsJson()).filter((a) => a.state === "online").length >= 2, 5000, "2 online");
  console.log("§ alice + bob connected");

  // 4. ls shows both
  const ls = await cli("ls");
  ok("ls lists alice and bob", ls.out.includes("alice") && ls.out.includes("bob"));

  // 5. DM alice→bob, delivered only to bob
  const dm = await tool(alice, "send", { to: "bob", text: "hello bob" });
  ok("send tool reports success", !dm.isError && /sent to bob/i.test(dm.text), dm.text.trim());
  await waitFor(() => bob.notes.some((n) => n.content === "hello bob"), 4000, "bob receives DM");
  const got = bob.notes.find((n) => n.content === "hello bob")!;
  ok("DM arrived with from=alice attribution", got.meta.from === "alice" && !!got.meta.message_id);
  ok("DM did NOT leak to sender alice", alice.notes.length === 0);

  // 6. broadcast
  const carol = await startSession("carol", "id-carol");
  await waitFor(async () => (await agentsJson()).some((a) => a.name === "carol" && a.state === "online"));
  await tool(alice, "broadcast", { text: "standup in 5" });
  await waitFor(() => bob.notes.some((n) => n.content === "standup in 5") && carol.notes.some((n) => n.content === "standup in 5"), 4000, "broadcast to bob+carol");
  ok("broadcast reached bob+carol with broadcast flag", bob.notes.find((n) => n.content === "standup in 5")!.meta.broadcast === "true");

  // 7. CLI human say → bob
  const say = await cli("say", "bob", "ship", "it");
  ok("CLI say exits 0", say.code === 0, say.out.trim());
  await waitFor(() => bob.notes.some((n) => n.content.includes("ship it")), 4000, "bob receives human say");
  ok("human say arrived with from=human", bob.notes.find((n) => n.content.includes("ship it"))!.meta.from === "human");

  // 8. offline mailbox + flush on reconnect (same id)
  await carol.client.close(); // kills the carol connector subprocess
  await waitFor(async () => (await agentsJson()).some((a) => a.name === "carol" && a.state === "offline"), 5000, "carol offline");
  const sayOff = await cli("say", "carol", "queued", "while", "away");
  ok("say to OFFLINE carol still accepted (mailboxed)", sayOff.code === 0, sayOff.out.trim());
  const carol2 = await startSession("carol", "id-carol"); // reconnect, same id
  await waitFor(() => carol2.notes.some((n) => n.content === "queued while away"), 5000, "carol2 mailbox flush");
  ok("offline mailbox flushed to carol on reconnect", true);

  // 9. set_name then address by new name
  await tool(bob, "set_name", { name: "bobby" });
  await waitFor(async () => (await agentsJson()).some((a) => a.name === "bobby" && a.state === "online"), 4000, "bobby online");
  await tool(alice, "send", { to: "bobby", text: "new name works" });
  await waitFor(() => bob.notes.some((n) => n.content === "new name works"), 4000, "bob (bobby) receives");
  ok("set_name + address-by-new-name works", true);

  // 10. unknown target errors
  const ghost = await tool(alice, "send", { to: "ghost", text: "anyone?" });
  ok("DM to unknown agent errors", ghost.isError && /no such agent/i.test(ghost.text), ghost.text.trim());

  // 11. id-based addressing (unambiguous)
  await tool(alice, "send", { to: "id-carol", text: "by id" });
  await waitFor(() => carol2.notes.some((n) => n.content === "by id"), 4000, "carol receives by-id");
  ok("addressing by stable id works", true);

  // 12. hub log via CLI
  const log = await cli("log");
  ok("hub log records routed messages", /alice/.test(log.out) && /bob|bobby/.test(log.out));

  // 13. shutdown via CLI down, then status reports down
  const down = await cli("down");
  ok("CLI down exits 0", down.code === 0, down.out.trim());
  await sleep(300);
  const s1 = await cli("status");
  ok("status after down reports not running (non-zero exit)", s1.code !== 0, s1.out.trim() || s1.err.trim());

  // sample of the real hub log
  console.log("\n── real hub log (sample) ──");
  console.log(hubLog.join("").split("\n").filter(Boolean).slice(0, 14).join("\n"));
}

main()
  .catch((e) => {
    fail++;
    console.error("\n❌ DRIVER ERROR:", e?.message ?? e);
  })
  .finally(async () => {
    for (const s of sessions) {
      try {
        await s.client.close();
      } catch {}
    }
    try {
      hub?.kill();
    } catch {}
    console.log(`\n──────────\nRESULT: ${pass} passed, ${fail} failed\n`);
    process.exit(fail === 0 ? 0 : 1);
  });
