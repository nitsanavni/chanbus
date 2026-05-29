/**
 * cli.test.ts — unit + integration tests for chanbus CLI.
 */

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  parseArgs,
  hubBaseUrl,
  relativeTime,
  formatAgents,
  buildInstallCommand,
  buildLaunchCommand,
  run,
} from "./cli.ts";
import { DEFAULT_PORT, DEFAULT_HOST } from "./protocol.ts";
import { serveHub, MemoryStore } from "./hub.ts";
import type { AgentInfo } from "./protocol.ts";

// ─────────────────────────────── unit tests ───────────────────────────────

describe("parseArgs", () => {
  test("no args → help cmd", () => {
    const r = parseArgs([]);
    expect(r.cmd).toBe("help");
    expect(r.positionals).toEqual([]);
    expect(r.flags).toEqual({});
  });

  test("subcommand only", () => {
    const r = parseArgs(["status"]);
    expect(r.cmd).toBe("status");
    expect(r.positionals).toEqual([]);
  });

  test("--port flag with value", () => {
    const r = parseArgs(["status", "--port", "1234"]);
    expect(r.cmd).toBe("status");
    expect(r.flags["port"]).toBe("1234");
    expect(r.positionals).toEqual([]);
  });

  test("boolean flag (no value)", () => {
    const r = parseArgs(["install", "--run"]);
    expect(r.cmd).toBe("install");
    expect(r.flags["run"]).toBe(true);
  });

  test("say with positionals and multi-word text", () => {
    const r = parseArgs(["say", "alice", "hello", "world"]);
    expect(r.cmd).toBe("say");
    expect(r.positionals).toEqual(["alice", "hello", "world"]);
  });

  test("--port before subcommand", () => {
    // flags before positionals
    const r = parseArgs(["--port", "5000", "ls"]);
    expect(r.cmd).toBe("ls");
    expect(r.flags["port"]).toBe("5000");
  });

  test("flag after positional", () => {
    const r = parseArgs(["say", "bob", "hi", "--port", "9000"]);
    expect(r.cmd).toBe("say");
    expect(r.positionals).toEqual(["bob", "hi"]);
    expect(r.flags["port"]).toBe("9000");
  });
});

describe("hubBaseUrl", () => {
  test("uses flag port", () => {
    const url = hubBaseUrl({ port: "1234" }, {});
    expect(url).toBe(`http://${DEFAULT_HOST}:1234`);
  });

  test("flag > env > default precedence", () => {
    expect(hubBaseUrl({ port: "1" }, { CHANBUS_PORT: "2" })).toBe(
      `http://${DEFAULT_HOST}:1`
    );
    expect(hubBaseUrl({}, { CHANBUS_PORT: "2" })).toBe(
      `http://${DEFAULT_HOST}:2`
    );
    expect(hubBaseUrl({}, {})).toBe(
      `http://${DEFAULT_HOST}:${DEFAULT_PORT}`
    );
  });

  test("uses flag host", () => {
    const url = hubBaseUrl({ host: "0.0.0.0", port: "4900" }, {});
    expect(url).toBe("http://0.0.0.0:4900");
  });

  test("default host when no flag", () => {
    const url = hubBaseUrl({}, {});
    expect(url).toContain(DEFAULT_HOST);
  });
});

describe("relativeTime", () => {
  const now = 1_000_000_000;

  test("just now (< 5s)", () => {
    expect(relativeTime(now - 4000, now)).toBe("just now");
    expect(relativeTime(now, now)).toBe("just now");
  });

  test("seconds ago", () => {
    expect(relativeTime(now - 10_000, now)).toBe("10s ago");
    expect(relativeTime(now - 59_000, now)).toBe("59s ago");
  });

  test("minutes ago", () => {
    expect(relativeTime(now - 60_000, now)).toBe("1m ago");
    expect(relativeTime(now - 300_000, now)).toBe("5m ago");
  });

  test("hours ago", () => {
    expect(relativeTime(now - 3_600_000, now)).toBe("1h ago");
  });

  test("days ago", () => {
    expect(relativeTime(now - 86_400_000, now)).toBe("1d ago");
  });
});

describe("formatAgents", () => {
  const now = Date.now();

  test("empty list", () => {
    expect(formatAgents([], now)).toBe("(no agents)");
  });

  test("includes header row", () => {
    const agents: AgentInfo[] = [
      { id: "1", name: "alice", state: "online", lastSeen: now },
    ];
    const out = formatAgents(agents, now);
    expect(out).toContain("NAME");
    expect(out).toContain("STATE");
    expect(out).toContain("LAST-SEEN");
    expect(out).toContain("CWD");
  });

  test("online agents sort first", () => {
    const agents: AgentInfo[] = [
      { id: "2", name: "bob", state: "offline", lastSeen: now - 60000 },
      { id: "1", name: "alice", state: "online", lastSeen: now },
    ];
    const out = formatAgents(agents, now);
    const lines = out.split("\n");
    // header is first, then alice (online), then bob (offline)
    const aliceIdx = lines.findIndex((l) => l.includes("alice"));
    const bobIdx = lines.findIndex((l) => l.includes("bob"));
    expect(aliceIdx).toBeGreaterThan(0); // not header
    expect(aliceIdx).toBeLessThan(bobIdx);
  });

  test("includes cwd", () => {
    const agents: AgentInfo[] = [
      { id: "1", name: "alice", state: "online", lastSeen: now, cwd: "/home/user" },
    ];
    const out = formatAgents(agents, now);
    expect(out).toContain("/home/user");
  });
});

describe("buildInstallCommand", () => {
  test("returns correct argv", () => {
    const cmd = buildInstallCommand("/project");
    expect(cmd).toEqual([
      "claude",
      "mcp",
      "add",
      "--scope",
      "user",
      "chanbus",
      "--",
      "bun",
      "/project/src/connector.ts",
    ]);
  });

  test("uses absolute path", () => {
    const cmd = buildInstallCommand("/abs/path/to/project");
    const connectorArg = cmd[cmd.length - 1];
    expect(connectorArg.startsWith("/")).toBe(true);
    expect(connectorArg).toContain("connector.ts");
  });
});

describe("buildLaunchCommand", () => {
  test("sets required env vars", () => {
    const { env, argv } = buildLaunchCommand("myagent", {
      id: "test-id-123",
      hubUrl: "ws://127.0.0.1:4900/ws",
    });
    expect(env.CHANHUB_NAME).toBe("myagent");
    expect(env.CHANHUB_ID).toBe("test-id-123");
    expect(env.CHANHUB_URL).toBe("ws://127.0.0.1:4900/ws");
  });

  test("argv contains dev-channels flag", () => {
    const { argv } = buildLaunchCommand("myagent", {
      id: "x",
      hubUrl: "ws://127.0.0.1:4900/ws",
    });
    expect(argv).toContain("--dangerously-load-development-channels");
    expect(argv[0]).toBe("claude");
    expect(argv).toContain("server:chanbus");
  });
});

// ─────────────────────────────── integration tests ───────────────────────────────

function capture(): { lines: string[]; writer: (s: string) => void } {
  const lines: string[] = [];
  return { lines, writer: (s: string) => lines.push(s) };
}

/** Open a WS to the hub and register. Returns the socket + a promise resolving when registered. */
function connectAgent(
  port: number,
  name: string,
  id: string
): { ws: WebSocket; registered: Promise<void> } {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  const registered = new Promise<void>((resolve) => {
    ws.addEventListener("open", () => {
      ws.send(JSON.stringify({ type: "register", id, name }));
    });
    ws.addEventListener("message", (ev) => {
      const frame = JSON.parse(ev.data as string);
      if (frame.type === "registered") resolve();
    });
  });
  return { ws, registered };
}

describe("integration: status & ls", () => {
  let port: number;
  let stopHub: () => Promise<void>;

  beforeEach(() => {
    const h = serveHub({ port: 0, store: new MemoryStore() });
    port = h.port;
    stopHub = h.stop;
  });

  afterEach(async () => {
    await stopHub();
  });

  test("status reports hub up with 0 agents", async () => {
    const out = capture();
    const code = await run(["status", "--port", String(port)], {
      out: out.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(code).toBe(0);
    expect(out.lines.join("\n")).toContain("hub up on");
    expect(out.lines.join("\n")).toContain("0 agents");
  });

  test("status counts online agents after WS connect", async () => {
    const { ws, registered } = connectAgent(port, "alice", "id-alice");
    await registered;

    const out = capture();
    const code = await run(["status", "--port", String(port)], {
      out: out.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    ws.close();

    expect(code).toBe(0);
    const text = out.lines.join("\n");
    expect(text).toContain("1 agents");
    expect(text).toContain("1 online");
  });

  test("ls shows online agent", async () => {
    const { ws, registered } = connectAgent(port, "bob", "id-bob");
    await registered;

    const out = capture();
    const code = await run(["ls", "--port", String(port)], {
      out: out.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    ws.close();

    expect(code).toBe(0);
    const text = out.lines.join("\n");
    expect(text).toContain("bob");
    expect(text).toContain("online");
  });

  test("status reports not-running on wrong port", async () => {
    const out = capture();
    const errOut = capture();
    // Use a port that nothing is listening on
    const code = await run(["status", "--port", "19991"], {
      out: out.writer,
      err: errOut.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(code).toBe(1);
    const text = [...out.lines, ...errOut.lines].join("\n");
    expect(text).toContain("not running");
  });
});

describe("integration: say", () => {
  let port: number;
  let stopHub: () => Promise<void>;

  beforeEach(() => {
    const h = serveHub({ port: 0, store: new MemoryStore() });
    port = h.port;
    stopHub = h.stop;
  });

  afterEach(async () => {
    await stopHub();
  });

  test("say delivers message to connected agent", async () => {
    const { ws, registered } = connectAgent(port, "carol", "id-carol");
    await registered;

    const received: string[] = [];
    const msgReceived = new Promise<void>((resolve) => {
      ws.addEventListener("message", (ev) => {
        const frame = JSON.parse(ev.data as string);
        if (frame.type === "message") {
          received.push(frame.message.text as string);
          resolve();
        }
      });
    });

    const out = capture();
    const code = await run(["say", "--port", String(port), "carol", "hi", "there"], {
      out: out.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });

    // Wait for message delivery
    await msgReceived;
    ws.close();

    expect(code).toBe(0);
    expect(out.lines.join("\n")).toContain("sent to carol");
    expect(received).toContain("hi there");
  }, 5000);

  test("say to unknown agent → exit non-zero (404)", async () => {
    const errOut = capture();
    const code = await run(["say", "--port", String(port), "nobody", "x"], {
      err: errOut.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(code).toBe(1);
    expect(errOut.lines.join("\n")).toContain("unknown agent");
  });
});

describe("integration: bcast", () => {
  let port: number;
  let stopHub: () => Promise<void>;

  beforeEach(() => {
    const h = serveHub({ port: 0, store: new MemoryStore() });
    port = h.port;
    stopHub = h.stop;
  });

  afterEach(async () => {
    await stopHub();
  });

  test("bcast reaches 2 connected clients", async () => {
    const a1 = connectAgent(port, "agent1", "id-1");
    const a2 = connectAgent(port, "agent2", "id-2");
    await Promise.all([a1.registered, a2.registered]);

    const received1: string[] = [];
    const received2: string[] = [];
    const msg1Promise = new Promise<void>((resolve) => {
      a1.ws.addEventListener("message", (ev) => {
        const frame = JSON.parse(ev.data as string);
        if (frame.type === "message") {
          received1.push(frame.message.text as string);
          resolve();
        }
      });
    });
    const msg2Promise = new Promise<void>((resolve) => {
      a2.ws.addEventListener("message", (ev) => {
        const frame = JSON.parse(ev.data as string);
        if (frame.type === "message") {
          received2.push(frame.message.text as string);
          resolve();
        }
      });
    });

    const out = capture();
    const code = await run(["bcast", "--port", String(port), "yo", "world"], {
      out: out.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });

    await Promise.all([msg1Promise, msg2Promise]);
    a1.ws.close();
    a2.ws.close();

    expect(code).toBe(0);
    expect(out.lines.join("\n")).toContain("broadcast to 2");
    expect(received1).toContain("yo world");
    expect(received2).toContain("yo world");
  }, 5000);
});

describe("integration: down", () => {
  test("down stops hub; subsequent status is non-zero", async () => {
    const h = serveHub({ port: 0, store: new MemoryStore() });
    const port = h.port;

    // Confirm it's up
    const statusBefore = capture();
    const codeBefore = await run(["status", "--port", String(port)], {
      out: statusBefore.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(codeBefore).toBe(0);

    // Shut it down
    const downOut = capture();
    const downCode = await run(["down", "--port", String(port)], {
      out: downOut.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(downCode).toBe(0);
    expect(downOut.lines.join("\n")).toContain("shutting down");

    // Wait briefly for the hub to actually stop
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Now status should fail
    const statusAfter = capture();
    const errAfter = capture();
    const codeAfter = await run(["status", "--port", String(port)], {
      out: statusAfter.writer,
      err: errAfter.writer,
      fetchImpl: globalThis.fetch,
      env: {},
    });
    expect(codeAfter).toBe(1);
  }, 5000);
});
