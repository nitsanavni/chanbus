/**
 * chanbus CLI — the human control plane.
 *
 * Usage: chanbus <subcommand> [args]
 *
 * Subcommands: up, down, status, ls, log, tail, say, bcast, kick, install, launch, help
 */

import { DEFAULT_HOST, DEFAULT_PORT, HTTP, type AgentInfo, type RoutingEvent } from "./protocol.ts";
import { serveHub, MemoryStore } from "./hub.ts";
import { join, basename } from "node:path";
import { homedir } from "node:os";

// ─────────────────────────────── pure helpers ───────────────────────────────

export function parseArgs(argv: string[]): {
  cmd: string;
  positionals: string[];
  flags: Record<string, string | boolean>;
} {
  const flags: Record<string, string | boolean> = {};
  const positionals: string[] = [];
  let cmd = "help";

  let i = 0;
  // First non-flag arg is the command
  while (i < argv.length) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith("--")) {
        flags[key] = next;
        i += 2;
      } else {
        flags[key] = true;
        i += 1;
      }
    } else {
      positionals.push(arg);
      i += 1;
    }
  }

  if (positionals.length > 0) {
    cmd = positionals[0];
    positionals.shift();
  }

  return { cmd, positionals, flags };
}

export function hubBaseUrl(
  flags: Record<string, string | boolean>,
  env: Record<string, string | undefined>
): string {
  const port =
    flags["port"] !== undefined
      ? String(flags["port"])
      : env["CHANBUS_PORT"] ?? String(DEFAULT_PORT);
  const host =
    flags["host"] !== undefined ? String(flags["host"]) : DEFAULT_HOST;
  return `http://${host}:${port}`;
}

export function relativeTime(ts: number, now: number): string {
  const diff = Math.floor((now - ts) / 1000);
  if (diff < 5) return "just now";
  if (diff < 60) return `${diff}s ago`;
  const mins = Math.floor(diff / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export function formatAgents(agents: AgentInfo[], now: number): string {
  // Sort: online first, then by name
  const sorted = [...agents].sort((a, b) => {
    if (a.state === "online" && b.state !== "online") return -1;
    if (a.state !== "online" && b.state === "online") return 1;
    return a.name.localeCompare(b.name);
  });

  if (sorted.length === 0) return "(no agents)";

  const home = process.env.HOME;
  const collapseHome = (s: string): string =>
    home && s.startsWith(home) ? "~" + s.slice(home.length) : s;

  const header = ["NAME", "STATE", "LAST-SEEN", "WORKSPACE", "CWD"];
  const rows = sorted.map((a) => [
    a.name,
    a.state,
    relativeTime(a.lastSeen, now),
    collapseHome(a.workspace ?? "default"),
    a.cwd ?? "",
  ]);

  const all = [header, ...rows];
  const colWidths = header.map((_, ci) =>
    Math.max(...all.map((row) => row[ci].length))
  );

  return all
    .map((row) =>
      row.map((cell, ci) => cell.padEnd(colWidths[ci])).join("  ").trimEnd()
    )
    .join("\n");
}

export function buildInstallCommand(projectDir: string): string[] {
  return [
    "claude",
    "mcp",
    "add",
    "--scope",
    "user",
    "chanbus",
    "--",
    "bun",
    `${projectDir}/src/connector.ts`,
  ];
}

export function buildLaunchCommand(
  name: string,
  opts: { id: string; hubUrl: string; workspace?: string }
): { env: Record<string, string>; argv: string[] } {
  const env: Record<string, string> = {
    CHANHUB_NAME: name,
    CHANHUB_ID: opts.id,
    CHANHUB_URL: opts.hubUrl,
  };
  if (opts.workspace) {
    env.CHANHUB_WORKSPACE = opts.workspace;
  }
  const argv = ["claude", "--dangerously-load-development-channels", "server:chanbus"];
  return { env, argv };
}

// ─────────────────────────────── event formatter ───────────────────────────────

function formatEvent(e: RoutingEvent): string {
  const time = new Date(e.ts).toISOString();
  switch (e.kind) {
    case "register":
      return `[${time}] + ${e.from ?? ""} connected`;
    case "deregister":
      return `[${time}] - ${e.from ?? ""} disconnected${e.detail ? ` (${e.detail})` : ""}`;
    case "dm":
      return `[${time}] ${e.from ?? ""} → ${e.to ?? ""}  "${e.text ?? ""}"`;
    case "broadcast":
      return `[${time}] ${e.from ?? ""} → (broadcast)  "${e.text ?? ""}"`;
    case "mailbox":
      return `[${time}] ${e.from ?? ""} → ${e.to ?? ""}  (mailbox)  "${e.text ?? ""}"`;
    case "flush":
      return `[${time}] flush → ${e.to ?? ""}  ${e.detail ?? ""}`;
    case "rename":
      return `[${time}] rename ${e.from ?? ""} → ${e.to ?? ""}`;
    case "receipt":
      return `[${time}] receipt ${e.from ?? ""} ack ${e.detail ?? ""}`;
    default:
      return `[${time}] ${e.kind}`;
  }
}

// ─────────────────────────────── run ───────────────────────────────

export async function run(
  argv: string[],
  io?: {
    out?: (s: string) => void;
    err?: (s: string) => void;
    fetchImpl?: typeof fetch;
    env?: Record<string, string | undefined>;
  }
): Promise<number> {
  const out = io?.out ?? ((s: string) => process.stdout.write(s + "\n"));
  const err = io?.err ?? ((s: string) => process.stderr.write(s + "\n"));
  const fetchImpl = io?.fetchImpl ?? globalThis.fetch;
  const env = io?.env ?? (process.env as Record<string, string | undefined>);

  const { cmd, positionals, flags } = parseArgs(argv);
  const baseUrl = hubBaseUrl(flags, env);

  switch (cmd) {
    case "up": {
      // Start hub in-process
      const port = flags["port"] !== undefined ? Number(flags["port"]) : DEFAULT_PORT;
      const host = flags["host"] !== undefined ? String(flags["host"]) : DEFAULT_HOST;
      const { port: actualPort, hub, stop } = serveHub({
        port,
        host,
        store: new MemoryStore(),
      });
      out(`chanbus hub listening on ${host}:${actualPort}`);

      hub.subscribe((e) => {
        out(formatEvent(e));
      });

      // Set up graceful shutdown
      const shutdown = async () => {
        out("\nchanbus hub stopping…");
        await stop();
      };

      process.on("SIGINT", () => void shutdown().then(() => process.exit(0)));
      process.on("SIGTERM", () => void shutdown().then(() => process.exit(0)));

      // Block forever (until signal)
      await new Promise<void>(() => {/* intentionally never resolves */});
      return 0;
    }

    case "down": {
      try {
        const res = await fetchImpl(`${baseUrl}${HTTP.shutdown}`, { method: "POST" });
        if (res.ok) {
          const text = await res.text();
          out(`hub on ${baseUrl}: ${text}`);
          return 0;
        } else {
          err(`error: ${res.status} ${res.statusText}`);
          return 1;
        }
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "status": {
      try {
        const res = await fetchImpl(`${baseUrl}${HTTP.healthz}`);
        if (!res.ok) {
          err(`hub not running on ${baseUrl}`);
          return 1;
        }
        // Also fetch agents
        const agentsRes = await fetchImpl(`${baseUrl}${HTTP.agents}`);
        const agents = (await agentsRes.json()) as AgentInfo[];
        const online = agents.filter((a) => a.state === "online").length;
        // Extract port from baseUrl
        const portMatch = baseUrl.match(/:(\d+)$/);
        const portStr = portMatch ? portMatch[1] : "?";
        out(`hub up on :${portStr}, ${agents.length} agents (${online} online)`);
        return 0;
      } catch {
        const portMatch = baseUrl.match(/:(\d+)$/);
        const portStr = portMatch ? portMatch[1] : "?";
        out(`hub not running on :${portStr}`);
        return 1;
      }
    }

    case "ls": {
      try {
        const res = await fetchImpl(`${baseUrl}${HTTP.agents}`);
        if (!res.ok) {
          err(`error: ${res.status}`);
          return 1;
        }
        let agents = (await res.json()) as AgentInfo[];
        const wsFilter = flags["workspace"];
        if (typeof wsFilter === "string") {
          agents = agents.filter((a) => {
            const ws = a.workspace ?? "default";
            return ws === wsFilter || basename(ws) === wsFilter;
          });
        }
        out(formatAgents(agents, Date.now()));
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "log": {
      try {
        const res = await fetchImpl(`${baseUrl}${HTTP.log}`);
        if (!res.ok) {
          err(`error: ${res.status}`);
          return 1;
        }
        const text = await res.text();
        out(text.trimEnd());
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "tail": {
      const agentFilter = positionals[0];
      const url = agentFilter
        ? `${baseUrl}${HTTP.events}?agent=${encodeURIComponent(agentFilter)}`
        : `${baseUrl}${HTTP.events}`;

      try {
        const res = await fetchImpl(url);
        if (!res.ok || !res.body) {
          err(`error: ${res.status}`);
          return 1;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";
          for (const line of lines) {
            if (line.startsWith("data: ")) {
              const data = line.slice(6).trim();
              if (data) {
                try {
                  const event = JSON.parse(data) as RoutingEvent;
                  out(formatEvent(event));
                } catch {
                  out(data);
                }
              }
            }
          }
        }
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "say": {
      const to = positionals[0];
      if (!to) {
        err("usage: chanbus say <to> <text...>");
        return 1;
      }
      const text = positionals.slice(1).join(" ");
      if (!text) {
        err("usage: chanbus say <to> <text...>");
        return 1;
      }
      try {
        const res = await fetchImpl(
          `${baseUrl}${HTTP.say}?to=${encodeURIComponent(to)}`,
          { method: "POST", body: text }
        );
        if (res.status === 404) {
          err(`unknown agent: ${to}`);
          return 1;
        }
        if (!res.ok) {
          err(`error: ${res.status} ${await res.text()}`);
          return 1;
        }
        out(`sent to ${to}`);
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "bcast": {
      const text = positionals.join(" ");
      if (!text) {
        err("usage: chanbus bcast <text...>");
        return 1;
      }
      try {
        const res = await fetchImpl(`${baseUrl}${HTTP.broadcast}`, {
          method: "POST",
          body: text,
        });
        if (!res.ok) {
          err(`error: ${res.status}`);
          return 1;
        }
        const { count } = (await res.json()) as { count: number };
        out(`broadcast to ${count}`);
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "kick": {
      const name = positionals[0];
      if (!name) {
        err("usage: chanbus kick <name>");
        return 1;
      }
      try {
        const res = await fetchImpl(
          `${baseUrl}${HTTP.kick}?name=${encodeURIComponent(name)}`,
          { method: "POST" }
        );
        if (res.status === 404) {
          err(`unknown agent: ${name}`);
          return 1;
        }
        if (!res.ok) {
          err(`error: ${res.status}`);
          return 1;
        }
        out(`kicked ${name}`);
        return 0;
      } catch {
        err(`hub not running on ${baseUrl}`);
        return 1;
      }
    }

    case "install": {
      // import.meta.dir points to src/, go up one level to project root
      const absDir = join(import.meta.dir, "..");
      const cmdArr = buildInstallCommand(absDir);
      out(cmdArr.join(" "));
      out("");
      out("This makes every Claude session send-capable with an auto identity.");
      out("To RECEIVE messages, start claude with:");
      out("  claude --dangerously-load-development-channels server:chanbus");
      out("Each session's workspace defaults to its git root; override with CHANHUB_WORKSPACE=<name>.");
      if (flags["run"] === true) {
        const proc = Bun.spawn(cmdArr, { stdio: ["inherit", "inherit", "inherit"] });
        await proc.exited;
      }
      return 0;
    }

    case "launch": {
      const name = positionals[0];
      if (!name) {
        err("usage: chanbus launch <name>");
        return 1;
      }
      const id = crypto.randomUUID();
      const portMatch = baseUrl.match(/:(\d+)$/);
      const port = portMatch ? portMatch[1] : String(DEFAULT_PORT);
      const hubUrl = `ws://${DEFAULT_HOST}:${port}/ws`;
      const workspace = typeof flags["workspace"] === "string" ? flags["workspace"] : undefined;
      const { env: launchEnv, argv: launchArgv } = buildLaunchCommand(name, { id, hubUrl, workspace });
      const envStr = Object.entries(launchEnv)
        .map(([k, v]) => `${k}=${v}`)
        .join(" ");
      out(`${envStr} ${launchArgv.join(" ")}`);
      if (flags["run"] === true) {
        const proc = Bun.spawn(launchArgv, {
          env: { ...process.env, ...launchEnv },
          stdio: ["inherit", "inherit", "inherit"],
        });
        await proc.exited;
      }
      return 0;
    }

    case "help":
    default: {
      out([
        "chanbus — human control plane for Claude agents",
        "",
        "Usage: chanbus <subcommand> [args] [--port N] [--host H]",
        "",
        "Subcommands:",
        "  up [--port N] [--host H]       Start hub in-process; stream events",
        "  down [--port N]                Stop hub (POST /shutdown)",
        "  status [--port N]              Show hub/agent status",
        "  ls [--workspace X] [--port N]  List agents (table: NAME STATE LAST-SEEN WORKSPACE CWD); --workspace X filters",
        "  log [--port N]                 Print recent routed messages",
        "  tail [agent] [--port N]        Stream live events via SSE",
        "  say <to> <text...> [--port N]  DM an agent",
        "  bcast <text...> [--port N]     Broadcast to all agents",
        "  kick <name> [--port N]         Force-disconnect an agent",
        "  install [--run]                Print (or exec) claude mcp add command",
        "  launch <name> [--workspace W] [--port N] [--run]  Print (or exec) claude launch command",
        "  help                           Show this usage",
        "",
        "Env: CHANBUS_PORT — default port (overridden by --port)",
        "",
        "Workspaces: agents are isolated by workspace; only same-workspace agents see each other.",
        "  The default workspace is the session's git root; override with CHANHUB_WORKSPACE=<name>.",
        "",
        "Inbound: a session only RECEIVES messages if started with",
        "  claude --dangerously-load-development-channels server:chanbus",
        "  (no config/env bypass — by design). See docs/workspaces.md.",
      ].join("\n"));
      return 0;
    }
  }
}

// ─────────────────────────────── main ───────────────────────────────

if (import.meta.main) {
  run(process.argv.slice(2)).then((code) => process.exit(code));
}
