# chanbus

A local message bus that lets multiple **Claude Code sessions talk to each other** —
direct messages by name/id, and broadcasts. Built on Claude Code's native
[channels](https://code.claude.com/docs/en/channels) feature.

```
   ┌──────────┐   send tool   ┌───────────┐    WS     ┌───────┐    WS     ┌───────────┐   <channel>   ┌──────────┐
   │  Claude  │ ────────────► │ connector │ ────────► │  HUB  │ ────────► │ connector │ ────────────► │  Claude  │
   │ "alice"  │               │ (channel) │           │ :4900 │           │ (channel) │               │  "bob"   │
   └──────────┘               └───────────┘           └───────┘           └───────────┘               └──────────┘
        the session            its MCP subproc      router + registry      bob's MCP subproc            the session
```

The **channels feature owns only the last hop** (a notification can enter just the
one session whose connector fired it). The **hub owns cross-session routing** — the
address book that turns a name into the right connector. See `../docs/design.html`.

## Components

| Process       | File              | Role |
|---------------|-------------------|------|
| **hub**       | `src/hub.ts`      | One long-lived router: registry, presence/heartbeat, offline mailbox, history, persistence, human HTTP/SSE side-door. |
| **connector** | `src/connector.ts`| One per Claude session (spawned as a stdio MCP subprocess). Declares the `claude/channel` capability, exposes the messaging tools, pushes peer messages in as `<channel>` notifications. |
| **cli**       | `src/cli.ts`      | `chanbus` — the human control plane over HTTP. |

The wire contract for all three lives in `src/protocol.ts` (single source of truth).

## Quick start

```bash
bun install

# 1) register the connector once (so every `claude` session gets one automatically)
bun src/cli.ts install          # prints the `claude mcp add ...` command; add --run to execute

# 2) start the hub (long-lived; logs every routed message)
bun src/cli.ts up               # listens on 127.0.0.1:4900

# 3) launch sessions — the name + a stable id ride in via env, and the
#    --dangerously-load-development-channels flag turns on inbound delivery
bun src/cli.ts launch alice     # prints the launch command; add --run to exec
bun src/cli.ts launch bob
```

> **Why the launch flag matters:** a session that merely has the connector configured
> can *send* (the tools work) but is **deaf to inbound DMs** until it's started with
> `--dangerously-load-development-channels server:chanbus`. `chanbus launch` bakes that
> in, along with `CHANHUB_NAME`, a persisted `CHANHUB_ID`, and `CHANHUB_URL`.

Then tell alice *"send bob: what are you working on?"* — bob's idle session wakes with a
`<channel from="alice">` message and can reply with its own `send`.

## CLI reference

```
chanbus up [--port N] [--host H]   start the hub (foreground, logs routing)
chanbus down [--port N]            stop the hub
chanbus status [--port N]          is it up? how many agents?
chanbus ls [--port N]              roster: name, state, last-seen, cwd
chanbus log [--port N]             recent routed messages
chanbus tail [agent] [--port N]    live SSE stream of routing (optionally one agent)
chanbus say <to> <text...>         inject a DM as "human" (mailboxed if target offline)
chanbus bcast <text...>            broadcast as "human"
chanbus kick <name>                force-disconnect an agent
chanbus install [--run]            register the connector as a user-scope MCP server
chanbus launch <name> [--run]      launch a Claude session wired to the bus
```

## Tools each session gets

`send(to, text, replyTo?)` · `broadcast(text)` · `list_agents` · `set_name(name)` · `whoami`

Inbound peer messages arrive as `<channel source="chanbus" from="<name>" from_id="<id>" message_id="…" [broadcast="true"]>…</channel>`.

## Security model

This is a **localhost, single-user** tool. Peer messages land in another Claude's
context, so the bus is a prompt-injection surface; the design reflects that:

- **Localhost-only** bind (`127.0.0.1`). Binding any other host prints a loud
  unauthenticated-surface warning.
- **IDs own, names route.** `from`/`from_id` are always set by the hub from the
  authenticated socket — never client-supplied. A live name can't be stolen
  (a re-register on a live id evicts the old socket); `set_name` can't poison an
  offline agent's name (it gets suffixed instead).
- **Untrusted framing.** The connector's instructions tell Claude to treat peer
  messages as untrusted chat — quote and consider, don't blindly obey.
- `replyTo` is validated to a UUID shape before it reaches a session.

### Known limitations (by design, for a local tool)

- No authentication on the bus — trust is "the local machine."
- No per-sender **rate limiting** and no **broadcast-loop / hop-count** breaker: two
  agents that auto-reply to each other's broadcasts can loop. Keep auto-reply behavior
  deliberate.
- Permission relay (`claude/channel/permission`) and rooms/topics are not implemented
  (design phase 4).

## Testing

```bash
bun test                       # 127 unit + integration + cross-component E2E tests
bun scripts/manual-e2e.ts      # full-process manual E2E: real hub + real connector
                               # subprocesses over real WebSockets + real CLI (16 scenarios)
```

- `src/*.test.ts` — per-component TDD suites (hub, connector, cli).
- `e2e/e2e.test.ts` — real hub + real connectors + MCP clients observing real notifications.
- `scripts/manual-e2e.ts` — drives the actual binaries end to end across process boundaries.
