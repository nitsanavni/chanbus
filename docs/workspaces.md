# Workspaces — project isolation on one shared hub

## Problem

The connector is installed **user-scope**, so every Claude session on the machine
auto-joins the single hub at `:4900`. That's the feature we want to keep — one bus,
zero per-session wiring. But it means agents from *unrelated projects* all land in the
same roster and can DM/broadcast each other. When you're driving two isolated projects,
their agents should **not** be able to see or reach each other.

## Model

A **workspace** is an isolation partition on the one shared hub. Every agent belongs to
**exactly one** workspace. The hub scopes *every* routing decision to the caller's
workspace:

| Operation | Scoped to caller's workspace? |
|---|---|
| `send` (DM by name/id) | ✅ target must be in the same workspace, else `no such agent` |
| `broadcast` | ✅ only same-workspace agents receive it |
| `list_agents` | ✅ roster shows only the caller's workspace |
| presence / mailbox | ✅ keyed within the workspace |

The wall is total: an agent cannot **see, address, or even discover** agents in another
workspace. A cross-workspace DM fails identically to a nonexistent agent — no probing.

## How an agent picks its workspace (connector)

Resolved once at register, in priority order:

1. `CHANHUB_WORKSPACE` env — explicit override (e.g. a shared `standup` workspace).
2. **Nearest git root** above `cwd` (absolute path) — the natural "this project" key.
3. `cwd` — fallback when not in a git repo.

Because it derives from the **project root**, two sessions opened anywhere inside
project A's tree share workspace A automatically; project B's sessions share B. Zero
manual joining, and the isolation you want falls out for free.

The key is an **opaque exact-match string** (the absolute path). Display surfaces may
shorten it (basename, `~`), but partitioning always uses the full key so two projects
that happen to share a basename never merge.

## Global / shared workspaces are opt-in

There is no implicit global channel. To get a shared space across projects, agents
**explicitly** set the same `CHANHUB_WORKSPACE` (e.g. `CHANHUB_WORKSPACE=global`). It's
just a workspace name they agree on — no special-casing in the hub.

## The operator (CLI / human plane) sees across workspaces

The local human running `chanbus` is trusted and cross-cutting:

- `chanbus ls` shows **all** agents with a `WORKSPACE` column; `--workspace X` filters.
- `chanbus say <to>` / `bcast` may target a workspace (`--workspace X`) to disambiguate
  names that exist in more than one workspace.
- `GET /agents` returns every agent, each carrying its `workspace`.

## Why the operator sees more: two planes

Isolation is enforced on the **agent plane**. A session's connector exposes MCP tools
(`send` / `broadcast` / `list_agents`) and speaks them to the hub over a **WebSocket**
whose socket was bound to an identity + workspace at its `register` frame. The hub scopes
by *that socket* — so an agent's own tools only ever resolve within its own workspace, and
it can never name a different workspace per call.

The operator uses a **different plane**: the `chanbus` CLI over plain **HTTP**
(`GET /agents`, `POST /say`), which carries no agent identity. The hub treats it as the
cross-cutting local operator and returns every workspace.

> **Known limitation — the operator plane crosses the wall.** Because the HTTP plane is an
> unauthenticated localhost port, an agent that shells out to the `chanbus` CLI (or curls
> `:4900`) gets the operator view and can peek past the wall. The wall prevents *accidental*
> cross-talk between agents using their normal tools; it is **not** a security sandbox
> (consistent with the "local, single-user, trust = the machine" threat model).

## Backward compatibility

A register frame with no workspace is bucketed into `"default"`. With no workspace set
anywhere, every agent shares `"default"` and behaviour is exactly as before.

## Inbound delivery is still a per-session opt-in (unchanged, by design)

Workspaces do not change the fact that a session only *receives* `<channel>` messages if
it was started with `--dangerously-load-development-channels server:chanbus`. Claude Code
provides no settings/env bypass — it's an intentional injection-surface guard. We surface
this through clear `--help`, hints in command output, and docs rather than a shell alias.
