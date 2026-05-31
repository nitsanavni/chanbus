/**
 * chanbus protocol — the single source of truth for every component.
 *
 * Three processes speak these contracts:
 *   - HUB        a long-lived router (one per machine)
 *   - CONNECTOR  a channel MCP server, one per Claude session, child of `claude`
 *   - CLI        the human control plane (`chanbus ...`)
 *
 * Two planes:
 *   1. AGENT PLANE  connector <-> hub, JSON frames over a WebSocket at WS_PATH.
 *   2. HUMAN PLANE  CLI <-> hub, HTTP + SSE (see HTTP contract below).
 *
 * Identity rules (decided in design):
 *   - id    stable per session. Connector sources it as:
 *             process.env.CHANHUB_ID  ||  persisted per-cwd uuid  ||  randomUUID()
 *           (CLAUDE_SESSION_ID is NOT reliably present in the MCP subprocess env.)
 *   - name  human label. Connector sources it as:
 *             process.env.CHANHUB_NAME  ||  basename(process.cwd())
 *           The hub enforces uniqueness among LIVE agents by suffixing -2, -3, ...
 *   - Names route; ids own. A reconnecting id reclaims its previous name and
 *     drains its mailbox. Targeting prefers id; ambiguous name -> error.
 */

export const DEFAULT_PORT = 4900;
export const WS_PATH = "/ws";

/** Bind host. localhost-only by default — channels are a prompt-injection bus. */
export const DEFAULT_HOST = "127.0.0.1";

/** Presence / reliability tunables. */
export const HEARTBEAT_MS = 15_000; // hub pings each socket this often
export const HEARTBEAT_GRACE_MS = 35_000; // no pong within this -> mark offline
export const MAILBOX_MAX = 100; // per-target queued messages cap (drop oldest)
export const MAILBOX_TTL_MS = 24 * 60 * 60 * 1000; // queued msg lifetime
export const HISTORY_MAX = 500; // routed-message ring buffer for GET /log
export const REQUEST_TIMEOUT_MS = 5_000; // connector req/resp timeout

/** Where the hub persists state. Override with env CHANBUS_STATE. */
export const DEFAULT_STATE_PATH = ".chanbus/state.json"; // resolved under $HOME

// ─────────────────────────── shared value types ───────────────────────────

export type AgentState = "online" | "offline";

/** Public roster entry (what list/ls/GET /agents return). */
export interface AgentInfo {
  id: string;
  name: string;
  state: AgentState;
  lastSeen: number; // epoch ms
  cwd?: string;
  host?: string;
  workspace?: string; // isolation partition; "default" when unset
}

/** A routed message as it travels and is stored. */
export interface Message {
  messageId: string;
  fromId: string;
  fromName: string;
  text: string;
  ts: number; // epoch ms
  broadcast?: boolean;
  replyTo?: string; // messageId this answers, if any
}

// ───────────────────────── AGENT PLANE: connector -> hub ─────────────────────────
// Every request that expects a reply carries a `reqId` the hub echoes back.

export type ClientFrame =
  | { type: "register"; id: string; name: string; workspace?: string; meta?: Record<string, unknown> }
  | { type: "send"; to: string; text: string; replyTo?: string; reqId: string }
  | { type: "broadcast"; text: string; reqId: string }
  | { type: "list"; reqId: string }
  | { type: "setname"; name: string; reqId: string }
  | { type: "ack"; messageId: string } // delivery receipt, fire-and-forget
  | { type: "pong" }; // reply to a hub ping

// ───────────────────────── AGENT PLANE: hub -> connector ─────────────────────────

export type ServerFrame =
  | { type: "registered"; id: string; name: string } // name may be suffixed
  | { type: "sent"; reqId: string; ok: boolean; to?: string; count?: number; error?: string }
  | { type: "agents"; reqId: string; agents: AgentInfo[] }
  | { type: "renamed"; reqId: string; ok: boolean; name?: string; error?: string }
  | { type: "message"; message: Message } // inbound push (DM or broadcast)
  | { type: "receipt"; messageId: string; byId: string; byName: string } // ack relayed to sender
  | { type: "ping" }
  | { type: "error"; reqId?: string; error: string };

// ─────────────────────────── CONNECTOR -> CLAUDE ───────────────────────────
// The connector renders an inbound Message as the channel notification below.
// Claude Code injects it as: <channel source="chanbus" k="v" ...>content</channel>
//   method: "notifications/claude/channel"
//   params: { content: string, meta: Record<string,string> }   // identifier keys only
export const CHANNEL_NOTIFICATION = "notifications/claude/channel";

/** Build the meta map for a channel notification from a Message. Keys are
 *  identifiers ([A-Za-z0-9_]); hyphenated keys are silently dropped by Claude Code. */
export function channelMeta(m: Message): Record<string, string> {
  const meta: Record<string, string> = {
    from: m.fromName,
    from_id: m.fromId,
    message_id: m.messageId,
  };
  if (m.broadcast) meta.broadcast = "true";
  if (m.replyTo) meta.reply_to = m.replyTo;
  return meta;
}

// ──────────────────────────── HUMAN PLANE (HTTP) ────────────────────────────
// The CLI talks to the hub over these. All on DEFAULT_HOST only.
//
//   GET  /                 -> text status banner
//   GET  /healthz          -> "ok" (liveness; used by `chanbus status`)
//   GET  /agents           -> AgentInfo[]   (JSON)
//   GET  /log              -> text, recent routed messages (newest last)
//   GET  /events           -> SSE stream of RoutingEvent JSON (one per `data:` line)
//                             optional ?agent=NAME filters to that agent's traffic
//   POST /say?to=NAME      -> body=text; inject a DM as sender "human"; 404 if unknown
//   POST /broadcast        -> body=text; inject a broadcast as "human"
//   POST /kick?name=NAME   -> force-disconnect a live agent; 404 if unknown
//   POST /shutdown         -> graceful stop (used by `chanbus down`)
//
// Responses use 2xx on success, 400 bad input, 404 unknown target.

export const HTTP = {
  status: "/",
  healthz: "/healthz",
  agents: "/agents",
  log: "/log",
  events: "/events",
  say: "/say",
  broadcast: "/broadcast",
  kick: "/kick",
  shutdown: "/shutdown",
} as const;

/** Emitted on GET /events and appended to GET /log. */
export interface RoutingEvent {
  ts: number;
  kind: "register" | "deregister" | "dm" | "broadcast" | "mailbox" | "flush" | "rename" | "receipt";
  from?: string; // name
  to?: string; // name, or "(broadcast N)" / "(mailbox)" etc.
  text?: string;
  detail?: string;
}

// ─────────────────────────────── helpers ───────────────────────────────

/** Pick a name not taken by any name in `taken`, suffixing -2, -3, ... */
export function uniqueName(base: string, taken: Set<string>): string {
  const b = base.trim() || "agent";
  if (!taken.has(b)) return b;
  for (let n = 2; n < 100000; n++) {
    const cand = `${b}-${n}`;
    if (!taken.has(cand)) return cand;
  }
  return `${b}-${Math.floor(performance.now())}`;
}

/** Safe JSON parse for wire frames; returns null on malformed input. */
export function parseFrame<T = unknown>(raw: string | Buffer): T | null {
  try {
    return JSON.parse(typeof raw === "string" ? raw : raw.toString("utf8")) as T;
  } catch {
    return null;
  }
}
