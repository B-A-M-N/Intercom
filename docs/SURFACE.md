# Surface Bridge

Intercom core delivers messages; the Surface Bridge lets a human see
them where they already work. It is distinct from agent ACP/MCP
interoperability: supporting an agent's ACP or MCP interface does not
make Intercom visible in that agent's UI, and does not make a durable
write a presentation.

## Layers

- Intercom core: identities, messages, permissions, durable delivery, journal.
- Agent adapters: how Muse, Codex, Hermes, OpenCode receive or send.
- Human-surface adapters (this bridge): how a TUI, CLI, IDE or web UI
  displays messages and lets a human respond.

## Process

`intercom-surface --socket <sock> --session <id> --delegation-token-file <file>`

NDJSON over stdio. An existing app launches it and consumes events
without linking Go/Python or rebuilding the app.

Surface requests:

- `surface/replay` {afterPosition,cursor,limit} -> {sessionId,events,cursor}
  Displayable events: full body, from, mode, createdAt, receipt status.
  Pure reads; never claims the agent inbox.
- `surface/present` {messageId} -> {presented:true}
  Confirms actual rendering. Sole source of `presented`.
- `surface/reply` {messageId,commandId?,text} -> SendResult
  Human reply with correlation preserved. Requires reply scope.
- `surface/subscribe` (streaming) emits {event:"display",message}
  lines as new messages arrive.

The surface poller uses replay under the hood (500ms) so a single
durable journal backs both replay and subscription. No second store.

## Delegated scope

Surface tokens use delegation scope `surface` with explicit
permissions: `read`, `present`, `reply`. Owner may issue via
session/delegate {scope:"surface",permissions:"read,present,reply"}.
Attached connections re-verify per-request (scope, expiry, revocation,
owner instance). Attached surface methods are allowlisted to
hello, surface/*, message/status, session/state|snapshot, heartbeat.
Denied for surface: message/listPending (would claim work),
message/report, message/send, session/delegate, permission/*,
remote/*, approval paths. Seeing a message never grants approval
authority.

## Receipt truth

| Event | Truth |
|---|---|
| delivered | Durably accepted; human may not have seen it |
| presented | Connected visual surface confirmed rendering |
| handled | Human/recipient explicitly acted |
| unknown | Presentation or handling could not be established |

Durable JSONL notification sinks are persistence only, not
presentation. Only surface/present moves a receipt to presented.
Replay reconnect (cursor resume) never fabricates presented.

## Drivers

- Python: adapters/python/intercom_adapter/surface_client.py
  SurfaceBridge (replay/present/reply) + render_status_line for
  TUI/status bars. IDE/web apps can port the same NDJSON protocol.
- Web: optional loopback-only streaming bridge may be built on this
  contract; not required for qualification.
- No mandatory dashboard: a terminal/IDE extension can render the same
  display events without a new server.

## What a UI must still implement

An existing UI must provide an integration point (status line, panel,
notification API, or web view). Agnostic means one protocol + thin
drivers, not zero integration code under every host. intercom-surface
cannot inject controls into arbitrary third-party interfaces.
