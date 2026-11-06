# Harness integration evidence

How each installed harness actually talks to Intercom, what was
verified live, and what remains impossible without upstream support.
Nothing here is based on documentation claims alone: every wire shape
below was read from the binary's own exported schema or captured from
a live run.

## Architecture in one paragraph

Intercom's Go core owns durability, identity, and receipts. Two kinds
of participants connect to it:

- **Adapter-host participants** (Python): the adapter host polls the
  durable inbox and delivers into the harness's native surface —
  Muse MSP, Codex app-server, OpenCode/Hermes ACP, Claude stream-json.
  Receipts are reported back (`presented` / `delivered` / `injected` /
  `rejected`) by the adapter, never invented by the core.
- **MCP participants**: `intercom-mcp` is an MCP server bound to one
  Intercom session. An agent that consumes MCP tools discovers peers,
  sends, claims its inbox, acks, and replies during its own turns.
  Claim is `presented`; only `message_ack` with `handled` asserts the
  agent acted; `injected` is refused on this path by design.

Intercom session ids (`ses_<uuidv7>`) and harness session ids (Muse
UUIDs, Codex thread ids, ACP `ses_…`) live in different namespaces.
Every adapter maps between them and caches the mapping; no adapter
ever guesses a harness id.

## Muse (`muse`, `musefi`, `openmuse`)

All three wrappers launch the same Muse binary (1.4.14) with a
different `XDG_CONFIG_HOME` profile. The adapter launches the wrapper
the operator names (`--muse-bin musefi`), so profile selection is the
wrapper's job, not a config rewrite.

Wire contract (from `muse schema generate-json-schema`):
initialize (clientInfo.name must match `^[a-z0-9_]+$`) → `initialized`
notification → `session/start` (UUIDv7 commandId) → `turn/start`
(ifBusy=queue) → `turn/steer` (expectedTurnId). `session/start`
accepts `config.mcpServers`, so a Muse session can be given
intercom-mcp per-session with no global change.

Live evidence (2026-11-05, echo provider, zero inference spend):

- initialize/session/turn complete; echo reply `echo: hello echo probe`
  observed in `item/delta` + `turn/completed`.
- `turn/steer` with a wrong turn id errors on the wire (the failure
  semantics steer honesty depends on).
- Steer race, 5 attempts: `turn/start` then immediate `turn/steer`
  accepted 5/5 (`status: accepted`, same turnId).
- Through Intercom: adapter host registered with `delivery.steer:
  native`; a queue message from a scripted MCP sender reached receipt
  `delivered` with detail "submitted through MSP turn/start
  (disposition started)"; a steer with no active turn was `rejected`
  and was never retried as a new turn.

Adapter: `adapters/python/intercom_adapter/muse.py` + `msp.py`.
Muse `--provider echo` is the no-spend test provider; `meta`/`local`
providers use the same code path with a real model.

## Codex (stock `codex app-server --stdio`)

Wire contract (from `codex app-server generate-json-schema`):
initialize → `initialized` → `thread/start` (ephemeral; no model
call) → `turn/start` → `turn/steer` {threadId, expectedTurnId, input}.
The schema states expectedTurnId is a "Required active turn id
precondition. The request fails when it does not match the currently
active turn" — a genuine native steer point.

Live evidence (2026-11-05, one model turn on the authorized account):

- initialize + `thread/start` with no inference; thread idle.
- One turn: `turn/start` "Reply with exactly: BASE", then
  `turn/steer` "Correction: reply with exactly: STEERED".
  Steer accepted; `turn/completed` status `completed`; agent final
  text `STEERED.` — the steer changed the running turn's output.
- MCP: `codex exec` with `mcp_servers.intercom` ran
  `mcp: intercom/peers_list started/completed` on the wire
  (tools/call name `peers_list`), replied `CX-OK`, and its session
  was visible to peers.

Adapter: `codex.py` + `codex_client.py`. Steer + queue are both
native. The adapter launches the stock binary and selects the profile
via `CODEX_HOME` (per-process env, `--codex-home`), never by editing
global config.

## OpenCode (`opencode acp`)

Wire contract (verified against OpenCode 2.2.5 with a raw JSON-RPC
client; `clientInfo: null` is required — a non-null clientInfo object
gets no response): initialize (protocolVersion 1) → `initialized` →
`session/new` (slow, ~10s model-catalog load) → `session/prompt`
(blocks until the turn completes).

Live evidence: adapter host registered with steer `unsupported` +
limitation; a queue message was delivered, receipt `delivered`,
detail "submitted as the next turn through ACP session/prompt"
(one FreeInference turn). OpenCode also accepts per-session
`mcpServers` in ACP `session/new`, so an OpenCode session can be
given intercom-mcp the same way Muse sessions are.

Adapter: `opencode.py` + `acp_client.py` (raw ACP client; the
reference `agent_client_protocol` package is not a dependency).
Queue + notify native; steer unsupported (ACP has no active-turn
injection point).

## Hermes

Same ACP client and shape as OpenCode (`hermes.py` now shares
`acp_client.py` and session mapping). Queue + notify native; steer
unsupported. The reference-ACP-client conformance test
(`test_real_acp_client.py`) still passes against the Go extension.

## Claude Code

No server protocol with an injection point exists. The adapter uses
`claude --print --input-format=stream-json --output-format=stream-json`:
a user message on the input stream becomes the next turn (queue).
Steer is unsupported; there is no documented active-turn injection
API, and a new user message is queueing, not steering.

Live evidence (2026-11-05):

- `--print` turn works on the authorized account (one $0.09 probe).
- MCP participation with isolated `--mcp-config` (no global config
  change): session registered as `claude-live`; first attempt hit
  `permission_denials: [mcp__intercom__peers_list]` under
  `--permission-mode dontAsk` — MCP tools need an explicit
  `--allowedTools=mcp__intercom__peers_list` grant. With it: no
  denials, 3 turns, `CL-OK`, $0.031, under a $0.25 cap.
- Claude tool naming is `mcp__intercom__peers_list`; intercom-mcp
  normalizes both `mcp__x.y` and `mcp__x__y` forms.

Adapter: `claude.py` (`create_claude_client` takes `allowed_tools`;
host flag `--claude-allowed-tools`). Every queue delivery is a paid
Anthropic turn; the client defaults to requiring a budget cap
(`--claude-budget-usd`).

## MCP as a universal path

`intercom-mcp` (Go) serves: peers_list, peer_route,
peer_capabilities, message_send, message_reply, inbox_list,
**message_ack** (presented/handled/rejected; `injected` refused),
message_status, conversation_history. Negotiation echoes the client's
protocol version (2025-06-18 / 2025-03-26 / 2024-11-05), answers
`ping`, and handles concurrent tool calls with unique daemon request
ids (a constant id previously made parallel calls indistinguishable —
fixed, with a conformance test).

Registration is honest: deliveryModes `notify` + `queue`,
`delivery.steer: unsupported` with the limitation stated, matching
the Python `MCPHarnessAdapter` exactly. Live evidence: scripted
MCP A→B claim/present/handle/reply with `replyTo` visible in A's
inbox; Go phase-7 tests assert the handled receipt, the injected
refusal, the honest capabilities, and 12 concurrent calls.

## Provider failures are not harness failures

Two failures encountered were provider/wrapper-side, with the
Intercom path proven working underneath:

- **harvardcodex app-server**: the launcher selects
  `~/.harvardcodex/bin/codex-workflow` when present, and that build
  has no `app-server` subcommand at all (`--stdio` rejected; bare
  `app-server` falls through to a TUI that dies on non-terminal
  stdin). It is also TUI-only (no exec mode). Upstream wrapper
  limitation; the stock binary with `CODEX_HOME=~/.harvardcodex`
  speaks app-server normally, which is what the Codex adapter uses.
- **harvardcodex MCP tool calls**: the MCP server connected, the
  session registered (`harvardcodex-live` visible to peers), and
  tools/list succeeded on the wire — but the Codex router rejected
  every call (`unsupported call: mcp__intercom.peers_list`) before
  any tools/call was sent, for FreeInference catalog models. The
  identical server, binary, and prompt work with the OpenAI-backed
  model (tools/call on the wire, completed). The generated FI model
  catalog marks no MCP/tool support, so the router never registers
  the tools for those models. That is a model-catalog/provider
  limitation; fixing it needs upstream catalog support, not an
  Intercom change. No global harvardcodex config was modified to
  chase it.

## What cannot be implemented without upstream support

- **Steer for ACP harnesses** (OpenCode, Hermes): no active-turn
  injection point exists in ACP. Any claim would be misrepresentation.
- **Steer for Claude Code**: no documented injection API.
- **Steer/wake for MCP-only participants**: an MCP inbox claim is a
  next-turn channel; it cannot inject into a running turn, and wake
  only exists while the adapter is connected.
- **harvardcodex headless app-server**: needs the wrapper to stop
  selecting the TUI-only workflow binary for `app-server`.
- **MCP tool invocation for FI catalog models in Codex**: needs the
  catalog/provider to declare tool support for those models.

## Spend accounting

Zero-spend verification covered: Muse echo turns, Codex
initialize/thread-start, OpenCode initialize/session-new, all MCP
protocol tests (scripted consumer), and the full Go/Python suites.
Model turns actually run: Codex 1 (steer proof), OpenCode 1–2
(queue, FI), Claude 3 (one $0.09 probe, one $0.11 denied attempt,
one $0.031 success), harvardcodex 3 attempts (router-rejected, no
tool execution). No paid inference was consumed beyond those.
