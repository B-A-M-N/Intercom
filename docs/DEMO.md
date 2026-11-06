# End-to-end demo (repeatable, zero inference spend)

This demo proves discovery, messaging, replies, receipts, and native
steering with real harness binaries and no model spend, using Muse's
echo provider and scripted MCP participants. It starts everything in
`/tmp/intercom-demo` and touches no global configuration.

## 1. Build

```sh
cd ~/Intercom
go build -o /tmp/intercom-demo/bin/intercomd ./cmd/intercomd
go build -o /tmp/intercom-demo/bin/intercom-mcp ./cmd/intercom-mcp
go build -o /tmp/intercom-demo/bin/intercom ./cmd/intercom
```

## 2. Start the daemon

```sh
mkdir -p /tmp/intercom-demo
/tmp/intercom-demo/bin/intercomd \
  --socket /tmp/intercom-demo/i.sock \
  --data-dir /tmp/intercom-demo/data \
  --alias demo-host
```

Leave it running. Presence heartbeats keep live sessions `online`
(the daemon marks a session disconnected after 90s without a beat;
all clients now beat every 10s).

## 3. Start a Muse participant (echo provider)

```sh
cd ~/Intercom/adapters/python
PYTHONPATH=. MUSE_NO_AUTO_UPDATE=1 python3 -m intercom_adapter.host \
  --socket /tmp/intercom-demo/i.sock \
  --harness muse --session ses_muse00000000000000000000001 \
  --alias muse-echo --poll-interval 0.1 \
  --muse-bin musefi --muse-provider echo \
  --workspace /tmp/intercom-demo/work
```

Expect a `{"event":"registered",...,"deliveryModes":["steer","queue","notify"]}`
line. This process owns a real `musefi serve --provider echo`
subprocess; delivery goes through MSP `turn/start`.

## 4. Talk to it through MCP (scripted participant)

```sh
/tmp/intercom-demo/bin/intercom-mcp \
  --socket /tmp/intercom-demo/i.sock \
  --session ses_scripteda000000000000000001 \
  --alias scripted-a --harness mcp-scripted
```

Drive it with any MCP client, or reuse the repo's scripted consumer
(`adapters/python/intercom_adapter/mcp_consumer.py`) in a Python
shell:

- `peers_list` → muse-echo, scripted-a, scripted-b (discovery)
- `message_send {to: ses_muse..., text: "hello", mode: "queue"}`
  → `accepted`; then `message_status` walks
  accepted → unknown (durable inbox) → delivered
  ("submitted through MSP turn/start").
- `message_send {..., mode: "steer"}` with no active turn → `rejected`
  (never retried as a new turn).
- A second scripted participant claims via `inbox_list`, acks
  `presented` then `handled` via `message_ack`, and replies with
  `message_reply`; the reply appears in the first participant's inbox
  with `replyTo` preserved.

## 5. Native steering (direct, still zero spend)

With the echo provider, a turn runs ~0.5s. Send `turn/start` via MSP
and immediately `turn/steer` with the returned turnId — it is
accepted while the turn runs (verified 5/5). The Python client is in
`adapters/python/intercom_adapter/msp.py`; the live tests in
`adapters/python/tests/test_live_protocols.py` exercise this in CI.

## 6. Other harnesses

- **Codex**: `--harness codex --codex-bin codex` (stock binary;
  `--codex-home ~/.harvardcodex` selects the harvard profile through
  the environment). One real turn proves queue+steer (see
  docs/HARNESSES.md).
- **OpenCode**: `--harness opencode` (ACP). One FI turn proves queue.
- **Claude**: `--harness claude --claude-budget-usd 0.25
  --claude-allowed-tools mcp__intercom__peers_list` for MCP use.
- **harvardcodex**: MCP connects and registers, but FI catalog models
  cannot invoke MCP tools upstream (see HARNESSES.md). Do not debug
  Intercom for that failure.

## 7. Qualification

```sh
scripts/qualify.sh          # full: suites + live no-spend protocols
scripts/qualify.sh --quick  # suites only
```

Adversarial coverage (conformance/phase8_test.go): cross-session
ack/inbox theft forbidden, commandId idempotency incl. concurrent
duplicates, claim races (exactly one winner), heartbeat presence,
unknown-recipient refusal.
