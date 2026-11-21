# Intercom

Agent-agnostic session and inter-agent communication protocol.

Intercom lets independently running agents discover one another, exchange messages, and coordinate without a central orchestrator and without giving up their own execution authority, context, or permission boundaries. It is an implementation of [ICSP-001](docs/spec/ICSP-001.md); the specification text ships in this repository.

Intercom is an experimental project maintained alongside the agent harnesses it connects to. Work continues as time permits. See [Project status](#project-status).

## Why

Agent sessions are usually isolated: one harness cannot tell another what it is doing, hand work off, or ask for attention without a shared orchestrator that owns everybody's state. Intercom adds a small, durable communication plane between peers. Each agent keeps its own authority. The protocol carries messages and receipts, not commands that override a harness's permission boundary.

The implementation is deliberately honest about what it can do. An adapter must not advertise a capability it cannot perform, and unsupported behavior is never silently simulated with a semantically different operation.

## Protocol profiles

| Profile | Status | Notes |
|---|---|---|
| `IC-LOCAL` | Implemented | Local authenticated peer communication and delivery receipts |
| `IC-DURABLE` | Implemented | Command deduplication, durable admission, event replay, crash recovery |
| `IC-ACP` | Implemented | ACP v1 extension compatibility and capability negotiation |
| `IC-REMOTE` | Implemented (opt-in) | mTLS federation with pinned host fingerprints and mutual enrollment; remote messaging is opt-in with `--remote-listen` and never plaintext |
| `IC-STEER` | Implemented for Muse and Codex | Verified live-turn injection through MSP `turn/steer` and Codex app-server `turn/steer` |

Exchanging JSON is not compliance. The conformance suite in `conformance/` is the evidence, numbered to match the required protocol tests in section 14 of the specification.

## Design

Three layers, as the specification defines them:

- **Session core.** Durable command intake and settlement, an event journal, state projection, and recovery classification. Independent of any agent harness.
- **Peer communication plane.** The primary primitive: discovery, direct addressing, delivery, wake, receipts, and access control. Works with no orchestrator.
- **Interoperability layer.** Adapters that translate a harness's native surface into the Intercom contract.

The ACP extension is published under the reverse-DNS namespace `dev.bamn.intercom`, as section 8.1 requires before a stable extension is published. The specification's `_intercom/` examples are provisional and are not used here.

### Language split

The core is Go; the adapters are Python. That is a deliberate boundary.

The Go core owns durability, identity, authorization, and delivery truth. It is the only component that writes the store. Two durable cores would mean two sources of truth for idempotency, which the specification forbids. Three properties were verified rather than assumed:

- `SO_PEERCRED` through `golang.org/x/sys/unix` gives the kernel-verified peer identity that section 11.1 requires.
- `modernc.org/sqlite` is pure Go, so the SQLite-in-WAL store needs no cgo and no build-time toolchain.
- Both dependencies resolve from the local module cache, so the build works offline.

Python owns harness quirks: the reference ACP implementation (`agent_client_protocol`), Hermes' ACP adapter, and the ecosystem's plugin surfaces are Python. Those adapters stay thin. Rust was ruled out on evidence: no crates were cached, the sandbox had no DNS, and an MSP bridge would have had to reimplement the wire format that `muse schema generate-json-schema` already exports.

### Capability honesty

The rule the implementation is most careful about: an adapter must not advertise functionality it cannot implement.

- **Muse** speaks MSP natively. Its `turn/steer` injects into a named turn and fails when that turn is gone, which is exactly the STEER semantics, so this adapter claims native steering.
- **Hermes** speaks ACP. ACP offers `session/prompt`, a next-turn channel, but no active-turn injection point. This adapter claims queue and notify and reports steer as unsupported.

The Muse notify path deliberately avoids `session/sideChat`: that opens a new side session and runs a model turn in it, which is a different operation from a notification. Notifications are recorded in the adapter host's operator surface and reported as `presented`, never as `injected`.

## Requirements

- Go 1.25 or newer. The module cache on the build host must contain `github.com/google/uuid`, `golang.org/x/sys`, and `modernc.org/sqlite`.
- Python 3 for the adapter host. Its own protocol client has no third-party dependencies; the Muse and Hermes adapters use the `acp` package when a harness needs it.
- A Unix host for `SO_PEERCRED` peer identity and the local socket transport.

## Quick start

```sh
go build ./...
go test ./...

cd adapters/python && python3 -m unittest discover -s tests
```

The Go build needs no network access when the module cache is populated.

Start a host and talk to it:

```sh
go run ./cmd/intercomd --socket /tmp/intercom/intercom.sock --alias my-host

go run ./cmd/intercom --socket /tmp/intercom/intercom.sock hello
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock peers
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock send ses_other "hello" --mode notify
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock status msg_...
```

The CLI is a peer, not an operator tool. It has no path to the durable store and no special authority. The host creates its durable store under `--data-dir`, recovers commands that were admitted but never settled, then serves the local socket.

### ACP over stdio

An adapter host talks ACP to the extension over stdio. `intercom-acp` is a thin forwarder: it connects to `intercomd` over the daemon socket, registers its session on that connection, and forwards every stdio request to the daemon. It never opens the durable store; only `intercomd` owns durable state.

```sh
go run ./cmd/intercom-acp --socket /tmp/intercom/intercom.sock --session ses_x --harness muse
```

The binary is also an ordinary ACP agent for a client that knows nothing about Intercom: standard methods keep their own semantics, and the extension is advertised only through `_meta`.

## Repository layout

```
cmd/intercom         CLI: peers, send, status, replay, subscribe
cmd/intercomd        The host: durable store, local socket, ACP surface
cmd/intercom-acp     The extension over stdio, for adapter hosts

internal/protocol    Wire types: messages, receipts, commands, events
internal/ids         The four identifiers and their lifetimes
internal/store       SQLite in WAL mode; the durable records
internal/node        The session core and peer communication plane
internal/transport   Unix sockets with SO_PEERCRED; JSON-RPC 2.0 framing
internal/adapter     The adapter contract and its honesty rules
internal/adapter/acp The _intercom/* extension surface

adapters/python      Adapter host and Muse, Codex, OpenCode, Hermes, Claude, MCP adapters
conformance          Section 14 protocol tests and section 16 acceptance
docs/spec            The ICSP-001 specification text
docs/                Architecture, capabilities, demo, harnesses, surface notes
```

Further reading: [architecture](docs/ARCHITECTURE.md), [capabilities](docs/CAPABILITIES.md), [harness support](docs/HARNESSES.md), [surface notes](docs/SURFACE.md), [demo](docs/DEMO.md).

## Verification

Evidence lives in three places:

- `conformance/` holds the section 14 protocol tests, numbered to match the specification, plus the section 16 acceptance criteria. Acceptance tests spawn real processes and use real client connections, so they exercise transport, durable store, and process lifecycle rather than an in-process shortcut.
- `adapters/python/tests/` holds adapter contract tests: capability honesty, MSP mapping, and the rule that a failed steer is rejected rather than retried as a new turn. One test drives the extension through the reference ACP implementation, the evidence for acceptance criterion 8.
- `internal/` has unit-level coverage of the store, protocol, and transport.

## Project status

Experimental and maintained intermittently. Intercom is tied to the applications it interoperates with: the Muse, Codex, OpenCode, Hermes, and Claude harnesses, the ACP and MSP surfaces, and the ICSP-001 draft it implements. It is not a hosted service and does not require an orchestrator. Work continues as time permits and higher-priority work allows.

Known gaps:

- **Cross-host transport is opt-in and loopback-qualified.** `IC-REMOTE` runs only with `--remote-listen`, uses mTLS with pinned host fingerprints and mutual enrollment, and refuses plaintext. Live cross-host exchange with real harnesses is not yet qualified; the two-host exchange is tested in-process.
- **Steering is verified for Muse and Codex.** Other harnesses report steer as unsupported until their native surface is verified.
- **No tool-level approval surface.** `approval.owner_control` is advertised and the authority separation is enforced and tested, but the approval request belongs to a harness's own permission mechanism.
- **Retention is time-based.** The journal compacts by age; sequence-based compaction would need per-session event counts.

## License

MIT. See [LICENSE](LICENSE).

## Contributing

Issues and focused pull requests are welcome. Protocol changes should include the matching numbered conformance test.

## Disclaimer

This project is an independent, experimental work by the repository author. It is not affiliated with, directed, endorsed, or approved by FreeInference.org ("FI"). FI has no involvement in this project's design, development, or publication, and bears no responsibility for it. Every decision to create, modify, and publish this work was made by me, at my own discretion.

If you find this project useful or helpful, please consider sponsoring or otherwise supporting FreeInference.org. FI provides a useful and valuable service to people who might not otherwise have the opportunity to access this kind of tooling.
