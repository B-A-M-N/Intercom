# Intercom

Agent-agnostic session and inter-agent communication protocol.

Intercom lets independently running agents discover one another, exchange
messages, and coordinate, without a central orchestrator and without giving
up their own execution authority, context, or permission boundaries.

This is an implementation of [ICSP-001](docs/spec/ICSP-001.md), the protocol
design draft. The specification text is in the repository; this document
describes what is built.

## Status

| Profile | Status | Notes |
|---|---|---|
| `IC-LOCAL` | Implemented | Local authenticated peer communication and delivery receipts |
| `IC-DURABLE` | Implemented | Command deduplication, durable admission, event replay, crash recovery |
| `IC-ACP` | Implemented | ACP v1 extension compatibility and capability negotiation |
| `IC-REMOTE` | Implemented (opt-in) | mTLS federation with pinned host fingerprints and mutual enrollment; remote messaging is opt-in (`--remote-listen`) and never plaintext |
| `IC-STEER` | Implemented for Muse and Codex | Verified live-turn injection through MSP `turn/steer` and Codex app-server `turn/steer` (see docs/HARNESSES.md) |

An implementation cannot claim compliance merely by exchanging JSON
messages. The conformance suite in `conformance/` is the evidence, and it is
numbered to match the required protocol tests in section 14 of the
specification.

## Design

Three layers, as the specification defines them.

**Session core.** Durable command intake and settlement, an event journal,
state projection, and recovery classification. Independent of any agent
harness.

**Peer communication plane.** The primary primitive: discovery, direct
addressing, delivery, wake, receipts, and access control. Works with no
orchestrator.

**Interoperability layer.** Adapters that translate a harness's native
surface into the Intercom contract.

The ACP extension is published under the reverse-DNS namespace
`dev.bamn.intercom`, as section 8.1 requires before a stable extension is
published. The specification's `_intercom/` examples are provisional and
are not used here.

### Language split

The core is Go. The adapters are Python.

This is a deliberate boundary, not a preference.

The Go core owns durability, identity, authorization, and delivery truth. It
is the only component that writes the store. That matters because two
durable cores would mean two sources of truth for idempotency, which is
exactly what the specification forbids.

Python owns harness quirks, because that is where they are. The reference
ACP implementation (`agent_client_protocol`) is Python, Hermes' ACP adapter
is Python, and the agent ecosystem's plugin surfaces are Python. Adapters
there are thin.

Go was chosen for the core for three reasons that were verified rather than
assumed:

- **Verified local process identity.** `SO_PEERCRED` through
  `golang.org/x/sys/unix` gives the kernel-verified peer identity that
  section 11.1 requires. Verified working.
- **Embedded transactional storage.** `modernc.org/sqlite` is pure Go, so
  the SQLite-in-WAL store that section 13 recommends needs no cgo and no
  build-time toolchain. Verified building and running offline.
- **Reproducible offline builds.** Both dependencies resolve from the local
  module cache, so the build does not need network access.

Rust was ruled out on evidence: no crates are cached and the sandbox has no
DNS. A Rust MSP bridge would have had to reimplement the wire format that
`muse schema generate-json-schema` already exports exactly.

### Capability honesty

This is the rule the implementation is most careful about.

An adapter must not advertise functionality it cannot implement, and
unsupported behavior must not be silently simulated with a semantically
different operation. Concretely:

- **Muse** speaks MSP natively. Its `turn/steer` injects into a named turn
  and fails when that turn is gone, which is exactly the STEER semantics,
  so this adapter claims native steering.
- **Hermes** speaks ACP. ACP offers `session/prompt`, a next-turn channel,
  but no active-turn injection point. This adapter claims queue and notify,
  and reports steer as unsupported.

The Muse notify path deliberately avoids `session/sideChat`: that opens a
new side session and runs a model turn in it, which is a different operation
from a notification. Notifications are recorded in the adapter host's
operator surface and reported as `presented`, never as `injected`.

## Layout

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

adapters/python      The adapter host and the Muse, Codex, OpenCode, Hermes, Claude, and MCP adapters
conformance          The section 14 protocol tests and section 16 acceptance
docs/spec            The ICSP-001 specification text
```

## Building

```sh
go build ./...
go test ./...
```

The build needs no network access. The Go module cache on the build host
must contain `github.com/google/uuid`, `golang.org/x/sys`, and
`modernc.org/sqlite`.

The Python adapter host has no third-party dependencies for its own
protocol client. The Muse and Hermes adapters use the `acp` package when a
harness needs it.

```sh
cd adapters/python && python3 -m unittest discover -s tests
```

## Running

Start a host:

```sh
go run ./cmd/intercomd --socket /tmp/intercom/intercom.sock --alias my-host
```

The host creates its durable store under `--data-dir`, recovers any command
that was admitted but never settled, and then serves the local socket.

Talk to it:

```sh
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock hello
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock peers
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock send ses_other "hello" --mode notify
go run ./cmd/intercom --socket /tmp/intercom/intercom.sock status msg_...
```

The CLI is a peer, not an operator tool. It has no path to the durable store
and no special authority.

### ACP over stdio

An adapter host talks ACP to the extension over stdio. `intercom-acp` is a
thin forwarder: it connects to `intercomd` over the daemon socket, registers
its session on that connection (binding the connection to the session), and
forwards every stdio request to the daemon. It never opens the durable
store; only `intercomd` owns durable state:

```sh
go run ./cmd/intercom-acp --socket /tmp/intercom/intercom.sock --session ses_x --harness muse
```

The binary is also an ordinary ACP agent for a client that knows nothing
about Intercom: standard methods keep their own semantics, and the
extension is advertised only through `_meta`.

## Verification

The evidence lives in three places.

`conformance/` holds the protocol tests from section 14, numbered to match
the specification, plus the acceptance criteria from section 16. The
acceptance tests spawn real processes and use real client connections, so
they exercise the transport, the durable store, and the process lifecycle
rather than an in-process shortcut.

`adapters/python/tests/` holds the adapter contract tests: the capability
honesty rules, the MSP mapping, and the rule that a failed steer is
rejected rather than retried as a new turn. One test drives the extension
through the reference ACP implementation, which is the evidence for
acceptance criterion 8.

`internal/` has unit-level coverage of the store, the protocol, and the
transport.

## Known gaps

- **Cross-host transport is opt-in and loopback-qualified.**
  `IC-REMOTE` federation runs only with `--remote-listen`, uses mTLS
  with pinned host fingerprints and mutual enrollment, and refuses
  plaintext. Live cross-host exchange with real harnesses is not yet
  qualified (the two-host protocol exchange is tested in-process).
- **Steering is verified for Muse and Codex.** Other harnesses report steer as
  unsupported until someone verifies their native surface.
- **No tool-level approval surface.** `approval.owner_control` is
  advertised, and the authority separation is enforced and tested, but the
  approval request itself belongs to a harness's own permission mechanism
  rather than to Intercom.
- **Retention is time-based.** The journal compacts by age. A
  sequence-based compaction point would need per-session event counts.

## License

See `LICENSE`.
