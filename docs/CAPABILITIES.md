# Capability matrix

Intercom separates three things that are easy to conflate:

- **Native support** — the harness directly exposes the behavior.
- **Emulated support** — an adapter implements an equivalent, documented
  behavior without misrepresenting its limitations.
- **Unsupported** — no safe implementation is available.

Unsupported behavior is never silently simulated with a semantically
different operation. That is the rule this matrix exists to make checkable.

## Harness summary

| Harness | Native surface | notify | queue | steer |
|---|---|---|---|---|
| Muse Code (`muse`, `musefi`, `openmuse` wrappers) | MSP over stdio (`muse serve`) | native | native | **native** |
| Codex (stock `codex app-server --stdio`) | app-server JSON-RPC | native | native | **native** |
| OpenCode (`opencode acp`) | ACP agent, protocolVersion 1 | native | native | **unsupported** |
| Hermes (`hermes-acp`) | ACP agent | native | native | **unsupported** |
| Claude Code (`claude --print`, stream-json) | stream-json input channel | native | native | **unsupported** |
| Any MCP-consuming agent (Claude, Codex, OpenCode, Muse sessions) | `intercom-mcp` MCP tools | native | native (durable inbox claim) | **unsupported** |
| Generic / synthetic | adapter contract | native | native | native |

Every row above is backed by the live evidence in
[docs/HARNESSES.md](HARNESSES.md): real binaries, real wire protocols,
receipt trails, and — for Muse and Codex — a live steer that changed a
running turn's output. Rows for harvardcodex and for MCP tools through
the FreeInference backend record a provider-side limitation, not an
Intercom one (see HARNESSES.md, "Provider failures are not harness
failures").

## Why Muse can steer

MSP's `turn/steer` takes a `commandId`, a `sessionId`, an
`expectedTurnId`, and an input array. It injects into the named turn and
fails if that turn is no longer active.

That is precisely the STEER semantics of ICSP-001 section 5.2: deliver to
the active turn at a supported safe execution boundary, without forcibly
interrupting unsafe operations. The contract was read from the schema that
`muse schema generate-json-schema` exports, which is the binary's own
authority on its wire format, rather than inferred from documentation.

## Why Hermes cannot steer

ACP offers `session/prompt`, which submits a user turn. It has no
equivalent of `turn/steer` that injects into a turn already running.

So the Hermes adapter reports steer as unsupported. Claiming it would be
the exact misrepresentation section 12.3 forbids: a newly launched prompt
is not active-turn steering.

Queueing is different, and is claimed: submitting when the session is idle
and holding otherwise is queue behavior.

## The Muse notification path

MSP has no notification-only method. `session/sideChat` is not a
substitute: it opens a new side session and runs a model turn in it, which
is a different operation from a notification and would misrepresent the
delivery.

So the Muse adapter records notifications in the adapter host's
operator-visible surface and reports them as `presented`. It never reports
`injected`, because nothing was injected into a model's context.

## The extension namespace

The extension is published under `dev.bamn.intercom`, so its methods are
`_dev.bamn.intercom/hello` and so on.

Section 8.1 requires a collision-resistant reverse-DNS namespace controlled
by the implementer before a stable extension is published. The
specification's own examples use the provisional `_intercom/` prefix, which
is explicitly not a claim of collision resistance.

The namespace is reported by the hello method and advertised through
`_meta`, so a client can tell which implementer owns the extension. The
reference ACP client routes any underscore-prefixed method to the
extension handler, which is why the prefix form is the wire form.

## Capability identifiers

The identifiers from section 8.3, and what this implementation reports for
its own host:

| Identifier | Level | Limitation |
|---|---|---|
| `peer.discovery` | native | |
| `peer.send` | native | |
| `peer.reply` | native | |
| `delivery.notify` | native | |
| `delivery.queue` | native | |
| `delivery.steer` | unsupported | Requires a harness-specific native integration |
| `delivery.wake` | emulated | Honoured only while the recipient adapter is connected |
| `session.observe` | native | |
| `session.replay` | native | |
| `command.durable` | native | |
| `approval.owner_control` | native | Peer authority never includes approval authority |

A capability reported as unsupported always carries a limitation saying
why. The adapter contract enforces this, so an adapter cannot advertise a
capability it did not declare without an explanation.

## Qualification levels

Every capability is qualified at four independent levels:

| Level | Meaning |
|---|---|
| advertised | the adapter's capability table declares the feature |
| implemented | real harness code executes it (not a simulation) |
| tested | automated tests exercise the real code path |
| live-verified | observed working against the real harness binary, documented in `docs/HARNESSES.md` |

Available means advertised AND implemented. Tested and live-verified
describe evidence, never capability alone: one harness's verified steer
does not make another harness able to steer. The Go node exposes the
per-harness table via `protocol.HarnessQualificationMatrix()` and the
extension hello response; Python adapters expose the same rows via
`BaseHarness.qualifications()`, derived from their declared modes so
they cannot drift from the capability table.

## How the matrix is enforced

The recipient's advertised modes drive the delivery decision:

| Requested | Recipient supports | Policy | Outcome |
|---|---|---|---|
| steer | steer | any | delivered, `injected` |
| steer | queue only | `reject` | refused, `rejected` |
| steer | queue only | `downgrade` | delivered as queue, downgrade reported |
| steer | queue only | `notify` | delivered as notification only |
| steer | nothing | any | refused, `rejected` |

The `notify` policy is notification-only rather than a general downgrade. A
queue fallback would still make the message available for the next turn,
which is a different operation from the notification the sender asked to
fall back to.

## Conformance profiles

| Profile | Claimed | Evidence |
|---|---|---|
| `IC-LOCAL` | yes | Local authenticated peer communication and delivery receipts |
| `IC-DURABLE` | yes | Command deduplication, durable admission, event replay, crash recovery |
| `IC-ACP` | yes | ACP v1 extension compatibility and capability negotiation |
| `IC-REMOTE` | mTLS loopback only | Two independent hosts over mTLS with pinned fingerprints, host-qualified ids, mutual enrollment, revocation/rotation. Non-loopback plaintext is impossible (TLS only). Live cross-host with real harnesses not qualified. |
| `IC-STEER` | Muse, Codex | Verified live: MSP `turn/steer` (5/5 accepted during running echo turns) and Codex app-server `turn/steer` (steered instruction changed the turn's output). See HARNESSES.md. |

Profiles are cumulative only where their prerequisites apply, and each
implementation identifies its supported profiles individually. The
supported set is reported by `_intercom/hello`.

Principal isolation note: the daemon principal is the local Unix user.
// All local clients sharing that user are one principal; they are NOT
// strongly isolated agent principals. Per-agent isolation requires
// separate agent credentials, which the daemon does not mint. Remote
// sender identity is host-qualified ("hostId/sessionId") and grants are
// checked against that qualified id on the receiving host.

## Adding a harness

An adapter implements the `Harness` contract and must:

1. Declare only the delivery modes the harness genuinely supports.
2. Report a capability as unsupported rather than emulating it with a
   different operation.
3. Carry a limitation on every unsupported capability.
4. Report what actually happened, distinguishing `presented` from
   `injected`.
5. Refuse a steer request outright when the harness cannot steer, rather
   than launching a new prompt and calling it steering.

The Python contract tests in `adapters/python/tests/` check the first
three and the refusal rule. The fourth is checked by the Go conformance
suite, which asserts on receipt states.
