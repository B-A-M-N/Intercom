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
| Muse Code | MSP over stdio (`muse serve`) | native | native | **native** |
| Hermes | ACP agent (`hermes-acp`) | native | native | **unsupported** |
| Codex | `codex app-server` | not yet integrated | not yet integrated | unsupported |
| Claude Code | `--print --output-format=stream-json` | not yet integrated | not yet integrated | unsupported |
| Generic / synthetic | adapter contract | native | native | native |

Only the first two are integrated. The other three rows record what was
found while investigating their surfaces, not a claim of support.

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
| `IC-REMOTE` | no | No cross-host transport; the profile requires one |
| `IC-STEER` | Muse only | Verified through MSP `turn/steer` |

Profiles are cumulative only where their prerequisites apply, and each
implementation identifies its supported profiles individually. The
supported set is reported by `_intercom/hello`.

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
