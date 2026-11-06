# Architecture

How the pieces fit, and why they are split the way they are.

## Layers

```
   harness processes (codex, claude, hermes, muse, ...)
                        |
                  adapter host  (Python)
                        |  ACP over stdio
                        v
              intercom-acp / intercomd
                        |
              _intercom/* extension
                        |
                        v
                  session core  (Go)
                        |
            durable store + local socket
```

The adapter host is deliberately thin. It translates one harness's native
surface into the Intercom adapter contract and reports what it observed.
It never decides a durability outcome, and it has no write path to the
store.

## The authority boundary

There are exactly two kinds of authority in the system.

**Owner authority** can manage a session and, subject to policy, approve
protected actions. **Peer authority** can submit permitted communications
and retrieve explicitly authorized status.

The distinction is enforced structurally rather than by convention:

- Authority over a session is per session, not per principal. A session
  always owns itself. Authority over any *other* session requires an
  explicit owner-role grant.
- A peer grant never carries owner authority, so a peer cannot grant
  access, revoke it, or reconfigure the recipient's delivery policy.
- The receiving session enforces its own delivery policy independently of
  the sender's authority. A grant permits a submission; the recipient's
  policy decides which modes it accepts.

Two sessions that share a principal are peers for communication purposes.
They are not peers for permission-management purposes.

## Why the core is Go

Three requirements decided it, and each was verified rather than assumed.

**Verified local process identity.** Section 11.1 requires authenticated
local process identities. On Linux, `SO_PEERCRED` returns the peer
credentials from the kernel, so they cannot be forged by the connecting
process. Go reaches it through `golang.org/x/sys/unix` with no cgo.

**Embedded transactional storage.** Section 13 recommends a small embedded
transactional database such as SQLite in WAL mode. `modernc.org/sqlite` is
a pure-Go SQLite, so the store needs no cgo, no system SQLite, and no
build-time toolchain.

**One durability boundary.** Section 13 requires atomic admission and
consistent sequence assignment within the local durability boundary. A
single process owning a single SQLite database keeps that boundary
unambiguous.

Rust was considered and rejected on evidence: the crate cache is empty and
the build environment has no DNS, so a Rust implementation would not build.
Node was rejected because the reference ACP implementation is Python and
`node:sqlite` is still experimental.

## Durability model

The store is SQLite in WAL mode. Atomicity comes from SQLite transactions,
not from application-level locking.

### Command ledger

Every state-changing command has a durable record with a request
fingerprint. The fingerprint covers the operation and the canonical request
body.

That gives the two idempotency behaviors the specification requires:

- An identical retry returns the previously established admission outcome,
  including the original message id, and does not admit a second time.
- A reuse with a conflicting request is rejected with a conflict error.

The ledger also drives recovery. After a restart, commands that were
admitted but never settled are **abandoned**, not re-executed. Re-execution
could duplicate effects, and the specification forbids it.

### Command lifecycle

```
received → accepted → executing → settled
                 ↘ rejected
                 ↘ abandoned   (recovery: outcome not established)
```

A terminal delivery receipt is what settles a command. The acknowledgement
reports admission, never completion.

### Message path

```
send ──► admission (durable)  ──► acceptance receipt
             │
             ├──► capability check against the recipient's honest matrix
             ├──► recipient delivery policy check
             ├──► delivery attempt
             │       └──► receipt trail (presented / injected / rejected / ...)
             └──► expiry check
```

Ordering is preserved within a sender-recipient stream: each stream has its
own queue and worker, so messages from one sender to one recipient arrive
in submission order while unrelated streams proceed concurrently.

### Event journal

Each session has an append-only journal with monotonically increasing
sequence numbers, assigned inside the same transaction as the insert.

Compaction records a per-session floor. A replay request for a cursor
below the floor returns an explicit cursor-gap error carrying the snapshot
path and the earliest available sequence, rather than silently returning a
short history.

### Retention

A maintenance pass expires messages, voids them so they cannot be injected,
purges messages past the retention window, and compacts the journal. The
purge cutoff respects the deduplication horizon, because retention must
preserve the idempotency obligations for that horizon.

## Delivery semantics

### Modes and wake policies are independent

The delivery mode decides **what happens to the message**. The wake policy
decides **whether the recipient is initiated**. Conflating them is easy
and wrong: a `none` wake must still deliver a queued message, it just must
not start a turn.

### Receipts are facts

Eight distinct states, and they are not interchangeable:

| State | Meaning |
|---|---|
| `accepted` | Durably admitted. Not delivery. |
| `delivered` | The recipient inbox durably accepted it. |
| `presented` | A recipient surface displayed it. |
| `injected` | The adapter confirmed model-context injection. |
| `handled` | The recipient reported handling it. Not correctness. |
| `rejected` | Delivery or admission refused. |
| `expired` | It expired before delivery completed. |
| `unknown` | The outcome could not be established. |

`presented` and `injected` are independent. A notification surface may
present a message that is never injected into the model's context, and the
receipt trail must be able to say so.

### Capability matrix

Each recipient advertises its delivery modes with a support level. The
core consults that matrix before every delivery:

- supported natively: delivered as requested
- unsupported, `reject` policy: refused with a rejection receipt
- unsupported, `downgrade` policy: delivered at the closest fidelity, with
  the downgrade reported on the receipt
- unsupported, `notify` policy: delivered as a notification only

Notification is the floor, because it never claims model attention.

## Transport

The local transport is a Unix domain socket with JSON-RPC 2.0 framing,
newline delimited so a reader never has to guess where a message ends.

Peers are authenticated by `SO_PEERCRED`. The socket file is created with
owner-only permissions.

The ACP extension runs over the same JSON-RPC framing, either on the socket
or over stdio. Over stdio, `intercom-acp` forwards to the daemon socket on
one bound connection; it does not open the store. Only `intercomd` owns
durable state, which is the arrangement an adapter host uses.

## ACP interoperability

The extension is implemented directly over JSON-RPC rather than through an
ACP SDK. The surface is deliberately small: the standard ACP lifecycle plus
the `_intercom/*` methods.

Two consequences worth stating:

- The extension is advertised only through `_meta`, so a client that
  ignores `_meta` is entirely unaffected.
- The reference ACP client prefixes extension methods with an underscore,
  so `ext_method("intercom/hello")` reaches the wire as
  `_intercom/hello`. The namespace a real client uses is therefore exactly
  the one the specification names.

Standard ACP methods keep their own semantics. A method the extension does
not implement is reported as method-not-found, which is what keeps an
ordinary ACP client operable.

## Resource bounds

Every bound is configurable, defaults are conservative, and exhaustion is
explicit rather than unbounded:

| Bound | Default |
|---|---|
| Message size | 1 MiB |
| Undelivered messages per recipient | 1000 |
| Admitted-but-unsettled commands | 10000 |
| Replay batch | 500 |
| Concurrent connections | 64 |
| Messages per peer per minute | 600 |
| Message retention | 7 days |
| Event retention | 30 days |
| Deduplication horizon | 24 hours |

## What is deliberately absent

- **No central scheduler.** Peer communication works without one.
- **No shared context.** Sessions keep their own transcripts.
- **No merged filesystem permissions.** Section 11.4 is explicit that
  separate sessions imply no isolation, and Intercom makes no such claim.
- **No distributed consensus.** The local profile needs none.
- **Cross-host transport is opt-in.** `IC-REMOTE` federation requires
  `--remote-listen`, mTLS with pinned host fingerprints, and mutual
  enrollment; there is no plaintext path. It is qualified by the
  two-host protocol tests, not yet by live cross-host harness runs.
