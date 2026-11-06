# Intercom
## Agent-Agnostic Session & Inter-Agent Communication Protocol

**Specification:** ICSP-001  
**Version:** 0.1.0 — Initial Design Draft  
**Status:** Proposed  
**Base protocols:** JSON-RPC 2.0, ACP v1  
**Architecture inspiration:** Muse Session Protocol (MSP), Muse inter-session messaging

---

## 1. Purpose

Intercom is a provider-independent protocol enabling autonomous software agents to discover, communicate with, coordinate, and observe one another while retaining independent execution contexts and permission boundaries.

Intercom SHALL support agents operating:

- Under different model providers.
- Through different agent harnesses.
- Within independent processes and sessions.
- On the same machine or across authenticated hosts.
- Without requiring a central orchestrator to mediate conversations.

Intercom is a **communication and session-control primitive**, not an agent framework, task planner, or model inference service.

### 1.1 Design principles

1. **Agent independence:** Each participating agent retains its own lifecycle, context, execution authority, and resources.
2. **Direct peer semantics:** Agents address other agents without a mandatory orchestration intermediary.
3. **Recoverability:** Commands, messages, and observable session transitions have durable identities and recoverable outcomes.
4. **Protocol interoperability:** ACP-compatible clients and agents can access Intercom through negotiated extensions.
5. **Capability honesty:** An adapter MUST NOT advertise functionality it cannot implement.
6. **Least authority:** Receiving a message MUST NOT confer execution privileges or user authorization.
7. **Minimal coupling:** No participating agent must depend on another agent's model provider, internal reasoning format, or proprietary session representation.

### 1.2 Non-goals

Intercom SHALL NOT:

- Replace ACP or MCP.
- Require a centralized multi-agent scheduler.
- Merge agent context windows or transcripts.
- Share filesystem access or sandbox privileges.
- Guarantee correctness of agent output.
- Guarantee exactly-once execution of arbitrary external side effects.
- Require modification of proprietary agent binaries.
- Implicitly authorize shared workspaces or concurrent file editing.

---

## 2. Architecture

Intercom defines three principal layers.

### 2.1 Session Core

An MSP-inspired session kernel responsible for:

- Session identity and lifecycle.
- Durable command intake and settlement.
- Event sequencing and replay.
- State projection and snapshots.
- Recovery and interruption classification.
- Approval state and authorization boundaries.

The kernel is independent of individual agent harnesses.

### 2.2 Peer Communication Plane

The primary Intercom primitive, responsible for:

- Peer discovery and resolution.
- Direct instance addressing.
- Message delivery.
- Active-turn steering.
- Next-turn queueing.
- Notification-only delivery.
- Reply correlation.
- Delivery receipts.
- Authentication and access control.

This layer SHALL function independently of a central task orchestrator.

### 2.3 Interoperability Layer

Adapters translate native agent interfaces to the Intercom contract.

Initial adapter targets:

| Adapter | Integration strategy |
|---|---|
| Muse Code | Published MSP interfaces and supported native messaging surfaces |
| ACP generic | Standard ACP lifecycle plus negotiated extensions |
| Codex | Native supported agent interface or ACP bridge |
| Claude Code | Native supported agent interface or ACP bridge |
| Hermes | Native supported interface, plugin, or ACP bridge |

Adapter compatibility is subject to verification against the actual installed harness and version.

No integration SHALL rely on undocumented internal behavior as a required protocol guarantee.

---

## 3. Identity Model

Intercom distinguishes four identifiers:

| Identifier | Purpose | Lifetime |
|---|---|---|
| `principalId` | Authenticated user/service identity | Credential lifecycle |
| `hostId` | Authenticated Intercom node | Host registration lifecycle |
| `instanceId` | Particular agent process incarnation | Process lifetime |
| `sessionId` | Persistent agent conversation identity | Session lifecycle |

An agent process restart MUST create a new `instanceId` while preserving `sessionId` only when the underlying session is genuinely recoverable.

Each session MAY have human-readable aliases. Aliases MUST resolve to stable canonical identities and MUST NOT serve as authentication credentials.

A message sender identity MUST be established by the authenticated transport or trusted local adapter, never solely by a sender-controlled JSON field.

---

## 4. Peer Discovery

Intercom SHALL provide authenticated peer discovery.

### Required operations

- `peer/list`: List visible eligible peers.
- `peer/resolve`: Resolve an alias or session identifier.
- `peer/capabilities`: Retrieve effective capabilities.
- `peer/presence`: Query the last observed presence state.

Presence states:

- `online`
- `busy`
- `idle`
- `disconnected`
- `unknown`

`disconnected` describes loss of the transport or heartbeat, not necessarily agent termination.

Discovery responses SHOULD include:

- Canonical session identity.
- Human-readable alias.
- Agent harness type.
- Host identity.
- Supported delivery modes.
- Negotiated capabilities.
- Presence state.
- Last observed timestamp.

Workspace paths and other potentially sensitive metadata MUST be visibility-controlled.

Discovery MUST NOT reveal sessions outside the authenticated principal's discovery permissions.

Peer discovery MUST NOT require an orchestrator.

---

## 5. Messaging Model

Every Intercom message has a unique, durable identity.

### 5.1 Message fields

The canonical message envelope contains:

- `messageId`: Unique message identity.
- `commandId`: Idempotency identity of the submission operation.
- `from`: Authenticated sender session identity.
- `to`: Canonical recipient session identity.
- `body`: Typed message content.
- `delivery`: Requested delivery semantics.
- `replyTo`: Optional parent message identity.
- `correlationId`: Optional conversation or task correlation identity.
- `createdAt`: Trusted server timestamp.
- `expiresAt`: Optional expiry deadline.
- `metadata`: Namespaced optional metadata.

Command and message identities SHOULD use UUIDv7.

Message payloads SHALL be treated as untrusted peer-supplied data, never privileged system or user instructions.

### 5.2 Delivery modes

Intercom SHALL define three primary modes.

**STEER**

Deliver a message to the recipient's active agent turn at a supported safe execution boundary.

The recipient retains ownership of its running task. Steering does not forcibly interrupt unsafe or non-interruptible operations.

**QUEUE**

Persist the message and make it available when the recipient begins its next eligible turn.

The current turn MUST remain unaffected.

**NOTIFY**

Present or record the message without injecting its contents into the model's context.

Notifications MAY be consumed by user interfaces, monitoring systems, or agent-specific notification handlers.

### 5.3 Wake policies

A sender MAY request:

- `none`: Do not initiate or wake execution.
- `if_idle`: Attempt delivery when the recipient is idle.
- `next_safe_point`: Request an eligible safe-point wake or steer.

Wake policy is independent of delivery mode.

Unsupported combinations MUST be rejected or explicitly downgraded under a negotiated fallback policy.

An adapter MUST NOT claim active-turn steering when it can only start a new prompt.

### 5.4 Delivery receipts

Intercom SHALL distinguish:

- `accepted`: Submission durably admitted.
- `delivered`: Recipient inbox durably accepted the message.
- `presented`: Message presented by a recipient UI or notification handler.
- `injected`: Adapter confirmed introduction into the model's input context.
- `handled`: Recipient explicitly reported handling the message.
- `rejected`: Delivery or admission refused.
- `expired`: Message expired without completing requested delivery.
- `unknown`: Outcome cannot be established.

These are observable facts, not interchangeable success states. Some may occur independently; `presented` and `injected` are not necessarily sequential.

`handled` MUST NOT imply the requested task was completed correctly.

A sender MUST NOT infer delivery from submission acceptance.

### 5.5 Messaging guarantees

- Reusing an identical `commandId` with an identical request SHALL return the previously established admission outcome.
- Reusing a `commandId` with a conflicting request SHALL be rejected.
- Local durable acceptance SHALL occur before acknowledgment.
- Messages MUST retain sender identity and original delivery intent.
- A recipient MUST deduplicate a previously delivered `messageId`.
- Ordering SHALL be preserved within each ordered sender-recipient stream.
- No total ordering is guaranteed across unrelated senders.
- Expired messages SHALL NOT be newly injected into agent context.
- An unresolved delivery MUST NOT be reported as successful.

Durable inbox delivery does not guarantee model attention, task execution, or external side-effect completion.

---

## 6. Command Durability

Intercom SHALL adopt MSP-inspired command admission and settlement semantics.

### 6.1 Command lifecycle

A state-changing command transitions through:

`received → accepted → executing → settled`

Commands MAY instead be rejected at admission or become abandoned during recovery.

The command acknowledgment reports admission, not execution completion.

Every durable command record MUST include:

- `commandId`
- Authenticated principal.
- Target session.
- Operation type.
- Canonical request fingerprint.
- Admission decision.
- Associated event sequence.
- Terminal disposition, when known.

### 6.2 Crash recovery

After restart, Intercom MUST:

1. Reconstruct committed command intake records.
2. Detect admitted commands lacking settlement.
3. Recover known outcomes or classify unresolved execution conservatively.
4. Reject conflicting duplicate submissions.
5. Make recorded events available for replay.

An unresolved command MUST NOT be silently re-executed if doing so could duplicate effects.

Intercom's exactly-once guarantees apply to protocol admission and state transitions within a declared durability domain—not arbitrary shell commands, file modifications, or network requests issued by agents.

---

## 7. Event Journal & Session Recovery

Every persistent session SHALL expose a logical append-only event journal.

### 7.1 Event envelope

Each event contains:

- `eventId`
- `sessionId`
- `sequence`
- `eventType`
- `timestamp`
- `causationId`
- `payload`

Sequence numbers MUST be monotonically increasing within a session.

Clients consume events using a durable cursor.

### 7.2 Recovery operations

- `session/snapshot`: Obtain a coherent state projection.
- `session/replay`: Retrieve events after a cursor.
- `session/subscribe`: Subscribe to subsequent changes.
- `session/state`: Query current state and recovery classification.

If requested history has been compacted, the server MUST return an explicit cursor-gap indication and a safe snapshot-recovery path.

It MUST NOT silently omit missing history while claiming continuous replay.

### 7.3 Session state

Normalized session states:

- `created`
- `idle`
- `running`
- `waiting_approval`
- `interrupted`
- `ended`
- `unavailable`
- `unknown`

A host disconnect alone SHALL NOT automatically terminate the logical session.

Adapters MUST distinguish native session state from observations inferred by the adapter.

---

## 8. ACP Integration

Intercom SHALL preserve standard ACP behavior.

The initial compatibility target is ACP v1, with ACP v2 treated as a separate future compatibility profile until stabilized.

### 8.1 Extension model

Intercom SHALL use ACP's supported extension mechanisms:

- Namespaced custom capabilities.
- Underscore-prefixed extension methods.
- Namespaced `_meta` fields where the ACP schema permits them.

A collision-resistant reverse-DNS namespace controlled by the implementer MUST be selected before publishing a stable ACP extension. The provisional examples below use `_intercom/`.

### 8.2 Proposed extension methods

| Method | Function |
|---|---|
| `_intercom/hello` | Negotiate protocol features |
| `_intercom/peer/list` | Discover peers |
| `_intercom/peer/resolve` | Resolve peer identity |
| `_intercom/message/send` | Submit peer message |
| `_intercom/message/status` | Read delivery receipts |
| `_intercom/message/reply` | Send correlated reply |
| `_intercom/session/state` | Query session state |
| `_intercom/session/snapshot` | Obtain state snapshot |
| `_intercom/session/replay` | Replay event stream |
| `_intercom/session/subscribe` | Subscribe to events |
| `_intercom/event` | Send an event notification |

ACP's standard session lifecycle, updates, permissions, and cancellation methods SHALL remain authoritative within ACP sessions.

Intercom MUST NOT redefine the semantics of existing ACP methods.

### 8.3 Capability negotiation

Capabilities SHALL be advertised explicitly.

Initial capability identifiers:

- `peer.discovery`
- `peer.send`
- `peer.reply`
- `delivery.notify`
- `delivery.queue`
- `delivery.steer`
- `delivery.wake`
- `session.observe`
- `session.replay`
- `command.durable`
- `approval.owner_control`

Every capability SHOULD identify its support level and limitations.

Negotiation MUST fail safely when a required feature is unsupported.

An ACP client without Intercom support MUST remain operable as an ordinary ACP client.

---

## 9. Example Wire Exchange

The following JSON is a proposed Intercom ACP extension request, not an existing MSP method.

### Submit peer message

```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "method": "_intercom/message/send",
  "params": {
    "commandId": "019a0000-0000-7000-8000-000000000001",
    "to": {
      "sessionId": "session-b"
    },
    "delivery": {
      "mode": "steer",
      "wake": "next_safe_point",
      "onUnsupported": "reject"
    },
    "body": {
      "type": "text",
      "text": "The shared API contract changed. Review the new schema before continuing."
    },
    "ttlSeconds": 300
  }
}
```

The authenticated transport supplies the sender identity. The request MUST NOT be allowed to spoof another principal.

### Admission acknowledgment

```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "result": {
    "commandId": "019a0000-0000-7000-8000-000000000001",
    "messageId": "019a0000-0000-7000-8000-000000000002",
    "status": "accepted"
  }
}
```

This confirms durable admission, not recipient processing.

Subsequent events report delivery, injection, rejection, or other outcomes independently.

---

## 10. Transport Architecture

Intercom SHALL separate messaging semantics from physical transport.

### 10.1 Local transport

Initial required transport:

- Unix domain sockets on supported POSIX systems.
- Authenticated local process identity.
- JSON-RPC 2.0 message framing.
- Persistent message storage independent of individual agent processes.

Stdio MAY be used for adapter-host communication.

### 10.2 Remote transport

A future remote profile SHALL support authenticated communication between independent Intercom hosts.

Acceptable transport families include:

- Mutually authenticated TLS connections.
- WebSocket over TLS.
- Authenticated relay transport.

Physical relays MAY carry messages but MUST NOT become mandatory orchestration authorities.

Intercom SHALL NOT require public inbound ports or a particular VPN provider.

Remote messaging MUST be opt-in, not enabled implicitly.

### 10.3 Delivery architecture

Each host MAY run a lightweight local Intercom service responsible for peer registration, message journaling, transport, and adapter connections.

Agent A communicates with Agent B through Intercom without requiring a central agent to interpret or authorize the content.

Local services may relay messages between hosts while preserving direct logical addressing.

### 10.4 Resource bounds

The implementation MUST enforce configurable limits for:

- Maximum message size.
- Inbox and journal storage.
- Per-peer send rate.
- Concurrent connections.
- Pending messages.
- Replay batch size.
- Message retention and expiry.

Defaults SHALL be conservative. Exhaustion MUST result in explicit backpressure or rejection, not unbounded resource growth.

---

## 11. Authentication, Authorization & Trust

### 11.1 Required controls

- Verified local process identities.
- Explicit cross-host trust enrollment.
- Authenticated transport endpoints.
- Per-principal authorization policies.
- Per-recipient delivery permissions.
- Revocable peer access.
- Audit records for security-relevant transitions.

### 11.2 Authority separation

Intercom distinguishes two authorities:

**Owner authority**

Can manage its sessions and, subject to applicable policy, approve protected actions.

**Peer authority**

Can submit permitted communications and retrieve explicitly authorized status information.

Peer communication MUST NOT grant owner authority.

A peer MUST NOT use messaging to:

- Approve tool execution.
- Modify another session's permission policy.
- Forge human consent.
- Bypass sandbox restrictions.
- Impersonate system or developer instructions.
- Access secrets unavailable to its own principal.

### 11.3 Receiving-session enforcement

The receiving adapter MUST apply its local permission policy independently.

Messages injected into model context MUST retain provenance identifying them as peer-supplied content.

A compromised peer MUST NOT be able to expand the recipient's execution authority.

### 11.4 Shared resources

Intercom SHALL NOT assume that separate sessions imply isolated worktrees, filesystems, credentials, or execution environments.

Resource isolation and write ownership require separately established controls.

---

## 12. Adapter Contract

Every adapter SHALL expose:

- Harness identity and version.
- Supported lifecycle operations.
- Supported delivery modes.
- Native versus emulated capability classification.
- Recovery and durability guarantees.
- Session availability status.
- Known limitations and failure modes.

### 12.1 Native support

The underlying harness directly exposes the requested capability.

### 12.2 Emulated support

An adapter implements an equivalent, documented behavior without misrepresenting its limitations.

### 12.3 Unsupported

No safe implementation is available.

Unsupported behavior MUST NOT be silently simulated with a semantically different operation.

For example, if a harness cannot accept a message during an active turn, an adapter MUST NOT label a newly launched prompt as successful active-turn steering.

---

## 13. Storage & Operational Model

The reference implementation SHOULD initially use a small embedded transactional database, such as SQLite with WAL mode.

Minimum logical records:

- Principals
- Hosts
- Agent instances
- Sessions
- Commands
- Messages
- Delivery receipts
- Session events
- Peer permissions

The implementation MUST ensure atomic admission and consistent sequence assignment within the local durability boundary.

Remote durable delivery requires an independently persisted receiving inbox acknowledgment.

Compaction and retention MUST preserve idempotency obligations for the advertised deduplication horizon.

No distributed consensus system or external database shall be required for the initial local profile.

---

## 14. Conformance Requirements

An implementation cannot claim Intercom compliance solely because it exchanges JSON messages.

### Required protocol tests

1. Two different harness adapters discover one another.
2. An authorized peer sends and receives a notification.
3. A peer message is queued without altering the active turn.
4. Active-turn steering succeeds only when genuinely supported.
5. Unsupported steering fails explicitly.
6. A duplicate command does not cause duplicate admission.
7. Conflicting duplicate command payloads are rejected.
8. Crash-after-acceptance recovery preserves the durable record.
9. Recipient disconnect does not falsely produce a delivery receipt.
10. Session replay reconstructs the expected observable state.
11. Cursor gaps generate explicit recovery responses.
12. Unauthorized discovery and message delivery are rejected.
13. Peer messages cannot grant execution approvals.
14. Agent restart does not improperly reuse instance identity.
15. Messages from different principals retain provenance.
16. Backpressure prevents unbounded resource consumption.
17. An ordinary ACP client continues to operate without Intercom support.
18. Multiple writers are not presumed isolated merely because they have different sessions.

Tests SHALL distinguish deterministic protocol qualification from live agent interoperability.

### Compliance profiles

**IC-LOCAL**

Local authenticated peer communication and delivery receipts.

**IC-DURABLE**

IC-LOCAL plus command deduplication, durable admission, event replay, and crash recovery.

**IC-ACP**

ACP v1 extension compatibility and capability negotiation.

**IC-REMOTE**

Authenticated cross-host delivery with independent durable receipt tracking.

**IC-STEER**

Verified live-turn message injection for one or more explicitly identified harnesses.

Profiles are cumulative only where their stated prerequisites apply. Implementations MUST identify their supported profiles individually.

---

## 15. Implementation Sequence

### Phase 1 — Protocol foundation

Implement:

- Formal protocol schemas.
- Identity and capability model.
- Local transport.
- Peer registry.
- Authenticated message submission.
- Durable inbox.
- Delivery receipts.
- One generic ACP adapter.

Validate with two independent synthetic agents before integrating proprietary harnesses.

### Phase 2 — Session durability

Implement:

- Durable command ledger.
- Idempotent retry handling.
- Event journaling.
- Session snapshots.
- Cursor-based replay.
- Crash-recovery conformance tests.

### Phase 3 — Real harness interoperability

Integrate supported native interfaces and establish actual capability matrices for Muse, Codex, Claude Code, and Hermes.

Start with the most accessible two independent harnesses.

Native steering MUST be separately verified; it is not a prerequisite for notification or queueing interoperability.

### Phase 4 — Remote communication

Implement:

- Host enrollment.
- Authenticated remote transport.
- Cross-host peer resolution.
- Delivery relay and retry handling.
- Revocation and partition-recovery tests.

Avoid introducing a distributed control plane unless a demonstrated requirement demands one.

---

## 16. Initial Acceptance Criteria

The first meaningful Intercom release is acceptable when:

1. Two independently running agents from different harnesses can discover one another.
2. They can exchange authenticated messages without an orchestrator.
3. Sender and recipient identities are preserved.
4. Messages can be queued or delivered as notifications.
5. Delivery outcomes are distinguishable from acknowledgment and task completion.
6. Retrying a message after connection loss cannot create a duplicate recipient inbox entry.
7. A process restart preserves durable admitted messages and recoverable session events.
8. ACP interoperability is demonstrable through an actual client.
9. Unauthorized peers cannot obtain session access or modify execution permissions.
10. Unsupported capabilities are accurately reported rather than falsely emulated.

**Active-turn steering and remote federation are separate capabilities and may qualify after the initial interoperable core.**

---

## 17. Architectural Decision

Intercom SHALL use an **MSP-inspired durable session model as its internal consistency foundation**, an independent peer-communication plane as its principal functionality, and an ACP-compatible extension as its external interoperability surface.

Intercom will not claim wire-level MSP compatibility unless a separately implemented bridge passes the relevant MSP conformance requirements.

The defining behavior is:

**Any authorized agent can communicate with another independently running agent, across supported harness boundaries, while preserving identity, session continuity, delivery accountability, and the recipient's authority.**

Intercom shall be useful without a central orchestrator, distributed scheduler, shared model context, or single-vendor runtime.

## 18. Remediation Addendum (federation security, 2026)

IC-REMOTE requires mTLS with per-host ed25519 identity, fingerprint
pinning at enrollment, and host-qualified sender identities
("hostId/sessionId"). Bearer tokens MUST NOT traverse the wire.
Non-loopback plaintext federation MUST be rejected (TLS only by
construction). Remote admission MUST reuse atomic admission with a
full canonical fingerprint (authenticated origin, original command/
message ids, recipient, payload, mode, correlation, expiry, hops).
Duplicate StoreMessage MUST NOT append a second acceptance receipt.
Outbound transfers MUST persist the full envelope and retry with
bounded backoff, TTL, and terminal classification. Revoked hosts MUST
lose permission for new communication immediately; rotation replaces
the pinned fingerprint. Trust domains are server-assigned
("local:<hostId>"), never client-supplied security evidence. The
daemon principal is the local Unix user; local clients sharing that
user are not strongly isolated agent principals.
