-- Intercom durable store schema (ICSP-001 section 13).
--
-- All records required by the spec are represented here. Atomic admission and
-- consistent sequence assignment are provided by SQLite transactions in WAL
-- mode; no distributed consensus or external database is required for the
-- local profile.

CREATE TABLE IF NOT EXISTS principals (
    principal_id  TEXT PRIMARY KEY,
    display_name  TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hosts (
    host_id       TEXT PRIMARY KEY,
    principal_id  TEXT NOT NULL,
    display_name  TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    FOREIGN KEY (principal_id) REFERENCES principals(principal_id)
);

CREATE TABLE IF NOT EXISTS instances (
    instance_id   TEXT PRIMARY KEY,
    host_id       TEXT NOT NULL,
    session_id    TEXT NOT NULL DEFAULT '',
    harness       TEXT NOT NULL DEFAULT '',
    harness_version TEXT NOT NULL DEFAULT '',
    pid           INTEGER NOT NULL DEFAULT 0,
    started_at    TEXT NOT NULL,
    FOREIGN KEY (host_id) REFERENCES hosts(host_id)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id    TEXT PRIMARY KEY,
    instance_id   TEXT NOT NULL DEFAULT '',
    host_id       TEXT NOT NULL DEFAULT '',
    principal_id  TEXT NOT NULL DEFAULT '',
    alias         TEXT NOT NULL DEFAULT '',
    harness       TEXT NOT NULL DEFAULT '',
    harness_version TEXT NOT NULL DEFAULT '',
    state         TEXT NOT NULL,
    -- state_source records whether the state came from the harness itself
    -- or was inferred by the adapter. Section 7.3 requires adapters to
    -- distinguish native session state from adapter-inferred observations.
    state_source  TEXT NOT NULL DEFAULT 'inferred',
    presence      TEXT NOT NULL DEFAULT 'unknown',
    capabilities  TEXT NOT NULL DEFAULT '[]',
    delivery_modes TEXT NOT NULL DEFAULT '[]',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    last_seen     TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (host_id) REFERENCES hosts(host_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_host ON sessions(host_id);
CREATE INDEX IF NOT EXISTS idx_sessions_principal ON sessions(principal_id);

-- Durable command ledger (ICSP-001 section 6.1).
--
-- The request fingerprint makes idempotent retry distinguishable from a
-- conflicting reuse of the same commandId.
CREATE TABLE IF NOT EXISTS commands (
    command_id    TEXT PRIMARY KEY,
    principal_id  TEXT NOT NULL,
    target_session TEXT NOT NULL DEFAULT '',
    operation     TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    state         TEXT NOT NULL,
    message_id    TEXT NOT NULL DEFAULT '',
    event_seq     INTEGER NOT NULL DEFAULT 0,
    disposition   TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_commands_state ON commands(state);

-- Durable message store (ICSP-001 section 5).
CREATE TABLE IF NOT EXISTS messages (
    message_id    TEXT PRIMARY KEY,
    command_id    TEXT NOT NULL,
    from_session  TEXT NOT NULL,
    to_session    TEXT NOT NULL,
    from_principal TEXT NOT NULL DEFAULT '',
    body          TEXT NOT NULL,
    delivery      TEXT NOT NULL,
    reply_to      TEXT NOT NULL DEFAULT '',
    correlation_id TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    expires_at    TEXT NOT NULL DEFAULT '',
    metadata      TEXT NOT NULL DEFAULT '{}',
    inbox_state   TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY (command_id) REFERENCES commands(command_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_session);
CREATE INDEX IF NOT EXISTS idx_messages_command ON messages(command_id);

-- Delivery receipts (ICSP-001 section 5.4).
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id    TEXT NOT NULL,
    command_id    TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL,
    from_session  TEXT NOT NULL DEFAULT '',
    to_session    TEXT NOT NULL DEFAULT '',
    detail        TEXT NOT NULL DEFAULT '',
    observed_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_receipts_message ON receipts(message_id);

-- Append-only session event journal (ICSP-001 section 7).
CREATE TABLE IF NOT EXISTS session_events (
    session_id    TEXT NOT NULL,
    sequence      INTEGER NOT NULL,
    event_id      TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    timestamp     TEXT NOT NULL,
    causation_id  TEXT NOT NULL DEFAULT '',
    payload       TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (session_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_event_id ON session_events(event_id);

-- Compaction floor per session.
--
-- Records the highest sequence that has been compacted away, so a replay
-- request for a cursor below the floor can be answered with an explicit
-- cursor-gap indication instead of silently omitting history.
CREATE TABLE IF NOT EXISTS session_compaction (
    session_id    TEXT PRIMARY KEY,
    compacted_through INTEGER NOT NULL DEFAULT 0,
    compacted_at  TEXT NOT NULL
);

-- Per-peer authorization (ICSP-001 section 11).
--
-- A peer must never gain owner authority through messaging; the role column
-- encodes that distinction directly.
CREATE TABLE IF NOT EXISTS peer_permissions (
    peer_session  TEXT NOT NULL,
    owner_session TEXT NOT NULL,
    role          TEXT NOT NULL,
    granted_at    TEXT NOT NULL,
    revoked_at    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (peer_session, owner_session)
);

-- Per-recipient delivery policy (ICSP-001 section 11.1).
--
-- The receiving session enforces its own policy independently of the
-- sender's authority: a grant lets a peer submit, and this table decides
-- which delivery modes the recipient will accept from that peer.
CREATE TABLE IF NOT EXISTS delivery_policies (
    owner_session TEXT NOT NULL,
    peer_session  TEXT NOT NULL,
    allowed_modes TEXT NOT NULL DEFAULT '[]',
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (owner_session, peer_session)
);

-- Audit records for security-relevant transitions (ICSP-001 section 11.1).
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id  TEXT NOT NULL DEFAULT '',
    subject       TEXT NOT NULL DEFAULT '',
    action        TEXT NOT NULL,
    detail        TEXT NOT NULL DEFAULT '',
    recorded_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_principal ON audit_log(principal_id);
