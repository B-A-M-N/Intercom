package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bamn/intercom/internal/protocol"
)

// AppendEvent appends an event to a session's journal and assigns its
// sequence number.
//
// Sequence numbers are monotonically increasing within a session. The
// assignment happens inside the same transaction as the insert, so concurrent
// writers to one session cannot produce a duplicate or out-of-order sequence.
func (s *Store) AppendEvent(ctx context.Context, ev protocol.EventEnvelope, causationID string) (protocol.EventEnvelope, error) {
	var out protocol.EventEnvelope

	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var maxSeq sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			`SELECT MAX(sequence) FROM session_events WHERE session_id = ?`,
			ev.SessionID).Scan(&maxSeq); err != nil {
			return fmt.Errorf("store: max sequence: %w", err)
		}
		seq := uint64(1)
		if maxSeq.Valid && maxSeq.Int64 > 0 {
			seq = uint64(maxSeq.Int64) + 1
		}
		ev.Sequence = seq
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now().UTC()
		}
		if ev.CausationID == "" {
			ev.CausationID = causationID
		}

		payload := ev.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO session_events (session_id, sequence, event_id, event_type, timestamp, causation_id, payload)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			ev.SessionID, ev.Sequence, ev.EventID, ev.EventType,
			ev.Timestamp.UTC().Format(TimestampLayout), ev.CausationID, string(payload))
		if err != nil {
			return fmt.Errorf("store: insert event: %w", err)
		}
		out = ev
		return nil
	})
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	return out, nil
}

// ReplayResult is the outcome of a replay request (ICSP-001 section 7.2).
//
// When history was compacted, Gap is set and the caller must recover through
// a snapshot rather than assuming continuous replay.
type ReplayResult struct {
	Events []protocol.EventEnvelope
	// Cursor is the sequence of the last returned event.
	Cursor uint64
	// Gap is true when requested history was compacted away.
	Gap bool
	// EarliestAvailable is the lowest sequence still retained.
	EarliestAvailable uint64
	// SnapshotPath describes how to recover coherently when Gap is true.
	SnapshotPath string
}

// Replay returns events for a session after the given cursor.
//
// If the requested cursor is older than the retained history, the result
// carries an explicit gap indication rather than silently omitting events.
func (s *Store) Replay(ctx context.Context, sessionID string, afterCursor uint64, limit int) (ReplayResult, error) {
	if limit <= 0 {
		limit = 100
	}

	var earliest sql.NullInt64
	if err := s.db.QueryRowContext(ctx,
		`SELECT MIN(sequence) FROM session_events WHERE session_id = ?`, sessionID).Scan(&earliest); err != nil {
		return ReplayResult{}, fmt.Errorf("store: min sequence: %w", err)
	}

	res := ReplayResult{EarliestAvailable: 0}
	if earliest.Valid {
		res.EarliestAvailable = uint64(earliest.Int64)
	}

	// A cursor at or below the compaction floor means the requested history
	// has been compacted away.
	floor, err := s.compactionFloor(ctx, sessionID)
	if err != nil {
		return ReplayResult{}, err
	}
	if floor > 0 && afterCursor < floor {
		res.Gap = true
		res.SnapshotPath = protocol.MethodSessionSnapshot
	}
	if res.EarliestAvailable > 0 && afterCursor+1 < res.EarliestAvailable {
		res.Gap = true
		res.SnapshotPath = protocol.MethodSessionSnapshot
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, sequence, event_id, event_type, timestamp, causation_id, payload
		 FROM session_events WHERE session_id = ? AND sequence > ?
		 ORDER BY sequence ASC LIMIT ?`,
		sessionID, afterCursor, limit)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("store: query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ev protocol.EventEnvelope
		var timestamp, payload string
		var ts scanTime
		if err := rows.Scan(&ev.SessionID, &ev.Sequence, &ev.EventID, &ev.EventType,
			&ts, &ev.CausationID, &payload); err != nil {
			return ReplayResult{}, fmt.Errorf("store: scan event: %w", err)
		}
		_ = timestamp
		ev.Timestamp = ts.Time()
		ev.Payload = json.RawMessage(payload)
		res.Events = append(res.Events, ev)
		res.Cursor = ev.Sequence
	}
	return res, rows.Err()
}

// LatestSequence returns the highest assigned sequence for a session.
func (s *Store) LatestSequence(ctx context.Context, sessionID string) (uint64, error) {
	var maxSeq sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT MAX(sequence) FROM session_events WHERE session_id = ?`, sessionID).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("store: latest sequence: %w", err)
	}
	if !maxSeq.Valid {
		return 0, nil
	}
	return uint64(maxSeq.Int64), nil
}

// CompactEvents drops events older than the given sequence, preserving the
// idempotency obligations for the advertised deduplication horizon.
//
// Messages and commands are not removed: compaction only affects the event
// journal, so a compacted session recovers through a snapshot.
func (s *Store) CompactEvents(ctx context.Context, sessionID string, before uint64) (int64, error) {
	var removed int64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM session_events WHERE session_id = ? AND sequence < ?`, sessionID, before)
		if err != nil {
			return fmt.Errorf("store: compact events: %w", err)
		}
		removed, _ = res.RowsAffected()

		// Record the compaction floor so a later replay can report a cursor
		// gap rather than silently returning a short history.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO session_compaction (session_id, compacted_through, compacted_at)
			 VALUES (?, ?, ?)
			 ON CONFLICT(session_id) DO UPDATE SET
			   compacted_through = MAX(compacted_through, excluded.compacted_through),
			   compacted_at = excluded.compacted_at`,
			sessionID, before, time.Now().UTC().Format(TimestampLayout))
		if err != nil {
			return fmt.Errorf("store: record compaction floor: %w", err)
		}
		return nil
	})
	return removed, err
}

// compactionFloor returns the highest sequence compacted away for a session.
func (s *Store) compactionFloor(ctx context.Context, sessionID string) (uint64, error) {
	var floor uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT compacted_through FROM session_compaction WHERE session_id = ?`, sessionID).Scan(&floor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: compaction floor: %w", err)
	}
	return floor, nil
}

// AppendEventTx appends an event inside an existing transaction, so that an
// event and the state change it describes commit atomically.
func (s *Store) AppendEventTx(ctx context.Context, tx *sql.Tx, ev protocol.EventEnvelope, causationID string) (protocol.EventEnvelope, error) {
	var maxSeq sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MAX(sequence) FROM session_events WHERE session_id = ?`, ev.SessionID).Scan(&maxSeq); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("store: max sequence: %w", err)
	}
	seq := uint64(1)
	if maxSeq.Valid && maxSeq.Int64 > 0 {
		seq = uint64(maxSeq.Int64) + 1
	}
	ev.Sequence = seq
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.CausationID == "" {
		ev.CausationID = causationID
	}
	payload := ev.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO session_events (session_id, sequence, event_id, event_type, timestamp, causation_id, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ev.SessionID, ev.Sequence, ev.EventID, ev.EventType,
		ev.Timestamp.UTC().Format(TimestampLayout), ev.CausationID, string(payload))
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("store: insert event: %w", err)
	}
	return ev, nil
}

// EventExists reports whether an event id is already present. Event ids are
// globally unique so a replayed write cannot duplicate a journal entry.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM session_events WHERE event_id = ?`, eventID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: event exists: %w", err)
	}
	return true, nil
}
