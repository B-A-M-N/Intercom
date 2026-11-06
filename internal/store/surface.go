package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// DisplayEvent is one displayable unit for a human surface. Human
// presentation and agent transport are independent fields and must not
// be collapsed into one status.
type DisplayEvent struct {
	Message protocol.Message
	ReceiptStatus protocol.ReceiptStatus // human truth (compat)
	ReceiptDetail string
	HumanStatus protocol.ReceiptStatus
	AgentStatus protocol.ReceiptStatus
	AgentDetail string
}

// presentationStatus reports human presentation truth: presented/handled
// if observed, else unknown. It never claims delivered (agent transport).
func (s *Store) presentationStatus(ctx context.Context, messageID string) (protocol.ReceiptStatus, string) {
	receipts, err := s.ReceiptsFor(ctx, messageID)
	if err != nil || len(receipts) == 0 {
		return protocol.ReceiptUnknown, ""
	}
	status := protocol.ReceiptUnknown
	detail := ""
	for _, r := range receipts {
		if r.Status == protocol.ReceiptPresented {
			status = protocol.ReceiptPresented
			detail = r.Detail
		} else if r.Status == protocol.ReceiptHandled {
			status = protocol.ReceiptHandled
			detail = r.Detail
		}
	}
	return status, detail
}

// agentStatus reports the latest agent transport outcome independently.
func (s *Store) agentStatus(ctx context.Context, messageID string) (protocol.ReceiptStatus, string) {
	receipts, err := s.ReceiptsFor(ctx, messageID)
	if err != nil || len(receipts) == 0 {
		return protocol.ReceiptUnknown, ""
	}
	status := protocol.ReceiptUnknown
	detail := ""
	for _, r := range receipts {
		switch r.Status {
		case protocol.ReceiptDelivered, protocol.ReceiptRejected, protocol.ReceiptExpired, protocol.ReceiptInjected, protocol.ReceiptAccepted:
			status = r.Status
			detail = r.Detail
		}
	}
	return status, detail
}

// RecordPresentationObservation records a human surface rendering as an
// independent observation: receipt row only, no inbox_state update, no
// command settle, no monotonic guard.
func (s *Store) RecordPresentationObservation(ctx context.Context, receipt protocol.Receipt) error {
	if receipt.Status != protocol.ReceiptPresented {
		return nil
	}
	if _, err := s.GetMessage(ctx, receipt.MessageID); err != nil {
		return err
	}
	observed := receipt.ObservedAt
	if observed.IsZero() {
		observed = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO receipts (message_id, command_id, status, from_session, to_session, detail, observed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		receipt.MessageID, receipt.CommandID, receipt.Status, receipt.From, receipt.To, receipt.Detail,
		observed.UTC().Format(TimestampLayout))
	return err
}

// SurfaceReplayResult is a journal-sequence replay with explicit gap.
type SurfaceReplayResult struct {
	Events []DisplayEvent
	Cursor uint64
	Gap bool
	EarliestAvailable uint64
	MissingMessageID string
}

// ListDisplayEventsBySequence returns displayable events using the journal
// sequence as the stable cursor. Gap is true when required history was
// compacted or a message was purged.
func (s *Store) ListDisplayEventsBySequence(ctx context.Context, sessionID string, afterSeq uint64, limit int) (SurfaceReplayResult, error) {
	if limit <= 0 {
		limit = 100
	}
	var earliest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(sequence) FROM session_events WHERE session_id = ?`, sessionID).Scan(&earliest); err != nil {
		return SurfaceReplayResult{}, fmt.Errorf("store: surface earliest: %w", err)
	}
	earliestVal := uint64(0)
	if earliest.Valid {
		earliestVal = uint64(earliest.Int64)
	}
	floor, fErr := s.compactionFloor(ctx, sessionID)
	if fErr != nil {
		return SurfaceReplayResult{}, fErr
	}
	if floor > 0 && afterSeq < floor-1 {
		return SurfaceReplayResult{Events: nil, Cursor: afterSeq, Gap: true, EarliestAvailable: earliestVal}, nil
	}
	if earliest.Valid && afterSeq != 0 && afterSeq+1 < earliestVal {
		// Cursor below retained earliest event: gap (events compacted).
		// But if afterSeq >= floor-1 and earliest is the first retained non-message event,
		// allow replay from floor-1 (message.accepted filter handles it). Only gap if the
		// next message.accepted event itself was removed, which floor check already ensures.
		// For safety when floor==0 (no compaction), any earliest gap is real.
		if floor == 0 {
			return SurfaceReplayResult{Events: nil, Cursor: afterSeq, Gap: true, EarliestAvailable: earliestVal}, nil
		}
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT sequence, payload FROM session_events WHERE session_id = ? AND event_type = ? AND sequence > ? ORDER BY sequence ASC LIMIT ?`,
		sessionID, protocol.EventMessageAccepted, afterSeq, limit)
	if err != nil {
		return SurfaceReplayResult{}, fmt.Errorf("store: surface events: %w", err)
	}
	type evRec struct {
		seq uint64
		payload string
	}
	var recs []evRec
	for rows.Next() {
		var seq uint64
		var payload string
		if err := rows.Scan(&seq, &payload); err != nil {
			rows.Close()
			return SurfaceReplayResult{}, fmt.Errorf("store: scan surface event: %w", err)
		}
		recs = append(recs, evRec{seq: seq, payload: payload})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return SurfaceReplayResult{}, err
	}
	rows.Close()
	out := make([]DisplayEvent, 0, len(recs))
	next := afterSeq
	for _, rec := range recs {
		var obj struct {
			MessageID string `json:"messageId"`
			CommandID string `json:"commandId"`
		}
		if err := json.Unmarshal([]byte(rec.payload), &obj); err != nil || obj.MessageID == "" {
			// Malformed payload: treat as gap (history not intact).
			return SurfaceReplayResult{Events: out, Cursor: next, Gap: true, EarliestAvailable: earliestVal}, nil
		}
		msg, err := s.GetMessage(ctx, obj.MessageID)
		if err != nil {
			// Message purged but journal remains: required history gone.
			return SurfaceReplayResult{Events: out, Cursor: next, Gap: true, EarliestAvailable: earliestVal, MissingMessageID: obj.MessageID}, nil
		}
		human, hdetail := s.presentationStatus(ctx, obj.MessageID)
		agent, adetail := s.agentStatus(ctx, obj.MessageID)
		out = append(out, DisplayEvent{Message: msg, ReceiptStatus: human, ReceiptDetail: hdetail, HumanStatus: human, AgentStatus: agent, AgentDetail: adetail})
		next = rec.seq
	}
	return SurfaceReplayResult{Events: out, Cursor: next, Gap: false, EarliestAvailable: earliestVal}, nil
}

// ListDisplayEvents is the legacy offset API, now stable via sequence
// under the hood for backwards compat (maps offset to sequence 0 probe).
// New callers must use ListDisplayEventsBySequence with explicit cursors.
func (s *Store) ListDisplayEvents(ctx context.Context, sessionID string, afterPosition int, limit int) ([]DisplayEvent, int, error) {
	// For compat, treat afterPosition as sequence cursor when small else fallback to sequence method.
	res, err := s.ListDisplayEventsBySequence(ctx, sessionID, uint64(afterPosition), limit)
	if err != nil {
		return nil, afterPosition, err
	}
	if res.Gap {
		return res.Events, int(res.Cursor), fmt.Errorf("store: surface cursor gap at %d", afterPosition)
	}
	return res.Events, int(res.Cursor), nil
}
