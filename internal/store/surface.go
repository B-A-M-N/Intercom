package store

import (
	"context"
	"fmt"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

// DisplayEvent is one displayable unit for a human surface.
type DisplayEvent struct {
	Message protocol.Message
	ReceiptStatus protocol.ReceiptStatus
	ReceiptDetail string
}

// ListDisplayEvents returns messages addressed to sessionID in creation
// order without mutating inbox state. Pure SELECT, never a claim, so a
// human UI cannot claim work from an agent inbox.
func (s *Store) ListDisplayEvents(ctx context.Context, sessionID string, afterPosition int, limit int) ([]DisplayEvent, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if afterPosition < 0 {
		afterPosition = 0
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id FROM messages WHERE to_session = ? ORDER BY created_at ASC, message_id ASC LIMIT ? OFFSET ?`,
		sessionID, limit, afterPosition)
	if err != nil {
		return nil, afterPosition, fmt.Errorf("store: list display: %w", err)
	}
	var mids []string
	for rows.Next() {
		var mid string
		if err := rows.Scan(&mid); err != nil {
			rows.Close()
			return nil, afterPosition, fmt.Errorf("store: scan display: %w", err)
		}
		mids = append(mids, mid)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, afterPosition, err
	}
	rows.Close()
	out := make([]DisplayEvent, 0, len(mids))
	pos := afterPosition
	for _, mid := range mids {
		msg, err := s.GetMessage(ctx, mid)
		if err != nil {
			continue
		}
		status := protocol.ReceiptUnknown
		detail := ""
		receipts, rerr := s.ReceiptsFor(ctx, mid)
		if rerr == nil && len(receipts) > 0 {
			for _, r := range receipts {
				if r.Status == protocol.ReceiptPresented || r.Status == protocol.ReceiptHandled || r.Status == protocol.ReceiptDelivered || r.Status == protocol.ReceiptInjected {
					status = r.Status
					detail = r.Detail
				}
			}
		} else {
			status = protocol.ReceiptDelivered
		}
		out = append(out, DisplayEvent{Message: msg, ReceiptStatus: status, ReceiptDetail: detail})
		pos++
	}
	return out, pos, nil
}
