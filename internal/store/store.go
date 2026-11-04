// Package store implements the durable local storage for Intercom.
//
// The reference implementation uses SQLite in WAL mode, as recommended by
// ICSP-001 section 13. All writes that must be observed together are performed
// inside a single transaction, which gives atomic admission and consistent
// sequence assignment within the local durability boundary.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a commandId is reused with a conflicting request.
var ErrConflict = errors.New("conflicting command id")

// Store is the durable local store for one Intercom host.
type Store struct {
	db *sql.DB
}

// Options configures store construction.
type Options struct {
	// Path is the SQLite database path. Use ":memory:" for an ephemeral store.
	Path string
	// BusyTimeout bounds how long a writer waits for the lock.
	BusyTimeout time.Duration
}

// Open opens or creates the durable store.
//
// The database is configured with WAL journaling and foreign key enforcement
// so that admission and sequence assignment are transactional.
func Open(ctx context.Context, opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, errors.New("store: path is required")
	}
	if opts.Path != ":memory:" && !strings.HasPrefix(opts.Path, "file:") {
		if dir := filepath.Dir(opts.Path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return nil, fmt.Errorf("store: create dir: %w", err)
			}
		}
	}

	dsn := opts.Path
	if opts.Path == ":memory:" {
		dsn = "file:intercom-mem?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}

	// SQLite tolerates only one writer; a small pool with a long busy timeout
	// turns lock contention into backpressure rather than an error.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	busy := opts.BusyTimeout
	if busy <= 0 {
		busy = 5 * time.Second
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=" + fmt.Sprint(busy.Milliseconds()),
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the underlying handle for tests and diagnostics.
func (s *Store) DB() *sql.DB { return s.db }

// inTx runs fn inside a transaction, rolling back on error or panic.
func (s *Store) inTx(ctx context.Context, fn func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func scanString(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return ""
}

// TimestampLayout is the fixed-width layout used for every stored timestamp.
//
// A fixed width matters: time.RFC3339Nano trims trailing zeros, so two
// timestamps in the same second can compare incorrectly as strings. The
// expiry query and the inbox ordering both compare stored timestamps
// lexicographically, so the stored form must sort chronologically.
const TimestampLayout = "2006-01-02T15:04:05.000000000Z07:00"

// formatTime renders a timestamp in the fixed-width storage layout.
func formatTime(t time.Time) string {
	return t.UTC().Format(TimestampLayout)
}

// parseTime decodes a stored timestamp.
//
// Timestamps are persisted as RFC3339Nano text so that the stored form is
// stable and comparable as a string, which the expiry and retention queries
// rely on.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{TimestampLayout, time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("store: unparsable timestamp %q", s)
}

// scanTime is a scan target for a stored timestamp column.
type scanTime struct {
	t time.Time
}

// Scan implements sql.Scanner.
func (s *scanTime) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		s.t = time.Time{}
		return nil
	case string:
		t, err := parseTime(v)
		if err != nil {
			return err
		}
		s.t = t
		return nil
	case []byte:
		t, err := parseTime(string(v))
		if err != nil {
			return err
		}
		s.t = t
		return nil
	case int64:
		s.t = time.Unix(v, 0).UTC()
		return nil
	default:
		return fmt.Errorf("store: cannot scan %T into a timestamp", src)
	}
}

// Time returns the decoded timestamp.
func (s *scanTime) Time() time.Time { return s.t }
