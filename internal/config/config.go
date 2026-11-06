// Package config holds Intercom's configurable operational limits.
//
// Intercom MUST enforce configurable limits for message size, storage, send
// rate, concurrency, pending messages, replay batch size, and retention
// (ICSP-001 section 10.4). Defaults are conservative, and exhaustion results
// in explicit backpressure or rejection rather than unbounded growth.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Limits are the resource bounds enforced by a host.
type Limits struct {
	// MaxMessageBytes bounds the serialized size of one message.
	MaxMessageBytes int `json:"maxMessageBytes"`
	// MaxInboxMessages bounds undelivered messages per recipient session.
	MaxInboxMessages int `json:"maxInboxMessages"`
	// MaxPendingCommands bounds admitted-but-unsettled commands.
	MaxPendingCommands int `json:"maxPendingCommands"`
	// MaxReplayBatch bounds one replay response.
	MaxReplayBatch int `json:"maxReplayBatch"`
	// MaxConcurrentConnections bounds simultaneously served connections.
	MaxConcurrentConnections int `json:"maxConcurrentConnections"`
	// PerPeerRatePerMinute bounds messages accepted from one peer.
	PerPeerRatePerMinute int `json:"perPeerRatePerMinute"`
	// MessageRetention bounds how long messages are retained.
	MessageRetention time.Duration `json:"messageRetention"`
	// EventRetention bounds how long journal events are retained.
	EventRetention time.Duration `json:"eventRetention"`
	// DeduplicationHorizon is how long a command id stays deduplicable.
	//
	// Retention and compaction must preserve the idempotency obligations
	// for this horizon, so a message is never purged while its command
	// could still be retried (ICSP-001 section 13).
	DeduplicationHorizon time.Duration `json:"deduplicationHorizon"`
	// MaxDatabaseBytes bounds the durable store size, 0 meaning unbounded.
	MaxDatabaseBytes int64 `json:"maxDatabaseBytes"`
}

// DefaultLimits returns conservative defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxMessageBytes:          1 << 20, // 1 MiB
		MaxInboxMessages:         1000,
		MaxPendingCommands:       10000,
		MaxReplayBatch:           500,
		MaxConcurrentConnections: 64,
		PerPeerRatePerMinute:     600,
		MessageRetention:         7 * 24 * time.Hour,
		EventRetention:           30 * 24 * time.Hour,
		DeduplicationHorizon:     24 * time.Hour,
		MaxDatabaseBytes:         1 << 30, // 1 GiB
	}
}

// Config is the full host configuration.
type Config struct {
	// SocketPath is the local Unix socket the host listens on.
	SocketPath string `json:"socketPath"`
	// DataDir holds the durable store.
	DataDir string `json:"dataDir"`
	// Alias is the human-readable name for this host's sessions.
	Alias string `json:"alias"`
	// Harness identifies the adapter driving this host.
	Harness string `json:"harness"`
	// HarnessVersion identifies the adapter version.
	HarnessVersion string `json:"harnessVersion"`
	// PrincipalID is the authenticated principal this host acts as.
	PrincipalID string `json:"principalId"`
	// HostID is the stable host identity. Empty means load-or-create from
	// the durable store so identity survives daemon restarts.
	HostID string `json:"hostId,omitempty"`
	// Limits are the enforced resource bounds.
	Limits Limits `json:"limits"`
}

// Default returns a configuration with conservative defaults.
func Default() Config {
	dataDir := filepath.Join(os.TempDir(), "intercom")
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		dataDir = filepath.Join(home, ".local", "share", "intercom")
	}
	return Config{
		SocketPath: filepath.Join(os.TempDir(), "intercom", "intercom.sock"),
		DataDir:    dataDir,
		Alias:      "intercom",
		Harness:    "generic",
		Limits:     DefaultLimits(),
	}
}

// Load reads a JSON configuration file, applying defaults for absent fields.
// A missing path yields the default configuration.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("config: read: %w", err)
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("config: parse: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Save writes the configuration as JSON.
func (c Config) Save(path string) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("config: create dir: %w", err)
		}
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("config: marshal: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("config: write: %w", err)
	}
	return nil
}

// Validate checks that the configuration is internally consistent.
func (c Config) Validate() error {
	if c.SocketPath == "" {
		return errors.New("config: socketPath is required")
	}
	if c.DataDir == "" {
		return errors.New("config: dataDir is required")
	}
	l := c.Limits
	switch {
	case l.MaxMessageBytes <= 0:
		return errors.New("config: limits.maxMessageBytes must be positive")
	case l.MaxInboxMessages <= 0:
		return errors.New("config: limits.maxInboxMessages must be positive")
	case l.MaxReplayBatch <= 0:
		return errors.New("config: limits.maxReplayBatch must be positive")
	case l.MaxConcurrentConnections <= 0:
		return errors.New("config: limits.maxConcurrentConnections must be positive")
	case l.PerPeerRatePerMinute <= 0:
		return errors.New("config: limits.perPeerRatePerMinute must be positive")
	case l.MaxPendingCommands < 0:
		return errors.New("config: limits.maxPendingCommands must not be negative")
	case l.MaxDatabaseBytes < 0:
		return errors.New("config: limits.maxDatabaseBytes must not be negative")
	case l.MessageRetention < 0 || l.EventRetention < 0 || l.DeduplicationHorizon < 0:
		return errors.New("config: retention windows must not be negative")
	}
	return nil
}

// DatabasePath returns the durable store path inside the data directory.
func (c Config) DatabasePath() string {
	return filepath.Join(c.DataDir, "intercom.db")
}
