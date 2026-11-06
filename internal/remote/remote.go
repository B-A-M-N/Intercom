// Package remote implements IC-REMOTE over mutually authenticated TLS:
// each host presents a self-signed certificate with SAN = hostId and an
// ed25519 identity; enrollment pins the public-key fingerprint. There is
// no plaintext bearer token on the wire. Remote ack = durable inbox
// admission only, never injection/handling.
package remote

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	MaxFrameBytes = 64 << 10
	MaxBodyBytes  = 32 << 10
	MaxConcurrent = 32
)

// Envelope is the remote send payload.
type Envelope struct {
	OriginHost        string `json:"originHost"`
	OriginFingerprint string `json:"originFingerprint"`
	FromPrincipal     string `json:"fromPrincipal"`
	FromSession       string `json:"fromSession"`
	OriginalMessageID string `json:"originalMessageId"`
	OriginalCommandID string `json:"originalCommandId"`
	TargetSession     string `json:"targetSession"`
	Body              struct {
		Type string          `json:"type"`
		Text string          `json:"text,omitempty"`
		Data json.RawMessage `json:"data,omitempty"`
	} `json:"body"`
	Delivery struct {
		Mode string `json:"mode"`
	} `json:"delivery"`
	CorrelationID string `json:"correlationId,omitempty"`
	ExpiresAt     string `json:"expiresAt,omitempty"`
	Hops          int    `json:"hops,omitempty"`
}

// AdmitFunc admits an envelope; peerHost/peerFP are TLS-authenticated.
type AdmitFunc func(ctx context.Context, env Envelope, peerHost, peerFP string) (string, error)

// Server is a TLS remote endpoint (mutual TLS).
type Server struct {
	addr     string
	identity *Identity
	admit    AdmitFunc
	listener net.Listener
	mu       sync.Mutex
	sem      chan struct{}
}

func NewServer(addr string, identity *Identity, admit AdmitFunc) *Server {
	return &Server{addr: addr, identity: identity, admit: admit, sem: make(chan struct{}, MaxConcurrent)}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	s.mu.Lock()
	wantAddr := s.addr
	s.mu.Unlock()
	cfg, err := serverTLSConfig(s.identity)
	if err != nil {
		return err
	}
	cfg.ClientAuth = tls.RequireAnyClientCert
	plain, err := net.Listen("tcp", wantAddr)
	if err != nil {
		return err
	}
	ln := tls.NewListener(plain, cfg)
	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr().String()
	s.mu.Unlock()
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case s.sem <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		tlsConn := conn.(*tls.Conn)
		go func(c *tls.Conn) {
			defer func() { <-s.sem }()
			s.handle(ctx, c)
		}(tlsConn)
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Server) handle(ctx context.Context, conn *tls.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.HandshakeContext(ctx); err != nil {
		return
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return
	}
	// Enforce the client certificate validity window on the server side too.
	now := time.Now()
	if now.Before(state.PeerCertificates[0].NotBefore) || now.After(state.PeerCertificates[0].NotAfter) {
		return
	}
	peerFP := fingerprintFromCert(state.PeerCertificates[0])
	if peerFP == "" || len(state.PeerCertificates[0].DNSNames) == 0 {
		return
	}
	peerHost := state.PeerCertificates[0].DNSNames[0]
	reader := bufio.NewReader(io.LimitReader(conn, MaxFrameBytes+1))
	line, err := reader.ReadBytes('\n')
	if err != nil || len(line) > MaxFrameBytes {
		return
	}
	var probe struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(line, &probe)
	if probe.Kind == "enroll" {
		_ = writeLine(conn, map[string]any{"ok": true, "hostId": s.identity.HostID, "fingerprint": s.identity.Fingerprint})
		return
	}
	var env Envelope
	if err := json.Unmarshal(line, &env); err != nil {
		_ = writeLine(conn, map[string]any{"ok": false, "error": "bad envelope"})
		return
	}
	if err := ValidateEnvelope(env); err != nil {
		_ = writeLine(conn, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	// Authoritative origin is the TLS peer, never client-supplied.
	env.OriginHost = peerHost
	env.OriginFingerprint = peerFP
	localID, err := s.admit(ctx, env, peerHost, peerFP)
	if err != nil {
		_ = writeLine(conn, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	_ = writeLine(conn, map[string]any{"ok": true, "messageId": localID})
}

func writeLine(conn net.Conn, v any) error {
	b, _ := json.Marshal(v)
	_, err := conn.Write(append(b, '\n'))
	return err
}

func readLine(conn net.Conn) ([]byte, error) {
	reader := bufio.NewReader(io.LimitReader(conn, MaxFrameBytes+1))
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) > MaxFrameBytes {
		return nil, errors.New("remote: response frame too large")
	}
	return line, nil
}

// Client sends over mTLS with pinned destination identity.
func Client(ctx context.Context, endpoint string, sender *Identity, expectedHostID, expectedFingerprint string, env Envelope) (string, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return "", fmt.Errorf("remote dial %s: %w", endpoint, err)
	}
	conn := tls.Client(raw, clientTLSConfig(expectedHostID, expectedFingerprint, sender))
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.HandshakeContext(ctx); err != nil {
		return "", fmt.Errorf("remote TLS handshake: %w", err)
	}
	env.OriginHost = sender.HostID
	env.OriginFingerprint = sender.Fingerprint
	b, _ := json.Marshal(env)
	if _, err := conn.Write(append(b, '\n')); err != nil {
		return "", fmt.Errorf("remote write: %w", err)
	}
	line, err := readLine(conn)
	if err != nil {
		return "", fmt.Errorf("remote read ack: %w", err)
	}
	var resp struct {
		OK        bool   `json:"ok"`
		MessageID string `json:"messageId"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return "", fmt.Errorf("remote decode ack: %w", err)
	}
	if !resp.OK {
		return "", fmt.Errorf("remote rejected: %s", resp.Error)
	}
	if resp.MessageID == "" {
		return "", fmt.Errorf("remote malformed ack: missing messageId")
	}
	return resp.MessageID, nil
}

// Enroll strictly verifies a host's identity over mTLS.
func Enroll(ctx context.Context, endpoint string, caller *Identity, expectedHostID, expectedFingerprint string) (string, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return "", err
	}
	conn := tls.Client(raw, clientTLSConfig(expectedHostID, expectedFingerprint, caller))
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.HandshakeContext(ctx); err != nil {
		return "", fmt.Errorf("remote enroll handshake: %w", err)
	}
	probe, _ := json.Marshal(map[string]any{"kind": "enroll"})
	if _, err := conn.Write(append(probe, '\n')); err != nil {
		return "", err
	}
	line, err := readLine(conn)
	if err != nil {
		return "", fmt.Errorf("remote enroll read: %w", err)
	}
	var resp struct {
		OK          bool   `json:"ok"`
		HostID      string `json:"hostId"`
		Fingerprint string `json:"fingerprint"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return "", fmt.Errorf("remote enroll malformed response: %w", err)
	}
	if !resp.OK {
		return "", fmt.Errorf("remote enroll rejected: %s", resp.Error)
	}
	if resp.HostID != expectedHostID {
		return "", fmt.Errorf("remote enroll host identity mismatch: got %q want %q", resp.HostID, expectedHostID)
	}
	if resp.Fingerprint != expectedFingerprint {
		return "", fmt.Errorf("remote enroll fingerprint mismatch")
	}
	return resp.Fingerprint, nil
}

// ValidateEnvelope enforces wire limits before persistence.
func ValidateEnvelope(env Envelope) error {
	if len(env.TargetSession) == 0 || env.FromSession == "" {
		return errors.New("missing target/from")
	}
	if len(env.TargetSession) > 256 || len(env.FromSession) > 256 {
		return errors.New("identity too long")
	}
	if len(env.Body.Text) > MaxBodyBytes || len(env.Body.Data) > MaxBodyBytes {
		return errors.New("body too large")
	}
	switch env.Delivery.Mode {
	case "", "notify", "queue", "steer":
	default:
		return errors.New("invalid delivery mode")
	}
	if env.Hops < 0 || env.Hops > 5 {
		return errors.New("invalid hop count")
	}
	if env.ExpiresAt != "" {
		exp, err := time.Parse(time.RFC3339, env.ExpiresAt)
		if err != nil {
			return errors.New("invalid expiry")
		}
		if exp.Before(time.Now().UTC()) {
			return errors.New("message expired")
		}
		if exp.After(time.Now().UTC().Add(24 * time.Hour)) {
			return errors.New("TTL exceeds 24h")
		}
	}
	return nil
}
