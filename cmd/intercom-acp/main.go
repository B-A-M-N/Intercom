// Command intercom-acp serves the Intercom ACP extension over stdio.
//
// This binary is a thin client-facing adapter: it connects to intercomd
// over the daemon Unix socket and forwards every stdio JSON-RPC request
// to the daemon on that single connection. It does NOT open the durable
// store and does NOT create a second Node: only intercomd owns durable
// state and delivery truth.
//
// On startup it registers (or resumes) its session on the daemon
// connection; that connection is then bound to the session in the daemon,
// so subsequent forwarded calls execute under that session's identity.
// Instance identity is minted per process, so a restart gets a new
// instance id while the session identity is preserved.
//
// The binary also works as an ordinary ACP agent for a client that knows
// nothing about Intercom: standard methods are forwarded to the daemon's
// ACP surface, which preserves their semantics (ICSP-001 section 8).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "intercom-acp: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		socketPath = flag.String("socket", "", "path to the Intercom daemon socket (required)")
		dataDir    = flag.String("data-dir", "", "deprecated: ignored; the daemon owns the store")
		session    = flag.String("session", "", "session identity this process represents")
		principal  = flag.String("principal", "", "deprecated: ignored; daemon principal is authoritative")
		alias      = flag.String("alias", "", "human-readable alias")
		harness    = flag.String("harness", "generic", "harness identity this process represents")
		version    = flag.String("harness-version", "", "harness version")
		secret     = flag.String("session-secret", "", "client-held resume secret for --session (required to resume an existing session; issued on first creation if empty)")
	)
	flag.Parse()

	if *socketPath == "" {
		return fmt.Errorf("intercom-acp: --socket is required")
	}
	if *dataDir != "" || *principal != "" {
		fmt.Fprintf(os.Stderr, "intercom-acp: warning: --data-dir/--principal are deprecated and ignored; intercomd is the sole durable core\n")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *session == "" {
		*session = mintSessionID()
	}
	if *secret == "" {
		*secret = os.Getenv("INTERCOM_SESSION_SECRET")
	}
	instanceID := mintInstanceID()

	conn, err := transport.DialUnix(ctx, *socketPath)
	if err != nil {
		return fmt.Errorf("intercom-acp: connect to daemon %s: %w (is intercomd running?)", *socketPath, err)
	}
	defer conn.Close()

	fwd := newForwarder(conn)

	// Register/resume on the daemon connection. This binds the connection
	// to the session in the daemon for all forwarded calls. The
	// capability set is the honest one for an ACP participant:
	// notify/queue via the durable inbox, steer unsupported (ACP has
	// no active-turn injection point).
	regParams, _ := json.Marshal(map[string]any{
		"sessionId":      *session,
		"instanceId":     instanceID,
		"alias":          *alias,
		"harness":        *harness,
		"harnessVersion": *version,
		"capabilities":   acpCapabilities(),
		"deliveryModes":  []string{"notify", "queue"},
		"sessionSecret":  *secret,
	})
	regResult, rpcErr := fwd.call(ctx, protocol.MethodSessionRegister, regParams)
	if rpcErr != nil {
		return fmt.Errorf("intercom-acp: register session with daemon: %v", rpcErr)
	}
	var regOut struct {
		SessionSecret string `json:"sessionSecret"`
	}
	if regResult != nil {
		_ = json.Unmarshal(regResult, &regOut)
		if regOut.SessionSecret != "" {
			fmt.Fprintf(os.Stderr, "intercom-acp: issued session secret for %s: %s (persist it; resume without it is forbidden)\n", *session, regOut.SessionSecret)
		}
	}

	// Heartbeat: the daemon marks a session disconnected after 90s of
	// silence. A live forwarded connection must report liveness.
	go heartbeatLoop(ctx, fwd)

	// Stdio loop: forward each stdio request to the daemon connection.
	reader := bufio.NewReaderSize(os.Stdin, 64<<10)
	dec := json.NewDecoder(reader)
	for {
		if ctx.Err() != nil {
			return nil
		}
		var req protocol.RPCRequest
		if err := dec.Decode(&req); err != nil {
			return nil
		}
		isNotification := len(req.ID) == 0 || string(req.ID) == "null"
		if isNotification {
			// Forward notifications to the daemon; no response is expected.
			_ = fwd.notify(ctx, req)
			continue
		}
		if _, rpcErr := fwd.callRaw(ctx, req); rpcErr != nil {
			// callRaw already wrote the daemon response to stdout.
			_ = rpcErr
		}
	}
}

// acpCapabilities is the honest capability advertisement for an ACP
// participant, mirroring intercom-mcp and the Python BaseHarness.
func acpCapabilities() []map[string]any {
	cap := func(id, support string, limitations ...string) map[string]any {
		m := map[string]any{"id": id, "support": support}
		if len(limitations) > 0 {
			m["limitations"] = limitations
		}
		return m
	}
	return []map[string]any{
		cap("peer.discovery", "native"),
		cap("peer.send", "native"),
		cap("peer.reply", "native"),
		cap("delivery.notify", "native"),
		cap("delivery.queue", "native"),
		cap("delivery.steer", "unsupported", "ACP exposes no native active-turn injection point; a submitted prompt is a next turn, not steering"),
		cap("delivery.wake", "emulated", "wake requests are honoured only while the adapter is connected"),
		cap("session.observe", "native"),
		cap("session.replay", "emulated"),
		cap("command.durable", "native"),
		cap("approval.owner_control", "native", "peer authority never includes approval authority"),
	}
}

// heartbeatLoop reports liveness at a third of the daemon's sweep
// interval. Failures are silent; the next tick retries.
func heartbeatLoop(ctx context.Context, fwd *forwarder) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, _ = fwd.call(hbCtx, protocol.MethodHeartbeat, json.RawMessage(`{}`))
			cancel()
		}
	}
}

// forwarder multiplexes stdio requests over one daemon connection.
// Responses are matched by id; daemon notifications are written straight
// to stdout.
type forwarder struct {
	conn    transport.Conn
	mu      sync.Mutex
	pending map[string]chan json.RawMessage
	wmu     sync.Mutex
	nextID  int64
}

func newForwarder(conn transport.Conn) *forwarder {
	f := &forwarder{conn: conn, pending: make(map[string]chan json.RawMessage)}
	go f.readLoop()
	return f
}

func (f *forwarder) writeStdout(b []byte) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	_, _ = os.Stdout.Write(append(b, '\n'))
}

func (f *forwarder) readLoop() {
	for {
		frame, err := readFrame(f.conn)
		if err != nil {
			return
		}
		if len(frame) == 0 {
			continue
		}
		var probe struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(frame, &probe); err != nil {
			continue
		}
		// Notification: no id, has method. Forward to stdout.
		if len(probe.ID) == 0 || string(probe.ID) == "null" {
			if probe.Method != "" {
				f.writeStdout(frame)
			}
			continue
		}
		key := string(probe.ID)
		f.mu.Lock()
		ch, ok := f.pending[key]
		f.mu.Unlock()
		if ok {
			ch <- frame
		}
		// Unmatched responses (e.g. for the register call's internal id
		// if already consumed) are dropped.
	}
}

func readFrame(conn transport.Conn) ([]byte, error) {
	type framer interface {
		ReadFrame(context.Context) ([]byte, error)
	}
	if fr, ok := conn.(framer); ok {
		return fr.ReadFrame(context.Background())
	}
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := conn.Read(one)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if one[0] == '\n' {
			return buf, nil
		}
		buf = append(buf, one[0])
	}
}

// call sends a request with a generated internal id and returns the result.
// Used only for the startup register call, before stdio forwarding begins.
func (f *forwarder) call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *protocol.RPCError) {
	id := atomic.AddInt64(&f.nextID, 1)
	req := protocol.RPCRequest{JSONRPC: "2.0", ID: json.RawMessage(strconv.FormatInt(id, 10)), Method: method, Params: params}
	frame, rpcErr := f.do(ctx, req)
	if rpcErr != nil {
		return nil, rpcErr
	}
	var resp protocol.RPCResponse
	if err := json.Unmarshal(frame, &resp); err != nil {
		return nil, protocol.Errorf(protocol.CodeParseError, "decode daemon response: %v", err)
	}
	return resp.Result, nil
}

// callRaw forwards one stdio request to the daemon and writes the daemon
// response frame verbatim to stdout (preserving the client's id).
func (f *forwarder) callRaw(ctx context.Context, req protocol.RPCRequest) (json.RawMessage, *protocol.RPCError) {
	frame, rpcErr := f.do(ctx, req)
	if frame != nil {
		f.writeStdout(frame)
	}
	return frame, rpcErr
}

func (f *forwarder) do(ctx context.Context, req protocol.RPCRequest) (json.RawMessage, *protocol.RPCError) {
	key := string(req.ID)
	ch := make(chan json.RawMessage, 1)
	f.mu.Lock()
	f.pending[key] = ch
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.pending, key)
		f.mu.Unlock()
	}()

	b, err := json.Marshal(req)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "marshal request: %v", err)
	}
	if _, err := f.conn.Write(append(b, '\n')); err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "write to daemon: %v", err)
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		return nil, protocol.Errorf(protocol.CodeInternalError, "cancelled")
	case frame := <-ch:
		var resp protocol.RPCResponse
		if err := json.Unmarshal(frame, &resp); err != nil {
			return frame, protocol.Errorf(protocol.CodeParseError, "decode daemon response: %v", err)
		}
		if resp.Error != nil {
			return frame, resp.Error
		}
		return frame, nil
	}
}

func (f *forwarder) notify(ctx context.Context, req protocol.RPCRequest) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = f.conn.Write(append(b, '\n'))
	return err
}
