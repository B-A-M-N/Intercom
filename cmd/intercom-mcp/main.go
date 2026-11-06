// Command intercom-mcp serves Intercom as MCP tools over stdio.
//
// It is a thin client of intercomd: one daemon socket connection, session
// registered (bound) on that connection at startup, every tools/call
// forwarded to the daemon under that bound identity and policy. It never
// opens the durable store. MCP itself does not deliver agent-to-agent
// messages; Intercom remains responsible - these tools are the access
// surface for harnesses that can consume MCP tools (including Codex and
// Claude Code via their MCP configuration) without protocol changes.
//
// --auto-start optionally starts intercomd if the socket is absent
// (opt-in bootstrap). Without it, a missing daemon is an explicit error.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "intercom-mcp: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		socketPath = flag.String("socket", "", "path to the Intercom daemon socket (required)")
		session    = flag.String("session", "", "session identity this process represents")
		alias      = flag.String("alias", "", "human-readable alias")
		harness    = flag.String("harness", "mcp", "harness identity")
		autoStart  = flag.Bool("auto-start", false, "start intercomd if the daemon socket is absent (opt-in bootstrap)")
		dataDir    = flag.String("data-dir", "", "data dir for auto-started daemon")
		daemonBin  = flag.String("daemon", "", "intercomd binary for auto-start (default: intercomd on PATH or next to this binary)")
		secret     = flag.String("session-secret", "", "client-held resume secret for --session (required to resume an existing session; issued on first creation if empty)")
		attach     = flag.Bool("attach", false, "attach as a delegated client instead of registering as owner (requires --delegation-token)")
		delegation = flag.String("delegation-token", "", "scoped delegation token issued by the session owner (required with --attach)")
		secretFile = flag.String("session-secret-file", "", "protected file (0600) to read/write the session secret; preferred over --session-secret")
		delegationFile = flag.String("delegation-token-file", "", "protected file (0600) containing the scoped delegation token; preferred over --delegation-token")
	)
	flag.Parse()
	if *socketPath == "" {
		return fmt.Errorf("intercom-mcp: --socket is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var daemonCmd *exec.Cmd
	if _, err := os.Stat(*socketPath); err != nil {
		if !*autoStart {
			return fmt.Errorf("intercom-mcp: daemon socket %s not found (start intercomd, or pass --auto-start): %w", *socketPath, err)
		}
		bin := *daemonBin
		if bin == "" {
			if self, err := os.Executable(); err == nil {
				candidate := filepath.Join(filepath.Dir(self), "intercomd")
				if _, err := os.Stat(candidate); err == nil {
					bin = candidate
				} else {
					bin = "intercomd"
				}
			} else {
				bin = "intercomd"
			}
		}
		dd := *dataDir
		if dd == "" {
			dd = filepath.Join(filepath.Dir(*socketPath), "data")
		}
		daemonCmd = exec.Command(bin, "--socket", *socketPath, "--data-dir", dd, "--alias", "mcp-bootstrap")
		daemonCmd.Stdout = os.Stderr
		daemonCmd.Stderr = os.Stderr
		if err := daemonCmd.Start(); err != nil {
			return fmt.Errorf("intercom-mcp: auto-start intercomd: %w", err)
		}
		defer func() {
			_ = daemonCmd.Process.Kill()
			_ = daemonCmd.Wait()
		}()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(*socketPath); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if _, err := os.Stat(*socketPath); err != nil {
			return fmt.Errorf("intercom-mcp: daemon did not create socket %s", *socketPath)
		}
	}

	if *attach {
		if *session == "" {
			return fmt.Errorf("intercom-mcp: --session is required with --attach")
		}
		if *delegationFile != "" {
			b, rerr := os.ReadFile(*delegationFile)
			if rerr != nil {
				return fmt.Errorf("intercom-mcp: read delegation token file: %w", rerr)
			}
			*delegation = strings.TrimSpace(string(b))
		} else if *delegation == "" {
			*delegation = os.Getenv("INTERCOM_MCP_DELEGATION_TOKEN")
		}
		if *delegation == "" {
			return fmt.Errorf("intercom-mcp: --delegation-token-file (or --delegation-token) is required with --attach")
		}
	} else {
		if *session == "" {
			*session = mintSessionID()
		}
		if *secret == "" {
			*secret = os.Getenv("INTERCOM_SESSION_SECRET")
		}
		if *secretFile != "" && *secret == "" {
			if b, rerr := os.ReadFile(*secretFile); rerr == nil {
				*secret = strings.TrimSpace(string(b))
			}
		}
	}
	conn, err := transport.DialUnix(ctx, *socketPath)
	if err != nil {
		return fmt.Errorf("intercom-mcp: connect to daemon: %w", err)
	}
	defer conn.Close()

	fwd := newForwarder(conn)
	if *attach {
		// Attached client: verify the scoped delegation token. This path
		// MUST NOT call session/register, or it would replace the owner's
		// instance identity and advertised delivery capabilities.
		attParams, _ := json.Marshal(map[string]any{"sessionId": *session, "delegationToken": *delegation})
		if _, rpcErr := fwd.call(ctx, protocol.MethodSessionAttach, attParams); rpcErr != nil {
			return fmt.Errorf("intercom-mcp: attach with daemon: %v", rpcErr)
		}
		fwd.session = *session
	} else {
		// Register the honest MCP capability set. An MCP peer can discover,
		// send, reply, and claim durable-inbox messages (notify/queue), but it
		// has no active-turn injection point: steer is unsupported, with the
		// limitation stated, exactly as the Python BaseHarness builds it.
		regParams, _ := json.Marshal(map[string]any{
			"sessionId": *session, "instanceId": mintInstanceID(), "alias": *alias,
			"harness": *harness, "capabilities": mcpCapabilities(),
			"deliveryModes": []string{"notify", "queue"},
			"sessionSecret": *secret,
		})
		regResult, rpcErr := fwd.call(ctx, protocol.MethodSessionRegister, regParams)
		if rpcErr != nil {
			return fmt.Errorf("intercom-mcp: register with daemon: %v", rpcErr)
		}
		var regOut struct {
			SessionSecret string `json:"sessionSecret"`
		}
		if regResult != nil {
			_ = json.Unmarshal(regResult, &regOut)
			if regOut.SessionSecret != "" {
				// Persist the issued secret via a protected file when
				// requested; never print it to logs.
				dest := *secretFile
				if dest == "" {
					dest = filepath.Join(os.TempDir(), "intercom-session-secret-"+*session+".txt")
				}
				if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
					return fmt.Errorf("intercom-mcp: write session secret file: %w", err)
				}
				if err := os.WriteFile(dest, []byte(regOut.SessionSecret+"\n"), 0o600); err != nil {
					return fmt.Errorf("intercom-mcp: write session secret file: %w", err)
				}
				fmt.Fprintf(os.Stderr, "intercom-mcp: issued session secret for %s persisted to %s (resume requires it; secret is not printed)\n", *session, dest)
			}
		}
		fwd.session = *session
	}

	// Heartbeat: the daemon sweeps presence every 30s and marks a
	// session disconnected after 90s of silence. A live MCP process
	// must report liveness well inside that window, or every peer would
	// see it as disconnected while it is still running.
	go heartbeatLoop(ctx, fwd)

	srv := &mcpServer{fwd: fwd}
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
		if len(req.ID) == 0 || string(req.ID) == "null" {
			// MCP notifications (e.g. notifications/initialized): acknowledge silently.
			continue
		}
		// Handle each request on its own goroutine: a harness may issue
		// parallel tool calls, and serialising them behind one slow daemon
		// round trip would stall the turn. Responses are matched by the
		// request id the client supplied, and writes are serialised.
		go func(req protocol.RPCRequest) {
			result, rpcErr := srv.handle(ctx, req.Method, req.Params)
			var resp protocol.RPCResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID
			if rpcErr != nil {
				resp.Error = rpcErr
			} else {
				b, _ := json.Marshal(result)
				resp.Result = b
			}
			b, _ := json.Marshal(resp)
			srv.wmu.Lock()
			_, _ = os.Stdout.Write(append(b, '\n'))
			srv.wmu.Unlock()
		}(req)
	}
}

type mcpServer struct {
	fwd *forwarder
	wmu sync.Mutex
}

func (s *mcpServer) handle(ctx context.Context, method string, params json.RawMessage) (any, *protocol.RPCError) {
	switch method {
	case "ping":
		// MCP clients keep the connection alive with ping; it is not an
		// Intercom operation and must not reach the daemon.
		return map[string]any{}, nil
	case "initialize":
		return map[string]any{
			"protocolVersion": negotiateMCPVersion(params),
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "intercom-mcp", "version": "0.2.0"},
		}, nil
	case "tools/list":
		return map[string]any{"tools": toolDefs()}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "invalid tools/call params: %v", err)
		}
		return s.callTool(ctx, p.Name, p.Arguments)
	default:
		return nil, protocol.Errorf(protocol.CodeMethodNotFound, "method %q not found", method)
	}
}

// supportedMCPVersions are the MCP protocol versions this server speaks.
// negotiateMCPVersion echoes the client's requested version when it is in
// this set, and otherwise falls back to the newest supported version, so a
// client that inventories tools/tools/call under a newer (or older)
// negotiation is not defeated by a hard-coded version string.
var supportedMCPVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

func negotiateMCPVersion(params json.RawMessage) string {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(params, &p); err == nil {
		for _, v := range supportedMCPVersions {
			if p.ProtocolVersion == v {
				return v
			}
		}
	}
	return supportedMCPVersions[0]
}

// mcpCapabilities is the honest capability advertisement for an MCP peer.
// It mirrors the Python BaseHarness construction for modes (notify, queue):
// steer and wake are not native, and the limitations say why.
func mcpCapabilities() []map[string]any {
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
		cap("delivery.steer", "unsupported", "the harness exposes no native active-turn injection point; MCP inbox claims are a next-turn channel, not steering"),
		cap("delivery.wake", "emulated", "wake requests are honoured only while the adapter is connected"),
		cap("session.observe", "native"),
		cap("session.replay", "emulated"),
		cap("command.durable", "native"),
		cap("approval.owner_control", "native", "peer authority never includes approval authority"),
	}
}

func toolDefs() []map[string]any {
	str := map[string]any{"type": "string"}
	obj := func(extra map[string]any, required ...string) map[string]any {
		m := map[string]any{
			"$schema":              "https://json-schema.org/draft/2020-12/schema",
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		}
		for k, v := range extra {
			m["properties"].(map[string]any)[k] = v
		}
		if len(required) > 0 {
			req := make([]any, len(required))
			for i, r := range required {
				req[i] = r
			}
			m["required"] = req
		}
		return m
	}
	mk := func(name, desc string, schema map[string]any) map[string]any {
		return map[string]any{"name": name, "description": desc, "inputSchema": schema}
	}
	return []map[string]any{
		mk("peers_list", "List peers visible to this session under Intercom policy. Intercom remains responsible for delivery; this only discovers.", obj(nil)),
		mk("peer_route", "Deterministic route explanation for a target or selector (harness/capability).", obj(map[string]any{"target": str, "harness": str, "capability": str})),
		mk("peer_capabilities", "Effective capabilities of a peer session.", obj(map[string]any{"sessionId": str}, "sessionId")),
		mk("message_send", "Send a message to a session (durable admission, not delivery). commandId is the idempotency key and is preserved across retries.", obj(map[string]any{"commandId": str, "to": str, "text": str, "mode": str}, "commandId", "to", "text")),
		mk("message_reply", "Reply to a message by id with correlation preserved. commandId is the idempotency key and is preserved across retries.", obj(map[string]any{"commandId": str, "messageId": str, "text": str}, "commandId", "messageId", "text")),
		mk("inbox_list", "Claim pending inbox messages for this bound session (exclusive lease). Claim is not acknowledgement: follow up with message_ack once the message has been read and acted on.", obj(nil)),
		mk("message_ack", "Report the recipient-observed outcome for a claimed message: presented (shown to the agent), handled (acted on), or rejected (refused). claimToken/claimGeneration are the lease fencing values returned by inbox_list. Injected is not available here: an MCP inbox claim never proves model-context injection.", obj(map[string]any{"messageId": str, "status": str, "detail": str, "claimToken": str, "claimGeneration": map[string]any{"type": "integer"}}, "messageId", "status")),
		mk("message_status", "Receipt trail for a message (accepted/presented/injected/handled distinct).", obj(map[string]any{"messageId": str}, "messageId")),
		mk("conversation_history", "Readable history of a conversation this session participates in.", obj(map[string]any{"conversationId": str}, "conversationId")),
	}
}

// callTool forwards one MCP tool call to the daemon extension methods.
func (s *mcpServer) callTool(ctx context.Context, name string, args json.RawMessage) (any, *protocol.RPCError) {
	// Accept Muse-style namespacing (mcp__intercom.message_send / mcp__intercom__message_send).
	// Normalize: take substring after last '.' or last '__'.
	if d := strings.LastIndex(name, "."); d >= 0 {
		name = name[d+1:]
	}
	if d := strings.LastIndex(name, "__"); d >= 0 {
		name = name[d+2:]
	}
	var a map[string]any
	_ = json.Unmarshal(args, &a)
	if a == nil {
		a = map[string]any{}
	}
	get := func(k string) string {
		if v, ok := a[k].(string); ok {
			return v
		}
		return ""
	}
	var method string
	var params map[string]any
	switch name {
	case "peers_list":
		method, params = protocol.MethodPeerList, map[string]any{}
	case "peer_route":
		method, params = protocol.MethodPeerRoute, map[string]any{"target": get("target"), "harness": get("harness"), "capability": get("capability")}
	case "peer_capabilities":
		method, params = protocol.MethodPeerCapabilities, map[string]any{"sessionId": get("sessionId")}
	case "message_send":
		if get("commandId") == "" {
			return map[string]any{"content": []any{map[string]any{"type": "text", "text": "message_send requires commandId (idempotency key)"}}, "isError": true}, nil
		}
		mode := get("mode")
		if mode == "" {
			mode = "notify"
		}
		method = protocol.MethodMessageSend
		params = map[string]any{
			"commandId": get("commandId"), "to": map[string]any{"sessionId": get("to")},
			"body":     map[string]any{"type": "text", "text": get("text")},
			"delivery": map[string]any{"mode": mode, "wake": "none", "onUnsupported": "reject"},
		}
	case "message_reply":
		if get("commandId") == "" {
			return map[string]any{"content": []any{map[string]any{"type": "text", "text": "message_reply requires commandId (idempotency key)"}}, "isError": true}, nil
		}
		method, params = protocol.MethodMessageReply, map[string]any{"commandId": get("commandId"), "messageId": get("messageId"), "text": get("text")}
	case "inbox_list":
		// Session is bound in the daemon; listPending with the bound session
		// is resolved by the daemon from the connection. We pass the
		// registered session via a sentinel: the daemon ignores the param
		// session for ownership (it checks bound session), so pass empty
		// and let the daemon use the bound session. To keep wire compat,
		// send the bound session id known from startup via env-less path:
		// the forwarder's register bound it, and the daemon enforces owner
		// = bound session. Passing an empty session would fail validation,
		// so MCP inbox uses the session id stored in the forwarder.
		method, params = protocol.MethodMessageListPending, map[string]any{"sessionId": s.fwd.session}
	case "message_status":
		method, params = protocol.MethodMessageStatus, map[string]any{"messageId": get("messageId")}
	case "message_ack":
		// The recipient reports what it observed. Only recipient-observable
		// statuses are accepted: presented (the claim returned the message
		// to the agent), handled (the agent acted on it), rejected (it
		// refused it). In particular "injected" is refused here — an MCP
		// inbox claim is a next-turn channel and never proves injection
		// into the model's context, and claiming it would misrepresent the
		// delivery (ICSP-001 sections 5.4 and 12).
		status := get("status")
		switch status {
		case string(protocol.ReceiptPresented), string(protocol.ReceiptHandled), string(protocol.ReceiptRejected):
		default:
			return map[string]any{
				"content": []any{map[string]any{"type": "text", "text": fmt.Sprintf("message_ack status %q is not a recipient-observable outcome (allowed: presented, handled, rejected)", status)}},
				"isError": true,
			}, nil
		}
		claimGen := int64(0)
		if v, ok := a["claimGeneration"].(float64); ok {
			claimGen = int64(v)
		}
		method, params = protocol.MethodMessageReport, map[string]any{"messageId": get("messageId"), "status": status, "detail": get("detail"), "claimToken": get("claimToken"), "claimGeneration": claimGen}
	case "conversation_history":
		method, params = protocol.MethodConversationHistory, map[string]any{"conversationId": get("conversationId")}
	default:
		return nil, protocol.Errorf(protocol.CodeInvalidParams, "unknown tool %q", name)
	}
	raw, _ := json.Marshal(params)
	result, rpcErr := s.fwd.call(ctx, method, raw)
	if rpcErr != nil {
		return map[string]any{"content": []any{map[string]any{"type": "text", "text": rpcErr.Message}}, "isError": true}, nil
	}
	// MCP tool result shape.
	return map[string]any{"content": []any{map[string]any{"type": "text", "text": string(result)}}}, nil
}

// heartbeatLoop reports this process's liveness to the daemon at a
// third of the daemon's sweep interval. A failed heartbeat is silent:
// the next tick retries, and the daemon's own sweep remains the
// authority for presence.
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

// forwarder is shared with the ACP thin-client pattern: one daemon
// connection, responses matched by id, notifications to stderr (MCP
// server-initiated notifications are not needed for these tools).
//
// Every daemon request carries a unique id. A harness may issue tool calls
// concurrently (parallel tool calls in one turn), and a constant id would
// make their responses indistinguishable: each caller would race to read
// the other's result. Writes are serialised for the same reason.
type forwarder struct {
	conn    transport.Conn
	session string
	mu      sync.Mutex
	pending map[string]chan json.RawMessage
	nextID  int64
	wmu     sync.Mutex
}

func newForwarder(conn transport.Conn) *forwarder {
	f := &forwarder{conn: conn, pending: make(map[string]chan json.RawMessage)}
	go f.readLoop()
	return f
}

func (f *forwarder) readLoop() {
	defer f.failPending()
	for {
		frame, err := readFrame(f.conn)
		if err != nil {
			return
		}
		var probe struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(frame, &probe); err != nil {
			continue
		}
		if len(probe.ID) == 0 || string(probe.ID) == "null" {
			continue
		}
		f.mu.Lock()
		ch, ok := f.pending[string(probe.ID)]
		f.mu.Unlock()
		if ok {
			ch <- frame
		}
	}
}

// failPending fails every in-flight call when the daemon connection
// dies. Without it, a tool call issued just before the daemon exits
// would block until the harness gave up, instead of returning an
// error the agent can act on.
func (f *forwarder) failPending() {
	f.mu.Lock()
	pending := f.pending
	f.pending = make(map[string]chan json.RawMessage)
	f.mu.Unlock()
	for key, ch := range pending {
		frame, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(key),
			"error":   map[string]any{"code": -32000, "message": "daemon connection closed"},
		})
		select {
		case ch <- frame:
		default:
		}
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

func (f *forwarder) call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *protocol.RPCError) {
	// A daemon call must never hang a harness turn forever: bound it
	// unless the caller already imposed an earlier deadline.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	id := atomic.AddInt64(&f.nextID, 1)
	req := protocol.RPCRequest{JSONRPC: "2.0", ID: json.RawMessage(strconv.FormatInt(id, 10)), Method: method, Params: params}
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
	b, _ := json.Marshal(req)
	f.wmu.Lock()
	_, err := f.conn.Write(append(b, '\n'))
	f.wmu.Unlock()
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "write to daemon: %v", err)
	}
	select {
	case <-ctx.Done():
		return nil, protocol.Errorf(protocol.CodeInternalError, "cancelled")
	case frame := <-ch:
		var resp protocol.RPCResponse
		if err := json.Unmarshal(frame, &resp); err != nil {
			return nil, protocol.Errorf(protocol.CodeParseError, "decode daemon response: %v", err)
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	}
}
