// Command intercom-surface is the reference Surface Bridge: a tiny,
// language-agnostic human-surface client that speaks NDJSON over stdio.
// An existing TUI, CLI, IDE extension or browser app launches it and
// consumes displayable events without linking Go or Python. It does not
// replace the harness interface; it is an embeddable observation surface.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
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
		fmt.Fprintf(os.Stderr, "intercom-surface: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		socketPath = flag.String("socket", "", "path to the Intercom daemon socket (required)")
		session    = flag.String("session", "", "session identity this surface presents (required)")
		tokenFile  = flag.String("delegation-token-file", "", "protected file (0600) containing surface delegation token (preferred)")
		token      = flag.String("delegation-token", "", "scoped surface delegation token (required with --attach)")
		attach     = flag.Bool("attach", true, "attach as a surface-scoped delegated client")
	)
	flag.Parse()
	if *socketPath == "" || *session == "" {
		return fmt.Errorf("intercom-surface: --socket and --session are required")
	}
	tok := *token
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			return fmt.Errorf("intercom-surface: read delegation token file: %w", err)
		}
		if filepath.Base(*tokenFile) == "" {
			return fmt.Errorf("intercom-surface: invalid token file path")
		}
		tok = strings.TrimSpace(string(b))
	}
	if tok == "" {
		tok = os.Getenv("INTERCOM_SURFACE_TOKEN")
	}
	if tok == "" {
		return fmt.Errorf("intercom-surface: --delegation-token-file or --delegation-token is required")
	}
	_ = *attach
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn, err := transport.DialUnix(ctx, *socketPath)
	if err != nil {
		return fmt.Errorf("intercom-surface: connect to daemon: %w", err)
	}
	defer conn.Close()
	fwd := newForwarder(conn)
	attParams, _ := json.Marshal(map[string]any{"sessionId": *session, "delegationToken": tok})
	if _, rpcErr := fwd.call(ctx, protocol.MethodSessionAttach, attParams); rpcErr != nil {
		return fmt.Errorf("intercom-surface: attach with daemon: %v", rpcErr)
	}
	fwd.session = *session
	go heartbeatLoop(ctx, fwd)
	outMu := sync.Mutex{}
	writeLine := func(v any) {
		b, _ := json.Marshal(v)
		outMu.Lock()
		fmt.Println(string(b))
		outMu.Unlock()
	}
	// Streaming: poll replay and emit display events. Pure reads.
	cursor := 0
	var pollWg sync.WaitGroup
	pollWg.Add(1)
	go func() {
		defer pollWg.Done()
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				body, rpcErr := fwd.call(context.Background(), protocol.MethodSurfaceReplay, json.RawMessage(fmt.Sprintf(`{"sessionId":%q,"afterPosition":%d,"limit":100}`, *session, cursor)))
				if rpcErr != nil {
					continue
				}
				var res struct {
					Events []json.RawMessage `json:"events"`
					Cursor int `json:"cursor"`
				}
				if json.Unmarshal(body, &res) == nil {
					for _, ev := range res.Events {
						writeLine(map[string]any{"event": "display", "message": json.RawMessage(ev)})
					}
					cursor = res.Cursor
				}
			}
		}
	}()
	reader := bufio.NewReaderSize(os.Stdin, 64<<10)
	dec := json.NewDecoder(reader)
	for {
		if ctx.Err() != nil {
			pollWg.Wait()
			return nil
		}
		var req struct {
			Op string `json:"op"`
			ID json.RawMessage `json:"id"`
			Args json.RawMessage `json:"args"`
		}
		if err := dec.Decode(&req); err != nil {
			return nil
		}
		go func(req struct {
			Op string `json:"op"`
			ID json.RawMessage `json:"id"`
			Args json.RawMessage `json:"args"`
		}) {
			var result any
			var resultRaw json.RawMessage
			var rpcErr *protocol.RPCError
			switch req.Op {
			case "surface/replay":
				resultRaw, rpcErr = fwd.call(ctx, protocol.MethodSurfaceReplay, req.Args)
				if rpcErr == nil {
					json.Unmarshal(resultRaw, &result)
				}
			case "surface/present":
				resultRaw, rpcErr = fwd.call(ctx, protocol.MethodSurfacePresent, req.Args)
				if rpcErr == nil {
					json.Unmarshal(resultRaw, &result)
				}
			case "surface/reply":
				resultRaw, rpcErr = fwd.call(ctx, protocol.MethodSurfaceReply, req.Args)
				if rpcErr == nil {
					json.Unmarshal(resultRaw, &result)
				}
			case "ping":
				result = map[string]any{"ok": true}
			default:
				rpcErr = protocol.Errorf(protocol.CodeInvalidParams, "unknown surface op %q", req.Op)
			}
			resp := map[string]any{"op": req.Op}
			if req.ID != nil {
				resp["id"] = req.ID
			}
			if rpcErr != nil {
				resp["error"] = rpcErr.Message
				resp["isError"] = true
			} else {
				resp["result"] = result
			}
			writeLine(resp)
		}(req)
	}
}

type forwarder struct {
	conn transport.Conn
	session string
	mu sync.Mutex
	pending map[string]chan json.RawMessage
	nextID int64
	wmu sync.Mutex
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
		if json.Unmarshal(frame, &probe) != nil {
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

func (f *forwarder) failPending() {
	f.mu.Lock()
	pending := f.pending
	f.pending = make(map[string]chan json.RawMessage)
	f.mu.Unlock()
	for key, ch := range pending {
		frame, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(key), "error": map[string]any{"code": -32000, "message": "daemon connection closed"}})
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
		if json.Unmarshal(frame, &resp) != nil {
			return nil, protocol.Errorf(protocol.CodeParseError, "decode daemon response")
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	}
}

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
