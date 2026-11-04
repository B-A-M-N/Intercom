// Command intercom-acp serves the Intercom ACP extension over stdio.
//
// This is the adapter-host transport: an Intercom adapter process speaks
// standard ACP to this binary, and this binary speaks the Intercom extension
// over the same JSON-RPC connection. The Go core's Unix socket stays private
// to the local host process.
//
// The binary also works as an ordinary ACP agent for a client that knows
// nothing about Intercom: the standard methods keep their own semantics
// (ICSP-001 section 8).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bamn/intercom/internal/adapter/acp"
	"github.com/bamn/intercom/internal/config"
	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "intercom-acp: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		socketPath = flag.String("socket", "", "path to the Intercom host socket")
		dataDir    = flag.String("data-dir", "", "directory for the durable store")
		session    = flag.String("session", "", "session identity this process represents")
		principal  = flag.String("principal", "", "authenticated principal identity")
		alias      = flag.String("alias", "", "human-readable alias")
		harness    = flag.String("harness", "", "harness identity this process represents")
	)
	flag.Parse()

	if *socketPath == "" {
		return fmt.Errorf("intercom-acp: --socket is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Default()
	cfg.SocketPath = *socketPath
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *principal != "" {
		cfg.PrincipalID = *principal
	}
	if *alias != "" {
		cfg.Alias = *alias
	}
	if *harness != "" {
		cfg.Harness = *harness
	}

	st, err := store.Open(ctx, store.Options{Path: cfg.DatabasePath()})
	if err != nil {
		return err
	}
	defer st.Close()

	n, err := node.New(ctx, node.Options{Config: cfg, Store: st})
	if err != nil {
		return err
	}

	// The instance identity is minted per process, so a restart produces a
	// new instance id.
	instanceID := mintInstanceID()
	if *session == "" {
		*session = mintSessionID()
	}

	peer := protocol.PeerInfo{
		SessionID:   *session,
		InstanceID:  instanceID,
		Alias:       cfg.Alias,
		Harness:     cfg.Harness,
		HostID:      n.HostID(),
		PrincipalID: n.PrincipalID(),
		Presence:    protocol.PresenceOnline,
	}
	if err := n.RegisterAdapterSession(ctx, peer); err != nil {
		return err
	}

	ext := acp.NewServer(n, n.PrincipalID(), *session)

	// The stdio transport: newline-delimited JSON-RPC 2.0 in both
	// directions, which is what ACP uses.
	reader := bufio.NewReaderSize(os.Stdin, 64<<10)
	writer := bufio.NewWriterSize(os.Stdout, 64<<10)

	enc := json.NewEncoder(writer)
	dec := json.NewDecoder(reader)

	// The extension's event notifications are written to the same stream.
	ext.SetEmitter(func(ctx context.Context, method string, params any) {
		payload, err := json.Marshal(params)
		if err != nil {
			return
		}
		_ = enc.Encode(protocol.RPCNotification{
			JSONRPC: "2.0",
			Method:  method,
			Params:  payload,
		})
		_ = writer.Flush()
	})

	for {
		if ctx.Err() != nil {
			return nil
		}

		var req protocol.RPCRequest
		if err := dec.Decode(&req); err != nil {
			return nil
		}

		isNotification := len(req.ID) == 0 || string(req.ID) == "null"
		result, rpcErr := ext.Handle(ctx, req.Method, req.Params)
		if isNotification {
			continue
		}
		if rpcErr != nil {
			_ = enc.Encode(protocol.RPCResponse{JSONRPC: "2.0", ID: req.ID, Error: rpcErr})
		} else {
			payload, err := json.Marshal(result)
			if err != nil {
				_ = enc.Encode(protocol.RPCResponse{
					JSONRPC: "2.0", ID: req.ID,
					Error: protocol.Errorf(protocol.CodeInternalError, "marshal result: %v", err),
				})
			} else {
				_ = enc.Encode(protocol.RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: payload})
			}
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}
