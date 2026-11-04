// Command intercomd runs an Intercom host.
//
// The host owns the durable store, the local Unix socket transport, and the
// ACP extension surface. Adapters connect over stdio or the local socket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bamn/intercom/internal/adapter/acp"
	"github.com/bamn/intercom/internal/config"
	"github.com/bamn/intercom/internal/node"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/store"
	"github.com/bamn/intercom/internal/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "intercomd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  = flag.String("config", "", "path to the JSON configuration file")
		socketPath  = flag.String("socket", "", "path to the local Unix socket (overrides config)")
		dataDir     = flag.String("data-dir", "", "directory for the durable store (overrides config)")
		alias       = flag.String("alias", "", "human-readable host alias")
		harness     = flag.String("harness", "", "harness identity this host represents")
		sessionID   = flag.String("session", "", "session identity this host represents")
		principalID = flag.String("principal", "", "authenticated principal identity")
		printJSON   = flag.Bool("json", false, "print the resolved configuration as JSON and exit")
		version     = flag.Bool("version", false, "print the protocol version and exit")
	)
	flag.Parse()

	if *version {
		fmt.Printf("intercom %s (%s)\n", protocol.SpecRevision, protocol.SpecVersion)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *socketPath != "" {
		cfg.SocketPath = *socketPath
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *alias != "" {
		cfg.Alias = *alias
	}
	if *harness != "" {
		cfg.Harness = *harness
	}
	if *principalID != "" {
		cfg.PrincipalID = *principalID
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	if *printJSON {
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, store.Options{Path: cfg.DatabasePath()})
	if err != nil {
		return err
	}
	defer st.Close()

	n, err := node.New(ctx, node.Options{Config: cfg, Store: st})
	if err != nil {
		return err
	}

	// Crash recovery runs before the host accepts traffic, so admitted
	// commands are classified rather than left ambiguous.
	report, err := n.Recover(ctx)
	if err != nil {
		return fmt.Errorf("recovery: %w", err)
	}
	fmt.Fprintf(os.Stderr, "intercomd: %s\n", report)

	ln, err := transport.ListenUnix(ctx, cfg.SocketPath)
	if err != nil {
		return err
	}
	defer ln.Close()

	srv := transport.NewServer(func(ctx context.Context, peer transport.PeerIdentity, method string, params json.RawMessage) (any, *protocol.RPCError) {
		// The peer identity comes from the transport, never from the payload.
		principal := peer.PrincipalID
		if principal == "" {
			principal = cfg.PrincipalID
		}
		session := peerSession(peer, cfg, *sessionID)
		return acp.NewServer(n, principal, session).Handle(ctx, method, params)
	})
	defer srv.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		srv.Close()
		ln.Close()
	}()

	fmt.Fprintf(os.Stderr, "intercomd: listening on %s (host %s)\n", cfg.SocketPath, n.HostID())

	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, transport.ErrClosed) {
				break
			}
			fmt.Fprintf(os.Stderr, "intercomd: accept: %v\n", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.Serve(ctx, conn); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "intercomd: connection: %v\n", err)
			}
		}()
	}

	wg.Wait()
	return nil
}

// peerSession determines the session identity a connection acts as.
//
// The transport establishes the process identity. The session identity is the
// host's configured session, or a per-connection identity derived from the
// authenticated peer when none is configured.
func peerSession(peer transport.PeerIdentity, cfg config.Config, configured string) string {
	if configured != "" {
		return configured
	}
	if peer.Authenticated && peer.PID > 0 {
		return fmt.Sprintf("ses_peer_%d", peer.PID)
	}
	return "ses_anonymous"
}

// keep the time import available for future deadline handling.
var _ = time.Second
