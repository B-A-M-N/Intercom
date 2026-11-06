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

	"github.com/B-A-M-N/intercom/internal/adapter/acp"
	"github.com/B-A-M-N/intercom/internal/config"
	"net"
	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/remote"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
	"github.com/B-A-M-N/intercom/internal/transport"
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
		sessionID   = flag.String("session", "", "deprecated: session identity is bound per-connection via session/register, not daemon-wide")
		principalID = flag.String("principal", "", "authenticated principal identity")
		hostID      = flag.String("host", "", "stable host identity (default: load-or-create from store)")
		remoteListen = flag.String("remote-listen", "", "TCP address for remote federation over mTLS (e.g. 127.0.0.1:19090)")
		remoteToken  = flag.String("remote-token", "", "deprecated: bearer tokens are not used; host identity is file-based")
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
	if *hostID != "" {
		cfg.HostID = *hostID
	}
	if *sessionID != "" {
		fmt.Fprintf(os.Stderr, "intercomd: warning: --session is deprecated and ignored; sessions bind per-connection via session/register\n")
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
	defer n.Close()
	if *remoteToken != "" {
		return fmt.Errorf("intercomd: --remote-token is deprecated and rejected; remote authentication uses host identity keys")
	}
	if *remoteListen != "" {
		host, _, _ := net.SplitHostPort(*remoteListen)
		if host != "" && !remote.IsLoopback(host) {
			// Non-loopback requires mTLS (always on); plaintext is
			// impossible by construction. Log the enforcement.
			fmt.Fprintf(os.Stderr, "intercomd: remote mTLS enforced for non-loopback %s\n", *remoteListen)
		}
		identity, err := remote.LoadOrCreateIdentity(cfg.DataDir, n.HostID())
		if err != nil {
			return err
		}
		n.SetRemoteIdentity(identity)
		remoteSrv := remote.NewServer(*remoteListen, identity, func(ctx context.Context, env remote.Envelope, peerHost, peerFP string) (string, error) {
			return n.AdmitRemote(ctx, env, peerHost, peerFP)
		})
		// Fail visibly when federation cannot bind; never silently run
		// without remote readiness.
		serveErr := make(chan error, 1)
		go func() {
			serveErr <- remoteSrv.ListenAndServe(ctx)
		}()
		select {
		case err := <-serveErr:
			if err != nil && ctx.Err() == nil {
				return fmt.Errorf("intercomd: remote listen failed: %w", err)
			}
		case <-time.After(2 * time.Second):
		}
		if addr := remoteSrv.Addr(); addr == "" {
			// Listener may still be starting under load; check for an
			// immediate failure before declaring readiness.
			select {
			case err := <-serveErr:
				if err != nil && ctx.Err() == nil {
					return fmt.Errorf("intercomd: remote listen failed: %w", err)
				}
			default:
			}
		} else {
			fmt.Fprintf(os.Stderr, "intercomd: remote listening on %s\n", addr)
		}
	}

	// Remote restart recovery: durable outbound transfers that were
	// interrupted by a crash are re-attempted before the host
	// accepts traffic (requires the remote identity; without it
	// there is nowhere to send them).
	if n.RemoteIdentity() != nil {
		if resumed, rerr := n.ResumeRemoteTransfers(ctx); rerr != nil {
			fmt.Fprintf(os.Stderr, "intercomd: remote recovery: %v\n", rerr)
		} else if resumed > 0 {
			fmt.Fprintf(os.Stderr, "intercomd: remote recovery completed %d transfer(s)\n", resumed)
		}
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

	// Per-connection session binding: a connection starts as an ephemeral
	// peer identity (PID-derived, unregistered) and becomes bound to a
	// logical session only after a successful session/register on that
	// same connection (which requires the client-held session secret).
	// A supplied sessionId in any other call is not authentication evidence.
	// A bound connection cannot rebind to a different session. Multiple
	// connections may be bound to the same session only with the same
	// valid secret (same owner); the store fences instances via instanceId.
	var bindMu sync.Mutex
	type binding struct {
		session string
		role    string // "owner" or "attached"
		token   string
	}
	bound := make(map[transport.Conn]binding)

	var srv *transport.Server
	srv = transport.NewServer(func(ctx context.Context, peer transport.PeerIdentity, conn transport.Conn, method string, params json.RawMessage) (any, *protocol.RPCError) {
		// The peer identity comes from the transport, never from the payload.
		principal := peer.PrincipalID
		if principal == "" {
			principal = n.PrincipalID()
		}
		bindMu.Lock()
		bind, ok := bound[conn]
		bindMu.Unlock()
		session := ""
		role := ""
		token := ""
		if ok {
			session, role, token = bind.session, bind.role, bind.token
		} else {
			session = peerSession(peer, cfg, "")
		}

		// The ACP server emits event notifications for subscriptions. They
		// go to the connection that made the request, so a subscription only
		// reaches the client that asked for it.
		ext := acp.NewServer(n, principal, session)
		ext.SetDelegation(role, token)
		ext.SetEmitter(func(ctx context.Context, method string, params any) {
			srv.Notify(conn, method, params)
		})
		result, rpcErr := ext.Handle(ctx, method, params)
		if (method == protocol.MethodSessionRegister || method == protocol.MethodSessionAttach) && rpcErr == nil {
			var reg struct {
				SessionID string `json:"sessionId"`
				Token     string `json:"delegationToken"`
			}
			if err := json.Unmarshal(params, &reg); err == nil && reg.SessionID != "" {
				bindMu.Lock()
				if existing, already := bound[conn]; already && existing.session != reg.SessionID {
					bindMu.Unlock()
					return nil, protocol.Errorf(protocol.CodeForbidden, "connection is already bound to session %q; rebind to %q is forbidden", existing.session, reg.SessionID)
				}
				newRole := "owner"
				newToken := ""
				if method == protocol.MethodSessionAttach {
					newRole = "attached"
					newToken = reg.Token
				}
				bound[conn] = binding{session: reg.SessionID, role: newRole, token: newToken}
				bindMu.Unlock()
			}
		}
		return result, rpcErr
	})
	srv.SetMaxConnections(cfg.Limits.MaxConcurrentConnections)
	defer srv.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		srv.Close()
		ln.Close()
	}()

	// Retention keeps the durable store bounded. It runs alongside the
	// listener rather than only at startup, so a long-lived host does not
	// accumulate messages and journal events without limit.
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		n.StartMaintenance(ctx, n.MaintenanceInterval())
	}()

	// The presence sweep marks peers that stopped reporting as
	// disconnected. It never ends a session.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		n.StartHeartbeat(ctx, node.HeartbeatInterval)
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
			defer func() {
				bindMu.Lock()
				delete(bound, conn)
				bindMu.Unlock()
			}()
			if err := srv.Serve(ctx, conn); err != nil && ctx.Err() == nil {
				// A refused connection is explicit backpressure, not a
				// silent drop (ICSP-001 section 10.4).
				if errors.Is(err, transport.ErrTooManyConnections) {
					fmt.Fprintf(os.Stderr, "intercomd: refused connection: %v\n", err)
					return
				}
				fmt.Fprintf(os.Stderr, "intercomd: connection: %v\n", err)
			}
		}()
	}

	wg.Wait()
	<-maintenanceDone
	<-heartbeatDone
	return nil
}

// peerSession determines the ephemeral session identity of an unbound
// connection. It is derived from the authenticated peer PID and is never a
// registered logical session: a connection becomes a logical session only
// by binding via a successful session/register on that connection. The
// deprecated daemon-wide --session flag is ignored for this reason.
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
