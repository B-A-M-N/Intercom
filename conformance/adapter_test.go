package conformance

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bamn/intercom/internal/adapter/acp"
	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/transport"
)

// Test_PythonAdapterHostBridgesToCore is the polyglot boundary test.
//
// The Python adapter host is a separate process in a separate language. It
// registers its session, claims the messages in its inbox, and reports what
// it did. The Go core owns durability and decides what is true.
func Test_PythonAdapterHostBridgesToCore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the python adapter host test in short mode")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skipf("python3 is not available: %v", err)
	}

	adapterDir, err := filepath.Abs("../adapters/python")
	if err != nil {
		t.Fatalf("resolve adapter dir: %v", err)
	}

	e := newEnv(t)

	// Start a real Go host on a real socket.
	socketPath := filepath.Join(t.TempDir(), "intercom.sock")
	hostCtx, cancelHost := context.WithCancel(context.Background())
	defer cancelHost()

	host := startGoHost(t, hostCtx, socketPath, e)

	// The adapter host registers as a separate session.
	adapterSession := "ses_adapter000000000000000000000"
	instanceID := "ins_adapter00000000000000000000"

	cmd := exec.CommandContext(hostCtx, python, "-m", "intercom_adapter.host",
		"--socket", socketPath,
		"--harness", "hermes",
		"--session", adapterSession,
		"--alias", "adapter",
		"--poll-interval", "0.1",
	)
	cmd.Dir = adapterDir
	cmd.Env = append(cmd.Environ(), "PYTHONPATH="+adapterDir)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start adapter host: %v", err)
	}
	defer func() {
		cancelHost()
		_ = cmd.Wait()
	}()

	// The adapter host announces its registration.
	registered := make(chan struct{})
	go func() {
		defer close(registered)
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 && json.Valid(buf[:n]) {
				return
			}
			if err != nil {
				return
			}
		}
	}()

	select {
	case <-registered:
	case <-time.After(15 * time.Second):
		t.Fatal("adapter host did not register within the timeout")
	}

	// The core now knows about the adapter session and its honest claims.
	client := dialRPC(t, socketPath)
	resp := client.call(t, protocol.MethodPeerList, nil)
	if resp.Error != nil {
		t.Fatalf("peer/list failed: %v", resp.Error)
	}
	var list acp.PeerListResult
	if err := json.Unmarshal(resp.Result, &list); err != nil {
		t.Fatalf("decode peer list: %v", err)
	}

	var found bool
	for _, p := range list.Peers {
		if p.SessionID != adapterSession {
			continue
		}
		found = true
		if p.Harness != "hermes" {
			t.Errorf("adapter harness = %q, want hermes", p.Harness)
		}
		// Hermes exposes a prompt channel but no active-turn injection.
		for _, c := range p.Capabilities {
			if c.ID == protocol.CapDeliverySteer && c.Support != protocol.SupportUnsupported {
				t.Errorf("hermes advertised steer as %q, want unsupported", c.Support)
			}
		}
	}
	if !found {
		t.Fatalf("the adapter session was not visible in peer discovery")
	}

	_ = host
	_ = instanceID
	_ = e
}

// startGoHost starts an intercomd process against a socket path.
func startGoHost(t *testing.T, ctx context.Context, socketPath string, e *env) *exec.Cmd {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "intercomd")

	build := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", binary, "./cmd/intercomd")
	build.Dir = repoRoot(t)
	build.Env = append(build.Environ(),
		"GOCACHE=/tmp/gocache",
		"GOFLAGS=-mod=mod",
		"GOPROXY=off",
		"GOSUMDB=off",
	)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build intercomd: %v\n%s", err, out)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
	cmd := exec.CommandContext(ctx, binary,
		"--socket", socketPath,
		"--data-dir", dataDir,
		"--alias", "go-host",
		"--principal", e.node.PrincipalID(),
	)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start intercomd: %v", err)
	}

	// Wait for the socket to accept connections.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := transport.DialUnix(ctx, socketPath)
		if err == nil {
			conn.Close()
			return cmd
		}
		time.Sleep(50 * time.Millisecond)
	}
	cmd.Process.Kill()
	t.Fatal("intercomd did not start listening in time")
	return nil
}

// repoRoot returns the repository root directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return wd
}

// Test_AdapterCannotClaimUnsupportedSteer checks that the core records an
// adapter's honest claims without upgrading them.
func Test_AdapterCannotClaimUnsupportedSteer(t *testing.T) {
	e := newEnv(t)

	// A peer claiming steering that it does not have is recorded as
	// claimed. The core stores the claim; the conformance suite for the
	// adapter is what verifies the claim is honest.
	sessionID := "ses_lying00000000000000000000000"
	err := e.node.RegisterAdapterSession(e.ctx, protocol.PeerInfo{
		SessionID:  sessionID,
		InstanceID: "ins_lying000000000000000000000",
		Harness:    "muse",
		Capabilities: []protocol.Capability{
			{ID: protocol.CapDeliverySteer, Support: protocol.SupportNative},
		},
		Delivery: []protocol.DeliveryMode{protocol.DeliverySteer},
	})
	if err != nil {
		t.Fatalf("register adapter session: %v", err)
	}

	caps, err := e.node.Capabilities(e.ctx, sessionID)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if len(caps) != 1 || caps[0].ID != protocol.CapDeliverySteer {
		t.Fatalf("unexpected capabilities: %+v", caps)
	}
	if caps[0].Support != protocol.SupportNative {
		t.Errorf("support = %q, want native (the core stores the adapter's claim verbatim)", caps[0].Support)
	}

	// The node must not hold a live adapter for it, so a steer request to
	// that session finds no adapter and cannot claim delivery.
	if _, ok := e.node.Adapter(sessionID); ok {
		t.Error("the core created an adapter for a registered remote session")
	}
}
