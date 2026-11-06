// Package conformance - crash/restart durability tests.
package conformance

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/B-A-M-N/intercom/internal/config"
	"github.com/B-A-M-N/intercom/internal/node"
	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/store"
)

// crashChildMain runs in a re-exec of the test binary: it opens the
// store, sends one message, and then kills its own process with
// SIGKILL (no graceful Close, no WAL deletion). The parent test
// then reopens the same file and verifies durability — a real
// process kill, exercising the store's actual recovery path.
func crashChildMain() {
	dir := os.Getenv("INTERCOM_CRASH_DIR")
	dbPath := filepath.Join(dir, "intercom.db")
	ctx := context.Background()
	st, err := store.Open(ctx, store.Options{Path: dbPath})
	if err != nil {
		os.Exit(2)
	}
	cfg := config.Default()
	cfg.DataDir = dir
	n, err := node.New(ctx, node.Options{Config: cfg, Store: st})
	if err != nil {
		os.Exit(2)
	}
	_ = n.RegisterAdapterSession(ctx, protocol.PeerInfo{SessionID: "ses_crash_a0000000000000", Alias: "crasha", InstanceID: "ins_crash_a0000000000000", HostID: n.HostID(), PrincipalID: n.PrincipalID(), Harness: "synthetic"})
	_ = n.RegisterAdapterSession(ctx, protocol.PeerInfo{SessionID: "ses_crash_b0000000000000", Alias: "crashb", InstanceID: "ins_crash_b0000000000000", HostID: n.HostID(), PrincipalID: n.PrincipalID(), Harness: "synthetic"})
	res, err := n.Send(ctx, node.Sender{PrincipalID: n.PrincipalID(), SessionID: "ses_crash_a0000000000000"}, node.SendRequest{
		CommandID: "cmd_crash00000000000000001", To: "ses_crash_b0000000000000",
		Body: protocol.MessageBody{Type: "text", Text: "survive crash"},
		Delivery: protocol.Delivery{Mode: protocol.DeliveryQueue, Wake: protocol.WakeNone, OnUnsupported: protocol.UnsupportedReject},
	})
	if err != nil {
		os.Exit(2)
	}
	// Report the admitted message ID to the parent before dying.
	_ = os.WriteFile(filepath.Join(dir, "msgid"), []byte(res.MessageID), 0o600)
	// Hard kill: no st.Close(), no checkpoint, no WAL juggling.
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
	os.Exit(3)
}

func TestMain(m *testing.M) {
	if os.Getenv("INTERCOM_CRASH_CHILD") == "1" {
		crashChildMain()
		return
	}
	os.Exit(m.Run())
}

// Test_CrashRestartPreservesAdmittedWork: a real subprocess is
// SIGKILLed after admitting a message; the parent reopens the same
// file, recovers, and verifies message + receipt + command survive.
func Test_CrashRestartPreservesAdmittedWork(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "intercom.db")
	ctx := context.Background()
	open := func() (*store.Store, *node.Node) {
		st, err := store.Open(ctx, store.Options{Path: dbPath})
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		cfg := config.Default()
		cfg.DataDir = dir
		n, err := node.New(ctx, node.Options{Config: cfg, Store: st})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		return st, n
	}
	// First incarnation: a real child process admits the message
	// and is SIGKILLed. Its expected death (signal) is success;
	// any other exit means the child failed before the kill.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	child := exec.Command(exe, "-test.run=Test_CrashRestartPreservesAdmittedWork", "-test.v")
	child.Env = append(os.Environ(),
		"INTERCOM_CRASH_CHILD=1",
		"INTERCOM_CRASH_DIR="+dir,
	)
	var childOut bytes.Buffer
	child.Stdout = &childOut
	child.Stderr = &childOut
	kerr := child.Run()
	if kerr == nil {
		t.Fatalf("crash child exited 0, expected SIGKILL; output: %s", childOut.String())
	}
	if exitErr, ok := kerr.(*exec.ExitError); !ok {
		t.Fatalf("crash child failed to run: %v; output: %s", kerr, childOut.String())
	} else if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("crash child did not die by SIGKILL: %v; output: %s", kerr, childOut.String())
	}
	msgIDBytes, rerr := os.ReadFile(filepath.Join(dir, "msgid"))
	if rerr != nil {
		t.Fatalf("crash child did not report message ID: %v; output: %s", rerr, childOut.String())
	}
	msgID := string(msgIDBytes)

	st2, n2 := open()
	defer st2.Close()
	report, err := n2.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	t.Logf("recovery: %s", report.String())
	// Message + acceptance receipt survive.
	if _, err := st2.GetMessage(ctx, msgID); err != nil {
		t.Fatalf("message lost after crash: %v", err)
	}
	receipts, err := n2.Receipts(ctx, msgID)
	if err != nil || len(receipts) == 0 {
		t.Fatalf("receipts lost: %v %v", receipts, err)
	}
	// Inbox still holds it for claim (unknown delivery stays claimable).
	pending, err := n2.PendingInbox(ctx, "ses_crash_b0000000000000", 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	found := false
	for _, m := range pending {
		if m.MessageID == msgID {
			found = true
		}
	}
	if !found {
		t.Errorf("admitted message not claimable after crash: %+v", pending)
	}
	// Session identity stable across restart: a fresh incarnation
	// on the same store must resolve the same host/principal.
	st3, n3 := open()
	defer st3.Close()
	if n3.HostID() != n2.HostID() || n3.PrincipalID() != n2.PrincipalID() {
		t.Errorf("identity changed across restart: host %q->%q principal %q->%q", n2.HostID(), n3.HostID(), n2.PrincipalID(), n3.PrincipalID())
	}
}
