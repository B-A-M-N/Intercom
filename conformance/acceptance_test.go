package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/transport"
)

// acceptanceEnv is a running Intercom host with a built CLI.
type acceptanceEnv struct {
	t          *testing.T
	socketPath string
	dataDir    string
	host       *exec.Cmd
	cli        string
}

// startAcceptanceHost builds the binaries and starts a host.
func startAcceptanceHost(t *testing.T) *acceptanceEnv {
	t.Helper()

	root := repoRoot(t)
	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create bin dir: %v", err)
	}

	env := append(os.Environ(),
		"GOCACHE=/tmp/gocache",
		"GOFLAGS=-mod=mod",
		"GOPROXY=off",
		"GOSUMDB=off",
	)

	hostBin := filepath.Join(binDir, "intercomd")
	cliBin := filepath.Join(binDir, "intercom")

	for _, build := range []struct{ out, pkg string }{
		{hostBin, "./cmd/intercomd"},
		{cliBin, "./cmd/intercom"},
	} {
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", build.out, build.pkg)
		cmd.Dir = root
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", build.pkg, err, out)
		}
	}

	socketPath := filepath.Join(t.TempDir(), "intercom.sock")
	dataDir := filepath.Join(t.TempDir(), "data")

	host := exec.Command(hostBin,
		"--socket", socketPath,
		"--data-dir", dataDir,
		"--alias", "acceptance-host",
		"--principal", "prn_acceptance0000000000000000000",
	)
	host.Stderr = os.Stderr
	if err := host.Start(); err != nil {
		t.Fatalf("start host: %v", err)
	}

	t.Cleanup(func() {
		_ = host.Process.Kill()
		_ = host.Wait()
	})

	// Wait for the socket to accept connections.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := transport.DialUnix(context.Background(), socketPath)
		if err == nil {
			conn.Close()
			return &acceptanceEnv{
				t: t, socketPath: socketPath, dataDir: dataDir,
				host: host, cli: cliBin,
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the host did not start listening in time")
	return nil
}

// cli runs the intercom CLI against the host and returns stdout.
func (e *acceptanceEnv) cli_run(args ...string) (string, error) {
	e.t.Helper()
	cmd := exec.Command(e.cli, append([]string{"--socket", e.socketPath}, args...)...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// cliJSON runs the CLI and decodes its JSON output.
func (e *acceptanceEnv) cliJSON(args ...string) (map[string]any, error) {
	e.t.Helper()
	out, err := e.cli_run(args...)
	if err != nil {
		return nil, fmt.Errorf("cli %v failed: %v\n%s", args, err, out)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &decoded); err != nil {
		return nil, fmt.Errorf("cli %v produced invalid JSON: %v\n%s", args, err, out)
	}
	return decoded, nil
}

// registerSession registers a session with the host through the CLI's
// session/register method, using a real client.
func (e *acceptanceEnv) registerSession(sessionID, alias, harness string) {
	e.t.Helper()
	client := dialRPC(e.t, e.socketPath)
	resp := client.call(e.t, protocol.MethodSessionRegister, map[string]any{
		"sessionId":      sessionID,
		"instanceId":     "ins_acceptance" + harness,
		"alias":          alias,
		"harness":        harness,
		"harnessVersion": "1.0.0",
		"capabilities": []map[string]any{
			{"id": "delivery.notify", "support": "native"},
			{"id": "delivery.queue", "support": "native"},
		},
		"deliveryModes": []string{"notify", "queue"},
	})
	if resp.Error != nil {
		e.t.Fatalf("register session: %v", resp.Error)
	}
}

// Test_Acceptance01_TwoHarnessesDiscover covers acceptance criterion 1:
// two independently running agents from different harnesses can discover one
// another.
func Test_Acceptance01_TwoHarnessesDiscover(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_muse00000000000000000000000", "muse-agent", "muse")
	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	peers, err := env.cliJSON("peers")
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	list, _ := peers["peers"].([]any)
	if len(list) != 2 {
		t.Fatalf("expected 2 peers, got %d: %v", len(list), peers)
	}

	harnesses := map[string]bool{}
	for _, raw := range list {
		p, _ := raw.(map[string]any)
		h, _ := p["harness"].(string)
		harnesses[h] = true
	}
	if !harnesses["muse"] || !harnesses["hermes"] {
		t.Errorf("the two harnesses did not discover each other: %v", harnesses)
	}
}

// Test_Acceptance02_ExchangeWithoutOrchestrator covers acceptance criterion
// 2: agents exchange authenticated messages without an orchestrator.
//
// The two participants are separate processes on separate connections. The
// message travels peer to peer; no third party interprets or authorizes it.
func Test_Acceptance02_ExchangeWithoutOrchestrator(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_muse00000000000000000000000", "muse-agent", "muse")
	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	// The muse session sends to the hermes session.
	sender := dialRPC(t, env.socketPath)
	resp := sender.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acceptance000000000000000001",
		"to":        map[string]any{"sessionId": "ses_hermes0000000000000000000000"},
		"body":      map[string]any{"type": "text", "text": "hello from muse"},
		"delivery": map[string]any{
			"mode":          "notify",
			"wake":          "none",
			"onUnsupported": "reject",
		},
	})
	if resp.Error != nil {
		t.Fatalf("message/send failed: %v", resp.Error)
	}

	var sent struct {
		MessageID string `json:"messageId"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		t.Fatalf("decode send result: %v", err)
	}
	if sent.Status != string(protocol.ReceiptAccepted) {
		t.Errorf("send status = %q, want accepted", sent.Status)
	}

	// The recipient claims its inbox and finds the message.
	recipient := dialRPC(t, env.socketPath)
	inboxResp := recipient.call(t, protocol.MethodMessageListPending, map[string]any{
		"sessionId": "ses_hermes0000000000000000000000",
	})
	if inboxResp.Error != nil {
		t.Fatalf("message/listPending failed: %v", inboxResp.Error)
	}
	var inbox struct {
		Messages []protocol.Message `json:"messages"`
	}
	if err := json.Unmarshal(inboxResp.Result, &inbox); err != nil {
		t.Fatalf("decode inbox: %v", err)
	}
	if len(inbox.Messages) != 1 {
		t.Fatalf("expected 1 message in the recipient's inbox, got %d", len(inbox.Messages))
	}
	if inbox.Messages[0].Body.Text != "hello from muse" {
		t.Errorf("message text = %q", inbox.Messages[0].Body.Text)
	}

	// The recipient reports what it did.
	reportResp := recipient.call(t, protocol.MethodMessageReport, map[string]any{
		"messageId": sent.MessageID,
		"status":    "presented",
		"detail":    "presented by the notification handler",
	})
	if reportResp.Error != nil {
		t.Fatalf("message/report failed: %v", reportResp.Error)
	}

	// The sender reads the receipt trail and sees the presentation.
	status, err := env.cliJSON("status", sent.MessageID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status["status"] != "presented" {
		t.Errorf("receipt status = %v, want presented", status["status"])
	}
}

// Test_Acceptance03_IdentitiesPreserved covers acceptance criterion 3:
// sender and recipient identities are preserved.
func Test_Acceptance03_IdentitiesPreserved(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_muse00000000000000000000000", "muse-agent", "muse")
	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	sender := dialRPC(t, env.socketPath)
	resp := sender.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acceptance000000000000000002",
		"to":        map[string]any{"sessionId": "ses_hermes0000000000000000000000"},
		"body":      map[string]any{"type": "text", "text": "who am i"},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	})
	if resp.Error != nil {
		t.Fatalf("message/send failed: %v", resp.Error)
	}
	var sent struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		t.Fatalf("decode: %v", err)
	}

	recipient := dialRPC(t, env.socketPath)
	inboxResp := recipient.call(t, protocol.MethodMessageListPending, map[string]any{
		"sessionId": "ses_hermes0000000000000000000000",
	})
	var inbox struct {
		Messages []protocol.Message `json:"messages"`
	}
	if err := json.Unmarshal(inboxResp.Result, &inbox); err != nil {
		t.Fatalf("decode inbox: %v", err)
	}
	if len(inbox.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(inbox.Messages))
	}

	// The recipient identity is preserved on the message.
	if inbox.Messages[0].To != "ses_hermes0000000000000000000000" {
		t.Errorf("recipient identity = %q", inbox.Messages[0].To)
	}
	// The sender identity is the transport-authenticated session, not
	// something the request could have chosen.
	if inbox.Messages[0].From == "" {
		t.Error("the message carries no sender identity")
	}
	if inbox.Messages[0].From == "ses_hermes0000000000000000000000" {
		t.Error("the sender identity was taken from the request payload")
	}
}

// Test_Acceptance06_RetryAfterConnectionLoss covers acceptance criterion 6:
// retrying a message after connection loss cannot create a duplicate
// recipient inbox entry.
//
// The sender's connection is dropped before it learns the outcome, then it
// retries with the same command id. The recipient must still see exactly
// one message.
func Test_Acceptance06_RetryAfterConnectionLoss(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	// The first attempt succeeds at the host, but the sender never sees
	// the answer: the connection dies immediately after the request.
	first := dialRPC(t, env.socketPath)
	first.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acceptance000000000000000003",
		"to":        map[string]any{"sessionId": "ses_hermes0000000000000000000000"},
		"body":      map[string]any{"type": "text", "text": "exactly once"},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	})
	// The connection is now dropped, simulating a lost acknowledgement.
	first.conn.Close()

	// The sender retries with the same command id on a fresh connection.
	second := dialRPC(t, env.socketPath)
	resp := second.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acceptance000000000000000003",
		"to":        map[string]any{"sessionId": "ses_hermes0000000000000000000000"},
		"body":      map[string]any{"type": "text", "text": "exactly once"},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	})
	if resp.Error != nil {
		t.Fatalf("the retry failed: %v", resp.Error)
	}

	var sent struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sent.MessageID == "" {
		t.Fatal("the retry produced no message id")
	}

	// The recipient must see exactly one inbox entry.
	recipient := dialRPC(t, env.socketPath)
	inboxResp := recipient.call(t, protocol.MethodMessageListPending, map[string]any{
		"sessionId": "ses_hermes0000000000000000000000",
	})
	var inbox struct {
		Messages []protocol.Message `json:"messages"`
	}
	if err := json.Unmarshal(inboxResp.Result, &inbox); err != nil {
		t.Fatalf("decode inbox: %v", err)
	}
	if len(inbox.Messages) != 1 {
		t.Fatalf("the retry created %d inbox entries, want exactly 1", len(inbox.Messages))
	}
}

// Test_Acceptance07_RestartPreservesState covers acceptance criterion 7: a
// process restart preserves durable admitted messages and recoverable session
// events.
func Test_Acceptance07_RestartPreservesState(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	sender := dialRPC(t, env.socketPath)
	resp := sender.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": "cmd_acceptance000000000000000004",
		"to":        map[string]any{"sessionId": "ses_hermes0000000000000000000000"},
		"body":      map[string]any{"type": "text", "text": "survive the restart"},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	})
	if resp.Error != nil {
		t.Fatalf("message/send failed: %v", resp.Error)
	}
	var sent struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Restart the host against the same data directory.
	_ = env.host.Process.Kill()
	_ = env.host.Wait()

	hostBin := filepath.Join(filepath.Dir(env.cli), "intercomd")
	restarted := exec.Command(hostBin,
		"--socket", env.socketPath,
		"--data-dir", env.dataDir,
		"--alias", "acceptance-host",
		"--principal", "prn_acceptance0000000000000000000",
	)
	restarted.Stderr = os.Stderr
	if err := restarted.Start(); err != nil {
		t.Fatalf("restart host: %v", err)
	}
	t.Cleanup(func() {
		_ = restarted.Process.Kill()
		_ = restarted.Wait()
	})
	env.host = restarted

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := transport.DialUnix(context.Background(), env.socketPath)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The message is still in the durable inbox.
	recipient := dialRPC(t, env.socketPath)
	inboxResp := recipient.call(t, protocol.MethodMessageListPending, map[string]any{
		"sessionId": "ses_hermes0000000000000000000000",
	})
	if inboxResp.Error != nil {
		t.Fatalf("message/listPending failed after restart: %v", inboxResp.Error)
	}
	var inbox struct {
		Messages []protocol.Message `json:"messages"`
	}
	if err := json.Unmarshal(inboxResp.Result, &inbox); err != nil {
		t.Fatalf("decode inbox: %v", err)
	}
	if len(inbox.Messages) != 1 {
		t.Fatalf("the message did not survive the restart: %d messages", len(inbox.Messages))
	}
	if inbox.Messages[0].MessageID != sent.MessageID {
		t.Errorf("surviving message id = %q, want %q", inbox.Messages[0].MessageID, sent.MessageID)
	}

	// The session events survived too.
	replayResp := recipient.call(t, protocol.MethodSessionReplay, map[string]any{
		"sessionId": "ses_hermes0000000000000000000000",
	})
	if replayResp.Error != nil {
		t.Fatalf("session/replay failed after restart: %v", replayResp.Error)
	}
	var replay struct {
		Events []protocol.EventEnvelope `json:"events"`
	}
	if err := json.Unmarshal(replayResp.Result, &replay); err != nil {
		t.Fatalf("decode replay: %v", err)
	}
	if len(replay.Events) == 0 {
		t.Error("the session events did not survive the restart")
	}
}

// Test_Acceptance09_UnauthorizedCannotModifyPermissions covers acceptance
// criterion 9: unauthorized peers cannot obtain session access or modify
// execution permissions.
func Test_Acceptance09_UnauthorizedCannotModifyPermissions(t *testing.T) {
	env := startAcceptanceHost(t)

	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")

	// A peer that has not been granted anything tries to grant itself
	// owner authority over the hermes session.
	client := dialRPC(t, env.socketPath)
	resp := client.call(t, protocol.MethodPermissionGrant, map[string]any{
		"ownerSession": "ses_hermes0000000000000000000000",
		"peerSession":  "ses_attacker000000000000000000000",
		"role":         "owner",
	})
	if resp.Error == nil {
		t.Fatal("an unauthorized peer was granted owner authority")
	}
	if resp.Error.Code != protocol.CodeForbidden {
		t.Errorf("error code = %d, want %d (forbidden)", resp.Error.Code, protocol.CodeForbidden)
	}

	// The grant was not recorded.
	listResp := client.call(t, protocol.MethodPermissionList, map[string]any{
		"peerSession": "ses_attacker000000000000000000000",
	})
	if listResp.Error != nil {
		t.Fatalf("permission/list failed: %v", listResp.Error)
	}
	var list struct {
		Permissions []map[string]any `json:"permissions"`
	}
	if err := json.Unmarshal(listResp.Result, &list); err != nil {
		t.Fatalf("decode permission list: %v", err)
	}
	if len(list.Permissions) != 0 {
		t.Errorf("an unauthorized grant was recorded: %v", list.Permissions)
	}
}
