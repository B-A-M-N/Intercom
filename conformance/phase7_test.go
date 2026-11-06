// Package conformance - Phase 7 MCP acknowledgement and honesty tests.
package conformance

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func toolText(t *testing.T, res map[string]any) string {
	t.Helper()
	content, _ := res["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("tool result has no content: %v", res)
	}
	text, _ := content[0].(map[string]any)["text"].(string)
	return text
}

func Test_Phase7_MCPAckProducesHandledReceipt(t *testing.T) {
	env := startAcceptanceHost(t)
	target := "ses_target00000000000000000000"
	env.registerSession(target, "target-agent", "hermes")

	// Sender: an MCP session. Recipient claimer: a second MCP process
	// bound to the target session, the way a real MCP agent would run.
	sender := startMCP(t, env.socketPath, "ses_sender00000000000000000000")
	_ = sender.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})

	sendRes := sender.tool("message_send", map[string]any{"commandId": "cmd_phase70000000000000000001", "to": target, "text": "please handle this", "mode": "notify"})
	if sendRes["isError"] == true {
		t.Fatalf("message_send isError: %v", sendRes)
	}
	var parsed struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal([]byte(toolText(t, sendRes)), &parsed); err != nil || parsed.MessageID == "" {
		t.Fatalf("could not parse messageId from %q", toolText(t, sendRes))
	}

	claimer := startMCP(t, env.socketPath, target)
	_ = claimer.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})

	inbox := claimer.tool("inbox_list", map[string]any{})
	if inbox["isError"] == true {
		t.Fatalf("inbox_list isError: %v", inbox)
	}
	if !strings.Contains(toolText(t, inbox), parsed.MessageID) {
		t.Fatalf("inbox_list did not claim %s: %s", parsed.MessageID, toolText(t, inbox))
	}
	var inboxParsed struct {
		Messages []struct {
			MessageID       string `json:"messageId"`
			ClaimToken      string `json:"claimToken"`
			ClaimGeneration int64  `json:"claimGeneration"`
		} `json:"messages"`
	}
	_ = json.Unmarshal([]byte(toolText(t, inbox)), &inboxParsed)
	claimToken := ""
	var claimGen int64
	for _, m := range inboxParsed.Messages {
		if m.MessageID == parsed.MessageID {
			claimToken = m.ClaimToken
			claimGen = m.ClaimGeneration
		}
	}

	// "injected" is not recipient-observable through MCP: claiming it
	// would misrepresent delivery, so it must be refused as a tool error.
	badAck := claimer.tool("message_ack", map[string]any{"messageId": parsed.MessageID, "status": "injected", "claimToken": claimToken, "claimGeneration": claimGen})
	if badAck["isError"] != true {
		t.Fatalf("message_ack injected must be an error, got: %v", badAck)
	}

	ack := claimer.tool("message_ack", map[string]any{"messageId": parsed.MessageID, "status": "handled", "detail": "acted on it", "claimToken": claimToken, "claimGeneration": claimGen})
	if ack["isError"] == true {
		t.Fatalf("message_ack isError: %v", ack)
	}

	status := sender.tool("message_status", map[string]any{"messageId": parsed.MessageID})
	if status["isError"] == true {
		t.Fatalf("message_status isError: %v", status)
	}
	if !strings.Contains(toolText(t, status), "handled") {
		t.Fatalf("status trail does not show handled: %s", toolText(t, status))
	}
}

func Test_Phase7_MCPRegistrationIsHonest(t *testing.T) {
	env := startAcceptanceHost(t)
	mcpSession := "ses_mcp0000000000000000000000"
	mcp := startMCP(t, env.socketPath, mcpSession)
	_ = mcp.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})

	res := mcp.tool("peer_capabilities", map[string]any{"sessionId": mcpSession})
	if res["isError"] == true {
		t.Fatalf("peer_capabilities isError: %v", res)
	}
	text := toolText(t, res)
	for _, want := range []string{"delivery.notify", "delivery.queue", "delivery.steer", "unsupported"} {
		if !strings.Contains(text, want) {
			t.Fatalf("capabilities missing %q: %s", want, text)
		}
	}
	if !strings.Contains(text, "active-turn") {
		t.Fatalf("steer limitation missing: %s", text)
	}
	// Tools list must advertise message_ack.
	list := mcp.call("tools/list", map[string]any{})
	tools, _ := list["tools"].([]any)
	found := false
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		if tool["name"] == "message_ack" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tools/list missing message_ack: %v", tools)
	}
}

func Test_Phase7_MCPConcurrentToolCalls(t *testing.T) {
	env := startAcceptanceHost(t)
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "intercom-mcp")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "./cmd/intercom-mcp")
	build.Dir = root
	build.Env = append(build.Environ(), "GOCACHE=/tmp/gocache", "GOFLAGS=-mod=mod", "GOPROXY=off", "GOSUMDB=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build intercom-mcp: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "--socket", env.socketPath, "--session", "ses_mcp0000000000000000000000", "--session-secret", testSessionSecret("ses_mcp0000000000000000000000"), "--harness", "mcp-test")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	reader := bufio.NewReader(stdout)
	var writeMu sync.Mutex
	send := func(id int, method string, params any) {
		raw, _ := json.Marshal(params)
		req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": json.RawMessage(raw)}
		b, _ := json.Marshal(req)
		writeMu.Lock()
		_, _ = stdin.Write(append(b, '\n'))
		writeMu.Unlock()
	}
	send(1, "initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read initialize: %v", err)
	}
	var initResp map[string]any
	if err := json.Unmarshal(line, &initResp); err != nil {
		t.Fatalf("decode initialize: %v", err)
	}

	// Fire N parallel tools/call requests with distinct ids; every
	// response must arrive with its own id and a non-error result.
	const n = 12
	var wg sync.WaitGroup
	results := make(chan map[string]any, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args, _ := json.Marshal(map[string]any{})
			send(100+i, "tools/call", map[string]any{"name": "peers_list", "arguments": json.RawMessage(args)})
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	// Drain responses until we have n with ids 100..100+n-1.
	got := map[int]bool{}
	timeout := make(chan struct{})
	go func() {
		for len(got) < n {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			var resp map[string]any
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idf, ok := resp["id"].(float64)
			if !ok {
				continue
			}
			id := int(idf)
			if id >= 100 && id < 100+n {
				if _, dup := got[id]; dup {
					t.Errorf("duplicate response for id %d", id)
				}
				got[id] = true
				if resp["error"] != nil {
					t.Errorf("response %d error: %v", id, resp["error"])
				}
			}
		}
		close(timeout)
	}()
	_ = results
	<-timeout
	if len(got) != n {
		t.Fatalf("got %d/%d concurrent responses: %v", len(got), n, got)
	}
	fmt.Println("concurrent MCP responses:", len(got))
}
