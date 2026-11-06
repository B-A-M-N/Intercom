// Package conformance - Phase 8 adversarial tests: authorization,
// idempotency races, claim races, and heartbeat presence.
package conformance

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/B-A-M-N/intercom/internal/protocol"
)

func bindViaRPC(t *testing.T, socketPath, sessionID, alias, harness string) *rpcClient {
	t.Helper()
	c := dialRPC(t, socketPath)
	resp := c.call(t, protocol.MethodSessionRegister, map[string]any{
		"sessionId":      sessionID,
		"instanceId":     "ins_adv" + harness + sessionID[len(sessionID)-4:],
		"alias":          alias,
		"harness":        harness,
		"harnessVersion": "1.0.0",
		"capabilities":   []map[string]any{{"id": "delivery.notify", "support": "native"}},
		"deliveryModes":  []string{"notify"},
		"sessionSecret":  testSessionSecret(sessionID),
	})
	if resp.Error != nil {
		t.Fatalf("bind %s: %v", sessionID, resp.Error)
	}
	return c
}

func sendViaRPC(t *testing.T, c *rpcClient, commandID, to, text string) *protocol.RPCResponse {
	t.Helper()
	return c.call(t, protocol.MethodMessageSend, map[string]any{
		"commandId": commandID,
		"to":        map[string]any{"sessionId": to},
		"body":      map[string]any{"type": "text", "text": text},
		"delivery":  map[string]any{"mode": "notify", "wake": "none", "onUnsupported": "reject"},
	})
}

// A session must not be able to ack/report another session's message.
func Test_Phase8_CrossSessionAckForbidden(t *testing.T) {
	env := startAcceptanceHost(t)
	target := "ses_victim0000000000000000000"
	attacker := "ses_attacker00000000000000000"
	env.registerSession(target, "victim", "hermes")
	att := bindViaRPC(t, env.socketPath, attacker, "attacker", "mcp-test")

	sent := sendViaRPC(t, att, "cmd_adv00000000000000000001", target, "for victim")
	if sent.Error != nil {
		t.Fatalf("send: %v", sent.Error)
	}
	var res struct {
		MessageID string `json:"messageId"`
	}
	if err := json.Unmarshal(sent.Result, &res); err != nil || res.MessageID == "" {
		t.Fatalf("parse send result: %v (%s)", err, sent.Result)
	}

	// Attacker tries to report handled on victim's message.
	report := att.call(t, protocol.MethodMessageReport, map[string]any{
		"messageId": res.MessageID, "status": "handled", "detail": "forged",
	})
	if report.Error == nil {
		t.Fatalf("cross-session report must be forbidden, got result %s", report.Result)
	}

	// Attacker tries to claim victim's inbox.
	list := att.call(t, protocol.MethodMessageListPending, map[string]any{"sessionId": target})
	if list.Error == nil {
		t.Fatalf("cross-session inbox claim must be forbidden, got %s", list.Result)
	}

	// Victim (bound) can claim and report its own message.
	victim := bindViaRPC(t, env.socketPath, target, "victim", "hermes")
	own := victim.call(t, protocol.MethodMessageListPending, map[string]any{"sessionId": target})
	if own.Error != nil {
		t.Fatalf("own inbox claim: %v", own.Error)
	}
	if !strings.Contains(string(own.Result), res.MessageID) {
		t.Fatalf("own claim missing message: %s", own.Result)
	}
}

// Identical retries return the original message; conflicting reuse fails;
// concurrent duplicates admit exactly once.
func Test_Phase8_CommandIDIdempotency(t *testing.T) {
	env := startAcceptanceHost(t)
	to := "ses_recv00000000000000000000"
	sender := "ses_sender0000000000000000000"
	env.registerSession(to, "receiver", "hermes")
	c := bindViaRPC(t, env.socketPath, sender, "sender", "mcp-test")

	first := sendViaRPC(t, c, "cmd_dup00000000000000000001", to, "same body")
	if first.Error != nil {
		t.Fatalf("first send: %v", first.Error)
	}
	var r1 struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal(first.Result, &r1)

	retry := sendViaRPC(t, c, "cmd_dup00000000000000000001", to, "same body")
	if retry.Error != nil {
		t.Fatalf("identical retry: %v", retry.Error)
	}
	var r2 struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal(retry.Result, &r2)
	if r1.MessageID == "" || r1.MessageID != r2.MessageID {
		t.Fatalf("retry must return original message id: %q vs %q", r1.MessageID, r2.MessageID)
	}

	conflict := sendViaRPC(t, c, "cmd_dup00000000000000000001", to, "different body")
	if conflict.Error == nil {
		t.Fatalf("conflicting commandId reuse must fail, got %s", conflict.Result)
	}

	// Concurrent identical submissions: exactly one message id.
	const n = 8
	ids := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc := dialRPC(t, env.socketPath)
			// Bind this connection to the sender session first. Each
			// concurrent bind fences the previous holder (valid secret),
			// so all goroutines can bind and race the identical send.
			br := cc.call(t, protocol.MethodSessionRegister, map[string]any{
				"sessionId": sender, "instanceId": fmt.Sprintf("ins_conc%d", i),
				"alias": "sender", "harness": "mcp-test",
				"capabilities": []map[string]any{}, "deliveryModes": []string{},
				"sessionSecret": testSessionSecret(sender),
			})
			if br.Error != nil {
				t.Errorf("bind: %v", br.Error)
				return
			}
			resp := sendViaRPC(t, cc, "cmd_race0000000000000000001", to, "race body")
			if resp.Error != nil {
				t.Errorf("race send: %v", resp.Error)
				return
			}
			var rr struct {
				MessageID string `json:"messageId"`
			}
			_ = json.Unmarshal(resp.Result, &rr)
			ids <- rr.MessageID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]int{}
	for id := range ids {
		seen[id]++
	}
	if len(seen) != 1 {
		t.Fatalf("concurrent duplicates admitted %d distinct messages: %v", len(seen), seen)
	}
}

// Two MCP processes bound to the same session race to claim one message:
// exactly one gets it.
func Test_Phase8_ClaimRaceExactlyOneWins(t *testing.T) {
	env := startAcceptanceHost(t)
	target := "ses_racey0000000000000000000"
	sender := "ses_sender0000000000000000000"
	env.registerSession(target, "racey", "hermes")

	s := startMCP(t, env.socketPath, sender)
	_ = s.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "1"}})
	sendRes := s.tool("message_send", map[string]any{"commandId": "cmd_claimrace0000000000001", "to": target, "text": "one message", "mode": "notify"})
	if sendRes["isError"] == true {
		t.Fatalf("send isError: %v", sendRes)
	}
	var parsed struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal([]byte(toolText(t, sendRes)), &parsed)

	a := startMCP(t, env.socketPath, target)
	b := startMCP(t, env.socketPath, target)
	for _, m := range []*mcpClient{a, b} {
		_ = m.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "1"}})
	}
	var wg sync.WaitGroup
	got := make(chan string, 2)
	for _, m := range []*mcpClient{a, b} {
		wg.Add(1)
		go func(m *mcpClient) {
			defer wg.Done()
			inbox := m.tool("inbox_list", map[string]any{})
			got <- toolText(t, inbox)
		}(m)
	}
	wg.Wait()
	close(got)
	wins := 0
	for text := range got {
		if strings.Contains(text, parsed.MessageID) {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("exactly one claimer must win, got %d", wins)
	}
}

// Heartbeat from a bound session reports online presence.
func Test_Phase8_HeartbeatReportsOnline(t *testing.T) {
	env := startAcceptanceHost(t)
	ses := "ses_beat00000000000000000000"
	c := bindViaRPC(t, env.socketPath, ses, "beater", "mcp-test")
	resp := c.call(t, protocol.MethodHeartbeat, map[string]any{})
	if resp.Error != nil {
		t.Fatalf("heartbeat: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "online") {
		t.Fatalf("heartbeat presence not online: %s", resp.Result)
	}
	// Unbound caller cannot heartbeat as a session.
	anon := dialRPC(t, env.socketPath)
	bad := anon.call(t, protocol.MethodHeartbeat, map[string]any{"sessionId": ses})
	if bad.Error == nil {
		t.Fatalf("unbound heartbeat must fail, got %s", bad.Result)
	}
}

// Sending to a nonexistent session is forbidden (no existence oracle
// beyond the generic refusal).
func Test_Phase8_SendToUnknownSessionForbidden(t *testing.T) {
	env := startAcceptanceHost(t)
	sender := "ses_sender0000000000000000000"
	c := bindViaRPC(t, env.socketPath, sender, "sender", "mcp-test")
	resp := sendViaRPC(t, c, "cmd_unknown0000000000000001", "ses_nope000000000000000000000", "hello?")
	if resp.Error == nil {
		t.Fatalf("send to unknown session must fail, got %s", resp.Result)
	}
}
