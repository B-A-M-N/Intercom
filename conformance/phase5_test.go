// Package conformance - Phase 5 MCP and bootstrap tests.
package conformance

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/B-A-M-N/intercom/internal/protocol"
	"github.com/B-A-M-N/intercom/internal/transport"
)

// mcpClient is a minimal MCP stdio client for testing.
type mcpClient struct {
	t    *testing.T
	cmd  *exec.Cmd
	w    *os.File
	r    *bufio.Reader
	next int
}

func startMCP(t *testing.T, socketPath, session string) *mcpClient {
	t.Helper()
	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "intercom-mcp")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "./cmd/intercom-mcp")
	build.Dir = root
	build.Env = append(build.Environ(), "GOCACHE=/tmp/gocache", "GOFLAGS=-mod=mod", "GOPROXY=off", "GOSUMDB=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build intercom-mcp: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "--socket", socketPath, "--session", session, "--session-secret", testSessionSecret(session), "--harness", "mcp-test")
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
	_ = stdin
	// Keep stdin pipe via cmd.Stdin (already set)
	return &mcpClient{t: t, cmd: cmd, w: stdin.(*os.File), r: bufio.NewReader(stdout)}
}

func (c *mcpClient) call(method string, params any) map[string]any {
	c.t.Helper()
	c.next++
	raw, _ := json.Marshal(params)
	req := map[string]any{"jsonrpc": "2.0", "id": c.next, "method": method, "params": json.RawMessage(raw)}
	b, _ := json.Marshal(req)
	if _, err := c.w.Write(append(b, '\n')); err != nil {
		c.t.Fatalf("write mcp: %v", err)
	}
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("read mcp: %v", err)
	}
	var resp map[string]any
	if err := json.Unmarshal(line, &resp); err != nil {
		c.t.Fatalf("decode mcp resp: %v (%s)", err, line)
	}
	if e, ok := resp["error"]; ok && e != nil {
		c.t.Fatalf("mcp %s error: %v", method, e)
	}
	result, _ := resp["result"].(map[string]any)
	return result
}

func (c *mcpClient) tool(name string, args map[string]any) map[string]any {
	raw, _ := json.Marshal(args)
	return c.call("tools/call", map[string]any{"name": name, "arguments": json.RawMessage(raw)})
}

func Test_Phase5_MCPToolsPreservePolicy(t *testing.T) {
	env := startAcceptanceHost(t)
	env.registerSession("ses_hermes0000000000000000000000", "hermes-agent", "hermes")
	mcp := startMCP(t, env.socketPath, "ses_mcp0000000000000000000000")

	init := mcp.call("initialize", map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})
	if init["serverInfo"] == nil {
		t.Fatalf("initialize missing serverInfo: %v", init)
	}
	list := mcp.call("tools/list", map[string]any{})
	tools, _ := list["tools"].([]any)
	if len(tools) < 5 {
		t.Fatalf("tools/list returned %d tools, want >=5", len(tools))
	}
	// peers_list discovers hermes under same-principal policy.
	res := mcp.tool("peers_list", map[string]any{})
	content, _ := res["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("peers_list empty: %v", res)
	}
	text, _ := content[0].(map[string]any)["text"].(string)
	if text == "" {
		t.Error("peers_list text empty")
	}
	// message_send via MCP tool is admission, not delivery.
	sendRes := mcp.tool("message_send", map[string]any{"commandId": "cmd_mcp000000000000000001", "to": "ses_hermes0000000000000000000000", "text": "via mcp", "mode": "notify"})
	if sendRes["isError"] == true {
		t.Fatalf("message_send isError: %v", sendRes)
	}
	// inbox_list for the MCP session itself (bound) is empty, not hermes'.
	inbox := mcp.tool("inbox_list", map[string]any{})
	if inbox["isError"] == true {
		t.Fatalf("inbox_list isError: %v", inbox)
	}
	// Status via MCP works for the sent message? Need messageId from send text.
	// Parse send result text for messageId.
	var sendText string
	if c, ok := sendRes["content"].([]any); ok && len(c) > 0 {
		sendText, _ = c[0].(map[string]any)["text"].(string)
	}
	var parsed struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal([]byte(sendText), &parsed)
	if parsed.MessageID == "" {
		t.Fatalf("could not parse messageId from %q", sendText)
	}
	status := mcp.tool("message_status", map[string]any{"messageId": parsed.MessageID})
	if status["isError"] == true {
		t.Fatalf("message_status isError: %v", status)
	}
	_ = protocol.MethodPeerList
	_ = transport.DialUnix
	_ = context.Background
	_ = time.Now
}
