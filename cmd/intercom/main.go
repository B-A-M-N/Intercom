// Command intercom is the command-line interface to an Intercom host.
//
// It speaks the same JSON-RPC extension as any other client, so the CLI is
// a peer rather than a privileged operator tool: it has no path to the
// durable store and no special authority.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bamn/intercom/internal/protocol"
	"github.com/bamn/intercom/internal/transport"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "intercom: %v\n", err)
		os.Exit(1)
	}
}

func usage() string {
	return `intercom - agent-agnostic inter-agent communication

Usage:
  intercom [--socket PATH] <command> [args]

Commands:
  hello                       Negotiate protocol features and capabilities
  peers                       List visible peers
  resolve <alias|session>     Resolve a peer identity
  presence <session>          Read a peer's presence
  capabilities <session>      Read a peer's capabilities
  send <session> <text>       Send a message
  reply <message> <text>      Send a correlated reply
  status <message>            Read delivery receipts
  state <session>             Read session state and recovery classification
  snapshot <session>          Obtain a coherent state snapshot
  replay <session> [cursor]   Replay the event stream
  subscribe <session>         Stream session events

Send options:
  --mode <steer|queue|notify>   Delivery mode (default: queue)
  --wake <none|if_idle|next_safe_point>
                                 Wake policy (default: none)
  --on-unsupported <reject|downgrade|notify>
                                 Fallback when the mode is unsupported
  --command-id <id>             Idempotency identity (default: generated)
  --reply-to <message>          Parent message identity
  --correlation-id <id>         Conversation or task correlation identity
  --ttl <seconds>               Message expiry

Global options:
  --socket PATH                 Host socket (default: $INTERCOM_SOCKET)
`
}

func run(args []string) error {
	socketPath := os.Getenv("INTERCOM_SOCKET")
	if socketPath == "" {
		socketPath = defaultSocketPath()
	}

	// Parse global options, then the command.
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--socket":
			if i+1 >= len(args) {
				return errors.New("--socket requires a path")
			}
			socketPath = args[i+1]
			i++
		case "-h", "--help":
			fmt.Print(usage())
			return nil
		default:
			rest = args[i:]
			i = len(args)
		}
	}
	if len(rest) == 0 {
		fmt.Print(usage())
		return errors.New("a command is required")
	}

	command := rest[0]
	opts := parseSendOptions(rest[1:])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := transport.DialUnix(ctx, socketPath)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", socketPath, err)
	}
	defer client.Close()

	c := &rpc{conn: client}

	switch command {
	case "hello":
		return c.callPrint(protocol.MethodHello, nil)
	case "peers":
		return c.callPrint(protocol.MethodPeerList, nil)
	case "resolve":
		if len(rest) < 2 {
			return errors.New("resolve requires an alias or session id")
		}
		return c.callPrint(protocol.MethodPeerResolve, map[string]any{"alias": rest[1]})
	case "presence":
		if len(rest) < 2 {
			return errors.New("presence requires a session id")
		}
		return c.callPrint(protocol.MethodPeerPresence, map[string]any{"sessionId": rest[1]})
	case "capabilities":
		if len(rest) < 2 {
			return errors.New("capabilities requires a session id")
		}
		return c.callPrint(protocol.MethodPeerCapabilities, map[string]any{"sessionId": rest[1]})
	case "send":
		return c.send(ctx, rest, opts)
	case "reply":
		return c.reply(ctx, rest, opts)
	case "status":
		if len(rest) < 2 {
			return errors.New("status requires a message id")
		}
		return c.callPrint(protocol.MethodMessageStatus, map[string]any{"messageId": rest[1]})
	case "state":
		if len(rest) < 2 {
			return errors.New("state requires a session id")
		}
		return c.callPrint(protocol.MethodSessionState, map[string]any{"sessionId": rest[1]})
	case "snapshot":
		if len(rest) < 2 {
			return errors.New("snapshot requires a session id")
		}
		return c.callPrint(protocol.MethodSessionSnapshot, map[string]any{"sessionId": rest[1]})
	case "replay":
		if len(rest) < 2 {
			return errors.New("replay requires a session id")
		}
		params := map[string]any{"sessionId": rest[1]}
		if len(rest) > 2 {
			cursor, err := strconv.ParseUint(rest[2], 10, 64)
			if err != nil {
				return fmt.Errorf("cursor must be a number: %w", err)
			}
			params["cursor"] = cursor
		}
		return c.callPrint(protocol.MethodSessionReplay, params)
	case "subscribe":
		if len(rest) < 2 {
			return errors.New("subscribe requires a session id")
		}
		return c.subscribe(ctx, rest[1])
	default:
		fmt.Print(usage())
		return fmt.Errorf("unknown command %q", command)
	}
}

// sendOptions carries the delivery options for a message submission.
type sendOptions struct {
	mode          string
	wake          string
	onUnsupported string
	commandID     string
	replyTo       string
	correlationID string
	ttl           int
}

// parseSendOptions parses the delivery options from the argument list.
func parseSendOptions(args []string) sendOptions {
	opts := sendOptions{
		mode:          string(protocol.DeliveryQueue),
		wake:          string(protocol.WakeNone),
		onUnsupported: string(protocol.UnsupportedReject),
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--mode":
			if i+1 < len(args) {
				opts.mode = args[i+1]
				i++
			}
		case "--wake":
			if i+1 < len(args) {
				opts.wake = args[i+1]
				i++
			}
		case "--on-unsupported":
			if i+1 < len(args) {
				opts.onUnsupported = args[i+1]
				i++
			}
		case "--command-id":
			if i+1 < len(args) {
				opts.commandID = args[i+1]
				i++
			}
		case "--reply-to":
			if i+1 < len(args) {
				opts.replyTo = args[i+1]
				i++
			}
		case "--correlation-id":
			if i+1 < len(args) {
				opts.correlationID = args[i+1]
				i++
			}
		case "--ttl":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					opts.ttl = v
				}
				i++
			}
		}
	}
	return opts
}

// rpc is a minimal JSON-RPC client over the transport.
type rpc struct {
	conn transport.Conn
	next int
}

// call sends a request and returns the decoded result.
func (c *rpc) call(ctx context.Context, method string, params any) (json.RawMessage, *protocol.RPCError) {
	c.next++
	id := c.next

	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, protocol.Errorf(protocol.CodeInvalidParams, "marshal params: %v", err)
		}
		raw = b
	}

	req := protocol.RPCRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage(strconv.Itoa(id)),
		Method:  method,
		Params:  raw,
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "marshal request: %v", err)
	}
	if _, err := c.conn.Write(append(b, '\n')); err != nil {
		return nil, protocol.Errorf(protocol.CodeInternalError, "write: %v", err)
	}

	_ = c.conn.SetDeadline(time.Now().Add(60 * time.Second))
	buf := make([]byte, 0, 4096)
	one := make([]byte, 1)
	for {
		n, err := c.conn.Read(one)
		if err != nil {
			return nil, protocol.Errorf(protocol.CodeInternalError, "read: %v", err)
		}
		if n == 0 {
			continue
		}
		if one[0] == '\n' {
			break
		}
		buf = append(buf, one[0])
	}

	var resp protocol.RPCResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, protocol.Errorf(protocol.CodeParseError, "decode response: %v", err)
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Result, nil
}

// callPrint sends a request and prints the result as indented JSON.
func (c *rpc) callPrint(method string, params any) error {
	result, rpcErr := c.call(context.Background(), method, params)
	if rpcErr != nil {
		return rpcErr
	}
	var pretty any
	if err := json.Unmarshal(result, &pretty); err != nil {
		fmt.Println(string(result))
		return nil
	}
	out, err := json.MarshalIndent(pretty, "", "  ")
	if err != nil {
		fmt.Println(string(result))
		return nil
	}
	fmt.Println(string(out))
	return nil
}

// send submits a message.
func (c *rpc) send(ctx context.Context, rest []string, opts sendOptions) error {
	target, text, err := parseTargetAndText(rest[1:])
	if err != nil {
		return err
	}
	commandID := opts.commandID
	if commandID == "" {
		commandID = newCommandID()
	}
	params := map[string]any{
		"commandId": commandID,
		"to":        map[string]any{"sessionId": target},
		"body":      map[string]any{"type": "text", "text": text},
		"delivery": map[string]any{
			"mode":          opts.mode,
			"wake":          opts.wake,
			"onUnsupported": opts.onUnsupported,
		},
	}
	if opts.replyTo != "" {
		params["replyTo"] = opts.replyTo
	}
	if opts.correlationID != "" {
		params["correlationId"] = opts.correlationID
	}
	if opts.ttl != 0 {
		params["ttlSeconds"] = opts.ttl
	}
	return c.callPrint(protocol.MethodMessageSend, params)
}

// reply sends a correlated reply.
func (c *rpc) reply(ctx context.Context, rest []string, opts sendOptions) error {
	if len(rest) < 3 {
		return errors.New("reply requires a message id and text")
	}
	commandID := opts.commandID
	if commandID == "" {
		commandID = newCommandID()
	}
	params := map[string]any{
		"commandId": commandID,
		"messageId": rest[1],
		"text":      strings.Join(rest[2:], " "),
	}
	if opts.correlationID != "" {
		params["correlationId"] = opts.correlationID
	}
	return c.callPrint(protocol.MethodMessageReply, params)
}

// subscribe streams session events until interrupted.
func (c *rpc) subscribe(ctx context.Context, sessionID string) error {
	if _, rpcErr := c.call(ctx, protocol.MethodSessionSubscribe, map[string]any{"sessionId": sessionID}); rpcErr != nil {
		return rpcErr
	}
	fmt.Fprintf(os.Stderr, "subscribed to %s; press Ctrl-C to stop\n", sessionID)

	_ = c.conn.SetDeadline(time.Time{})
	buf := make([]byte, 0, 4096)
	one := make([]byte, 1)
	for {
		if ctx.Err() != nil {
			return nil
		}
		n, err := c.conn.Read(one)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 {
			continue
		}
		if one[0] != '\n' {
			buf = append(buf, one[0])
			continue
		}
		if len(buf) == 0 {
			buf = buf[:0]
			continue
		}
		fmt.Println(string(buf))
		buf = buf[:0]
	}
}

// parseTargetAndText splits the target from the message text.
func parseTargetAndText(args []string) (string, string, error) {
	if len(args) < 2 {
		return "", "", errors.New("a session id and message text are required")
	}
	return args[0], strings.Join(args[1:], " "), nil
}

// defaultSocketPath returns the conventional host socket path.
func defaultSocketPath() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return dir + "/intercom/intercom.sock"
	}
	return os.TempDir() + "/intercom/intercom.sock"
}
