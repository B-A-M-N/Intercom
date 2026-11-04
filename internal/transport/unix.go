package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Framing limits for the local transport (ICSP-001 section 10.4).
const (
	// DefaultMaxFrameBytes bounds a single JSON-RPC frame.
	DefaultMaxFrameBytes = 8 << 20 // 8 MiB
	// DefaultReadTimeout bounds how long a read may block.
	DefaultReadTimeout = 5 * time.Minute
)

// UnixListener listens on a Unix domain socket and authenticates peers with
// SO_PEERCRED.
//
// Local process identity is verified by the kernel, so a connection's peer
// identity cannot be forged by the connecting process.
type UnixListener struct {
	ln       *net.UnixListener
	path     string
	maxFrame int
	owned    bool
}

// ListenUnix binds a Unix domain socket at path.
//
// The socket file is created with restrictive permissions so that only the
// owning user can connect.
func ListenUnix(ctx context.Context, path string) (*UnixListener, error) {
	if path == "" {
		return nil, errors.New("transport: socket path is required")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("transport: create socket dir: %w", err)
		}
	}
	// Remove a stale socket left by a previous run. A live socket would fail
	// to bind anyway, so this cannot disconnect a running host.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("transport: remove stale socket: %w", err)
	}

	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		return nil, fmt.Errorf("transport: resolve socket addr: %w", err)
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen unix: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("transport: chmod socket: %w", err)
	}

	return &UnixListener{ln: ln, path: path, maxFrame: DefaultMaxFrameBytes, owned: true}, nil
}

// Accept waits for the next connection and authenticates it.
func (l *UnixListener) Accept(ctx context.Context) (Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	raw, err := l.ln.Accept()
	if err != nil {
		return nil, fmt.Errorf("transport: accept: %w", err)
	}
	uc, ok := raw.(*net.UnixConn)
	if !ok {
		raw.Close()
		return nil, errors.New("transport: unexpected connection type")
	}

	identity, err := peerCredentials(uc)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("transport: peer credentials: %w", err)
	}

	return &unixConn{
		UnixConn: uc,
		reader:   bufio.NewReaderSize(uc, 64<<10),
		writer:   bufio.NewWriterSize(uc, 64<<10),
		identity: identity,
		maxFrame: l.maxFrame,
	}, nil
}

// Addr returns the listener's socket path.
func (l *UnixListener) Addr() net.Addr { return l.ln.Addr() }

// Close shuts the listener down and removes the socket file.
func (l *UnixListener) Close() error {
	err := l.ln.Close()
	if l.owned && l.path != "" {
		_ = os.Remove(l.path)
	}
	return err
}

// peerCredentials reads the kernel-verified peer identity of a Unix socket.
func peerCredentials(c *net.UnixConn) (PeerIdentity, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return PeerIdentity{}, fmt.Errorf("transport: syscall conn: %w", err)
	}

	var cred *unix.Ucred
	var sockErr error
	ctrlErr := raw.Control(func(fd uintptr) {
		cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if ctrlErr != nil {
		return PeerIdentity{}, fmt.Errorf("transport: control: %w", ctrlErr)
	}
	if sockErr != nil {
		return PeerIdentity{}, fmt.Errorf("transport: getsockopt ucred: %w", sockErr)
	}

	return PeerIdentity{
		PID:           int(cred.Pid),
		UID:           cred.Uid,
		GID:           cred.Gid,
		Authenticated: true,
		Method:        "SO_PEERCRED",
	}, nil
}

// unixConn is an authenticated Unix socket connection.
type unixConn struct {
	*net.UnixConn
	reader   *bufio.Reader
	writer   *bufio.Writer
	identity PeerIdentity
	maxFrame int

	writeMu sync.Mutex
}

// PeerIdentity returns the kernel-verified peer identity.
func (c *unixConn) PeerIdentity() PeerIdentity { return c.identity }

// ReadFrame reads one newline-delimited JSON-RPC frame.
//
// Frames are newline delimited so that a reader never has to guess where a
// message ends. Oversized frames are rejected rather than buffered.
func (c *unixConn) ReadFrame(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = c.SetReadDeadline(time.Now().Add(DefaultReadTimeout))

	var buf []byte
	for {
		chunk, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			return nil, err
		}
		if len(buf)+len(chunk) > c.maxFrame {
			return nil, fmt.Errorf("transport: frame exceeds %d bytes", c.maxFrame)
		}
		buf = append(buf, chunk...)
		if !isPrefix {
			return buf, nil
		}
	}
}

// WriteFrame writes one newline-delimited JSON-RPC frame.
func (c *unixConn) WriteFrame(ctx context.Context, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_ = c.SetWriteDeadline(time.Now().Add(DefaultReadTimeout))
	if _, err := c.writer.Write(frame); err != nil {
		return err
	}
	if err := c.writer.WriteByte('\n'); err != nil {
		return err
	}
	return c.writer.Flush()
}

// Read implements io.Reader for callers that want raw byte access.
func (c *unixConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// Write implements io.Writer for callers that want raw byte access.
func (c *unixConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writer.Write(p)
}

// Close shuts the connection down.
func (c *unixConn) Close() error { return c.UnixConn.Close() }

// DialUnix connects to a Unix socket and returns the local side.
func DialUnix(ctx context.Context, path string) (Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	raw, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("transport: dial unix: %w", err)
	}
	uc, ok := raw.(*net.UnixConn)
	if !ok {
		raw.Close()
		return nil, errors.New("transport: unexpected connection type")
	}
	return &unixConn{
		UnixConn: uc,
		reader:   bufio.NewReaderSize(uc, 64<<10),
		writer:   bufio.NewWriterSize(uc, 64<<10),
		maxFrame: DefaultMaxFrameBytes,
	}, nil
}

// WriteJSONFrame writes a JSON value as a single frame.
func WriteJSONFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("transport: marshal frame: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}
