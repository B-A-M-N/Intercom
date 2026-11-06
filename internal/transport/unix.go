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
	"syscall"
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
	// lockFile holds the exclusive flock that proves single ownership of
	// path for this process's lifetime.
	lockFile *os.File
	// ownsDev/ownsIno identify the filesystem object this listener bound,
	// so Close only unlinks the path if it still names that object.
	ownsDev uint64
	ownsIno uint64
}

// ListenUnix binds a Unix domain socket at path.
//
// The socket file is created with restrictive permissions so that only the
// owning user can connect. Single ownership is enforced with an exclusive
// flock on <path>.lock held for the listener's lifetime: a second daemon
// starting on the same socket fails without disturbing the first. A stale
// socket file is removed only after a liveness check proves no listener
// answers on it; a live socket is never unlinked.
func ListenUnix(ctx context.Context, path string) (*UnixListener, error) {
	if path == "" {
		return nil, errors.New("transport: socket path is required")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("transport: create socket dir: %w", err)
		}
	}
	// Single-owner lock: exclusive, non-blocking. The lock file survives
	// only as a lock; the socket itself is what clients dial.
	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("transport: open socket lock: %w", err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("transport: socket %s is already owned by another daemon: %w", path, err)
	}
	// From here on, any failure must release the lock.
	owned := false
	defer func() {
		if !owned {
			_ = unlockAndClose(lockFile)
		}
	}()

	// Liveness check before treating an existing socket as stale. A live
	// listener accepts the dial; a stale file refuses. If the dial
	// succeeds, refuse to start rather than stealing the pathname.
	if _, statErr := os.Lstat(path); statErr == nil {
		if live := socketAlive(path); live {
			return nil, fmt.Errorf("transport: socket %s is live (another daemon owns it)", path)
		}
		// Stale: must be a socket file before removing.
		if fi, lerr := os.Lstat(path); lerr == nil {
			if fi.Mode()&os.ModeSocket == 0 {
				return nil, fmt.Errorf("transport: %s exists and is not a socket", path)
			}
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("transport: remove stale socket: %w", err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("transport: stat socket: %w", statErr)
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
	var dev, ino uint64
	if fi, serr := os.Lstat(path); serr == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			dev, ino = uint64(st.Dev), uint64(st.Ino)
		}
	}
	owned = true
	return &UnixListener{ln: ln, path: path, maxFrame: DefaultMaxFrameBytes, owned: true, lockFile: lockFile, ownsDev: dev, ownsIno: ino}, nil
}

// socketAlive reports whether a listener answers on the Unix socket at path.
func socketAlive(path string) bool {
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := d.Dial("unix", path)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func unlockAndClose(f *os.File) error {
	if f == nil {
		return nil
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return f.Close()
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

// Close shuts the listener down and removes the socket file only if it
// still names the filesystem object this listener bound.
func (l *UnixListener) Close() error {
	err := l.ln.Close()
	if l.owned && l.path != "" {
		if fi, serr := os.Lstat(l.path); serr == nil {
			if st, ok := fi.Sys().(*syscall.Stat_t); ok && l.ownsIno != 0 {
				if uint64(st.Dev) == l.ownsDev && uint64(st.Ino) == l.ownsIno {
					_ = os.Remove(l.path)
				}
			} else {
				_ = os.Remove(l.path)
			}
		}
	}
	_ = unlockAndClose(l.lockFile)
	l.lockFile = nil
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
//
// The write is flushed before returning: a caller writing a complete frame
// expects the peer to receive it without a separate flush step.
func (c *unixConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.writer.Write(p)
	if err != nil {
		return n, err
	}
	if err := c.writer.Flush(); err != nil {
		return n, err
	}
	return n, nil
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
