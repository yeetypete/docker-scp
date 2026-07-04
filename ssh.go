package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	osuser "os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kevinburke/ssh_config"
	"github.com/skeema/knownhosts"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type sshTunnel struct {
	client *ssh.Client
}

func openSSHTunnel(ctx context.Context, target string) (*sshTunnel, error) {
	sshUser, host, port, err := resolveTarget(target)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(host, port)

	auth, err := sshAuthMethods()
	if err != nil {
		return nil, err
	}
	kh, err := openKnownHosts()
	if err != nil {
		return nil, err
	}

	// Pinning HostKeyAlgorithms to the types already present in known_hosts
	// avoids spurious "key mismatch" errors when the server offers a newer
	// algorithm than the one the user originally recorded.
	cfg := &ssh.ClientConfig{
		User:              sshUser,
		Auth:              auth,
		HostKeyCallback:   kh.HostKeyCallback(),
		HostKeyAlgorithms: kh.HostKeyAlgorithms(addr),
		Timeout:           10 * time.Second,
	}

	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	conn, err := d.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ssh handshake %s: %w", addr, err)
	}
	return &sshTunnel{client: ssh.NewClient(c, chans, reqs)}, nil
}

func (t *sshTunnel) Close() error { return t.client.Close() }

func (t *sshTunnel) queryRemoteCPUs() (int, error) {
	sess, err := t.client.NewSession()
	if err != nil {
		return 0, err
	}
	defer func() { _ = sess.Close() }()
	out, err := sess.Output("nproc")
	if err != nil {
		return 0, fmt.Errorf("nproc: %w", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("unexpected nproc output %q", out)
	}
	return n, nil
}

func (t *sshTunnel) dialer(socketPath string) func(ctx context.Context, _ string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return dialViaBridge(ctx, t.client, socketPath)
	}
}

// resolveTarget applies ~/.ssh/config overrides so Host aliases keep working.
// Config lookups use the host as typed (the alias), matching OpenSSH; the
// HostName substitution happens last.
func resolveTarget(target string) (string, string, string, error) {
	u, host, port := parseTarget(target)
	if host == "" {
		return "", "", "", fmt.Errorf("ssh target %q: missing host", target)
	}

	if u == "" {
		u = ssh_config.Get(host, "User")
	}
	if u == "" {
		cu, err := osuser.Current()
		if err != nil {
			return "", "", "", fmt.Errorf("current user: %w", err)
		}
		u = cu.Username
	}
	if port == "" {
		port = ssh_config.Get(host, "Port")
	}
	if port == "" {
		port = "22"
	}
	if hostname := ssh_config.Get(host, "HostName"); hostname != "" {
		host = hostname
	}
	return u, host, port, nil
}

// parseTarget splits [USER@]HOST[:PORT], leaving absent parts empty.
func parseTarget(target string) (user, host, port string) {
	hostport := target
	if before, after, ok := strings.Cut(target, "@"); ok {
		user, hostport = before, after
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		host, port = hostport, ""
	}
	return user, host, port
}

func sshAuthMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return methods, nil
	}
	var signers []ssh.Signer
	for _, name := range []string{"id_ed25519", "id_ecdsa", "id_rsa"} {
		pem, err := os.ReadFile(filepath.Join(home, ".ssh", name))
		if err != nil {
			continue
		}
		s, err := ssh.ParsePrivateKey(pem)
		if err != nil {
			continue
		}
		signers = append(signers, s)
	}
	if len(signers) > 0 {
		methods = append(methods, ssh.PublicKeys(signers...))
	}
	if len(methods) == 0 {
		return nil, errors.New("no ssh auth available (no SSH_AUTH_SOCK, no ~/.ssh/id_* key)")
	}
	return methods, nil
}

func openKnownHosts() (*knownhosts.HostKeyDB, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("user home: %w", err)
	}
	path := filepath.Join(home, ".ssh", "known_hosts")
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("known_hosts %s: %w", path, err)
	}
	return knownhosts.NewDB(path)
}

// dialViaBridge relays stdin/stdout to the remote unix socket via `nc -U`
// in an exec session. Avoids direct-streamlocal@openssh.com, which
// gliderlabs/ssh (Tailscale SSH) rejects as an unknown channel type.
func dialViaBridge(ctx context.Context, c *ssh.Client, path string) (net.Conn, error) {
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		sess, err := c.NewSession()
		if err != nil {
			done <- result{err: fmt.Errorf("ssh session: %w", err)}
			return
		}
		stdin, err := sess.StdinPipe()
		if err != nil {
			_ = sess.Close()
			done <- result{err: err}
			return
		}
		stdout, err := sess.StdoutPipe()
		if err != nil {
			_ = sess.Close()
			done <- result{err: err}
			return
		}
		// Without this, a missing or busybox nc surfaces only as EOF at grpc.
		sess.Stderr = os.Stderr
		if err := sess.Start("nc -U " + path); err != nil {
			_ = sess.Close()
			done <- result{err: fmt.Errorf("start bridge: %w", err)}
			return
		}
		done <- result{conn: &sessionConn{sess: sess, in: stdin, out: stdout}}
	}()
	select {
	case <-ctx.Done():
		// The session goroutine may still complete; reap its result so the
		// ssh session doesn't leak.
		go func() {
			if r := <-done; r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return nil, ctx.Err()
	case r := <-done:
		return r.conn, r.err
	}
}

// sessionConn adapts an ssh.Session's stdio to net.Conn. Set*Deadline are
// no-ops since gRPC manages its own timeouts via contexts.
type sessionConn struct {
	sess *ssh.Session
	in   io.WriteCloser
	out  io.Reader
}

func (c *sessionConn) Read(b []byte) (int, error)  { return c.out.Read(b) }
func (c *sessionConn) Write(b []byte) (int, error) { return c.in.Write(b) }
func (c *sessionConn) Close() error {
	_ = c.in.Close()
	return c.sess.Close()
}
func (c *sessionConn) LocalAddr() net.Addr              { return sshSessionAddr{} }
func (c *sessionConn) RemoteAddr() net.Addr             { return sshSessionAddr{} }
func (c *sessionConn) SetDeadline(time.Time) error      { return nil }
func (c *sessionConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sessionConn) SetWriteDeadline(time.Time) error { return nil }

type sshSessionAddr struct{}

func (sshSessionAddr) Network() string { return "ssh" }
func (sshSessionAddr) String() string  { return "ssh-session" }
