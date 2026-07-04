package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in               string
		user, host, port string
	}{
		{"host", "", "host", ""},
		{"user@host", "user", "host", ""},
		{"user@host:2222", "user", "host", "2222"},
		{"host:2222", "", "host", "2222"},
		{"user@[::1]:2222", "user", "::1", "2222"},
		{"::1", "", "::1", ""},
		{"@host", "", "host", ""},
		{"", "", "", ""},
	}
	for _, tt := range tests {
		user, host, port := parseTarget(tt.in)
		if user != tt.user || host != tt.host || port != tt.port {
			t.Errorf("parseTarget(%q) = (%q, %q, %q), want (%q, %q, %q)",
				tt.in, user, host, port, tt.user, tt.host, tt.port)
		}
	}
}

func TestDialWithContextSuccess(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	conn, err := dialWithContext(context.Background(), func() (net.Conn, error) { return a, nil })
	if err != nil {
		t.Fatalf("dialWithContext: %v", err)
	}
	if conn != a {
		t.Fatal("dialWithContext returned a different conn than the dial func")
	}
}

func TestDialWithContextError(t *testing.T) {
	sentinel := errors.New("dial failed")
	_, err := dialWithContext(context.Background(), func() (net.Conn, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("dialWithContext err = %v, want %v", err, sentinel)
	}
}

func TestDialWithContextCancelReapsLateConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release := make(chan struct{})
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()

	_, err := dialWithContext(ctx, func() (net.Conn, error) { <-release; return a, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dialWithContext err = %v, want context.Canceled", err)
	}

	// Once the late dial completes, the reaper must close the conn; the pipe
	// peer observes that as EOF.
	close(release)
	_ = b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("peer read err = %v, want io.EOF", err)
	}
}
