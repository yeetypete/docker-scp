package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestReadinessUntrackedReturnsImmediately(t *testing.T) {
	r := newReadiness(nil)
	if err := r.wait(context.Background(), digest.FromString("untracked")); err != nil {
		t.Fatalf("wait on untracked digest: %v", err)
	}
}

func TestReadinessBlocksUntilReady(t *testing.T) {
	d := ocispec.Descriptor{Digest: digest.FromString("a")}
	r := newReadiness([]ocispec.Descriptor{d})

	done := make(chan error, 1)
	go func() { done <- r.wait(context.Background(), d.Digest) }()
	select {
	case <-done:
		t.Fatal("wait returned before ready was called")
	case <-time.After(20 * time.Millisecond):
	}

	r.ready(d.Digest)
	if err := <-done; err != nil {
		t.Fatalf("wait after ready: %v", err)
	}

	// ready is idempotent and later waits return immediately.
	r.ready(d.Digest)
	if err := r.wait(context.Background(), d.Digest); err != nil {
		t.Fatalf("wait after second ready: %v", err)
	}
}

func TestReadinessWaitHonorsContext(t *testing.T) {
	d := ocispec.Descriptor{Digest: digest.FromString("b")}
	r := newReadiness([]ocispec.Descriptor{d})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.wait(ctx, d.Digest); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait on cancelled ctx = %v, want context.Canceled", err)
	}
}
