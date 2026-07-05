package main

import (
	"context"
	"sync"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// digestSet returns the set of digests in descs.
func digestSet(descs []ocispec.Descriptor) map[digest.Digest]bool {
	s := make(map[digest.Digest]bool, len(descs))
	for _, d := range descs {
		s[d.Digest] = true
	}
	return s
}

// childrenIn returns a child handler that yields only the children whose
// digest is in `planned`. Used to keep walks scoped to the blobs we actually
// transferred (skipping wrong-platform manifests in a multi-arch index).
func childrenIn(store content.Store, planned map[digest.Digest]bool) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := images.Children(ctx, store, desc)
		if err != nil {
			return nil, err
		}
		kept := children[:0]
		for _, c := range children {
			if planned[c.Digest] {
				kept = append(kept, c)
			}
		}
		return kept, nil
	}
}

// readiness gates consumers (unpack, applier) on per-digest transfer events
// so unpack can overlap with the tail of transfer instead of waiting for it.
type readiness struct {
	mu sync.Mutex
	ch map[digest.Digest]chan struct{}
}

// newReadiness tracks the given descriptors, which resolveAndEnumerate has
// already deduplicated by digest.
func newReadiness(descs []ocispec.Descriptor) *readiness {
	m := make(map[digest.Digest]chan struct{}, len(descs))
	for _, d := range descs {
		m[d.Digest] = make(chan struct{})
	}
	return &readiness{ch: m}
}

// ready fires for digest d. Idempotent.
func (r *readiness) ready(d digest.Digest) {
	r.mu.Lock()
	ch, ok := r.ch[d]
	if ok {
		delete(r.ch, d)
	}
	r.mu.Unlock()
	if ok {
		close(ch)
	}
}

// wait blocks until ready(d) is called or ctx is cancelled. Digests not in
// the initial set return immediately (they aren't part of the transfer).
func (r *readiness) wait(ctx context.Context, d digest.Digest) error {
	r.mu.Lock()
	ch, tracked := r.ch[d]
	r.mu.Unlock()
	if !tracked {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitingStore makes ReaderAt block until the descriptor has been transferred.
// Other content.Store methods pass through unchanged.
type waitingStore struct {
	content.Store
	ready *readiness
}

func (s *waitingStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	if err := s.ready.wait(ctx, desc.Digest); err != nil {
		return nil, err
	}
	return s.Store.ReaderAt(ctx, desc)
}

// waitingApplier gates diff.Apply on the layer's transfer. Needed because
// apply runs server-side in containerd, bypassing our waitingStore.
type waitingApplier struct {
	inner diff.Applier
	ready *readiness
}

func (a *waitingApplier) Apply(ctx context.Context, desc ocispec.Descriptor, mounts []mount.Mount, opts ...diff.ApplyOpt) (ocispec.Descriptor, error) {
	if err := a.ready.wait(ctx, desc.Digest); err != nil {
		return ocispec.Descriptor{}, err
	}
	return a.inner.Apply(ctx, desc, mounts, opts...)
}
