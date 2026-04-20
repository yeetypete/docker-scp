package main

import (
	"context"
	"fmt"
	"slices"

	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/unpack"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/containerd/v2/pkg/rootfs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/semaphore"
)

// unpackRemote uses core/unpack on snapshotters that advertise the "rebase"
// capability (containerd 2.2+). Older snapshotters need serial apply because
// parallel unpack races on the parent chain.
func unpackRemote(ctx context.Context, dst *remoteSink, store content.Store, img images.Image, plats []ocispec.Platform, descs []ocispec.Descriptor, tracker *readiness, ps *progressState) error {
	// waitingApplier must be the outer wrapper so the Extracting label only
	// fires after the layer has landed, not when apply is dispatched.
	applier := &waitingApplier{
		inner: &progressApplier{inner: dst.client.DiffService(), state: ps},
		ready: tracker,
	}

	plugin, err := findSnapshotter(ctx, dst, remoteSnapshotter)
	if err != nil {
		return fmt.Errorf("query snapshotter: %w", err)
	}
	if slices.Contains(plugin.GetCapabilities(), "rebase") {
		return unpackParallel(ctx, dst, store, img, unpackMatcher(plats), applier, descs)
	}
	if len(plats) == 0 {
		return unpackSerial(ctx, dst, store, img, platforms.All, applier)
	}
	for _, p := range plats {
		if err := unpackSerial(ctx, dst, store, img, platforms.Only(p), applier); err != nil {
			return err
		}
	}
	return nil
}

// unpackMatcher falls back to platforms.All when the caller has no filter;
// descs already scopes the walk to the user's selection.
func unpackMatcher(plats []ocispec.Platform) platforms.MatchComparer {
	if len(plats) == 0 {
		return platforms.All
	}
	return platforms.Any(plats...)
}

func findSnapshotter(ctx context.Context, dst *remoteSink, name string) (*introspectionapi.Plugin, error) {
	resp, err := dst.client.IntrospectionService().Plugins(ctx, "type==io.containerd.snapshotter.v1")
	if err != nil {
		return nil, err
	}
	for _, p := range resp.GetPlugins() {
		if p.GetID() == name {
			return p, nil
		}
	}
	return nil, fmt.Errorf("snapshotter %q not found on remote", name)
}

func unpackSerial(ctx context.Context, dst *remoteSink, cs content.Store, img images.Image, matcher platforms.MatchComparer, applier diff.Applier) error {
	sn := dst.client.SnapshotService(remoteSnapshotter)

	manifest, err := images.Manifest(ctx, cs, img.Target, matcher)
	if err != nil {
		return fmt.Errorf("resolve manifest: %w", err)
	}
	configDesc, err := images.Config(ctx, cs, img.Target, matcher)
	if err != nil {
		return fmt.Errorf("resolve config: %w", err)
	}
	diffIDs, err := images.RootFS(ctx, cs, configDesc)
	if err != nil {
		return fmt.Errorf("read rootfs: %w", err)
	}
	if len(manifest.Layers) != len(diffIDs) {
		return fmt.Errorf("manifest/config layer count mismatch: %d vs %d", len(manifest.Layers), len(diffIDs))
	}
	layers := make([]rootfs.Layer, len(manifest.Layers))
	for i, blob := range manifest.Layers {
		layers[i] = rootfs.Layer{
			Diff: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayer, Digest: diffIDs[i]},
			Blob: blob,
		}
	}

	chain := make([]digest.Digest, 0, len(layers))
	for i, layer := range layers {
		unpacked, err := rootfs.ApplyLayer(ctx, layer, chain, sn, applier)
		if err != nil {
			return fmt.Errorf("apply layer %d (%s): %w", i+1, short(layer.Blob.Digest), err)
		}
		if unpacked {
			if _, err := cs.Update(ctx, content.Info{
				Digest: layer.Blob.Digest,
				Labels: map[string]string{labels.LabelUncompressed: layer.Diff.Digest.String()},
			}, "labels."+labels.LabelUncompressed); err != nil {
				return fmt.Errorf("update uncompressed label: %w", err)
			}
		}
		chain = append(chain, layer.Diff.Digest)
	}

	gcLabel := "containerd.io/gc.ref.snapshot." + remoteSnapshotter
	if _, err := cs.Update(ctx, content.Info{
		Digest: configDesc.Digest,
		Labels: map[string]string{gcLabel: identity.ChainID(chain).String()},
	}, "labels."+gcLabel); err != nil {
		return fmt.Errorf("set gc.ref.snapshot: %w", err)
	}
	return nil
}

func unpackParallel(ctx context.Context, dst *remoteSink, store content.Store, img images.Image, matcher platforms.MatchComparer, applier diff.Applier, descs []ocispec.Descriptor) error {
	unpacker, err := unpack.NewUnpacker(ctx, store,
		unpack.WithUnpackPlatform(unpack.Platform{
			Platform:                matcher,
			SnapshotterKey:          remoteSnapshotter,
			Snapshotter:             dst.client.SnapshotService(remoteSnapshotter),
			SnapshotterCapabilities: []string{"rebase"},
			Applier:                 applier,
		}),
		unpack.WithLimiter(semaphore.NewWeighted(int64(dst.cpus))),
		unpack.WithUnpackLimiter(semaphore.NewWeighted(int64(dst.cpus))),
	)
	if err != nil {
		return fmt.Errorf("NewUnpacker: %w", err)
	}

	// Skip children we didn't transfer (e.g. wrong-platform manifests in a
	// multi-arch index). Checked against a local set rather than the remote
	// store so the filter doesn't stall waiting on per-child readiness.
	handler := unpacker.Unpack(childrenIn(store, digestSet(descs)))
	if err := images.Walk(ctx, handler, img.Target); err != nil {
		return fmt.Errorf("walk image: %w", err)
	}
	if _, err := unpacker.Wait(); err != nil {
		return fmt.Errorf("unpacker.Wait: %w", err)
	}
	return nil
}

func short(d digest.Digest) string {
	s := d.Encoded()
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

type progressApplier struct {
	inner diff.Applier
	state *progressState
}

func (a *progressApplier) Apply(ctx context.Context, desc ocispec.Descriptor, mounts []mount.Mount, opts ...diff.ApplyOpt) (ocispec.Descriptor, error) {
	lb := a.state.lookup(desc.Digest)
	if lb != nil {
		lb.extractBegin()
	}
	result, err := a.inner.Apply(ctx, desc, mounts, opts...)
	if lb != nil {
		if err != nil {
			lb.abort()
		} else {
			lb.extractFinish()
		}
	}
	return result, err
}
