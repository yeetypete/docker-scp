package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type localSource struct {
	client *client.Client
}

func openLocal(socketPath string) (*localSource, error) {
	if _, err := os.Stat(socketPath); err != nil {
		return nil, fmt.Errorf("local containerd socket %s: %w", socketPath, err)
	}
	c, err := client.New(socketPath, client.WithDefaultNamespace(localNamespace))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", socketPath, err)
	}
	return &localSource{client: c}, nil
}

func (l *localSource) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	return l.client.ContentStore().ReaderAt(ctx, desc)
}

func (l *localSource) Close() error { return l.client.Close() }

// resolvedImage is the outcome of resolveAndEnumerate: the image record, the
// descriptors to transfer, and the effective platforms for unpack.
type resolvedImage struct {
	img       images.Image
	descs     []ocispec.Descriptor
	platforms []ocispec.Platform
}

// resolveAndEnumerate resolves ref in the local containerd and enumerates the
// content to transfer. Empty requested means "every platform locally present".
func (l *localSource) resolveAndEnumerate(ctx context.Context, ref string, requested []ocispec.Platform) (resolvedImage, error) {
	named, err := reference.ParseDockerRef(ref)
	if err != nil {
		return resolvedImage{}, fmt.Errorf("parse ref: %w", err)
	}
	canonical := named.String()

	img, err := l.client.ImageService().Get(ctx, canonical)
	if err != nil {
		return resolvedImage{}, fmt.Errorf("image %q not found in local containerd: %w", canonical, err)
	}

	store := l.client.ContentStore()

	// Enumerate locally-present platforms up front so a missing --platform
	// errors cleanly here instead of deep in unpack as "content not found".
	localPlats, err := localIndexPlatforms(ctx, store, img.Target)
	if err != nil {
		return resolvedImage{}, fmt.Errorf("enumerate index: %w", err)
	}
	if images.IsIndexType(img.Target.MediaType) && len(localPlats) == 0 {
		return resolvedImage{}, fmt.Errorf("image %q has no platform variant fully present in local containerd", canonical)
	}
	if len(requested) > 0 && len(localPlats) > 0 {
		for _, p := range requested {
			if !anyMatches(p, localPlats) {
				return resolvedImage{}, fmt.Errorf(
					"local image has no %s variant. Available: %s",
					platforms.Format(p), formatPlatforms(localPlats))
			}
		}
	}

	var matcher platforms.MatchComparer
	if len(requested) > 0 {
		matcher = platforms.Any(requested...)
	}

	var descs []ocispec.Descriptor
	seen := make(map[digest.Digest]bool)
	handler := images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		// A blob can be reachable through several parents (e.g. a layer shared
		// by two platform manifests). Visit it once: duplicates in descs would
		// tie up an upload slot spin-waiting on the ingest ref lock.
		if seen[desc.Digest] {
			return nil, images.ErrSkipDesc
		}
		seen[desc.Digest] = true
		descs = append(descs, desc)
		children, err := images.Children(ctx, store, desc)
		if err != nil {
			return nil, err
		}
		// Following only locally-present children scopes the transfer to the
		// platforms the user actually pulled. Index entries carry a Platform
		// for the --platform filter, config and layer children don't.
		kept := children[:0]
		for _, c := range children {
			if matcher != nil && c.Platform != nil && !matcher.Match(*c.Platform) {
				continue
			}
			// Manifests count only when their whole subtree is local. Docker's
			// containerd store can hold a sibling platform's manifest json
			// without its content, and pushing that bare manifest leaves the
			// remote referencing blobs that were never transferred.
			if images.IsManifestType(c.MediaType) {
				ok, err := manifestComplete(ctx, store, c)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			} else if _, err := store.Info(ctx, c.Digest); err != nil {
				if errdefs.IsNotFound(err) {
					continue
				}
				return nil, err
			}
			kept = append(kept, c)
		}
		return kept, nil
	})
	if err := images.Walk(ctx, handler, img.Target); err != nil {
		return resolvedImage{}, fmt.Errorf("walk image: %w", err)
	}

	effective := localPlats
	if len(requested) > 0 {
		effective = requested
	}
	return resolvedImage{img: img, descs: descs, platforms: effective}, nil
}

// localIndexPlatforms returns the platforms an index declares that are fully
// present locally. Empty for single-manifest images. Checking with Only
// mirrors how unpack resolves manifests later, so a platform counts exactly
// when its unpack would find all content.
func localIndexPlatforms(ctx context.Context, store content.Provider, target ocispec.Descriptor) ([]ocispec.Platform, error) {
	if !images.IsIndexType(target.MediaType) {
		return nil, nil
	}
	declared, err := images.Platforms(ctx, store, target)
	if err != nil {
		return nil, err
	}
	var present []ocispec.Platform
	for _, p := range declared {
		available, _, _, missing, err := images.Check(ctx, store, target, platforms.Only(p))
		if err != nil {
			return nil, err
		}
		if available && len(missing) == 0 {
			present = append(present, p)
		}
	}
	return present, nil
}

// manifestComplete reports whether the manifest and its config and layers
// are all present in store. A bare presence check on the manifest blob
// misclassifies platforms: Docker's containerd store keeps sibling platform
// manifests whose content was never pulled. Check leaves available true when
// only blobs are missing, so both results matter.
func manifestComplete(ctx context.Context, store content.Provider, desc ocispec.Descriptor) (bool, error) {
	available, _, _, missing, err := images.Check(ctx, store, desc, platforms.All)
	if err != nil {
		return false, err
	}
	return available && len(missing) == 0, nil
}

func anyMatches(p ocispec.Platform, candidates []ocispec.Platform) bool {
	return slices.ContainsFunc(candidates, platforms.Only(p).Match)
}

func formatPlatforms(ps []ocispec.Platform) string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = platforms.Format(p)
	}
	return strings.Join(out, ", ")
}
