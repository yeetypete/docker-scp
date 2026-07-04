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
			_, err := store.Info(ctx, c.Digest)
			if err != nil {
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

// localIndexPlatforms returns platforms of index children whose manifest is
// locally present. Empty for single-manifest images.
func localIndexPlatforms(ctx context.Context, store content.Store, target ocispec.Descriptor) ([]ocispec.Platform, error) {
	if !images.IsIndexType(target.MediaType) {
		return nil, nil
	}
	children, err := images.Children(ctx, store, target)
	if err != nil {
		return nil, err
	}
	var present []ocispec.Platform
	for _, c := range children {
		if c.Platform == nil {
			continue
		}
		// Skip BuildKit attestation manifests, matching images.Platforms.
		if c.Platform.OS == "unknown" || c.Platform.Architecture == "unknown" {
			continue
		}
		_, err := store.Info(ctx, c.Digest)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		present = append(present, *c.Platform)
	}
	return present, nil
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
