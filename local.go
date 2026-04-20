package main

import (
	"context"
	"fmt"
	"os"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type localSource struct {
	client *client.Client
}

func openLocal() (*localSource, error) {
	if _, err := os.Stat(containerdSocketPath); err != nil {
		return nil, fmt.Errorf("local containerd socket %s: %w", containerdSocketPath, err)
	}
	c, err := client.New(containerdSocketPath, client.WithDefaultNamespace(localNamespace))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", containerdSocketPath, err)
	}
	return &localSource{client: c}, nil
}

func (l *localSource) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	return l.client.ContentStore().ReaderAt(ctx, desc)
}

func (l *localSource) Close() error { return l.client.Close() }

func (l *localSource) resolveAndEnumerate(ctx context.Context, ref string, plat *ocispec.Platform) (images.Image, []ocispec.Descriptor, error) {
	named, err := reference.ParseDockerRef(ref)
	if err != nil {
		return images.Image{}, nil, fmt.Errorf("parse ref: %w", err)
	}
	canonical := named.String()

	img, err := l.client.ImageService().Get(ctx, canonical)
	if err != nil {
		return images.Image{}, nil, fmt.Errorf("image %q not found in local containerd: %w", canonical, err)
	}

	var matcher platforms.MatchComparer
	if plat != nil {
		matcher = platforms.Only(*plat)
	}

	var descs []ocispec.Descriptor
	store := l.client.ContentStore()
	handler := images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		descs = append(descs, desc)
		children, err := images.Children(ctx, store, desc)
		if err != nil {
			return nil, err
		}
		// Only follow children whose content is locally present. This naturally
		// scopes the transfer to whatever platform(s) the user pulled locally.
		// When --platform is set, further restrict to descriptors matching it
		// (index entries carry a Platform, config/layer children don't).
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
	err = images.Walk(ctx, handler, img.Target)
	if err != nil {
		return images.Image{}, nil, fmt.Errorf("walk image: %w", err)
	}
	return img, descs, nil
}
