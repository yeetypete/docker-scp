package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	linuxAmd64 = ocispec.Platform{OS: "linux", Architecture: "amd64"}
	linuxArm64 = ocispec.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
)

func TestAnyMatches(t *testing.T) {
	candidates := []ocispec.Platform{linuxAmd64, linuxArm64}
	if !anyMatches(linuxAmd64, candidates) {
		t.Error("expected linux/amd64 to match")
	}
	if anyMatches(ocispec.Platform{OS: "linux", Architecture: "riscv64"}, candidates) {
		t.Error("expected linux/riscv64 not to match")
	}
}

func TestFormatPlatforms(t *testing.T) {
	got := formatPlatforms([]ocispec.Platform{linuxAmd64, linuxArm64})
	want := "linux/amd64, linux/arm64/v8"
	if got != want {
		t.Errorf("formatPlatforms = %q, want %q", got, want)
	}
}

// memStore is an in-memory content.InfoReaderProvider for tests.
type memStore map[digest.Digest][]byte

func (s memStore) Info(_ context.Context, d digest.Digest) (content.Info, error) {
	b, ok := s[d]
	if !ok {
		return content.Info{}, fmt.Errorf("digest %s: %w", d, errdefs.ErrNotFound)
	}
	return content.Info{Digest: d, Size: int64(len(b))}, nil
}

func (s memStore) ReaderAt(_ context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	b, ok := s[desc.Digest]
	if !ok {
		return nil, fmt.Errorf("digest %s: %w", desc.Digest, errdefs.ErrNotFound)
	}
	return &memReaderAt{b: b}, nil
}

type memReaderAt struct{ b []byte }

func (r *memReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return copy(p, r.b[off:]), nil
}
func (r *memReaderAt) Size() int64  { return int64(len(r.b)) }
func (r *memReaderAt) Close() error { return nil }

func TestLocalIndexPlatforms(t *testing.T) {
	store := memStore{}
	add := func(b []byte) ocispec.Descriptor {
		d := digest.FromBytes(b)
		store[d] = b
		return ocispec.Descriptor{Digest: d, Size: int64(len(b))}
	}
	// manifestFor builds a one-layer manifest, storing only the requested
	// pieces to model partially pulled platforms.
	manifestFor := func(name string, storeManifest, storeConfig, storeLayer bool) ocispec.Descriptor {
		config := []byte(`{"name":"` + name + `"}`)
		layer := []byte(name + "-layer")
		m, err := json.Marshal(ocispec.Manifest{
			MediaType: ocispec.MediaTypeImageManifest,
			Config: ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageConfig,
				Digest:    digest.FromBytes(config),
				Size:      int64(len(config)),
			},
			Layers: []ocispec.Descriptor{{
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Digest:    digest.FromBytes(layer),
				Size:      int64(len(layer)),
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if storeConfig {
			add(config)
		}
		if storeLayer {
			add(layer)
		}
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    digest.FromBytes(m),
			Size:      int64(len(m)),
		}
		if storeManifest {
			add(m)
		}
		return desc
	}

	amd64 := manifestFor("amd64", true, true, true)
	amd64.Platform = &linuxAmd64
	arm64 := manifestFor("arm64", true, false, true) // manifest json without config
	arm64.Platform = &linuxArm64
	armv7 := manifestFor("armv7", false, true, true) // manifest never pulled
	armv7.Platform = &ocispec.Platform{OS: "linux", Architecture: "arm", Variant: "v7"}
	att := manifestFor("attestation", true, true, true)
	att.Platform = &ocispec.Platform{OS: "unknown", Architecture: "unknown"}

	idx, err := json.Marshal(ocispec.Index{
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{amd64, arm64, armv7, att},
	})
	if err != nil {
		t.Fatal(err)
	}
	target := add(idx)
	target.MediaType = ocispec.MediaTypeImageIndex

	got, err := localIndexPlatforms(context.Background(), store, target)
	if err != nil {
		t.Fatal(err)
	}
	if want := "linux/amd64"; len(got) != 1 || platforms.Format(got[0]) != want {
		t.Errorf("localIndexPlatforms = %v, want [%s]", formatPlatforms(got), want)
	}
}

func TestManifestComplete(t *testing.T) {
	config := []byte(`{"architecture":"arm64","os":"linux"}`)
	layer := []byte("layer-bytes")
	configDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(config),
		Size:      int64(len(config)),
	}
	layerDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromBytes(layer),
		Size:      int64(len(layer)),
	}
	manifest, err := json.Marshal(ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    []ocispec.Descriptor{layerDesc},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifest),
		Size:      int64(len(manifest)),
	}
	ctx := context.Background()

	full := memStore{manifestDesc.Digest: manifest, configDesc.Digest: config, layerDesc.Digest: layer}
	if ok, err := manifestComplete(ctx, full, manifestDesc); err != nil || !ok {
		t.Errorf("full store: got (%v, %v), want (true, nil)", ok, err)
	}

	// The docker containerd store can hold a sibling platform's manifest
	// json without its config or layers.
	noConfig := memStore{manifestDesc.Digest: manifest, layerDesc.Digest: layer}
	if ok, err := manifestComplete(ctx, noConfig, manifestDesc); err != nil || ok {
		t.Errorf("missing config: got (%v, %v), want (false, nil)", ok, err)
	}

	noManifest := memStore{}
	if ok, err := manifestComplete(ctx, noManifest, manifestDesc); err != nil || ok {
		t.Errorf("missing manifest: got (%v, %v), want (false, nil)", ok, err)
	}
}
