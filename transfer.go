package main

import (
	"context"
	"fmt"
	"io"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
)

// transferBlobs uploads descs to the remote. ctx must carry the push lease.
func transferBlobs(ctx context.Context, src *localSource, dst *remoteSink, descs []ocispec.Descriptor, tracker *readiness, ps *progressState) error {
	// Sized to the concurrency limit so every in-flight upload gets a
	// dedicated connection (see remoteSink.uploads).
	stores := make(chan content.Store, len(dst.uploads))
	for _, u := range dst.uploads {
		stores <- u.ContentStore()
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(len(dst.uploads))
	for _, d := range descs {
		g.Go(func() error {
			store := <-stores
			defer func() { stores <- store }()
			if err := transferOne(gctx, src, store, d, ps.layerFor(d)); err != nil {
				return err
			}
			tracker.ready(d.Digest)
			return nil
		})
	}
	return g.Wait()
}

func transferOne(ctx context.Context, src *localSource, dst content.Store, d ocispec.Descriptor, lb *layerBar) (retErr error) {
	defer func() {
		if retErr != nil {
			lb.abort()
		} else {
			lb.transferFinish()
		}
	}()

	_, err := dst.Info(ctx, d.Digest)
	if err == nil {
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("stat remote %s: %w", d.Digest, err)
	}

	ra, err := src.ReaderAt(ctx, d)
	if err != nil {
		return fmt.Errorf("open local reader %s: %w", d.Digest, err)
	}
	defer func() { _ = ra.Close() }()

	w, err := content.OpenWriter(ctx, dst,
		content.WithRef("scp-upload-"+d.Digest.String()),
		content.WithDescriptor(d),
	)
	if err != nil {
		if errdefs.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("open writer %s: %w", d.Digest, err)
	}
	defer func() { _ = w.Close() }()

	reader := io.NewSectionReader(ra, 0, ra.Size())
	if err := content.Copy(ctx, w, lb.proxyReader(reader), d.Size, d.Digest); err != nil {
		return fmt.Errorf("copy %s: %w", d.Digest, err)
	}
	return nil
}

func finalizeImage(ctx context.Context, dst *remoteSink, img images.Image, transferred []ocispec.Descriptor) error {
	store := dst.client.ContentStore()

	// Skip children we didn't transfer. Otherwise ChildrenHandler reads a
	// blob for a platform we never pulled locally.
	handler := images.SetChildrenLabels(store, childrenIn(store, digestSet(transferred)))
	if err := images.Walk(ctx, handler, img.Target); err != nil {
		return fmt.Errorf("set gc labels: %w", err)
	}

	if _, err := dst.client.ImageService().Create(ctx, img); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return fmt.Errorf("create image: %w", err)
		}
		if _, err := dst.client.ImageService().Update(ctx, img); err != nil {
			return fmt.Errorf("update image: %w", err)
		}
	}
	return nil
}
