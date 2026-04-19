// Package main implements the scp docker CLI plugin: push docker/OCI images
// directly from a local containerd content store to a remote containerd over
// SSH, with no intermediate registry and no remote daemon.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	ctrdlog "github.com/containerd/log"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vbauerster/mpb/v8"
	"golang.org/x/sync/errgroup"
)

const usage = "Usage: docker scp [--platform os/arch[/variant]] IMAGE [user@]host[:port]"

const (
	version           = "0.0.1"
	remoteSocketPath  = "/run/containerd/containerd.sock"
	remoteNamespace   = "moby"
	remoteSnapshotter = "overlayfs"
	localNamespace    = "moby"
	uploadConcurrency = 6
)

type pluginMetadata struct {
	SchemaVersion    string `json:"SchemaVersion"`
	Vendor           string `json:"Vendor"`
	Version          string `json:"Version"`
	ShortDescription string `json:"ShortDescription"`
	URL              string `json:"URL,omitempty"`
}

func main() {
	if code := run(); code != 0 {
		os.Exit(code)
	}
}

func run() int {
	args := os.Args[1:]

	// Docker CLI plugins get their subcommand name as the first arg; strip
	// it so the rest of argv matches what the user typed.
	bin := filepath.Base(os.Args[0])
	if strings.HasPrefix(bin, "docker-") && len(args) > 0 {
		if args[0] == strings.TrimPrefix(bin, "docker-") {
			args = args[1:]
		}
	}

	if len(args) >= 1 && args[0] == "docker-cli-plugin-metadata" {
		if err := json.NewEncoder(os.Stdout).Encode(pluginMetadata{
			SchemaVersion:    "0.1.0",
			Vendor:           "scp",
			Version:          version,
			ShortDescription: "Push images directly to a remote containerd over SSH",
			URL:              "https://github.com/yeetypete/scp",
		}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}

	fs := flag.NewFlagSet("docker scp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	platformStr := fs.String("platform", "", "")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	positional := fs.Args()
	if len(positional) != 2 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}

	var platform *ocispec.Platform
	if *platformStr != "" {
		p, err := platforms.Parse(*platformStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --platform %q: %v\n", *platformStr, err)
			return 2
		}
		platform = &p
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	// Suppress containerd's internal log lines (snapshot cleanup noise on
	// cancel, etc.); our own slog surfaces anything push-level.
	_ = ctrdlog.SetLevel("fatal")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := pushConfig{ImageRef: positional[0], SSHTarget: positional[1], Platform: platform}
	err := push(ctx, cfg)
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "cancelled")
		return 130
	}
	slog.Error("push failed", "error", err)
	return 1
}

type pushConfig struct {
	ImageRef  string
	SSHTarget string
	// Platform, if set, restricts the push to a specific platform manifest
	// inside a multi-arch index and overrides remote platform detection for
	// unpack.
	Platform *ocispec.Platform
}

func push(ctx context.Context, cfg pushConfig) error {
	local, err := openLocal(ctx)
	if err != nil {
		return fmt.Errorf("open local containerd: %w", err)
	}
	defer func() { _ = local.Close() }()

	img, descs, err := local.resolveAndEnumerate(ctx, cfg.ImageRef, cfg.Platform)
	if err != nil {
		return fmt.Errorf("resolve image: %w", err)
	}

	remote, err := openRemote(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open remote containerd: %w", err)
	}
	defer func() { _ = remote.Close() }()

	var remotePlatform ocispec.Platform
	if cfg.Platform != nil {
		remotePlatform = *cfg.Platform
	} else {
		remotePlatform, err = remoteHostPlatform(ctx, remote)
		if err != nil {
			return fmt.Errorf("detect remote platform: %w", err)
		}
	}

	fmt.Fprintf(os.Stderr, "Pushing %s to %s\n", cfg.ImageRef, cfg.SSHTarget)
	start := time.Now()
	prog := mpb.New(mpb.WithOutput(os.Stderr), mpb.WithWidth(40))
	ps := newProgressState(prog)

	tracker := newReadiness(descs)
	waitStore := &waitingStore{Store: remote.client.ContentStore(), ready: tracker}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return transferBlobs(gctx, local, remote, descs, tracker, ps)
	})
	g.Go(func() error {
		return unpackRemote(gctx, remote, waitStore, img, remotePlatform, descs, tracker, ps)
	})
	if err := g.Wait(); err != nil {
		return err
	}

	if err := finalizeImage(ctx, remote, img, descs); err != nil {
		return fmt.Errorf("finalize image: %w", err)
	}

	ps.finalize()
	prog.Wait()
	fmt.Fprintf(os.Stderr, "Done in %s\n", time.Since(start).Round(time.Second))
	return nil
}
