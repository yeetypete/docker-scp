// Package main implements the scp docker CLI plugin: push docker/OCI images
// directly from a local containerd content store to a remote containerd over
// SSH, with no intermediate registry and no remote daemon.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	ctrdlog "github.com/containerd/log"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/pflag"
	"github.com/vbauerster/mpb/v8"
	"golang.org/x/sync/errgroup"
)

const usage = `Usage:  docker scp [OPTIONS] IMAGE [USER@]HOST[:PORT]

Push an image directly to a remote containerd over SSH

Options:
      --local-socket string    Local containerd socket path (default "/run/containerd/containerd.sock")
      --platform strings       Push specific platforms of a multi-platform image
                               (comma-separated, e.g. linux/amd64,linux/arm64)
      --remote-socket string   Remote containerd socket path (default "/run/containerd/containerd.sock")`

const (
	version              = "0.0.1"
	containerdSocketPath = "/run/containerd/containerd.sock"
	remoteNamespace      = "moby"
	remoteSnapshotter    = "overlayfs"
	localNamespace       = "moby"
	// uploadConcurrency is the number of concurrent blob uploads, each on a
	// dedicated gRPC connection (its own SSH channel).
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
	os.Exit(run())
}

func run() int {
	args := os.Args[1:]

	// Docker CLI plugins get their subcommand name as the first arg. Strip
	// it so the rest of argv matches what the user typed.
	bin := filepath.Base(os.Args[0])
	if sub, ok := strings.CutPrefix(bin, "docker-"); ok && len(args) > 0 && args[0] == sub {
		args = args[1:]
	}

	if len(args) > 0 && args[0] == "docker-cli-plugin-metadata" {
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

	fs := pflag.NewFlagSet("docker scp", pflag.ContinueOnError)
	fs.SetOutput(io.Discard)
	platformStrs := fs.StringSlice("platform", nil, "")
	localSocket := fs.String("local-socket", containerdSocketPath, "")
	remoteSocket := fs.String("remote-socket", containerdSocketPath, "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			_, _ = fmt.Fprintln(os.Stdout, usage)
			return 0
		}
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	positional := fs.Args()
	if len(positional) != 2 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}

	var reqPlatforms []ocispec.Platform
	for _, s := range *platformStrs {
		p, err := platforms.Parse(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --platform %q: %v\n", s, err)
			return 2
		}
		reqPlatforms = append(reqPlatforms, p)
	}

	// Suppress containerd's internal log lines (snapshot cleanup noise on
	// cancel, etc.). Push-level errors surface via stderr from push().
	_ = ctrdlog.SetLevel("fatal")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := pushConfig{
		ImageRef:     positional[0],
		SSHTarget:    positional[1],
		Platforms:    reqPlatforms,
		LocalSocket:  *localSocket,
		RemoteSocket: *remoteSocket,
	}
	err := push(ctx, cfg)
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "cancelled")
		return 130
	}
	fmt.Fprintf(os.Stderr, "push failed: %v\n", err)
	return 1
}

type pushConfig struct {
	ImageRef  string
	SSHTarget string
	// Empty Platforms means "every platform locally present".
	Platforms    []ocispec.Platform
	LocalSocket  string
	RemoteSocket string
}

func push(ctx context.Context, cfg pushConfig) error {
	local, err := openLocal(cfg.LocalSocket)
	if err != nil {
		return fmt.Errorf("open local containerd: %w", err)
	}
	defer func() { _ = local.Close() }()

	res, err := local.resolveAndEnumerate(ctx, cfg.ImageRef, cfg.Platforms)
	if err != nil {
		return fmt.Errorf("resolve image: %w", err)
	}

	remote, err := openRemote(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open remote containerd: %w", err)
	}
	defer func() { _ = remote.Close() }()

	fmt.Fprintf(os.Stderr, "Pushing %s to %s\n", cfg.ImageRef, cfg.SSHTarget)
	start := time.Now()
	prog := mpb.New(mpb.WithOutput(os.Stderr), mpb.WithWidth(40))
	ps := newProgressState(prog)

	tracker := newReadiness(res.descs)
	waitStore := &waitingStore{Store: remote.client.ContentStore(), ready: tracker}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return transferBlobs(gctx, local, remote, res.descs, tracker, ps)
	})
	g.Go(func() error {
		return unpackRemote(gctx, remote, waitStore, res.img, res.platforms, res.descs, tracker, ps)
	})
	if err := g.Wait(); err != nil {
		return err
	}

	if err := finalizeImage(ctx, remote, res.img, res.descs); err != nil {
		return fmt.Errorf("finalize image: %w", err)
	}

	ps.finalize()
	prog.Wait()
	fmt.Fprintf(os.Stderr, "Done in %s\n", time.Since(start).Round(time.Second))
	return nil
}
