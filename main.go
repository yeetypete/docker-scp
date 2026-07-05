// Package main implements the scp docker CLI plugin: push docker/OCI images
// directly from a local containerd content store to a remote containerd over
// SSH, with no intermediate registry and no remote daemon.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/core/leases"
	ctrdlog "github.com/containerd/log"
	"github.com/containerd/platforms"
	"github.com/docker/cli/cli"
	"github.com/docker/cli/cli-plugins/metadata"
	"github.com/docker/cli/cli-plugins/plugin"
	"github.com/docker/cli/cli/command"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"golang.org/x/sync/errgroup"
)

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

func main() {
	plugin.Run(newScpCommand, metadata.Metadata{
		SchemaVersion:    "0.1.0",
		Vendor:           "scp",
		Version:          version,
		ShortDescription: "Push images directly to a remote containerd over SSH",
		URL:              "https://github.com/yeetypete/docker-scp",
	})
}

func newScpCommand(_ command.Cli) *cobra.Command {
	var cfg pushConfig
	var platformStrs []string
	cmd := &cobra.Command{
		Use:   "scp [OPTIONS] IMAGE [USER@]HOST[:PORT]",
		Short: "Push an image directly to a remote containerd over SSH",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			reqPlatforms, err := platforms.ParseAll(platformStrs)
			if err != nil {
				return err
			}
			cfg.ImageRef, cfg.SSHTarget, cfg.Platforms = args[0], args[1], reqPlatforms

			// Suppress containerd's internal log lines (snapshot cleanup
			// noise on cancel, etc.). Push-level errors surface via RunE.
			_ = ctrdlog.SetLevel("fatal")

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			err = push(ctx, cfg)
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return cli.StatusError{StatusCode: 130, Status: "cancelled"}
			}
			if err != nil {
				return fmt.Errorf("push failed: %w", err)
			}
			return nil
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	flags := cmd.Flags()
	flags.StringSliceVar(&platformStrs, "platform", nil,
		"Push specific platforms of a multi-platform image (comma-separated, e.g. linux/amd64,linux/arm64)")
	flags.StringVar(&cfg.LocalSocket, "local-socket", containerdSocketPath, "Local containerd socket path")
	flags.StringVar(&cfg.RemoteSocket, "remote-socket", containerdSocketPath, "Remote containerd socket path")
	return cmd
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
	// PopCompletedMode prints each completed bar once and drops it from the
	// live region. Keeping every bar live ghosts duplicate lines into
	// scrollback on each refresh once the bar count exceeds the terminal
	// height.
	prog := mpb.New(mpb.WithOutput(os.Stderr), mpb.WithWidth(40), mpb.PopCompletedMode())
	ps := newProgressState(prog)

	tracker := newReadiness(res.descs)
	waitStore := &waitingStore{Store: remote.client.ContentStore(), ready: tracker}

	// Nothing references the remote content and snapshots created below
	// until finalizeImage sets the gc.ref labels, so tie them to the push
	// lease or a concurrent remote GC can collect them mid-push.
	leaseCtx := leases.WithLease(ctx, remote.lease.ID)

	g, gctx := errgroup.WithContext(leaseCtx)
	g.Go(func() error {
		return transferBlobs(gctx, local, remote, res.descs, tracker, ps)
	})
	g.Go(func() error {
		return unpackRemote(gctx, remote, waitStore, res.img, res.platforms, res.descs, tracker, ps)
	})
	if err := g.Wait(); err != nil {
		return err
	}

	if err := finalizeImage(leaseCtx, remote, res.img, res.descs); err != nil {
		return fmt.Errorf("finalize image: %w", err)
	}

	ps.finalize()
	prog.Wait()
	fmt.Fprintf(os.Stderr, "Done in %s\n", time.Since(start).Round(time.Second))
	return nil
}
