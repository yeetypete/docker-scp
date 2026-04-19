package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc"
)

type remoteSink struct {
	client *client.Client
	tunnel *sshTunnel
	lease  leases.Lease
	cpus   int
}

func openRemote(ctx context.Context, cfg pushConfig) (*remoteSink, error) {
	tunnel, err := openSSHTunnel(ctx, cfg.SSHTarget)
	if err != nil {
		return nil, fmt.Errorf("open ssh tunnel: %w", err)
	}

	// Default HTTP/2 windows (64 KiB) cap per-stream throughput at window/RTT.
	// Bumping both windows lets concurrent blob uploads saturate the link.
	const (
		grpcStreamWindow = 16 * 1024 * 1024
		grpcConnWindow   = 64 * 1024 * 1024
	)
	// Address is a placeholder; our context dialer ignores it and routes all
	// connections through the SSH tunnel. Needs a leading slash so containerd's
	// `unix://` prefix produces a valid URL (path, not authority).
	c, err := client.New(remoteSocketPath,
		client.WithDefaultNamespace(remoteNamespace),
		client.WithExtraDialOpts([]grpc.DialOption{
			grpc.WithContextDialer(tunnel.dialer()),
			grpc.WithInitialWindowSize(grpcStreamWindow),
			grpc.WithInitialConnWindowSize(grpcConnWindow),
		}),
	)
	if err != nil {
		_ = tunnel.Close()
		return nil, fmt.Errorf("containerd client: %w", err)
	}

	// containerd doesn't auto-create namespaces and most RPCs fail against a
	// missing one.
	if err := c.NamespaceService().Create(ctx, remoteNamespace, nil); err != nil && !errdefs.IsAlreadyExists(err) {
		_ = c.Close()
		_ = tunnel.Close()
		return nil, fmt.Errorf("ensure namespace %q: %w", remoteNamespace, err)
	}

	lease, err := c.LeasesService().Create(ctx,
		leases.WithRandomID(),
		leases.WithExpiration(1*time.Hour),
		leases.WithLabels(map[string]string{
			"scp/push.image": cfg.ImageRef,
		}),
	)
	if err != nil {
		_ = c.Close()
		_ = tunnel.Close()
		return nil, fmt.Errorf("create lease: %w", err)
	}

	cpus, err := tunnel.queryRemoteCPUs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not determine remote CPU count (%v), defaulting to 4\n", err)
		cpus = 4
	}

	return &remoteSink{client: c, tunnel: tunnel, lease: lease, cpus: cpus}, nil
}

func (r *remoteSink) Close() error {
	// Best-effort lease cleanup. The 1h gc.expire.at label is the safety net.
	_ = r.client.LeasesService().Delete(context.Background(), r.lease)
	_ = r.client.Close()
	return r.tunnel.Close()
}
