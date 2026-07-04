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
	// uploads are dedicated connections for blob data, one per concurrent
	// upload. SSH flow control caps in-flight data per channel at ~2 MiB,
	// so a single shared channel would bound aggregate upload throughput at
	// ~2 MiB per RTT. gRPC dials lazily so unused connections cost nothing.
	uploads []*client.Client
	tunnel  *sshTunnel
	lease   leases.Lease
	cpus    int
}

func openRemote(ctx context.Context, cfg pushConfig) (_ *remoteSink, retErr error) {
	tunnel, err := openSSHTunnel(ctx, cfg.SSHTarget)
	if err != nil {
		return nil, fmt.Errorf("open ssh tunnel: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = tunnel.Close()
		}
	}()

	// The address is a placeholder. The context dialer ignores it and routes
	// every connection through the SSH tunnel, but it needs a leading slash
	// so containerd's `unix://` prefix produces a valid URL. gRPC flow
	// control stays at its defaults: the client's windows only govern the
	// tiny response direction, the remote grows its receive windows via BDP
	// estimation, and the SSH channel window binds first anyway.
	newClient := func() (*client.Client, error) {
		return client.New(cfg.RemoteSocket,
			client.WithDefaultNamespace(remoteNamespace),
			client.WithExtraDialOpts([]grpc.DialOption{
				grpc.WithContextDialer(tunnel.dialer(cfg.RemoteSocket)),
			}),
		)
	}
	c, err := newClient()
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = c.Close()
		}
	}()

	uploads := make([]*client.Client, uploadConcurrency)
	defer func() {
		if retErr != nil {
			for _, u := range uploads {
				if u != nil {
					_ = u.Close()
				}
			}
		}
	}()
	for i := range uploads {
		if uploads[i], err = newClient(); err != nil {
			return nil, fmt.Errorf("containerd upload client: %w", err)
		}
	}

	// containerd doesn't auto-create namespaces and most RPCs fail against a
	// missing one.
	if err := c.NamespaceService().Create(ctx, remoteNamespace, nil); err != nil && !errdefs.IsAlreadyExists(err) {
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
		return nil, fmt.Errorf("create lease: %w", err)
	}

	cpus, err := tunnel.queryRemoteCPUs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not determine remote CPU count (%v), defaulting to 4\n", err)
		cpus = 4
	}

	return &remoteSink{client: c, uploads: uploads, tunnel: tunnel, lease: lease, cpus: cpus}, nil
}

func (r *remoteSink) Close() error {
	// Best-effort lease cleanup. The 1h gc.expire label is the safety net.
	_ = r.client.LeasesService().Delete(context.Background(), r.lease)
	for _, u := range r.uploads {
		_ = u.Close()
	}
	_ = r.client.Close()
	return r.tunnel.Close()
}
