# `docker-scp`

A `docker` CLI plugin which pushes Docker / OCI images directly from a local
containerd content store to a remote containerd runtime over SSH. No intermediate
registry, and no remote daemon required.

Layers transfer over a gRPC connection tunneled through the SSH session and unpack
on the remote in parallel where the snapshotter advertises the `rebase` capability
(containerd 2.2+), or serially otherwise.

## Installation

Requires Go 1.26+ and a working `docker` CLI. Build and drop the binary into
Docker's plugin directory with:

```bash
make install-docker-plugin
```

This installs the plugin to `~/.docker/cli-plugins/docker-scp`. Verify with:

```bash
docker scp --help
```

To remove:

```bash
make uninstall-docker-plugin
```

## Usage

```bash
docker scp [--platform os/arch[/variant]] IMAGE [user@]host[:port]
```

### Examples

Push an image to a remote host:

```bash
docker scp ubuntu:24.04 user@remote
```

Select a specific platform from a multi-platform image:

```bash
docker scp --platform linux/arm64 ubuntu:24.04 user@remote
```

## Prerequisites

The plugin reads from the local containerd content store and writes to the
remote one over SSH, so **both the local and remote machines** need:

1. Docker's [containerd image store](https://docs.docker.com/engine/storage/containerd/) enabled.

1. Group access to `/run/containerd/containerd.sock`. The user running `docker scp`
   locally and the SSH user on the remote both need to be in the socket's group.

### Adding a containerd Group

Run these steps on **both machines**. Create a `containerd` group and add
your user to it:

```bash
sudo groupadd containerd
sudo usermod -aG containerd $USER
```

Tell containerd to chown the socket to that group by editing
`/etc/containerd/config.toml`:

```toml
[grpc]
  gid = 999 # replace with: getent group containerd | cut -d: -f3
```

Restart containerd and start a new login session so the group membership takes
effect:

```bash
sudo systemctl restart containerd
# log out and back in (or `newgrp containerd` in the current shell)
```

## Related

- [psviderski/unregistry](https://github.com/psviderski/unregistry): Same
  goal, but requires temporarily running a registry on the remote.
- [`podman image scp`](https://docs.podman.io/en/stable/markdown/podman-image-scp.1.html):
  Built into Podman. Transfers the full image via save/load over SSH (no
  layer dedup).
