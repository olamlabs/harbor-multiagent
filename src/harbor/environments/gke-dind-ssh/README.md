# GKE Compose image

GKE Compose mode connects to its Docker-in-Docker pod over SSH. The image must
contain `/usr/sbin/sshd` and `dockerd-entrypoint.sh`; startup only writes the
trial's keys and configuration, starts sshd, and starts dockerd. It does not
download packages.

Build this image from the repository root:

```sh
docker build -t harbor-gke-dind-ssh:28.3.3 src/harbor/environments/gke-dind-ssh
```

Publish it to a registry the Kubernetes nodes can pull from, then pass its
immutable reference through `--ek dind_image=<image>@sha256:<digest>`.
The Harbor process does not pull this outer image; Kubernetes does.
`docker_config_path` configures the nested Docker daemon's task-image pulls,
not the Kubernetes node's pull of this image. Private registries need node-side
pull access. No registry is selected by the provider.

The base image pins Docker's version. Rebuild and republish when Docker or
OpenSSH needs an update, and update the configured image reference. The image
contains no trial keys; those are generated for each trial.

Original Harbor's GKE Compose environment starts the stock Docker-in-Docker
image and uses Kubernetes exec, without sshd. This fork uses SSH to avoid exec
stream interruption, frame-size limits, and binary-transfer corruption.
Preinstalling OpenSSH keeps that transport without a package-mirror dependency
on every pod startup. The tradeoff is a derived image to publish and maintain,
an extra image layer to pull, and an explicit `dind_image` for Compose mode.
