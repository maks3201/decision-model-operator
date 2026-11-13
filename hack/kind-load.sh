#!/usr/bin/env bash
# kind-load.sh — load a local Docker image into a kind cluster.
#
# Usage: hack/kind-load.sh <image> <cluster> [extra `kind load` args]
#
# Why not plain `kind load docker-image`: with the containerd image store (the
# default in Docker 29, e.g. ubuntu-26.04 runners) a freshly pulled multi-platform
# image is saved as an index whose other-platform blobs are absent, and kind's
# `ctr images import --all-platforms` fails with "content digest ...: not found".
# Saving only the node platform avoids that. On older Docker (no `save --platform`)
# or when the platform is unknown, fall back to `kind load docker-image`.
set -euo pipefail

image="${1:?usage: kind-load.sh <image> <cluster> [kind load args]}"
cluster="${2:?usage: kind-load.sh <image> <cluster> [kind load args]}"
shift 2

# kind nodes run the host architecture.
case "$(uname -m)" in
  x86_64 | amd64) platform="linux/amd64" ;;
  aarch64 | arm64) platform="linux/arm64" ;;
  *) platform="" ;;
esac

if [[ -n "${platform}" ]] && docker save --help 2>/dev/null | grep -q -- '--platform'; then
  archive="$(mktemp "${TMPDIR:-/tmp}/kind-load.XXXXXX")"
  trap 'rm -f "${archive}"' EXIT
  # An image built locally for another platform has no such variant: fall back.
  if docker save --platform "${platform}" -o "${archive}" "${image}" 2>/dev/null; then
    kind load image-archive "${archive}" --name "${cluster}" "$@"
    exit 0
  fi
fi

kind load docker-image "${image}" --name "${cluster}" "$@"
