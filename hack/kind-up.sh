#!/usr/bin/env bash
# kind-up.sh — ensure a kind cluster and load the Ollaya runtime image into it.
# Idempotent and safe to run repeatedly.
#
# Honours two env vars so the same script serves both the CPU workflow and the GPU
# workflow:
#   KIND_CLUSTER  (default "dmo") — an EXISTING cluster of this name is reused as-is,
#                 never recreated. The GPU workflow pre-creates a GPU-enabled cluster
#                 (e.g. "dmo-gpu") and sets KIND_CLUSTER=dmo-gpu; this script then only
#                 loads the image into it.
#   OLLAYA_IMAGE  (default the CPU tag) — the runtime image loaded into the cluster.
#                 The GPU workflow sets it to the :…-cuda tag.
#   KIND_NODE_IMAGE (optional) — a `kindest/node:vX.Y.Z@sha256:…` ref used only when
#                 this script CREATES the cluster, to pin the Kubernetes version
#                 (the version matrix, hack/k8s-versions.env). Ignored when the
#                 cluster already exists. Empty = kind's default node image.
#   KIND_CONFIG   (optional) — path to a kind cluster config, used only when this
#                 script CREATES the cluster. The multi-node suite (RWO co-location,
#                 Recreate) needs more than one node; it points KIND_CONFIG at
#                 hack/kind-recreate.yaml. Ignored when the cluster already exists.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-dmo}"
OLLAYA_IMAGE="${OLLAYA_IMAGE:-ghcr.io/ollaya-dev/ollaya:0.12.0}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
KIND_CONFIG="${KIND_CONFIG:-}"

# Ensure Docker is reachable (OrbStack on this project). Do not auto-start it
# from here; just fail with a clear hint.
if ! docker info >/dev/null 2>&1; then
  echo "docker is not reachable. Start the runtime first (e.g. 'orb start')." >&2
  exit 1
fi

# Create the cluster only if it does not already exist.
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "kind cluster '${CLUSTER}' already exists; reusing it."
else
  echo "creating kind cluster '${CLUSTER}'..."
  create_args=(--name "${CLUSTER}")
  if [[ -n "${KIND_NODE_IMAGE}" ]]; then
    echo "  pinning node image: ${KIND_NODE_IMAGE}"
    create_args+=(--image "${KIND_NODE_IMAGE}")
  fi
  if [[ -n "${KIND_CONFIG}" ]]; then
    echo "  using cluster config: ${KIND_CONFIG}"
    create_args+=(--config "${KIND_CONFIG}")
  fi
  kind create cluster "${create_args[@]}"
fi

# Make sure the Ollaya image is present locally, then load it into the cluster.
if ! docker image inspect "${OLLAYA_IMAGE}" >/dev/null 2>&1; then
  echo "pulling ${OLLAYA_IMAGE}..."
  docker pull "${OLLAYA_IMAGE}"
fi

echo "loading ${OLLAYA_IMAGE} into kind cluster '${CLUSTER}'..."
kind load docker-image "${OLLAYA_IMAGE}" --name "${CLUSTER}"

echo
kubectl cluster-info --context "kind-${CLUSTER}"
