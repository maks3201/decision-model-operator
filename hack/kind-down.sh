#!/usr/bin/env bash
# kind-down.sh — delete the local kind cluster `dmo`. Idempotent.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-dmo}"

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "deleting kind cluster '${CLUSTER}'..."
  kind delete cluster --name "${CLUSTER}"
else
  echo "kind cluster '${CLUSTER}' does not exist; nothing to do."
fi
