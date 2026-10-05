#!/usr/bin/env bash
# mirror-build.sh — build the e2e registry-mirror image.
#
# Fetches one Ollaya model (MIRROR_MODEL, default laya:en) — its manifest and every
# blob — directly over HTTP with curl, bakes that store plus a tiny static registry
# server (hack/mirror/) into an image, and loads it into the kind cluster. The e2e
# "registry mirror" spec then runs the operator with --ollaya-registry pointing at
# this in-cluster mirror, so resolve AND the prefetch Job run fully offline.
#
# Why curl and not `ollaya pull`: `ollaya pull` is a one-shot that spins up an
# in-process `ollaya serve` and self-checks it on http://0.0.0.0:11435 within a
# fixed 30 s. On the GitHub amd64 CI runner that server does not become ready in
# time and the pull fails ("started `ollaya serve`, but it did not answer ...
# within 30 s"). The store is just content-addressed files, so we fetch them
# directly: the manifest from the registry, each blob by the download URL recorded
# in the manifest (ollaya.dev and/or huggingface.co), verifying every sha256. No
# Ollaya binary, no server, no timeout.
#
# Prep-time network: this fetches the model once (ollaya.dev + the model's upstream
# blob hosts). That is the same network the normal lifecycle e2e already needs;
# nothing reaches the network at test time once the image is loaded.
#
# Env:
#   MIRROR_IMAGE        (default dmo-e2e-mirror:laya-en) — image tag to build/load.
#   MIRROR_MODEL        (default laya:en)                — <model>:<tag> to fetch.
#   MIRROR_NAMESPACE    (default library)                — registry namespace.
#   MIRROR_SRC_REGISTRY (default https://ollaya.dev)     — registry to fetch from.
#   KIND_CLUSTER        (default dmo)                     — cluster to load into.
set -euo pipefail

MIRROR_IMAGE="${MIRROR_IMAGE:-dmo-e2e-mirror:laya-en}"
MIRROR_MODEL="${MIRROR_MODEL:-laya:en}"
MIRROR_NAMESPACE="${MIRROR_NAMESPACE:-library}"
MIRROR_SRC_REGISTRY="${MIRROR_SRC_REGISTRY:-https://ollaya.dev}"
CLUSTER="${KIND_CLUSTER:-dmo}"

MODEL_NAME="${MIRROR_MODEL%%:*}"
MODEL_TAG="${MIRROR_MODEL##*:}"
if [[ "${MODEL_NAME}" = "${MODEL_TAG}" ]]; then
  echo "mirror-build: MIRROR_MODEL must be <model>:<tag> (got '${MIRROR_MODEL}')" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="${SCRIPT_DIR}/mirror"
# The mirror server discovers the manifest host dir; use the source registry host
# so the on-disk layout matches a real `ollaya pull` store.
SRC_HOST="$(printf '%s' "${MIRROR_SRC_REGISTRY}" | sed -E 's#^https?://##; s#/.*$##')"
STORE="${CTX}/store/models"
MANIFEST_DIR="${STORE}/manifests/${SRC_HOST}/${MIRROR_NAMESPACE}/${MODEL_NAME}"
BLOB_DIR="${STORE}/blobs"

for tool in curl jq docker kind; do
  command -v "${tool}" >/dev/null 2>&1 || { echo "mirror-build: '${tool}' not found" >&2; exit 1; }
done
if ! docker info >/dev/null 2>&1; then
  echo "docker is not reachable. Start the runtime first (e.g. 'orb start')." >&2
  exit 1
fi

echo "mirror-build: fetching ${MIRROR_MODEL} from ${MIRROR_SRC_REGISTRY} into ${STORE}..."
# Clean any previous store so stale/partial content is never baked in.
find "${CTX}/store" -mindepth 1 -delete 2>/dev/null || true
mkdir -p "${MANIFEST_DIR}" "${BLOB_DIR}"

MANIFEST_URL="${MIRROR_SRC_REGISTRY}/v2/${MIRROR_NAMESPACE}/${MODEL_NAME}/manifests/${MODEL_TAG}"
MANIFEST_FILE="${MANIFEST_DIR}/${MODEL_TAG}"
echo "mirror-build: GET ${MANIFEST_URL}"
curl -fsSL --retry 3 --retry-delay 5 "${MANIFEST_URL}" -o "${MANIFEST_FILE}"
if ! jq -e '.schemaVersion==2' "${MANIFEST_FILE}" >/dev/null 2>&1; then
  echo "mirror-build: fetched manifest is not a schemaVersion 2 document" >&2
  exit 1
fi

# Fetch every blob (config + layers) by the download URL in the manifest, naming
# each file by its digest in the on-disk store form (sha256-<hex>) and verifying
# the content hashes to that digest.
blob_count=0
while read -r digest url; do
  [[ -n "${digest}" ]] || continue
  hex="${digest#sha256:}"
  out="${BLOB_DIR}/sha256-${hex}"
  echo "mirror-build: GET blob ${hex:0:12}... <- ${url}"
  curl -fsSL --retry 3 --retry-delay 5 "${url}" -o "${out}"
  got="$(shasum -a 256 "${out}" 2>/dev/null | cut -d' ' -f1 || sha256sum "${out}" | cut -d' ' -f1)"
  if [[ "${got}" != "${hex}" ]]; then
    echo "mirror-build: blob digest mismatch for ${hex}: got ${got}" >&2
    exit 1
  fi
  blob_count=$((blob_count + 1))
done < <(jq -r '([.config] + .layers) | .[] | "\(.digest) \(.urls[0])"' "${MANIFEST_FILE}")

if [[ "${blob_count}" -eq 0 ]]; then
  echo "mirror-build: no blobs fetched (manifest had no config/layers?)" >&2
  exit 1
fi
echo "mirror-build: store populated (${blob_count} blobs, digest $(shasum -a 256 "${MANIFEST_FILE}" 2>/dev/null | cut -d' ' -f1 || sha256sum "${MANIFEST_FILE}" | cut -d' ' -f1))."

echo "mirror-build: building ${MIRROR_IMAGE}..."
docker build -t "${MIRROR_IMAGE}" -f "${CTX}/Dockerfile" "${CTX}"

echo "mirror-build: loading ${MIRROR_IMAGE} into kind cluster '${CLUSTER}'..."
kind load docker-image "${MIRROR_IMAGE}" --name "${CLUSTER}"

echo "mirror-build: done."
