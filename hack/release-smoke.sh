#!/usr/bin/env bash
# release-smoke.sh — release-artifact smoke test (RELEASING.md §3).
#
# On a fresh kind cluster, install a PUBLISHED release two ways and check it comes up:
#   1. kubectl apply -f install.yaml  (the kustomize artifact)
#   2. helm install the release chart .tgz  (after uninstalling #1)
# For each: the manager Deployment is Available, the CRD is Established, and one
# DecisionModel reaches Ready. Intended for RC tags.
#
# Usage:   hack/release-smoke.sh <tag>
# Env:     REPO, KIND_CLUSTER, KIND_NODE_IMAGE, OLLAYA_IMAGE, GH_TOKEN/GITHUB_TOKEN,
#          KEEP_CLUSTER (see hack/upgrade-e2e.sh for meanings).
# Requires: kind, kubectl, docker, gh, helm, make.
set -euo pipefail

tag="${1:?usage: hack/release-smoke.sh <tag>   (e.g. v0.1.0 or v0.2.0-rc.1)}"
REPO="${REPO:-maks3201/decision-model-operator}"
CLUSTER="${KIND_CLUSTER:-dmo-smoke}"
OPNS="decision-model-operator-system"
NS="dmo-smoke"
GHCR_IMAGE="ghcr.io/${REPO}:${tag}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${repo_root}"
work="$(mktemp -d)"
kctx="kind-${CLUSTER}"

cleanup() {
  if [[ -z "${KEEP_CLUSTER:-}" ]]; then
    kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  fi
  rm -rf "${work}" || true
}
trap cleanup EXIT

kc() { kubectl --context "${kctx}" "$@"; }
note() { echo "=== $* ==="; }

# One DecisionModel, wait for Ready, fail with diagnostics otherwise.
check_dm_ready() {
  local label="$1"
  kc create ns "${NS}" >/dev/null 2>&1 || true
  cat <<YAML | kc apply -f -
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: smoke, namespace: ${NS}}
spec:
  engine: ollaya
  model: laya:en
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
YAML
  local phase=""
  for i in $(seq 1 96); do
    phase="$(kc get decisionmodel smoke -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null)"
    [[ "${phase}" = "Ready" ]] && break
    sleep 5
  done
  if [[ "${phase}" != "Ready" ]]; then
    echo "FAIL (${label}): DecisionModel never reached Ready (phase=${phase:-<none>})"
    kc describe decisionmodel smoke -n "${NS}" | tail -40
    exit 1
  fi
  echo "OK (${label}): DecisionModel reached Ready"
  kc delete decisionmodel smoke -n "${NS}" --ignore-not-found >/dev/null 2>&1
}

crd_established() {
  kc wait --for=condition=Established crd/decisionmodels.decisionmodel.io --timeout=60s
}

# --- cluster + runtime image + the "from" operator image ----------------------
note "kind up (cluster=${CLUSTER} node=${KIND_NODE_IMAGE:-<default>})"
KIND_CLUSTER="${CLUSTER}" KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}" ./hack/kind-up.sh

token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -n "${token}" ]]; then
  echo "${token}" | docker login ghcr.io -u "${GITHUB_ACTOR:-token}" --password-stdin >/dev/null 2>&1 || \
    echo "warning: docker login ghcr.io failed; assuming the image is already pullable" >&2
fi
echo "pulling ${GHCR_IMAGE}"
docker pull "${GHCR_IMAGE}"
kind load docker-image "${GHCR_IMAGE}" --name "${CLUSTER}"

# --- 1. kustomize install.yaml ------------------------------------------------
note "smoke 1: kubectl apply -f install.yaml (${tag})"
gh release download "${tag}" -R "${REPO}" -p install.yaml -O "${work}/install.yaml" --clobber
kc apply -f "${work}/install.yaml"
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${OPNS}" --timeout=3m
crd_established
check_dm_ready "install.yaml"

note "uninstalling the kustomize release before the helm smoke"
kc delete -f "${work}/install.yaml" --ignore-not-found --wait=true || true
# The CRD is in install.yaml, so it is gone now; helm re-installs it.
kc wait --for=delete crd/decisionmodels.decisionmodel.io --timeout=60s 2>/dev/null || true

# --- 2. helm chart .tgz -------------------------------------------------------
note "smoke 2: helm install the release chart .tgz (${tag})"
chart_glob="decision-model-operator-*.tgz"
gh release download "${tag}" -R "${REPO}" -p "${chart_glob}" --dir "${work}" --clobber
chart_tgz="$(ls "${work}"/${chart_glob} | head -1)"
echo "chart: ${chart_tgz}"
helm --kube-context "${kctx}" install dmo "${chart_tgz}" \
  --namespace "${OPNS}" --create-namespace \
  --set image.pullPolicy=IfNotPresent \
  --wait --timeout 3m
crd_established
check_dm_ready "helm chart"
helm --kube-context "${kctx}" uninstall dmo --namespace "${OPNS}" --wait --timeout 2m || true

echo "PASS: release ${tag} artifacts (install.yaml + chart .tgz) both install and reach Ready."
