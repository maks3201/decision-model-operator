#!/usr/bin/env bash
# olm-e2e.sh — OLM bundle smoke for the release go/no-go checklist (RELEASING.md, item 67).
#
# Proves the operator installs and upgrades cleanly through OLM on a kind cluster:
#   1. install OLM on kind (operator-sdk olm install);
#   2. build the committed bundle (bundle/) into a bundle image, load it into kind,
#      and `operator-sdk run bundle` it — OLM installs the operator from the bundle;
#   3. create a DecisionModel, wait until it is Ready;
#   4. build a second bundle image with a bumped CSV version (the "source" upgrade
#      target) WITHOUT regenerating or mutating the committed bundle/ (workers never
#      run `make bundle`), load it, and `operator-sdk run bundle-upgrade` to it;
#   5. assert the DecisionModel stays Ready and is NOT re-rolled by the OLM upgrade
#      (same stable hash, no candidate).
#
# This is release-tier (slow; needs OLM). It is wired as a manual/release workflow
# input, not a per-PR check.
#
# Usage:   hack/olm-e2e.sh
# Env:
#   KIND_CLUSTER   kind cluster name (default dmo-olm)
#   OLLAYA_IMAGE   runtime image to preload (default the CPU tag, see hack/kind-up.sh)
#   IMG            operator image built from source and referenced by the bundle CSV
#                  (default example.com/decision-model-operator:v0.0.0-olm)
#   KEEP_CLUSTER   non-empty: do not delete the kind cluster on exit (debugging)
#
# Requires: kind, kubectl, docker, operator-sdk, make, yq (CSV edit). Run from anywhere.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-dmo-olm}"
IMG="${IMG:-example.com/decision-model-operator:v0.0.0-olm}"
NS="dmo-olm"
DM="olm-router"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${repo_root}"
work="$(mktemp -d)"
kctx="kind-${CLUSTER}"
sdk="${OPERATOR_SDK:-operator-sdk}"

cleanup() {
  if [[ -z "${KEEP_CLUSTER:-}" ]]; then
    kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  fi
  rm -rf "${work}" || true
}
trap cleanup EXIT

kc() { kubectl --context "${kctx}" "$@"; }
jp() { kc get "$1" "$2" -n "${NS}" -o "jsonpath=$3" 2>/dev/null; }
note() { echo "=== $* ==="; }

# wait_ready <dm> — block until the DecisionModel reaches Ready (cold pull + load).
wait_ready() {
  local dm="$1" phase=""
  for _ in $(seq 1 96); do
    phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    [[ "${phase}" = "Ready" ]] && return 0
    sleep 5
  done
  echo "FAIL: DecisionModel ${dm} never reached Ready (phase=${phase:-<none>})"
  kc describe decisionmodel "${dm}" -n "${NS}" | tail -40
  return 1
}

# bundle_image_version <version> <bundle-img> — build a bundle image from the
# committed bundle/ with the CSV's version/name set to <version>, into a temp
# build context so the repo's bundle/ is never modified (workers never run
# `make bundle`). The operator image reference in the CSV is rewritten to IMG.
bundle_image_version() {
  local version="$1" bundle_img="$2" ctx csv
  ctx="${work}/bundle-${version}"
  rm -rf "${ctx}"
  mkdir -p "${ctx}"
  cp -R bundle "${ctx}/bundle"
  cp bundle.Dockerfile "${ctx}/bundle.Dockerfile"
  csv="$(ls "${ctx}"/bundle/manifests/*clusterserviceversion.yaml)"
  # Point the CSV at the source operator image and set the version/name so OLM sees
  # a distinct, upgradeable release. yq edits in place.
  yq -i "
    (.spec.install.spec.deployments[].spec.template.spec.containers[]
      | select(.name == \"manager\").image) = \"${IMG}\" |
    .spec.version = \"${version}\" |
    .metadata.name = \"decision-model-operator.v${version}\"
  " "${csv}"
  ( cd "${ctx}" && docker build -f bundle.Dockerfile -t "${bundle_img}" . )
  kind load docker-image "${bundle_img}" --name "${CLUSTER}" >/dev/null
}

# --- cluster + runtime image + source operator image -------------------------
note "kind up (cluster=${CLUSTER})"
KIND_CLUSTER="${CLUSTER}" ./hack/kind-up.sh

note "building the operator image from source and loading it into kind"
make docker-build "IMG=${IMG}"
kind load docker-image "${IMG}" --name "${CLUSTER}"

# --- install OLM --------------------------------------------------------------
note "installing OLM on the cluster (operator-sdk olm install)"
"${sdk}" olm install --timeout 5m || "${sdk}" olm status

# --- bundle v1 (base) + run bundle --------------------------------------------
base_img="example.com/decision-model-operator-bundle:v0.0.0-olm-base"
up_img="example.com/decision-model-operator-bundle:v0.0.1-olm-up"
note "building the base bundle image from the committed bundle/"
bundle_image_version "0.0.0" "${base_img}"

kc create ns "${NS}" >/dev/null 2>&1 || true
note "installing the operator from the base bundle via OLM (operator-sdk run bundle)"
"${sdk}" run bundle "${base_img}" --namespace "${NS}" --timeout 7m \
  --security-context-config restricted

note "waiting for the controller-manager to be Available"
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${NS}" --timeout=5m

# --- a DecisionModel on the base bundle ---------------------------------------
note "creating a DecisionModel and waiting for Ready"
cat <<YAML | kc apply -f -
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM}, namespace: ${NS}}
spec:
  engine: ollaya
  model: laya:en
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
YAML
wait_ready "${DM}"
stable_before="$(jp decisionmodel "${DM}" '{.status.stableRevision.hash}')"
[[ -n "${stable_before}" ]] || { echo "FAIL: no stableRevision.hash before the OLM upgrade"; exit 1; }
echo "pre-upgrade: stable=${stable_before}"

# --- bundle v2 (upgrade target) + run bundle-upgrade --------------------------
note "building the upgrade bundle image and running operator-sdk run bundle-upgrade"
bundle_image_version "0.0.1" "${up_img}"
"${sdk}" run bundle-upgrade "${up_img}" --namespace "${NS}" --timeout 7m

note "waiting for the upgraded controller-manager to be Available"
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${NS}" --timeout=5m

# --- assert the DecisionModel survived the OLM upgrade with no roll -----------
note "asserting the DecisionModel stays Ready and is not re-rolled by the OLM upgrade"
ok=""
for _ in $(seq 1 60); do
  phase="$(jp decisionmodel "${DM}" '{.status.phase}')"
  stable_now="$(jp decisionmodel "${DM}" '{.status.stableRevision.hash}')"
  cand_now="$(jp decisionmodel "${DM}" '{.status.candidateRevision.hash}')"
  if [[ "${phase}" = "Ready" ]] && [[ "${stable_now}" = "${stable_before}" ]] && [[ -z "${cand_now}" ]]; then
    ok="yes"; break
  fi
  sleep 5
done
if [[ "${ok}" != "yes" ]]; then
  echo "FAIL: OLM upgrade re-rolled or disrupted the DecisionModel (phase=${phase:-}, stable ${stable_before} -> ${stable_now:-}, candidate=${cand_now:-<none>})"
  kc describe decisionmodel "${DM}" -n "${NS}" | tail -40
  exit 1
fi
echo "PASS: OLM install + bundle-upgrade kept ${DM} Ready on the same stable (${stable_before}), no re-roll."
