#!/usr/bin/env bash
# upgrade-e2e.sh — upgrade E2E for the release go/no-go checklist (RELEASING.md §3).
#
# Installs a PUBLISHED release ("from") from its GitHub Release assets, brings up two
# DecisionModels, upgrades the operator to the code under test (built from the current
# source tree), and asserts neither DecisionModel is re-rolled by the upgrade:
#   - phase returns to Ready,
#   - status.stableRevision.hash is unchanged,
#   - no candidateRevision is created,
#   - the stable Deployment's pod-template spec-hash annotation is unchanged,
#   - the stable Deployment's serving-container resources are unchanged.
# A changed stable template on a no-op upgrade is the template-drift class of bug and a release
# blocker — this script fails loudly with the before/after values.
#
# Two DMs on purpose:
#   1. explicit resources  — the common case.
#   2. NO resources        — the resource freeze: a running stable with no explicit
#      resources must keep its original (empty/old-default) pod-template resources after
#      the upgrade; the new engine's default resources must NOT appear. This is
#      the most likely upgrade roll, so it is the important leg.
#
# Usage:
#   hack/upgrade-e2e.sh <from-tag>
# Env:
#   REPO            GitHub repo (default maks3201/decision-model-operator)
#   KIND_CLUSTER    kind cluster name (default dmo-upgrade)
#   KIND_NODE_IMAGE kindest/node ref to pin the k8s version (default: kind's default)
#   OLLAYA_IMAGE    runtime image to preload (default the CPU tag, see hack/kind-up.sh)
#   IMG             operator image tag built from source for the "to" side
#                   (default example.com/decision-model-operator:v0.0.0-upgrade)
#   GH_TOKEN/GITHUB_TOKEN  token with `packages: read` to pull the private "from" image
#   KEEP_CLUSTER    non-empty: do not delete the kind cluster on exit (debugging)
#
# Requires: kind, kubectl, docker, gh, make. Run from the repo root (or anywhere;
# the script cd's to the repo root).
set -euo pipefail

from_tag="${1:?usage: hack/upgrade-e2e.sh <from-tag>   (e.g. v0.1.0)}"
REPO="${REPO:-maks3201/decision-model-operator}"
CLUSTER="${KIND_CLUSTER:-dmo-upgrade}"
IMG="${IMG:-example.com/decision-model-operator:v0.0.0-upgrade}"
NS="dmo-upgrade"
DM_RES="upgrade-router"      # DM 1: explicit resources
DM_NORES="upgrade-router-nores"  # DM 2: no resources (resource freeze)
GHCR_IMAGE="ghcr.io/${REPO}:${from_tag}"

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
# jp <kind> <name> <jsonpath> — namespaced get, empty on error.
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
  echo "FAIL: DecisionModel ${dm} never reached Ready on ${from_tag} (phase=${phase:-<none>})"
  kc describe decisionmodel "${dm}" -n "${NS}" | tail -40
  return 1
}

# Per-DM recorded identity, keyed by DM name (files under $work).
record_before() {
  local dm="$1" stable dep spechash res
  stable="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
  dep="${dm}-${stable}"
  spechash="$(jp deploy "${dep}" '{.metadata.annotations.decisionmodel\.io/spec-hash}')"
  res="$(jp deploy "${dep}" '{.spec.template.spec.containers[0].resources}')"
  [[ -n "${stable}" ]] || { echo "FAIL: no stableRevision.hash recorded for ${dm} on ${from_tag}"; exit 1; }
  [[ -n "${spechash}" ]] || { echo "FAIL: no spec-hash on the stable Deployment ${dep}"; exit 1; }
  printf '%s\n%s\n%s\n%s\n' "${stable}" "${dep}" "${spechash}" "${res}" >"${work}/${dm}.before"
  echo "pre-upgrade  ${dm}: stable=${stable} deploy=${dep} spec-hash=${spechash} resources=${res:-<empty>}"
}

# assert_not_rerolled <dm> — compare post-upgrade state to the recorded identity.
assert_not_rerolled() {
  local dm="$1" stable_before dep spechash_before res_before
  { read -r stable_before; read -r dep; read -r spechash_before; read -r res_before; } <"${work}/${dm}.before"

  local ok="" phase stable_now cand_now
  for _ in $(seq 1 60); do
    phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    stable_now="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
    cand_now="$(jp decisionmodel "${dm}" '{.status.candidateRevision.hash}')"
    if [[ "${phase}" = "Ready" ]] && [[ "${stable_now}" = "${stable_before}" ]] && [[ -z "${cand_now}" ]]; then
      ok="yes"; break
    fi
    sleep 5
  done

  local spechash_after res_after
  spechash_after="$(jp deploy "${dep}" '{.metadata.annotations.decisionmodel\.io/spec-hash}')"
  res_after="$(jp deploy "${dep}" '{.spec.template.spec.containers[0].resources}')"
  echo "post-upgrade ${dm}: phase=${phase:-} stable=${stable_now:-} candidate=${cand_now:-<none>} spec-hash=${spechash_after:-} resources=${res_after:-<empty>}"

  local fail=""
  [[ "${ok}" = "yes" ]] || fail="phase=${phase:-}, stable ${stable_before} -> ${stable_now:-}, candidate=${cand_now:-}"
  [[ "${stable_now:-}" = "${stable_before}" ]] || fail="stableRevision.hash changed ${stable_before} -> ${stable_now:-} (upgrade re-rolled ${dm})"
  [[ -z "${cand_now:-}" ]] || fail="a candidate was created by the upgrade: ${cand_now}"
  [[ "${spechash_after:-}" = "${spechash_before}" ]] || fail="stable spec-hash changed ${spechash_before} -> ${spechash_after:-} (pod template rewritten)"
  [[ "${res_after:-}" = "${res_before:-}" ]] || fail="stable pod-template resources changed on a no-op upgrade: '${res_before:-}' -> '${res_after:-}' (resource freeze violated — new engine defaults leaked onto a running stable)"

  if [[ -n "${fail}" ]]; then
    echo "FAIL (upgrade ${from_tag} -> source, ${dm}): ${fail}"
    kc describe decisionmodel "${dm}" -n "${NS}" | tail -40
    return 1
  fi
  echo "PASS ${dm}: Ready, same stable (${stable_before}), no candidate, pod template + resources unchanged."
}

# --- bring up the cluster + runtime image (reuses hack/kind-up.sh) ------------
note "kind up (cluster=${CLUSTER} node=${KIND_NODE_IMAGE:-<default>})"
KIND_CLUSTER="${CLUSTER}" KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}" ./hack/kind-up.sh

# --- install the published "from" release -------------------------------------
note "installing published release ${from_tag} from its assets"
gh release download "${from_tag}" -R "${REPO}" -p install.yaml -O "${work}/install.yaml" --clobber

# GHCR package is private: pull the "from" image with the token, then kind-load it,
# so the cluster never needs an imagePullSecret for a node-local image. (The
# alternative — an in-cluster imagePullSecret — needs patching every ServiceAccount
# the manifest creates; kind-load is simpler and keeps install.yaml unmodified.)
token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -n "${token}" ]]; then
  echo "${token}" | docker login ghcr.io -u "${GITHUB_ACTOR:-token}" --password-stdin >/dev/null 2>&1 || \
    echo "warning: docker login ghcr.io failed; assuming the image is already pullable" >&2
fi
echo "pulling ${GHCR_IMAGE}"
docker pull "${GHCR_IMAGE}"
kind load docker-image "${GHCR_IMAGE}" --name "${CLUSTER}"

echo "applying ${from_tag} install.yaml"
kc apply -f "${work}/install.yaml"
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n decision-model-operator-system --timeout=3m

# --- bring up both DecisionModels on the "from" release -----------------------
# Both in one cluster, replicas 1, same model/tag so they share the node-cached
# store image and keep the runner within memory budget.
note "creating two DecisionModels on ${from_tag} (one with resources, one without)"
kc create ns "${NS}" >/dev/null 2>&1 || true
cat <<YAML | kc apply -f -
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM_RES}, namespace: ${NS}}
spec:
  engine: ollaya
  model: laya:en
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
---
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM_NORES}, namespace: ${NS}}
spec:
  engine: ollaya
  model: laya:en
  device: cpu
  replicas: 1
YAML

note "waiting for both DecisionModels to be Ready"
wait_ready "${DM_RES}"
wait_ready "${DM_NORES}"
record_before "${DM_RES}"
record_before "${DM_NORES}"

# --- upgrade to the code under test (built from source) -----------------------
note "building the operator from source and upgrading"
make docker-build "IMG=${IMG}"
kind load docker-image "${IMG}" --name "${CLUSTER}"
# Apply the source manifests via the kustomize path (same as the main suite's deploy).
make install
make deploy "IMG=${IMG}"
# Roll the manager to the new image and wait for it to be Available again.
kc rollout status deployment -l control-plane=controller-manager \
  -n decision-model-operator-system --timeout=3m || \
  kc wait deployment.apps -l control-plane=controller-manager \
    --for=condition=Available -n decision-model-operator-system --timeout=3m

# --- assert neither DecisionModel was re-rolled -------------------------------
note "asserting neither DecisionModel is re-rolled by the upgrade"
rc=0
assert_not_rerolled "${DM_RES}" || rc=1
assert_not_rerolled "${DM_NORES}" || rc=1
[[ "${rc}" -eq 0 ]] || { echo "FAIL: upgrade ${from_tag} -> source re-rolled at least one DecisionModel"; exit 1; }

echo "PASS: upgrade ${from_tag} -> source kept both DecisionModels Ready and un-rerolled."
