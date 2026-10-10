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

# active_rs <dm> <stable> -- the ReplicaSet currently backing the stable serving
# Deployment, derived from the running serving Pod's pod-template-hash label
# (<deployment>-<pod-template-hash>). A roll of the pod template gives the Pods a new
# pod-template-hash and thus a new ReplicaSet name, so a changed value proves a roll.
active_rs() {
  local dm="$1" stable="$2" pth
  pth="$(kc get pods -n "${NS}" -l "decisionmodel.io/name=${dm},!decisionmodel.io/prefetch-revision" \
    -o 'jsonpath={.items[0].metadata.labels.pod-template-hash}' 2>/dev/null)"
  [[ -n "${pth}" ]] || return 0
  printf '%s-%s' "${dm}-${stable}" "${pth}"
}

# serving_pod_uids <dm> -- space-separated UIDs of the DM's serving Pods (prefetch
# Pods excluded via the !prefetch-revision selector), sorted so the comparison is
# order-independent. A rolled Pod gets a new UID, so a changed set proves a roll.
serving_pod_uids() {
  local dm="$1"
  kc get pods -n "${NS}" -l "decisionmodel.io/name=${dm},!decisionmodel.io/prefetch-revision" \
    -o 'jsonpath={.items[*].metadata.uid}' 2>/dev/null | tr ' ' '\n' | sort | tr '\n' ' ' | xargs
}

# registry_field_present -- true if the installed CRD carries
# status.stableRevision.registry (the recorded per-revision registry). The adoption
# assertion only applies once that field exists; before it, the check is skipped so
# this script stays correct on an older "to" side.
registry_field_present() {
  local t
  t="$(kc get crd decisionmodels.decisionmodel.io \
    -o 'jsonpath={.spec.versions[*].schema.openAPIV3Schema.properties.status.properties.stableRevision.properties.registry.type}' 2>/dev/null)"
  [[ -n "${t}" ]]
}

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
  local dm="$1" stable dep spechash res gen rs uids
  stable="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
  dep="${dm}-${stable}"
  spechash="$(jp deploy "${dep}" '{.metadata.annotations.decisionmodel\.io/spec-hash}')"
  res="$(jp deploy "${dep}" '{.spec.template.spec.containers[0].resources}')"
  gen="$(jp deploy "${dep}" '{.metadata.generation}')"
  rs="$(active_rs "${dm}" "${stable}")"
  uids="$(serving_pod_uids "${dm}")"
  [[ -n "${stable}" ]] || { echo "FAIL: no stableRevision.hash recorded for ${dm} on ${from_tag}"; exit 1; }
  [[ -n "${spechash}" ]] || { echo "FAIL: no spec-hash on the stable Deployment ${dep}"; exit 1; }
  [[ -n "${gen}" ]] || { echo "FAIL: no generation on the stable Deployment ${dep}"; exit 1; }
  [[ -n "${rs}" ]] || { echo "FAIL: no active ReplicaSet for the stable Deployment ${dep}"; exit 1; }
  [[ -n "${uids}" ]] || { echo "FAIL: no serving Pod for ${dm} on ${from_tag}"; exit 1; }
  printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n' \
    "${stable}" "${dep}" "${spechash}" "${res}" "${gen}" "${rs}" "${uids}" >"${work}/${dm}.before"
  echo "pre-upgrade  ${dm}: stable=${stable} deploy=${dep} spec-hash=${spechash} resources=${res:-<empty>}"
  echo "pre-upgrade  ${dm}: generation=${gen} replicaset=${rs} pod-uids=${uids}"
}

# assert_not_rerolled <dm> — compare post-upgrade state to the recorded identity.
assert_not_rerolled() {
  local dm="$1" stable_before dep spechash_before res_before gen_before rs_before uids_before
  { read -r stable_before; read -r dep; read -r spechash_before; read -r res_before
    read -r gen_before; read -r rs_before; read -r uids_before; } <"${work}/${dm}.before"

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

  local spechash_after res_after gen_after rs_after uids_after
  spechash_after="$(jp deploy "${dep}" '{.metadata.annotations.decisionmodel\.io/spec-hash}')"
  res_after="$(jp deploy "${dep}" '{.spec.template.spec.containers[0].resources}')"
  gen_after="$(jp deploy "${dep}" '{.metadata.generation}')"
  rs_after="$(active_rs "${dm}" "${stable_before}")"
  uids_after="$(serving_pod_uids "${dm}")"
  echo "post-upgrade ${dm}: phase=${phase:-} stable=${stable_now:-} candidate=${cand_now:-<none>} spec-hash=${spechash_after:-} resources=${res_after:-<empty>}"
  echo "post-upgrade ${dm}: generation=${gen_after:-} replicaset=${rs_after:-} pod-uids=${uids_after:-}"

  local fail=""
  [[ "${ok}" = "yes" ]] || fail="phase=${phase:-}, stable ${stable_before} -> ${stable_now:-}, candidate=${cand_now:-}"
  [[ "${stable_now:-}" = "${stable_before}" ]] || fail="stableRevision.hash changed ${stable_before} -> ${stable_now:-} (upgrade re-rolled ${dm})"
  [[ -z "${cand_now:-}" ]] || fail="a candidate was created by the upgrade: ${cand_now}"
  [[ "${spechash_after:-}" = "${spechash_before}" ]] || fail="stable spec-hash changed ${spechash_before} -> ${spechash_after:-} (pod template rewritten)"
  [[ "${res_after:-}" = "${res_before:-}" ]] || fail="stable pod-template resources changed on a no-op upgrade: '${res_before:-}' -> '${res_after:-}' (resource freeze violated — new engine defaults leaked onto a running stable)"
  # The Deployment itself must not roll at all on a no-op upgrade: generation, the
  # backing ReplicaSet and the serving Pod UIDs are all unchanged (a status-only
  # registry adoption must not touch the pod template).
  [[ "${gen_after:-}" = "${gen_before}" ]] || fail="stable Deployment generation changed ${gen_before} -> ${gen_after:-} (the Deployment was re-rendered)"
  [[ "${rs_after:-}" = "${rs_before}" ]] || fail="stable ReplicaSet changed ${rs_before} -> ${rs_after:-} (the Deployment rolled)"
  [[ "${uids_after:-}" = "${uids_before}" ]] || fail="stable serving Pod UIDs changed '${uids_before}' -> '${uids_after:-}' (Pods were recreated)"

  if [[ -n "${fail}" ]]; then
    echo "FAIL (upgrade ${from_tag} -> source, ${dm}): ${fail}"
    kc describe decisionmodel "${dm}" -n "${NS}" | tail -40
    echo "--- rollout history for deployment/${dep} ---"
    kc rollout history "deployment/${dep}" -n "${NS}" 2>&1 | tail -40 || true
    return 1
  fi

  # New status field populated by the upgrade without a roll: the source records
  # status.stableRevision.runtimeVersion (resolved engine runtime). An older "from"
  # release left it empty; after the upgrade it must be populated for the SAME stable
  # hash — the controller enriches status in place, it does not roll to do it.
  local rtver
  rtver="$(jp decisionmodel "${dm}" '{.status.stableRevision.runtimeVersion}')"
  if [[ -z "${rtver}" ]]; then
    echo "FAIL ${dm}: status.stableRevision.runtimeVersion not populated after the upgrade (new status field should be filled in place)"
    kc get decisionmodel "${dm}" -n "${NS}" -o yaml | grep -A20 'stableRevision:' | head -25
    return 1
  fi

  # Registry adoption (only once the CRD carries the field): after the upgrade the
  # recorded status.stableRevision.registry must be non-empty, proving the first
  # reconcile adopted the registry in place without any of the roll signals above
  # tripping. Skipped on a pre-recording build so the script stays correct there.
  # Adoption happens on the first reconcile AFTER the new manager wins the leader
  # lease, which can lag the Deployment becoming Available by seconds; phase was
  # already Ready before the upgrade, so the roll-check loop above does not wait for
  # it. Poll the field for up to 3 minutes (5 s steps) rather than reading it once.
  local reg_after=""
  if registry_field_present; then
    local adopt_epoch=""
    for _ in $(seq 1 36); do
      reg_after="$(jp decisionmodel "${dm}" '{.status.stableRevision.registry}')"
      if [[ -n "${reg_after}" ]]; then adopt_epoch="$(date +%s)"; break; fi
      sleep 5
    done
    if [[ -z "${reg_after}" ]]; then
      echo "FAIL ${dm}: status.stableRevision.registry is empty 3m after the upgrade (registry was not adopted)"
      kc get decisionmodel "${dm}" -n "${NS}" -o yaml | grep -A20 'stableRevision:' | head -25
      echo "--- manager logs mentioning ${dm} ---"
      kc logs -l control-plane=controller-manager -n decision-model-operator-system \
        --tail=-1 2>/dev/null | grep -- "${dm}" | tail -40 || true
      return 1
    fi
    local secs="?"
    if [[ -n "${adopt_epoch}" ]] && [[ -n "${MGR_AVAILABLE_EPOCH:-}" ]]; then
      secs="$(( adopt_epoch - MGR_AVAILABLE_EPOCH ))"
    fi
    echo "note ${dm}: registry adopted in place: ${reg_after} (${secs}s after manager Available)"
  else
    echo "note ${dm}: CRD has no status.stableRevision.registry; skipping the adoption check (pre-recording build)"
  fi
  echo "PASS ${dm}: Ready, same stable (${stable_before}), no candidate, Deployment not rolled (generation/ReplicaSet/Pods unchanged), runtimeVersion=${rtver} populated."
}

# assert_store_recovery <dm> — after the upgrade, delete the stable revision's model-
# store PVC and prove the operator rebuilds the store (from the owned manifest ConfigMap)
# without rolling the stable: the DM returns to Ready on the SAME stable hash and a store
# PVC for that revision exists again. This exercises the post-upgrade store-recovery path
# the new controller owns, on a stable created by the OLD release.
assert_store_recovery() {
  local dm="$1" stable dep pvc stable_now
  stable="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
  pvc="${dm}-store-${stable}"
  # Some older stables use the shared (non-per-revision) store name; skip gracefully
  # if the per-revision PVC is not the one in use (recovery for the shared store is a
  # separate migration path, not this check).
  if ! kc get pvc "${pvc}" -n "${NS}" >/dev/null 2>&1; then
    echo "SKIP ${dm}: no per-revision store PVC ${pvc} (shared-store migration path); store recovery not asserted here"
    return 0
  fi
  echo "deleting store PVC ${pvc} to exercise post-upgrade store recovery"
  kc delete pvc "${pvc}" -n "${NS}" --wait=false >/dev/null 2>&1 || true
  local ok=""
  for _ in $(seq 1 96); do
    stable_now="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
    local phase; phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    if [[ "${phase}" = "Ready" ]] && [[ "${stable_now}" = "${stable}" ]] && \
       kc get pvc "${pvc}" -n "${NS}" >/dev/null 2>&1; then
      ok="yes"; break
    fi
    sleep 5
  done
  if [[ "${ok}" != "yes" ]]; then
    echo "FAIL ${dm}: store not recovered after the upgrade (phase=$(jp decisionmodel "${dm}" '{.status.phase}'), stable ${stable} -> ${stable_now:-}, PVC ${pvc})"
    kc describe decisionmodel "${dm}" -n "${NS}" | tail -40
    return 1
  fi
  echo "PASS ${dm}: store PVC ${pvc} rebuilt, same stable (${stable}), Ready — post-upgrade store recovery works."
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
# Timestamp the moment the new manager is Available; the registry-adoption poll in
# assert_not_rerolled reports how long after this the first reconcile adopts in place.
MGR_AVAILABLE_EPOCH="$(date +%s)"

# --- assert neither DecisionModel was re-rolled -------------------------------
note "asserting neither DecisionModel is re-rolled by the upgrade"
rc=0
assert_not_rerolled "${DM_RES}" || rc=1
assert_not_rerolled "${DM_NORES}" || rc=1
[[ "${rc}" -eq 0 ]] || { echo "FAIL: upgrade ${from_tag} -> source re-rolled at least one DecisionModel"; exit 1; }

# --- post-upgrade store recovery (new controller owns it; stable made by old release) ---
note "asserting post-upgrade store recovery (delete the stable's store PVC, expect a rebuild with no roll)"
assert_store_recovery "${DM_RES}" || rc=1
[[ "${rc}" -eq 0 ]] || { echo "FAIL: upgrade ${from_tag} -> source: store recovery failed"; exit 1; }

echo "PASS: upgrade ${from_tag} -> source kept both DecisionModels Ready and un-rerolled."
