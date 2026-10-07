#!/usr/bin/env bash
# upgrade-chart-e2e.sh — prove a Helm-chart operator upgrade does NOT roll the fleet.
#
# The promise under test: with the default runtime-version policy (Pinned), upgrading
# the operator — even to one whose default engine runtime version differs — must NOT
# start a rollout for any existing DecisionModel. A rollout happens only when the user
# opts in by setting spec.runtimeVersion.
#
# Flow:
#   1. kind + the Ollaya runtime image (hack/kind-up.sh).
#   2. Install the PREVIOUS RELEASED chart (oci://ghcr.io/<repo>/charts/..., default
#      the latest release) with its released operator image; create two DecisionModels
#      (laya:en plain + laya:en eval-gated); wait Ready; record stable hashes, serving
#      Deployment names and serving Pod UIDs.
#   3. Build the operator image from the current source, load it into kind. Apply the
#      source CRD out of band FIRST (Helm never upgrades crds/ — see the chart README),
#      then `helm upgrade` to the local chart + the source image.
#   4. Assert for ~2 minutes: no candidate revision appears, stable hashes + serving
#      Deployments are unchanged, serving Pods are NOT restarted (same UIDs), phase stays
#      Ready, and /v1/systemone keeps answering.
#   5. Assert the CRD upgrade actually applied: the new spec.runtimeVersion field is
#      accepted (a server-side dry-run apply). Fails loudly if the chart README's
#      out-of-band CRD step and reality disagree.
#   6. Set spec.runtimeVersion explicitly (to the operator default) on ONE DM → expect
#      exactly one blue-green rollout on that DM only; the other DM stays put.
#
# Usage:
#   hack/upgrade-chart-e2e.sh [from-version]     # default: latest GitHub release tag
# Env:
#   REPO            GitHub repo (default maks3201/decision-model-operator)
#   KIND_CLUSTER    kind cluster name (default dmo-chart-upgrade)
#   KIND_NODE_IMAGE kindest/node ref to pin the k8s version (default: kind's default)
#   OLLAYA_IMAGE    runtime image to preload (default the CPU tag, see hack/kind-up.sh)
#   IMG             operator image tag built from source for the "to" side
#                   (default example.com/decision-model-operator:v0.0.0-chart-upgrade)
#   RUNTIME_VERSION the runtime version to pin in step 6 (default 0.7.3, the engine's
#                   minimum supported version, chosen so it differs from the stable's
#                   current runtime and therefore forces exactly one blue-green roll)
#   KEEP_CLUSTER    non-empty: do not delete the kind cluster on exit (debugging)
#   GITHUB_TOKEN    optional; authenticates the releases-API tag lookup (CI sets it)
#
# Requires: kind, kubectl, docker, helm, make, curl, jq. The released chart and
# operator image are public, so no GitHub token is needed to pull them; GITHUB_TOKEN
# is used only to authenticate the releases-API tag lookup when set (optional).
# Run from anywhere; the script cd's to the repo root.
set -euo pipefail

REPO="${REPO:-maks3201/decision-model-operator}"
CLUSTER="${KIND_CLUSTER:-dmo-chart-upgrade}"
IMG="${IMG:-example.com/decision-model-operator:v0.0.0-chart-upgrade}"
NS="dmo-chart-upgrade"
OPERATOR_NS="decision-model-operator-system"
CHART_REF="oci://ghcr.io/maks3201/charts/decision-model-operator"
RELEASE="dmo"
DM_PLAIN="upgrade-plain"      # no eval, plain stable
DM_EVAL="upgrade-eval"        # eval-gated stable
MODEL="laya:en"

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
hc() { helm --kube-context "${kctx}" "$@"; }
# jp <kind> <name> <jsonpath> — namespaced get, empty on error.
jp() { kc get "$1" "$2" -n "${NS}" -o "jsonpath=$3" 2>/dev/null; }
note() { echo "=== $* ==="; }
fail() { echo "FAIL: $*" >&2; kc get decisionmodel,deploy,pod -n "${NS}" -o wide >&2 || true; exit 1; }

# from-version: explicit arg or the latest GitHub release tag (strip a leading 'v').
# Resolve via the public releases API with curl. The repo is public, so no auth is
# required to read it, but an unauthenticated call shares the low 60/h IP rate limit
# and has flaked in CI; when GITHUB_TOKEN is set (CI) the call is authenticated to
# use the much higher per-token limit. One retry absorbs a transient blip. Works
# locally with or without a token.
from_arg="${1:-}"
if [[ -z "${from_arg}" ]]; then
  releases_url="https://api.github.com/repos/${REPO}/releases/latest"
  auth_header=()
  [[ -n "${GITHUB_TOKEN:-}" ]] && auth_header=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
  for attempt in 1 2; do
    from_arg="$(curl -sS --proto '=https' --tlsv1.2 \
      -H "Accept: application/vnd.github+json" "${auth_header[@]}" \
      "${releases_url}" 2>/dev/null | jq -r '.tag_name // empty')"
    [[ -n "${from_arg}" ]] && break
    [[ "${attempt}" = 1 ]] && { note "release lookup failed, retrying in 5s"; sleep 5; }
  done
  [[ -n "${from_arg}" ]] || fail "could not resolve the latest release tag; pass one explicitly"
fi
from_ver="${from_arg#v}"
# Pin target for step 6: a VALID engine runtime version that differs from the one the
# released stable already runs (its image is the then-current default), so pinning it
# forces exactly one blue-green roll. 0.7.3 is the engine's minimum supported version.
RUNTIME_VERSION="${RUNTIME_VERSION:-0.7.3}"
PIN_IMAGE="ghcr.io/ollaya-dev/ollaya:${RUNTIME_VERSION}"

note "upgrade: chart ${from_ver} (released image) -> source build; opt-in runtime pin target ${RUNTIME_VERSION}"

# wait_ready <dm> — block until the DecisionModel reaches Ready (cold pull + load).
wait_ready() {
  local dm="$1" phase=""
  for _ in $(seq 1 120); do
    phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    [[ "${phase}" = "Ready" ]] && return 0
    sleep 5
  done
  kc describe decisionmodel "${dm}" -n "${NS}" | tail -40 >&2
  fail "DecisionModel ${dm} never reached Ready on ${from_ver} (phase=${phase:-<none>})"
}

# serving_pod_uids <dm> — space-separated UIDs of the DM's serving Pods (sorted),
# normalised to a single trimmed line so before/after compare exactly.
serving_pod_uids() {
  kc get pods -l "decisionmodel.io/name=$1" -n "${NS}" \
    -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' 2>/dev/null \
    | sort | tr '\n' ' ' | sed 's/ *$//'
}

# answers_ok <dm> — true if /v1/systemone returns a choice through the Service.
answers_ok() {
  local dm="$1" lp=19435 pid out
  kc port-forward -n "${NS}" "svc/${dm}" "${lp}:11435" >/dev/null 2>&1 &
  pid=$!
  local ok=1
  for _ in $(seq 1 20); do curl -sS -o /dev/null "http://127.0.0.1:${lp}/" 2>/dev/null && break; sleep 0.5; done
  out="$(curl -sS --max-time 60 -X POST "http://127.0.0.1:${lp}/v1/systemone" \
    -H 'Content-Type: application/json' \
    --data-binary "{\"model\":\"${MODEL}\",\"keep_alive\":-1,\"state\":\"my card was charged twice\",\"questions\":{\"q\":{\"type\":\"choice\",\"criteria\":{\"billing\":\"billing, refunds\",\"other\":\"anything else\"}}}}" 2>/dev/null || true)"
  kill "${pid}" 2>/dev/null || true; wait "${pid}" 2>/dev/null || true
  [[ "$(printf '%s' "${out}" | jq -r '.answers.q.choice // empty' 2>/dev/null)" != "" ]] && ok=0
  return "${ok}"
}

# --- 1. cluster + runtime image ----------------------------------------------
note "kind up (cluster=${CLUSTER})"
KIND_CLUSTER="${CLUSTER}" KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}" ./hack/kind-up.sh

# --- 2. install the previous released chart + its image -----------------------
note "pulling + loading the released operator image ghcr.io/${REPO}:v${from_ver}"
docker pull "ghcr.io/${REPO}:v${from_ver}"
kind load docker-image "ghcr.io/${REPO}:v${from_ver}" --name "${CLUSTER}"

note "helm install the released chart ${from_ver}"
hc install "${RELEASE}" "${CHART_REF}" --version "${from_ver}" \
  --namespace "${OPERATOR_NS}" --create-namespace \
  --set image.pullPolicy=IfNotPresent \
  --wait --timeout 5m
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${OPERATOR_NS}" --timeout=3m

kc create ns "${NS}" --dry-run=client -o yaml | kc apply -f -

note "creating two DecisionModels (plain + eval-gated)"
kc apply -n "${NS}" -f - <<YAML
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM_PLAIN}}
spec:
  engine: ollaya
  model: ${MODEL}
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
YAML

kc apply -n "${NS}" -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: upgrade-golden}
data:
  cases.jsonl: |
    {"state":"my card was charged twice","questions":{"q":{"type":"choice","criteria":{"billing":"billing, refunds","other":"anything else"}}},"expected":{"q":"billing"}}
    {"state":"the app crashes on login","questions":{"q":{"type":"choice","criteria":{"billing":"billing, refunds","other":"anything else"}}},"expected":{"q":"other"}}
---
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM_EVAL}}
spec:
  engine: ollaya
  model: ${MODEL}
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
  rollout:
    evaluation:
      datasetRef: {configMapRef: {name: upgrade-golden, key: cases.jsonl}}
      minAccuracy: "0.0"
YAML

wait_ready "${DM_PLAIN}"
wait_ready "${DM_EVAL}"

# record_before <dm> — stable hash, serving Deployment name, serving Pod UIDs.
record_before() {
  local dm="$1" stable dep uids
  stable="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
  [[ -n "${stable}" ]] || fail "no stableRevision.hash for ${dm} on ${from_ver}"
  dep="${dm}-${stable}"
  uids="$(serving_pod_uids "${dm}")"
  [[ -n "${uids}" ]] || fail "no serving Pods for ${dm} on ${from_ver}"
  printf '%s\n%s\n%s\n' "${stable}" "${dep}" "${uids}" >"${work}/${dm}.before"
  echo "pre-upgrade  ${dm}: stable=${stable} deploy=${dep} pod-uids=[${uids}]"
}
record_before "${DM_PLAIN}"
record_before "${DM_EVAL}"

# --- 3. build the source image + upgrade --------------------------------------
note "building the operator image from source: ${IMG}"
make docker-build "IMG=${IMG}"
kind load docker-image "${IMG}" --name "${CLUSTER}"

# Helm never upgrades crds/ (chart README "CRD upgrade caveat"); apply the source
# CRD out of band FIRST so the new fields exist before the new controller runs.
# Use the exact command the README documents (a client-side `kubectl apply -f`),
# so this test fails loudly if that documented step stops working.
note "applying the source CRD out of band (per the chart README caveat)"
crd_path="charts/decision-model-operator/crds/decisionmodel.io_decisionmodels.yaml"
[[ -f "${crd_path}" ]] || fail "chart CRD not found at ${crd_path} (README references this path)"
kc apply -f "${crd_path}"

src_repo="${IMG%:*}"; src_tag="${IMG##*:}"
note "helm upgrade to the local chart + source image ${IMG}"
hc upgrade "${RELEASE}" charts/decision-model-operator \
  --namespace "${OPERATOR_NS}" \
  --set image.repository="${src_repo}" \
  --set image.tag="${src_tag}" \
  --set image.pullPolicy=IfNotPresent \
  --wait --timeout 5m
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${OPERATOR_NS}" --timeout=3m

# --- 4. assert no fleet rollout for ~2 minutes --------------------------------
note "watching for ~2 min: no candidate, same stable, same Pods, Ready, serving"
deadline=$(( $(date +%s) + 120 ))
while :; do
  for dm in "${DM_PLAIN}" "${DM_EVAL}"; do
    { read -r stable_before; read -r dep; read -r uids_before; } <"${work}/${dm}.before"
    phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    stable_now="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
    cand_now="$(jp decisionmodel "${dm}" '{.status.candidateRevision.hash}')"
    uids_now="$(serving_pod_uids "${dm}")"
    [[ "${stable_now}" = "${stable_before}" ]] || fail "${dm}: stable hash changed ${stable_before} -> ${stable_now} (upgrade re-rolled)"
    [[ -z "${cand_now}" ]] || fail "${dm}: a candidate was created by the upgrade (${cand_now})"
    [[ "${phase}" = "Ready" ]] || fail "${dm}: phase left Ready during the upgrade (${phase})"
    [[ "${uids_now}" = "${uids_before}" ]] || fail "${dm}: serving Pods were restarted ([${uids_before}] -> [${uids_now}])"
    kc get deploy "${dep}" -n "${NS}" >/dev/null 2>&1 || fail "${dm}: serving Deployment ${dep} disappeared"
  done
  (( $(date +%s) >= deadline )) && break
  sleep 10
done
echo "PASS: both DecisionModels unchanged across the upgrade (same stable, no candidate, same Pods, Ready)."

note "both DecisionModels still answer /v1/systemone after the upgrade"
answers_ok "${DM_PLAIN}" || fail "${DM_PLAIN} stopped answering /v1/systemone after the upgrade"
answers_ok "${DM_EVAL}"  || fail "${DM_EVAL} stopped answering /v1/systemone after the upgrade"
echo "PASS: both serve after the upgrade."

# --- 5. the CRD upgrade actually applied (new field accepted) -----------------
note "verifying the CRD upgrade: spec.runtimeVersion is accepted"
# A pre-A-048 CRD rejects an unknown spec.runtimeVersion (structural schema prunes/denies).
# A server-side dry-run apply that succeeds proves the out-of-band CRD apply in step 3
# took effect; failure means the README's CRD-upgrade step and reality disagree.
if ! kc apply --dry-run=server -f - >/dev/null 2>&1 <<YAML
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: crd-probe}
spec:
  engine: ollaya
  model: ${MODEL}
  device: cpu
  runtimeVersion: "${RUNTIME_VERSION}"
YAML
then
  fail "spec.runtimeVersion rejected after upgrade — the chart README's out-of-band CRD apply step does not match reality"
fi
echo "PASS: the upgraded CRD accepts spec.runtimeVersion."

# --- 6. opt-in roll: set spec.runtimeVersion on ONE DM ------------------------
note "preloading the pin-target runtime image ${PIN_IMAGE} into the cluster"
docker image inspect "${PIN_IMAGE}" >/dev/null 2>&1 || docker pull "${PIN_IMAGE}"
kind load docker-image "${PIN_IMAGE}" --name "${CLUSTER}"

note "opt-in: set spec.runtimeVersion=${RUNTIME_VERSION} on ${DM_EVAL} only -> expect one roll there"
{ read -r eval_stable_before; read -r _; read -r _; } <"${work}/${DM_EVAL}.before"
{ read -r plain_stable_before; read -r _; read -r plain_uids_before; } <"${work}/${DM_PLAIN}.before"

kc patch decisionmodel "${DM_EVAL}" -n "${NS}" --type=merge \
  -p "{\"spec\":{\"runtimeVersion\":\"${RUNTIME_VERSION}\"}}"

# The released stable runs the then-current default runtime (its recorded image).
# Pinning a DIFFERENT valid version resolves a different image, so the revision hash
# changes and exactly one blue-green rollout runs. Wait for the new stable to settle.
rolled=""
for _ in $(seq 1 120); do
  stable_now="$(jp decisionmodel "${DM_EVAL}" '{.status.stableRevision.hash}')"
  phase="$(jp decisionmodel "${DM_EVAL}" '{.status.phase}')"
  if [[ "${stable_now}" != "${eval_stable_before}" && "${phase}" = "Ready" ]]; then
    rolled="yes"; break
  fi
  sleep 5
done
[[ "${rolled}" = "yes" ]] || {
  kc describe decisionmodel "${DM_EVAL}" -n "${NS}" | tail -40 >&2
  fail "${DM_EVAL} did not roll after pinning spec.runtimeVersion=${RUNTIME_VERSION} (phase=${phase:-}, stable still ${eval_stable_before})"
}
rv="$(jp decisionmodel "${DM_EVAL}" '{.status.stableRevision.runtimeVersion}')"
[[ "${rv}" = "${RUNTIME_VERSION}" ]] || fail "${DM_EVAL} rolled but recorded runtimeVersion='${rv:-}' (want ${RUNTIME_VERSION})"
echo "PASS: ${DM_EVAL} rolled exactly once to a new stable (${eval_stable_before} -> ${stable_now}), runtimeVersion=${rv}."

note "the other DM (${DM_PLAIN}) must be untouched by the pin on ${DM_EVAL}"
plain_stable_now="$(jp decisionmodel "${DM_PLAIN}" '{.status.stableRevision.hash}')"
plain_uids_now="$(serving_pod_uids "${DM_PLAIN}")"
[[ "${plain_stable_now}" = "${plain_stable_before}" ]] || fail "${DM_PLAIN} rolled when only ${DM_EVAL} was pinned (${plain_stable_before} -> ${plain_stable_now})"
[[ "${plain_uids_now}" = "${plain_uids_before}" ]] || fail "${DM_PLAIN} Pods restarted when only ${DM_EVAL} was pinned"
echo "PASS: ${DM_PLAIN} unchanged — the opt-in roll was scoped to ${DM_EVAL} only."

note "upgrade-chart-e2e PASSED"
