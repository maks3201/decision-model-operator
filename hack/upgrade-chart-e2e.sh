#!/usr/bin/env bash
# upgrade-chart-e2e.sh — prove a Helm-chart operator upgrade does NOT roll the fleet,
# and that DecisionModels mid-rollout survive the upgrade with the expected outcome.
#
# The promise under test: with the default runtime-version policy (Pinned), upgrading
# the operator — even to one whose default engine runtime version differs — must NOT
# start a rollout for any existing DecisionModel. A rollout happens only when the user
# opts in by setting spec.runtimeVersion. In addition, DecisionModels that are mid-
# rollout at upgrade time must finish with the outcome they would have reached anyway.
#
# Flow:
#   1. kind + the Ollaya runtime image (hack/kind-up.sh).
#   2. Install the PREVIOUS RELEASED chart (oci://ghcr.io/<repo>/charts/..., default
#      the latest release) with its released operator image; create one quiet
#      DecisionModel (laya:en, no eval); wait Ready; record its stable hash, serving
#      Deployment name and serving Pod UIDs.
#   2b. Create one more DecisionModel and park it in AwaitingPromotion (Manual + eval)
#      so it straddles the upgrade (a stable Pod + a model-ready candidate Pod). Only
#      one mid-rollout DM plus one quiet stable are used because every laya:en Pod holds
#      the model in memory (~3 GiB) and a single kind node cannot run more than ~three
#      model Pods at once; AwaitingPromotion is the phase the task calls out (the
#      out (the evaluation-identity change requires re-approval) and its candidate is
#      itself an in-flight revision, covering "a candidate survives the upgrade".
#   3. Build the operator image from the current source, load it into kind. Apply the
#      source CRD out of band FIRST (Helm never upgrades crds/ — see the chart README),
#      then `helm upgrade` to the local chart + the source image.
#   4. Assert for ~2 minutes: the quiet DMs do not roll (no candidate, same stable,
#      same Pods, Ready) and keep serving /v1/systemone.
#   4b. Assert the parked DM: it stays in AwaitingPromotion, but the evaluation-identity
#      change in this release refreshes its approvalId (the OLD approvalId is ignored,
#      the NEW one promotes it).
#   4c. Every existing object round-trips cleanly under the new CRD (get -o yaml +
#      server-side dry-run apply) — no CRD validation error on existing objects.
#   5. Assert the CRD upgrade actually applied: the new spec.runtimeVersion field is
#      accepted (a server-side dry-run apply).
#   6. Set spec.runtimeVersion explicitly on ONE DM → expect exactly one blue-green
#      rollout on that DM only; the other quiet DM stays put.
#   7. Downgrade (new -> previous) on the quiet DM: helm upgrade back to the released
#      chart/image must NOT roll it (the CRD is not downgraded; schema changes are
#      additive). Documented no-roll invariant for the quiet fleet.
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
DM_PLAIN="upgrade-plain"      # no eval, plain quiet stable (the no-roll fleet)
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

# wait_phase <dm> <phase> <tries> — block until status.phase equals <phase>.
# Returns 0 on match, 1 on timeout (the caller decides whether a miss is fatal:
# short phases like Caching may fly past on a warm node).
wait_phase() {
  local dm="$1" want="$2" tries="${3:-120}" phase=""
  for _ in $(seq 1 "${tries}"); do
    phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
    [[ "${phase}" = "${want}" ]] && return 0
    sleep 2
  done
  return 1
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

note "creating the quiet DecisionModel (plain, no-roll fleet)"
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
YAML

wait_ready "${DM_PLAIN}"

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

# --- 2b. drive one more DecisionModel into an active rollout phase at upgrade --
# This DM is deliberately NOT quiet at upgrade time: it is parked in AwaitingPromotion
# (a stable Pod serving + a model-ready candidate Pod awaiting approval). The upgrade
# in step 3 happens while it is parked, and step 4b asserts the expected behaviour.
#
# Scope note: the chart-upgrade leg runs on ONE kind node, and every laya:en Pod holds
# the model in memory (~3 GiB RSS regardless of the request). An AwaitingPromotion DM
# already runs two such Pods (stable + candidate); together with the one quiet stable
# that is three model-loaded Pods, which is at the memory ceiling of a single
# ubuntu-latest / local node. Adding more simultaneous mid-rollout DMs (Caching,
# Evaluating, Stabilizing — each another one-or-two model Pods) overloads the node
# (verified: more DMs made the warmup probe time out and a stable go Degraded).
# AwaitingPromotion is the phase the task calls out specifically (the
# evaluation-identity change requires re-approval) and is the one that is both
# deterministic to hold and in budget; its candidate is itself an in-flight revision,
# so it also covers "a candidate mid-rollout survives the upgrade and converges", the
# invariant a Caching/Evaluating/Starting upgrade would check. A longer stabilization
# window and a slow eval cannot be added as their own DMs here without exceeding node
# memory.
DM_AWAIT="upgrade-await"   # eval-gated + Manual promotion -> parks in AwaitingPromotion

note "bringing up a stable that will be parked in AwaitingPromotion at upgrade time"
kc apply -n "${NS}" -f - <<YAML
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata: {name: ${DM_AWAIT}}
spec:
  engine: ollaya
  model: ${MODEL}
  device: cpu
  replicas: 1
  resources: {requests: {cpu: 250m, memory: 1Gi}, limits: {memory: 4Gi}}
YAML
wait_ready "${DM_AWAIT}"
record_before "${DM_AWAIT}"

# Park it in AwaitingPromotion: eval-gated + manual promotion, bump cpu to force a new
# candidate that passes its gate and then waits for approval (no progress timeout while
# parked). This DM is created and parked by the PREVIOUS released controller, so it uses
# that release's field name: v0.3.0 has `manualPromotion: true` (the `promotion: Manual`
# enum is newer). After the upgrade the new controller treats manualPromotion:true as an
# alias for promotion: Manual, so the candidate stays parked across the upgrade.
note "parking ${DM_AWAIT} in AwaitingPromotion"
kc patch decisionmodel "${DM_AWAIT}" -n "${NS}" --type=merge -p "$(cat <<JSON
{"spec":{"resources":{"requests":{"cpu":"300m","memory":"1Gi"},"limits":{"memory":"4Gi"}},
"rollout":{"manualPromotion":true,"evaluation":{"datasetRef":{"configMapRef":{"name":"upgrade-golden","key":"cases.jsonl"}},"minAccuracy":"0.0"},"timeouts":{"evaluating":"30m"}}}}
JSON
)"
wait_phase "${DM_AWAIT}" "AwaitingPromotion" 180 \
  || fail "${DM_AWAIT} did not reach AwaitingPromotion before the upgrade (phase=$(jp decisionmodel "${DM_AWAIT}" '{.status.phase}'))"
# On the previous release the approval token is the candidate's bare revision hash
# (status.evaluation.approvalId did not exist yet). Record both so we can show the
# identity change after the upgrade.
await_cand_before="$(jp decisionmodel "${DM_AWAIT}" '{.status.candidateRevision.hash}')"
await_approval_before="$(jp decisionmodel "${DM_AWAIT}" '{.status.evaluation.approvalId}')"
echo "pre-upgrade  ${DM_AWAIT}: parked, candidateHash=${await_cand_before} approvalId='${await_approval_before}' (empty on the old release)"

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
note "watching for ~2 min: the quiet DM has no candidate, same stable, same Pods, Ready"
dm="${DM_PLAIN}"
{ read -r stable_before; read -r dep; read -r uids_before; } <"${work}/${dm}.before"
deadline=$(( $(date +%s) + 120 ))
while :; do
  phase="$(jp decisionmodel "${dm}" '{.status.phase}')"
  stable_now="$(jp decisionmodel "${dm}" '{.status.stableRevision.hash}')"
  cand_now="$(jp decisionmodel "${dm}" '{.status.candidateRevision.hash}')"
  uids_now="$(serving_pod_uids "${dm}")"
  [[ "${stable_now}" = "${stable_before}" ]] || fail "${dm}: stable hash changed ${stable_before} -> ${stable_now} (upgrade re-rolled)"
  [[ -z "${cand_now}" ]] || fail "${dm}: a candidate was created by the upgrade (${cand_now})"
  [[ "${phase}" = "Ready" ]] || fail "${dm}: phase left Ready during the upgrade (${phase})"
  [[ "${uids_now}" = "${uids_before}" ]] || fail "${dm}: serving Pods were restarted ([${uids_before}] -> [${uids_now}])"
  kc get deploy "${dep}" -n "${NS}" >/dev/null 2>&1 || fail "${dm}: serving Deployment ${dep} disappeared"
  (( $(date +%s) >= deadline )) && break
  sleep 10
done
echo "PASS: the quiet ${DM_PLAIN} is unchanged across the upgrade (same stable, no candidate, same Pods, Ready)."

note "the quiet DM still answers /v1/systemone after the upgrade"
answers_ok "${DM_PLAIN}" || fail "${DM_PLAIN} stopped answering /v1/systemone after the upgrade"
echo "PASS: ${DM_PLAIN} serves after the upgrade."

# --- 4b. the parked DecisionModel across the upgrade --------------------------
# This release introduces the evaluation identity: status.evaluation.approvalId
# (sha256 of revision + policyHash + datasetDigest). On the previous release the
# approval token was the bare revision hash. After the upgrade a parked candidate must
# therefore be re-approved with the NEW approvalId; the old bare-hash token no longer
# promotes it. Document + assert exactly that.
note "AwaitingPromotion: ${DM_AWAIT} must still be parked, now with a populated approvalId"
wait_phase "${DM_AWAIT}" "AwaitingPromotion" 180 \
  || fail "${DM_AWAIT} left AwaitingPromotion unexpectedly after the upgrade (phase=$(jp decisionmodel "${DM_AWAIT}" '{.status.phase}'))"
# The new controller re-evaluates the parked candidate under the new evaluation
# identity and records an approvalId; poll until it is populated (a few reconciles
# after the manager restarts).
await_approval_after=""
for _ in $(seq 1 120); do
  await_approval_after="$(jp decisionmodel "${DM_AWAIT}" '{.status.evaluation.approvalId}')"
  [[ -n "${await_approval_after}" ]] && break
  sleep 5
done
[[ -n "${await_approval_after}" ]] || fail "${DM_AWAIT}: no approvalId populated after the upgrade (the evaluation-identity change was expected to add one)"
echo "post-upgrade ${DM_AWAIT}: approvalId now '${await_approval_after}' (was '${await_approval_before}' on the old release)"
[[ "${await_approval_after}" != "${await_approval_before}" ]] || fail \
  "${DM_AWAIT}: approvalId unchanged across the upgrade; a re-approval was expected"

note "approving ${DM_AWAIT} with the OLD token (pre-upgrade candidate hash) must be ignored"
kc annotate decisionmodel "${DM_AWAIT}" -n "${NS}" \
  "decisionmodel.io/promote=${await_cand_before}" --overwrite
# Give the controller a few reconciles; it must stay parked (the old token is stale).
sleep 20
stale_phase="$(jp decisionmodel "${DM_AWAIT}" '{.status.phase}')"
[[ "${stale_phase}" = "AwaitingPromotion" ]] || fail \
  "${DM_AWAIT}: a stale (pre-upgrade) approval token promoted it (phase=${stale_phase}) — it must be ignored"
echo "PASS: the stale pre-upgrade token was ignored; ${DM_AWAIT} stays parked."

note "approving ${DM_AWAIT} with the NEW approvalId promotes it"
kc annotate decisionmodel "${DM_AWAIT}" -n "${NS}" \
  "decisionmodel.io/promote=${await_approval_after}" --overwrite
wait_phase "${DM_AWAIT}" "Ready" 120 \
  || fail "${DM_AWAIT} did not promote after approving with the new approvalId (phase=$(jp decisionmodel "${DM_AWAIT}" '{.status.phase}'))"
echo "PASS: ${DM_AWAIT} promoted on the new approvalId after the upgrade."

# --- 4c. existing objects round-trip cleanly under the new CRD ----------------
note "no CRD validation error on existing objects (get -o yaml round-trip with the new CRD)"
for dm in "${DM_PLAIN}" "${DM_AWAIT}"; do
  kc get decisionmodel "${dm}" -n "${NS}" -o yaml >"${work}/${dm}.rt.yaml" \
    || fail "${dm}: get -o yaml failed under the new CRD"
  kc apply --dry-run=server -f "${work}/${dm}.rt.yaml" >/dev/null \
    || fail "${dm}: existing object failed server-side validation under the new CRD"
done
echo "PASS: all existing DecisionModels round-trip cleanly under the upgraded CRD."


# --- 5. the CRD upgrade actually applied (new field accepted) -----------------
note "verifying the CRD upgrade: spec.runtimeVersion is accepted"
# An older CRD (before the runtimeVersion field existed) rejects an unknown
# spec.runtimeVersion (structural schema prunes/denies).
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

note "opt-in: set spec.runtimeVersion=${RUNTIME_VERSION} on ${DM_AWAIT} only -> expect one roll there"
# ${DM_AWAIT} is Ready and quiet after its post-upgrade promotion; read its CURRENT
# stable so the roll comparison is against the live state, not the pre-upgrade .before.
await_stable_before="$(jp decisionmodel "${DM_AWAIT}" '{.status.stableRevision.hash}')"
[[ -n "${await_stable_before}" ]] || fail "no current stable for ${DM_AWAIT} before the opt-in pin"
{ read -r plain_stable_before; read -r _; read -r plain_uids_before; } <"${work}/${DM_PLAIN}.before"

# Clear the manual-promotion policy first (it still carries manualPromotion:true from
# the parking step); otherwise the runtimeVersion candidate would park in
# AwaitingPromotion instead of auto-rolling. The new controller honours promotion:
# Automatic. Done as a separate patch so the policy is cleared before the pin.
kc patch decisionmodel "${DM_AWAIT}" -n "${NS}" --type=merge \
  -p '{"spec":{"rollout":{"promotion":"Automatic","manualPromotion":null}}}'

kc patch decisionmodel "${DM_AWAIT}" -n "${NS}" --type=merge \
  -p "{\"spec\":{\"runtimeVersion\":\"${RUNTIME_VERSION}\"}}"

# The stable runs the default runtime (its recorded image). Pinning a DIFFERENT valid
# version resolves a different image, so the revision hash changes and exactly one
# blue-green rollout runs. Wait for the new stable to settle.
rolled=""
for _ in $(seq 1 120); do
  stable_now="$(jp decisionmodel "${DM_AWAIT}" '{.status.stableRevision.hash}')"
  phase="$(jp decisionmodel "${DM_AWAIT}" '{.status.phase}')"
  if [[ "${stable_now}" != "${await_stable_before}" && "${phase}" = "Ready" ]]; then
    rolled="yes"; break
  fi
  sleep 5
done
[[ "${rolled}" = "yes" ]] || {
  kc describe decisionmodel "${DM_AWAIT}" -n "${NS}" | tail -40 >&2
  fail "${DM_AWAIT} did not roll after pinning spec.runtimeVersion=${RUNTIME_VERSION} (phase=${phase:-}, stable still ${await_stable_before})"
}
rv="$(jp decisionmodel "${DM_AWAIT}" '{.status.stableRevision.runtimeVersion}')"
[[ "${rv}" = "${RUNTIME_VERSION}" ]] || fail "${DM_AWAIT} rolled but recorded runtimeVersion='${rv:-}' (want ${RUNTIME_VERSION})"
echo "PASS: ${DM_AWAIT} rolled exactly once to a new stable (${await_stable_before} -> ${stable_now}), runtimeVersion=${rv}."

note "the other DM (${DM_PLAIN}) must be untouched by the pin on ${DM_AWAIT}"
plain_stable_now="$(jp decisionmodel "${DM_PLAIN}" '{.status.stableRevision.hash}')"
plain_uids_now="$(serving_pod_uids "${DM_PLAIN}")"
[[ "${plain_stable_now}" = "${plain_stable_before}" ]] || fail "${DM_PLAIN} rolled when only ${DM_AWAIT} was pinned (${plain_stable_before} -> ${plain_stable_now})"
[[ "${plain_uids_now}" = "${plain_uids_before}" ]] || fail "${DM_PLAIN} Pods restarted when only ${DM_AWAIT} was pinned"
echo "PASS: ${DM_PLAIN} unchanged — the opt-in roll was scoped to ${DM_AWAIT} only."

# --- 7. downgrade (new -> previous) on a quiet fleet --------------------------
# Documented behaviour: downgrading only the controller (helm upgrade back to the
# released chart + image) on a quiet fleet must NOT roll it. The CRD is NOT
# downgraded — CRD schema changes are additive and the chart README's caveat says
# the operator never removes CRD fields, so the newer CRD stays in place and existing
# objects keep validating. DM_PLAIN was never touched by steps 4b/6, so it is the
# quiet fleet here. (Objects that set a new-release-only field like spec.runtimeVersion
# would keep that value stored; the older controller ignores unknown status it did not
# write. We assert the no-roll invariant for the quiet DM only.)
note "downgrade: helm upgrade back to the released chart ${from_ver} on the quiet ${DM_PLAIN}"
{ read -r dn_stable_before; read -r _; read -r dn_uids_before; } <"${work}/${DM_PLAIN}.before"
hc upgrade "${RELEASE}" "${CHART_REF}" --version "${from_ver}" \
  --namespace "${OPERATOR_NS}" \
  --set image.pullPolicy=IfNotPresent \
  --reuse-values \
  --wait --timeout 5m
kc wait deployment.apps -l control-plane=controller-manager \
  --for=condition=Available -n "${OPERATOR_NS}" --timeout=3m
# Watch the quiet DM for ~60s: no roll, no candidate, Pods not restarted.
dn_deadline=$(( $(date +%s) + 60 ))
while :; do
  dn_stable_now="$(jp decisionmodel "${DM_PLAIN}" '{.status.stableRevision.hash}')"
  dn_cand_now="$(jp decisionmodel "${DM_PLAIN}" '{.status.candidateRevision.hash}')"
  dn_uids_now="$(serving_pod_uids "${DM_PLAIN}")"
  dn_phase="$(jp decisionmodel "${DM_PLAIN}" '{.status.phase}')"
  [[ "${dn_stable_now}" = "${dn_stable_before}" ]] || fail "${DM_PLAIN}: downgrade re-rolled the stable (${dn_stable_before} -> ${dn_stable_now})"
  [[ -z "${dn_cand_now}" ]] || fail "${DM_PLAIN}: downgrade created a candidate (${dn_cand_now})"
  [[ "${dn_uids_now}" = "${dn_uids_before}" ]] || fail "${DM_PLAIN}: downgrade restarted serving Pods"
  [[ "${dn_phase}" = "Ready" ]] || fail "${DM_PLAIN}: downgrade left phase Ready (${dn_phase})"
  (( $(date +%s) >= dn_deadline )) && break
  sleep 10
done
echo "PASS: downgrade to ${from_ver} did not roll the quiet ${DM_PLAIN} (same stable, no candidate, same Pods, Ready)."

note "upgrade-chart-e2e PASSED"
