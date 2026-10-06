#!/usr/bin/env bash
# demo.sh — the eval-gated rollout demo on a local kind cluster.
#
# The story, end to end and with real output:
#   1. bring up a kind cluster, install the operator, pre-load the Ollaya runtime image;
#   2. deploy an eval-gated DecisionModel (stable model: laya:en) and wait for Ready;
#   3. send one /v1/systemone request through the Service and print the decision;
#   4. patch spec.model to a WEAKER candidate — the operator evaluates it, it misses
#      the accuracy gate, and the rollout is RolledBack; the Service selector never
#      moves and the stable model keeps answering;
#   5. patch spec.model to a STRONG candidate — it passes the gate and is promoted;
#      the previous revision is kept for a stabilization window (instant rollback) and
#      then collected (a Stabilized event);
#   6. `hack/demo.sh clean` deletes the cluster.
#
# Models and gate (measured on examples/demo/dataset.yaml, 40 English support
# tickets; see examples/demo/README.md for the numbers and how to reproduce):
#   stable laya:en            accuracy 0.925
#   bad    laya:multilingual  accuracy 0.800  -> below minAccuracy 0.90, rolled back
#   good   nli:latest         accuracy 0.925  -> passes, promoted
#
# Usage:
#   hack/demo.sh prepare   # create the cluster, install, pre-pull all models (slow, once)
#   hack/demo.sh run       # steps 2-5 (assumes prepare already ran; fast)
#   hack/demo.sh           # prepare + run
#   hack/demo.sh clean     # delete the kind cluster
#
# Env:
#   KIND_CLUSTER   kind cluster name (default dmo-demo)
#   INSTALL        how to install the operator: "local" (default, build from source)
#                  or "chart" (released Helm chart from ghcr.io)
#   CHART_VERSION  chart version for INSTALL=chart (default: latest)
#   OLLAYA_IMAGE   runtime image to preload (default the CPU tag, see hack/kind-up.sh)
#   IMG            operator image tag for INSTALL=local (default example.com/...:demo)
#   NO_WAIT        non-empty: do not pause between steps (used by the GIF recorder)
#
# Requires: kind, kubectl, docker, make (INSTALL=local) or helm (INSTALL=chart).
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-dmo-demo}"
INSTALL="${INSTALL:-local}"
CHART_VERSION="${CHART_VERSION:-}"
IMG="${IMG:-example.com/decision-model-operator:demo}"
OLLAYA_IMAGE="${OLLAYA_IMAGE:-ghcr.io/ollaya-dev/ollaya:0.10.0}"
NS="dmo-demo"
DM="intent-router"
OPERATOR_NS="decision-model-operator-system"
CHART_REF="oci://ghcr.io/maks3201/charts/decision-model-operator"

STABLE_MODEL="laya:en"
BAD_MODEL="laya:multilingual"
GOOD_MODEL="nli:latest"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${repo_root}"
kctx="kind-${CLUSTER}"

kc() { kubectl --context "${kctx}" "$@"; }

# --- presentation helpers (plain text, GIF-friendly) --------------------------

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
pause() { [[ -n "${NO_WAIT:-}" ]] || { printf '\n'; read -r -p '    (press Enter to continue) ' _; }; }

# phase <expected> <timeout-seconds> — poll status.phase until it equals <expected>.
phase() {
  local want="$1" timeout="$2" start now
  start=$(date +%s)
  while :; do
    local p
    p=$(kc get dm "${DM}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "${p}" == "${want}" ]] && return 0
    now=$(date +%s)
    if (( now - start > timeout )); then
      echo "timed out after ${timeout}s waiting for phase=${want} (last: ${p:-<none>})" >&2
      kc get dm "${DM}" -n "${NS}" -o wide >&2 || true
      return 1
    fi
    sleep 3
  done
}

# ask <model> — send one /v1/systemone request through the Service and print the
# routed department for a sample ticket. Uses a short-lived port-forward.
ask() {
  local model="$1" ticket="My credit card was charged twice, I need a refund."
  local pf_pid local_port=18435
  kc port-forward -n "${NS}" "svc/${DM}" "${local_port}:11435" >/dev/null 2>&1 &
  pf_pid=$!
  # Wait for the forward to accept connections.
  for _ in $(seq 1 20); do
    curl -sS -o /dev/null "http://127.0.0.1:${local_port}/" 2>/dev/null && break
    sleep 0.5
  done
  local body
  body=$(cat <<JSON
{"model":"${model}","keep_alive":-1,
 "state":"${ticket}",
 "questions":{"q":{"type":"choice","criteria":{
   "billing":"billing, payments, charges, refunds, invoices",
   "technical":"technical bugs, errors, crashes, outages",
   "sales":"pricing, plans, upgrades, purchasing, licenses, quotes",
   "account":"account settings, password, profile, login credentials, workspace members"}}}}
JSON
)
  local out choice conf
  out=$(curl -sS --max-time 120 -X POST "http://127.0.0.1:${local_port}/v1/systemone" \
    -H 'Content-Type: application/json' --data-binary "${body}" 2>/dev/null || true)
  kill "${pf_pid}" 2>/dev/null || true
  wait "${pf_pid}" 2>/dev/null || true
  choice=$(printf '%s' "${out}" | jq -r '.answers.q.choice // "?"' 2>/dev/null || echo '?')
  conf=$(printf '%s' "${out}" | jq -r '.answers.q.confidence // 0' 2>/dev/null || echo '0')
  info "ticket:  \"${ticket}\""
  info "routed:  ${choice}  (confidence ${conf}, served by model ${model})"
}

# service_selector — print the revision the Service currently routes to.
service_selector() {
  kc get svc "${DM}" -n "${NS}" -o jsonpath='{.spec.selector.decisionmodel\.io/revision}' 2>/dev/null || true
}

# --- cluster lifecycle --------------------------------------------------------

prepare() {
  step "Creating kind cluster '${CLUSTER}' and loading the Ollaya runtime image"
  KIND_CLUSTER="${CLUSTER}" OLLAYA_IMAGE="${OLLAYA_IMAGE}" ./hack/kind-up.sh

  install_operator
  info ""
  info "Cluster ready and operator installed. The three demo models"
  info "(${STABLE_MODEL}, ${BAD_MODEL}, ${GOOD_MODEL}) are fetched from the"
  info "registry by the operator's per-revision prefetch Job during 'run'."
}

install_operator() {
  case "${INSTALL}" in
    chart)
      step "Installing the operator from the released Helm chart"
      local args=(upgrade --install dmo "${CHART_REF}"
        --namespace "${OPERATOR_NS}" --create-namespace --wait --timeout 3m)
      [[ -n "${CHART_VERSION}" ]] && args+=(--version "${CHART_VERSION}")
      helm --kube-context "${kctx}" "${args[@]}"
      ;;
    local)
      step "Building the operator image from source and installing it"
      make docker-build "IMG=${IMG}"
      kind load docker-image "${IMG}" --name "${CLUSTER}"
      make install
      make deploy "IMG=${IMG}"
      ;;
    *)
      echo "unknown INSTALL=${INSTALL} (want 'local' or 'chart')" >&2
      exit 2
      ;;
  esac
  kc wait deployment.apps -l control-plane=controller-manager \
    --for=condition=Available -n "${OPERATOR_NS}" --timeout=3m
}

clean() {
  step "Deleting kind cluster '${CLUSTER}'"
  KIND_CLUSTER="${CLUSTER}" ./hack/kind-down.sh
}

# --- the demo run ------------------------------------------------------------

run() {
  step "1/5  Deploy an eval-gated DecisionModel (stable model: ${STABLE_MODEL})"
  kc create ns "${NS}" --dry-run=client -o yaml | kc apply -f -
  kc apply -n "${NS}" -f examples/demo/dataset.yaml
  kc apply -n "${NS}" -f examples/demo/decisionmodel.yaml
  info "Waiting for the first revision to become Ready (prefetch + model-ready gate)..."
  phase Ready 900
  kc get dm "${DM}" -n "${NS}" -o wide
  local stable_rev
  stable_rev=$(kc get dm "${DM}" -n "${NS}" -o jsonpath='{.status.stableRevision.hash}')
  info "Stable revision: ${stable_rev}  (Service routes to decisionmodel.io/revision=$(service_selector))"
  pause

  step "2/5  Ask the stable model to route a support ticket"
  ask "${STABLE_MODEL}"
  pause

  step "3/5  Roll out a WEAKER candidate: ${BAD_MODEL}"
  info "${BAD_MODEL} is a good model, but it routes THIS English support set less"
  info "accurately than ${STABLE_MODEL}. The gate should catch that before traffic moves."
  kc patch dm "${DM}" -n "${NS}" --type merge -p "{\"spec\":{\"model\":\"${BAD_MODEL}\"}}"
  info "Watching the phase advance (Caching -> Starting -> Evaluating -> RolledBack):"
  watch_phases RolledBack 900
  info ""
  info "The gate verdict (Events):"
  kc get events -n "${NS}" --field-selector reason=EvaluationFailed \
    -o custom-columns=REASON:.reason,MESSAGE:.message --no-headers | tail -1 || true
  info ""
  info "The Service selector is STILL on the stable revision ${stable_rev}:"
  info "  decisionmodel.io/revision=$(service_selector)"
  info "status.stableRevision is unchanged:"
  kc get dm "${DM}" -n "${NS}" \
    -o jsonpath='{"  stable="}{.status.stableRevision.hash}{"  model="}{.status.stableRevision.model}{"\n"}'
  info "status.evaluation (the failing candidate's score):"
  kc get dm "${DM}" -n "${NS}" \
    -o jsonpath='{"  accuracy="}{.status.evaluation.accuracy}{"  baseline="}{.status.evaluation.baselineAccuracy}{"\n"}'
  info ""
  info "The same stable model still answers — traffic was never exposed to the regression:"
  ask "${STABLE_MODEL}"
  pause

  step "4/5  Roll out a STRONG candidate: ${GOOD_MODEL}"
  info "${GOOD_MODEL} matches the stable accuracy on this set, so it should pass the gate."
  kc patch dm "${DM}" -n "${NS}" --type merge -p "{\"spec\":{\"model\":\"${GOOD_MODEL}\"}}"
  info "Watching the phase advance (Caching -> Starting -> Evaluating -> Ready):"
  watch_phases Ready 900
  info ""
  info "The gate verdict (Events):"
  kc get events -n "${NS}" --field-selector reason=EvaluationPassed \
    -o custom-columns=REASON:.reason,MESSAGE:.message --no-headers | tail -1 || true
  info "status.evaluation (the promoted candidate's score):"
  kc get dm "${DM}" -n "${NS}" \
    -o jsonpath='{"  accuracy="}{.status.evaluation.accuracy}{"  baseline="}{.status.evaluation.baselineAccuracy}{"\n"}'
  local new_rev
  new_rev=$(kc get dm "${DM}" -n "${NS}" -o jsonpath='{.status.stableRevision.hash}')
  info "The Service now routes to the promoted revision ${new_rev}:"
  info "  decisionmodel.io/revision=$(service_selector)"

  step "5/5  The previous revision is kept for a stabilization window, then collected"
  info "After promotion the operator keeps the previous revision's Deployment running"
  info "(out of the Service) for the stabilization window, so it can switch traffic back"
  info "instantly if the new model turns out unhealthy. Both Deployments are present now:"
  kc get deploy -l "decisionmodel.io/name=${DM}" -n "${NS}" \
    -o custom-columns=DEPLOYMENT:.metadata.name,REVISION:.metadata.labels.decisionmodel\\.io/revision --no-headers
  info ""
  info "The DecisionModel is Ready and in its stabilization window:"
  kc get dm "${DM}" -n "${NS}" \
    -o jsonpath='{"  phase="}{.status.phase}{"  stabilizing="}{.status.conditions[?(@.type=="Stabilizing")].status}{"  previous="}{.status.previousRevision.hash}{" (kept for instant rollback)\n"}'
  info ""
  info "Waiting for the stabilization window to pass and the previous revision to be collected..."
  local start now
  start=$(date +%s)
  while :; do
    local deps
    deps=$(kc get deploy -l "decisionmodel.io/name=${DM}" -n "${NS}" \
      -o jsonpath='{.items[*].metadata.labels.decisionmodel\.io/revision}' 2>/dev/null || true)
    if [[ "$(printf '%s' "${deps}" | wc -w | tr -d ' ')" == "1" && "${deps}" == *"${new_rev}"* ]]; then
      break
    fi
    now=$(date +%s)
    (( now - start > 180 )) && { info "serving Deployments: ${deps}"; break; }
    sleep 3
  done
  info "The Stabilized event (window elapsed, previous revision collected):"
  kc get events -n "${NS}" --field-selector reason=Stabilized \
    -o custom-columns=REASON:.reason,MESSAGE:.message --no-headers | tail -1 || true
  info "Only the promoted revision's Deployment remains:"
  kc get deploy -l "decisionmodel.io/name=${DM}" -n "${NS}" \
    -o custom-columns=DEPLOYMENT:.metadata.name,REVISION:.metadata.labels.decisionmodel\\.io/revision --no-headers
  info ""
  info "The new model now serves production traffic:"
  ask "${GOOD_MODEL}"
  step "Done. A regression was blocked; a good model was promoted. Run 'make demo-clean' to tear down."
}

# watch_phases <terminal-phase> <timeout> — print each distinct phase as the
# rollout advances, stopping when it reaches the terminal phase.
watch_phases() {
  local want="$1" timeout="$2" start now last=''
  start=$(date +%s)
  while :; do
    local p
    p=$(kc get dm "${DM}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [[ -n "${p}" && "${p}" != "${last}" ]]; then
      info "  phase: ${p}"
      last="${p}"
    fi
    [[ "${p}" == "${want}" ]] && return 0
    now=$(date +%s)
    if (( now - start > timeout )); then
      echo "timed out after ${timeout}s waiting for phase=${want} (last: ${last:-<none>})" >&2
      return 1
    fi
    sleep 2
  done
}

main() {
  case "${1:-all}" in
    prepare) prepare ;;
    run)     run ;;
    all)     prepare; run ;;
    clean)   clean ;;
    *) echo "usage: $0 [prepare|run|all|clean]" >&2; exit 2 ;;
  esac
}

main "$@"
