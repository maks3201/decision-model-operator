# GitOps: Argo CD and Flux

Keep your `DecisionModel` manifests in Git and let Argo CD or Flux apply them.
Both tools need to know when a DecisionModel is **healthy**, **progressing** or
**degraded** — otherwise a blue-green rollout that is rolling back can show up as
"Healthy" and hide a regression.

A DecisionModel reports its state through `status.phase`, the standard
`status.conditions` (`Ready`, `Degraded`, …) and `status.observedGeneration`.
The relevant phases are `Pending`, `Resolving`, `Caching`, `Starting`,
`Evaluating`, `Promoting`, `AwaitingPromotion`, `Ready`, `Degraded`,
`RolledBack` and `Failed` (see [quickstart.md](quickstart.md) for what each
means).

## What goes in Git

Put **only the `DecisionModel`** (and its ConfigMaps/Secrets, e.g. a golden
dataset or an API key) in Git. The operator creates and owns the Deployments,
Jobs, Service and PVCs for each revision — do **not** commit those; Argo CD/Flux
would fight the controller over them. Point the Service, replica counts, cache
and resources through the `DecisionModel` spec, not through hand-written child
objects.

You do **not** need `ignoreDifferences` for the `DecisionModel` itself: `status`
is a Kubernetes subresource on this CRD (`subresources.status: {}`), so neither
tool treats `status` as part of the desired manifest and there is nothing to
ignore there.

## Argo CD — custom health check

Argo CD does not know this CRD, so without a health check it reports a
DecisionModel as `Healthy` as soon as it exists. Add a Lua health check that maps
our phases and conditions. Put this under `resource.customizations` in the
`argocd-cm` ConfigMap:

```yaml
# argocd-cm ConfigMap
data:
  resource.customizations.health.decisionmodel.io_DecisionModel: |
    local hs = {}
    hs.status = "Progressing"
    hs.message = "Waiting for DecisionModel status"

    if obj.status == nil then
      return hs
    end

    -- Spec changed but not yet observed by the controller: still progressing.
    if obj.metadata ~= nil and obj.metadata.generation ~= nil then
      if obj.status.observedGeneration == nil or obj.status.observedGeneration < obj.metadata.generation then
        hs.status = "Progressing"
        hs.message = "Waiting for the operator to observe generation " .. tostring(obj.metadata.generation)
        return hs
      end
    end

    -- Prefer a condition message when present (richer than the phase alone).
    -- Use the Ready message normally; use the Degraded message only when the
    -- Degraded condition is actually True (a stale Degraded=False must not show).
    local readyMsg = nil
    local degradedMsg = nil
    if obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Ready" and c.message ~= nil and c.message ~= "" then
          readyMsg = c.message
        end
        if c.type == "Degraded" and c.status == "True" and c.message ~= nil and c.message ~= "" then
          degradedMsg = c.message
        end
      end
    end

    local phase = obj.status.phase
    if phase == "Ready" then
      hs.status = "Healthy"
      hs.message = readyMsg or "Serving"
      return hs
    elseif phase == "AwaitingPromotion" then
      -- Candidate passed its gate and is waiting for a human to approve it
      -- (spec.rollout.manualPromotion); the stable keeps serving. Surface it as
      -- Suspended so a sync does not sit in Progressing forever waiting on a
      -- human, and Argo CD shows it as "paused for input".
      hs.status = "Suspended"
      hs.message = "Awaiting manual promotion (set the decisionmodel.io/promote annotation)"
      return hs
    elseif phase == "Pending" or phase == "Resolving" or phase == "Caching"
        or phase == "Starting" or phase == "Evaluating" or phase == "Promoting" then
      hs.status = "Progressing"
      hs.message = "Rolling out: " .. phase
      return hs
    elseif phase == "Degraded" or phase == "RolledBack" or phase == "Failed" then
      hs.status = "Degraded"
      hs.message = degradedMsg or readyMsg or phase
      return hs
    end

    hs.status = "Progressing"
    hs.message = "Unknown phase: " .. tostring(phase)
    return hs
```

Behaviour:

| DecisionModel state | Argo CD health |
|---------------------|----------------|
| `Ready` | `Healthy` |
| `Pending` / `Resolving` / `Caching` / `Starting` / `Evaluating` / `Promoting` | `Progressing` |
| `AwaitingPromotion` (candidate passed its gate, waiting for human approval) | `Suspended` |
| `Degraded` (partial or zero readiness) | `Degraded` (with the `Degraded` condition message) |
| `RolledBack` (candidate failed, stable still serving) | `Degraded` |
| `Failed` | `Degraded` |
| `observedGeneration < generation` (spec not yet seen) | `Progressing` |

`AwaitingPromotion` and its `Promoted` condition (`False`/`AwaitingApproval` while
a candidate waits, `True` after promotion) exist only when
`spec.rollout.manualPromotion` is set; otherwise the phase never appears.

Unlike kstatus (below), the Argo CD script reads `status.phase`, so it reports
`RolledBack` and `Failed` as `Degraded` even though the stable's `Ready` condition
is still `True`.

This script was tested with `gopher-lua` (the Lua runtime Argo CD embeds) against
sample `status` JSON for every phase above, including the two `Degraded` sub-cases
and the stale-generation case — all mapped as in the table.

## Flux — kstatus and `healthCheckExprs`

Flux's **default** health evaluation uses **kstatus** (`fluxcd/cli-utils`, a fork
of [kubernetes-sigs/cli-utils](https://github.com/kubernetes-sigs/cli-utils/blob/master/pkg/kstatus/README.md)),
which has no rule specific to this CRD and applies its generic rules:

1. If `metadata.generation != status.observedGeneration`, the resource is
   **InProgress**. The operator always sets `observedGeneration`, so this works.
2. If the standard `Reconciling` / `Stalled` conditions are present, kstatus uses
   them. The operator sets **neither**.
3. Otherwise kstatus falls back to the **`Ready`** condition: `Ready=True` →
   **Current** (healthy); `Ready=False` → **InProgress**.

**Default kstatus misreports this CRD — do not rely on it alone.** The problem is
rule 3 together with how the controller keeps the stable serving during trouble
(verified in `internal/controller/decisionmodel_controller.go`):

- **`RolledBack` reports `Current` (healthy).** On a rollback the controller runs
  `applyStableReadiness` and sets `Ready=True` when the stable revision is fully
  ready, *then* sets the phase to `RolledBack` (the failed candidate was rolled
  back; the stable keeps serving). The `Ready` condition is `True`, so kstatus
  reports **Current** — a failed rollout looks healthy. A `Degraded=True` condition
  with the real reason is also set, but kstatus does not look at it.
- **A candidate rolling out over a healthy stable reports `Current` too.** While a
  new candidate is `Starting` / `Evaluating` / `AwaitingPromotion`, the controller
  updates `ModelReady` but does not touch `Ready`, so `Ready` keeps the stable's
  last `True`. kstatus sees `Ready=True` → **Current**, even though a rollout is in
  flight. (A *first* rollout, with no prior stable, has no `Ready=True` yet, so
  that case does show InProgress.)
- **The partial-`Degraded` case reports `Current`.** When some but not all replicas
  are model-ready, the phase is `Degraded` but `Ready` is still `True` (the model
  answers on the ready replicas), so kstatus reports **Current**.

So with default kstatus, a Flux `wait` returns "ready" during rollbacks, in-flight
rollouts over a healthy stable, and partial degradation. Use a custom check instead.

### Use `healthCheckExprs` (CEL on `status.phase`)

Flux **v2.5+** (Feb 2025) supports custom health checks via
`spec.healthCheckExprs` ([Flux CEL cheatsheet](https://fluxcd.io/flux/cheatsheets/cel-healthchecks/)).
Key the check on `status.phase` so it reflects rollouts rather than the `Ready`
condition. The input to each expression is the resource object; a resource is
InProgress while neither `current` nor `failed` matches:

```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: decision-models
  namespace: flux-system
spec:
  interval: 10m
  path: ./clusters/prod/decision-models
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
  timeout: 10m
  wait: true
  healthChecks:
    - apiVersion: decisionmodel.io/v1alpha1
      kind: DecisionModel
      name: support-router
      namespace: default
  healthCheckExprs:
    - apiVersion: decisionmodel.io/v1alpha1
      kind: DecisionModel
      # Healthy only when fully Ready and the controller has seen the latest spec.
      current: >-
        has(status.phase) && status.phase == 'Ready' &&
        status.observedGeneration == metadata.generation
      # Fail fast on a failed/rolled-back/degraded rollout instead of waiting for the timeout.
      failed: >-
        has(status.phase) &&
        (status.phase == 'Failed' || status.phase == 'RolledBack' || status.phase == 'Degraded')
```

With this, `Pending` / `Resolving` / `Caching` / `Starting` / `Evaluating` /
`Promoting` / `AwaitingPromotion` (none matching `current` or `failed`) are
**InProgress**, `Ready` is **Current**, and `Failed` / `RolledBack` / `Degraded`
are **Failed** so `wait: true` fails fast. Treat `AwaitingPromotion` as InProgress
here; if you use manual promotion and do not want Flux to block on it, add
`status.phase == 'AwaitingPromotion'` to the `current` expression instead.

> Not tested on a live cluster in this change: the `healthCheckExprs` schema,
> the v2.5 availability and the CEL field names (`current` / `failed`, the `has()`
> macro) are from the Flux docs linked above; the CEL expressions were not run
> through a Flux controller here. The kstatus misreport above **is** derived from
> the controller code. Validate the CEL on your Flux version (the
> [CEL Playground](https://playground.cel.dev/) accepts the resource object as
> input) before relying on it.

