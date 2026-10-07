# Operator metrics

The controller exports custom Prometheus metrics on controller-runtime's metrics
registry, served by the manager's metrics endpoint (`--metrics-bind-address`,
default `:8443` HTTPS in the shipped config; protected by authn/authz). They are
registered automatically at startup — no extra flags are required.

All per-DecisionModel series carry only the `namespace` and `name` labels (never
the revision hash), plus a small fixed set of enum labels, so cardinality stays
bounded. When a DecisionModel is deleted, every series for it is removed, so a
churn of short-lived objects does not leak series.

Per-DecisionModel gauges and counters are updated **only after a status write the
API server accepted** (the same rule the operator uses for Events). A conflicting
or failed status write emits no metric mutation, so metrics never advertise a
phase, replica count, or outcome that was not persisted.

## Metrics

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `decisionmodel_phase` | gauge | `namespace`, `name`, `phase` | `1` for the DecisionModel's current phase and `0` for every other phase (one-hot), so a dashboard never shows a stale phase as active. `phase` is one of `Pending`, `Resolving`, `Caching`, `Starting`, `Evaluating`, `Degraded`, `Promoting`, `Ready`, `Failed`, `RolledBack`. |
| `decisionmodel_model_ready_replicas` | gauge | `namespace`, `name` | Number of serving replicas that passed the model-ready gate (`status.replicas.modelReady`). |
| `decisionmodel_desired_replicas` | gauge | `namespace`, `name` | Desired number of serving replicas (`status.replicas.desired`). |
| `decisionmodel_rollouts_total` | counter | `namespace`, `name`, `result` | Rollout outcomes. `result` is `promoted` (a revision switch was promoted), `rolled_back` (a candidate failed but a stable revision kept serving), or `failed` (a candidate failed with no stable revision to fall back to). |
| `decisionmodel_phase_duration_seconds` | histogram | `namespace`, `name`, `phase` | Time spent in a phase, observed when that phase is left. Buckets span 5s to 1h (`5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600`). |
| `decisionmodel_evaluation_accuracy` | gauge | `namespace`, `name` | Accuracy of the last completed eval-gated evaluation (decimal in `[0,1]`). Only set once an evaluation has completed. |
| `decisionmodel_evaluation_ece` | gauge | `namespace`, `name` | Expected Calibration Error of the last completed evaluation (decimal in `[0,1]`). |
| `decisionmodel_probe_results_total` | counter | `namespace`, `name`, `result` | Model-readiness probe outcomes, counted on a gate transition (not re-counted for a steady-state Pod each requeue). `result` is `ready`, `digest_mismatch`, `device_mismatch`, `not_pinned`, or `error`. |
| `decisionmodel_registry_resolve_duration_seconds` | histogram | _(none)_ | Duration of a model registry tag→digest resolution. No per-DecisionModel labels (bounded by design); default Prometheus buckets. |

Notes:

- `digest_mismatch` / `device_mismatch` are the model-aware readiness signals: a
  Pod reported `ContainersReady` but `/api/ps` showed the wrong digest or ran on
  the wrong device (e.g. a silent CPU fallback). A rising rate here means Pods
  look healthy to Kubernetes but are not actually serving the pinned model.
- `not_pinned` means `/api/ps` showed the right model on the right device but
  **unpinned** (the runtime's `keep_alive` would evict it), so the Pod is held out
  of the Service until it is pinned again. Usually transient (the operator re-pins
  via warmup); a sustained rate means the runtime keeps dropping the pin.
- `decisionmodel_registry_resolve_duration_seconds` is only observed on an actual
  registry call; reconciles that reuse an already-recorded digest do not add a
  sample.

## Example alerts

The expressions below are illustrative PromQL for Prometheus alerting rules; tune
thresholds and `for` windows to your environment.

### Stuck in Starting for more than 15 minutes

A revision whose Pods never become model-ready (e.g. bad image, digest mismatch,
GPU never allocated) sits in `Starting`.

```yaml
- alert: DecisionModelStuckStarting
  expr: |
    max by (namespace, name) (decisionmodel_phase{phase="Starting"}) == 1
  for: 15m
  labels:
    severity: warning
  annotations:
    summary: "DecisionModel {{ $labels.namespace }}/{{ $labels.name }} stuck in Starting"
    description: "Serving Pods have not become model-ready for 15m."
```

The same pattern catches a model wedged in `Caching`:

```promql
max by (namespace, name) (decisionmodel_phase{phase="Caching"}) == 1
```

### Elevated rollback / failure rate

Repeated rollbacks or failures indicate a bad candidate or a flaky eval/probe.

```yaml
- alert: DecisionModelRolloutFailures
  expr: |
    sum by (namespace, name) (
      rate(decisionmodel_rollouts_total{result=~"rolled_back|failed"}[15m])
    ) > 0
  for: 15m
  labels:
    severity: warning
  annotations:
    summary: "DecisionModel {{ $labels.namespace }}/{{ $labels.name }} failing rollouts"
    description: "One or more rollbacks/failures in the last 15m."
```

Share of rollouts that did not promote, over the last hour:

```promql
sum by (namespace, name) (increase(decisionmodel_rollouts_total{result=~"rolled_back|failed"}[1h]))
  /
sum by (namespace, name) (increase(decisionmodel_rollouts_total[1h]))
```

### Degraded

`Degraded` means the stable revision is serving on a subset (or none) of its
replicas, or the model store cannot be shared for the requested replica count.

```yaml
- alert: DecisionModelDegraded
  expr: |
    max by (namespace, name) (decisionmodel_phase{phase="Degraded"}) == 1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "DecisionModel {{ $labels.namespace }}/{{ $labels.name }} is Degraded"
    description: "Not all desired replicas are model-ready, or the model store is not shareable."
```

A tighter, phase-independent variant that fires whenever replicas fall short:

```promql
decisionmodel_model_ready_replicas < decisionmodel_desired_replicas
```

### Silent model mismatch

Pods pass Kubernetes readiness but load the wrong digest/device.

```promql
sum by (namespace, name) (
  rate(decisionmodel_probe_results_total{result=~"digest_mismatch|device_mismatch"}[15m])
) > 0
```

### Poor calibration after an evaluation

```promql
decisionmodel_evaluation_ece > 0.1
```

## Shipped alerts and runbooks

The chart can ship these as a `PrometheusRule` (requires the Prometheus Operator) and a
Grafana dashboard ConfigMap, both **disabled by default**:

```yaml
prometheusRule:
  enabled: true
  labels: { release: kube-prometheus-stack }   # match your Prometheus ruleSelector
grafanaDashboard:
  enabled: true
metrics:
  serviceMonitor:
    enabled: true                              # scrape the metrics Service (HTTPS, bearer token)
    labels: { release: kube-prometheus-stack } # match your Prometheus serviceMonitorSelector
```

The rules fire only on the metrics listed above plus controller-runtime built-ins
(`controller_runtime_reconcile_errors_total`, `controller_runtime_reconcile_total`).
Each alert links a runbook in [`docs/runbooks/`](runbooks/):

| Alert | Signal | Runbook |
| --- | --- | --- |
| `DecisionModelRolloutStuck` | `decisionmodel_phase` in a non-terminal phase | [RolloutStuck](runbooks/DecisionModelRolloutStuck.md) |
| `DecisionModelCandidateTimeout` | `decisionmodel_phase` `Starting`/`Evaluating` | [CandidateTimeout](runbooks/DecisionModelCandidateTimeout.md) |
| `DecisionModelRolloutFailures` | `decisionmodel_rollouts_total` rolled_back/failed | [RolloutFailures](runbooks/DecisionModelRolloutFailures.md) |
| `DecisionModelEvaluationPoor` | `decisionmodel_evaluation_accuracy` / `_ece` | [EvaluationPoor](runbooks/DecisionModelEvaluationPoor.md) |
| `DecisionModelDegraded` | `decisionmodel_phase{phase="Degraded"}` | [Degraded](runbooks/DecisionModelDegraded.md) |
| `DecisionModelModelMismatch` | `decisionmodel_probe_results_total` digest/device mismatch | [ModelMismatch](runbooks/DecisionModelModelMismatch.md) |
| `DecisionModelOperatorReconcileErrors` | `controller_runtime_reconcile_errors_total` | [OperatorReconcileErrors](runbooks/DecisionModelOperatorReconcileErrors.md) |
| `DecisionModelOperatorDown` | `absent(controller_runtime_reconcile_total)` | [OperatorDown](runbooks/DecisionModelOperatorDown.md) |

Thresholds and `for` windows are illustrative — tune them (chart `prometheusRule.windows`
and the expressions) to your environment. There is no dedicated metric for a store-recovery
failure today; it surfaces via `Degraded`/`Failed` phase and the `PrefetchFailed` condition.
