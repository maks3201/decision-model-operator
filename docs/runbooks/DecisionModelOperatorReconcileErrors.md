# Runbook: DecisionModelOperatorReconcileErrors

## What it means
The DecisionModel controller is returning reconcile errors
(`controller_runtime_reconcile_errors_total{controller="decisionmodel"}`). The control
loop is failing to converge for at least one object.

## How to confirm
```sh
kubectl logs -n <operator-ns> deploy/<release>-controller-manager --since=15m | grep -i error
kubectl get dm -A        # which objects are not advancing
```
Correlate the error log lines (they carry the reconciled object's namespace/name) with
the affected DecisionModels.

## Common causes
- RBAC gap: the manager Role/ClusterRole is missing a verb on an owned resource
  (`Deployment`, `Job`, `PVC`, `Service`, `Pod`, `Event`, the CRD). Common after a
  namespace-scoped install if a watched namespace lacks the Role.
- API server throttling or conflicts (frequent `Status().Update` conflicts).
- A dependency the controller calls is wedged (registry, a Secret/ConfigMap it reads).
- A code bug (panic recovered as an error) — check for a stack trace in the logs.

## Fix
- Fix RBAC (reinstall/upgrade the chart; verify the Role exists in each watched namespace).
- Transient conflicts/throttling usually clear themselves; a sustained rate needs the
  root cause from the logs.
- If it is a panic/bug, capture the log and open an issue; the operator keeps retrying
  with backoff, so no state is lost by restarting the manager Pod.
