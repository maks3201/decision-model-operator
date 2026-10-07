# Runbook: DecisionModelRolloutStuck

## What it means
A DecisionModel has stayed in a non-terminal rollout phase (`Resolving`, `Caching`,
`Starting`, `Evaluating`, or `Promoting`) longer than the alert window. A revision is
not making progress toward `Ready`.

## How to confirm
```sh
kubectl get dm -A        # PHASE column
kubectl describe dm <name> -n <ns>   # conditions + Events
kubectl get events -n <ns> --field-selector involvedObject.name=<name> --sort-by=.lastTimestamp
```
Check which phase it is stuck in and the matching condition reason (`Resolved`,
`Cached`, `ModelReady`, `Evaluated`).

## Common causes
- `Resolving`: registry unreachable or the tag does not exist (`Resolved=False`,
  reason `NotFound` / a transient registry error).
- `Caching`: the prefetch Job is failing or pending (no PVC bound, image pull error,
  download slow). `kubectl get jobs,pods -n <ns> -l decisionmodel.io/name=<name>`.
- `Starting`: serving Pods never pass the model-ready gate — bad image, digest or
  device mismatch, or a GPU that was never allocated. See DecisionModelModelMismatch.
- `Evaluating`: the golden dataset is large/slow or the candidate Pod is unhealthy.
- `Promoting`: Service selector switch blocked by an admission webhook or RBAC.

## Fix
Resolve the underlying cause (fix the tag/registry, free capacity for the PVC/Job,
correct the image/device, shrink or fix the dataset). The reconcile is level-triggered;
once the blocker clears, the phase advances on the next reconcile. A permanently bad
revision rolls back to the stable one (if any) on its progress-deadline timeout.
