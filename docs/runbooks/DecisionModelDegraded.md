# Runbook: DecisionModelDegraded

## What it means
The DecisionModel is in `Degraded`: the stable revision is serving on a subset (or none)
of its replicas, or a spec change cannot be applied to the model store.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>    # Degraded condition reason
kubectl get pods -n <ns> -l decisionmodel.io/name=<name> -o wide
```
A phase-independent check for a replica shortfall:
```promql
decisionmodel_model_ready_replicas < decisionmodel_desired_replicas
```

## Common causes
- Nodes lost / evicted Pods; not enough schedulable capacity (CPU, memory, GPU).
- Model-ready gate failing on some replicas (digest/device mismatch) — see
  DecisionModelModelMismatch.
- `CacheSpecImmutable`: a `spec.cache` change (accessModes / storageClassName / a shrink)
  that a bound PVC cannot accept.

Note: `replicas > 1` on a `ReadWriteOnce` store is no longer Degraded. The operator
co-locates all replicas on the node holding the volume (informational Event
`ReplicasCoLocated`); a node failure then takes all replicas down. Use `ReadWriteMany`
to spread replicas across nodes.

## Fix
- Restore capacity or fix scheduling constraints (`spec.scheduling`).
- Fix any per-replica model mismatch. The stable revision keeps serving on its ready
  replicas while Degraded; the goal is to get `modelReady == desired`.
- For `CacheSpecImmutable`, delete the PVC to apply new access modes / storage class, or
  revert the `spec.cache` change (PVC access modes and storage class are immutable).
