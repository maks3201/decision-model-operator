# Runbook: DecisionModelDegraded

## What it means
The DecisionModel is in `Degraded`: the stable revision is serving on a subset (or none)
of its replicas, or the model store cannot be shared for the requested replica count.

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
- `CacheNotShareable`: `replicas > 1` across nodes but `spec.cache.accessModes` is
  `ReadWriteOnce` — a candidate scheduled on another node cannot attach the store.
- Nodes lost / evicted Pods; not enough schedulable capacity (CPU, memory, GPU).
- Model-ready gate failing on some replicas (digest/device mismatch) — see
  DecisionModelModelMismatch.

## Fix
- For `CacheNotShareable`, set `spec.cache.accessModes: [ReadWriteMany]` with an RWX
  StorageClass, or keep `replicas: 1` / pin to one node.
- Restore capacity or fix scheduling constraints (`spec.scheduling`).
- Fix any per-replica model mismatch. The stable revision keeps serving on its ready
  replicas while Degraded; the goal is to get `modelReady == desired`.
