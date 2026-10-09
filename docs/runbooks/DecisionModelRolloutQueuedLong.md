# Runbook: DecisionModelRolloutQueuedLong

## What it means
A DecisionModel has a pending change but its rollout has not started: it is sitting in
phase `Pending` (`decisionmodel_phase{phase="Pending"} == 1`) longer than the alert
window. The operator admits only a bounded number of rollouts at once; this one is
waiting for a free slot. Nothing is broken — the stable revision keeps serving — but the
new revision will not progress until a slot opens.

## How to confirm
```sh
kubectl get dm -A        # PHASE column; the queued one shows Pending
kubectl describe dm <name> -n <ns>   # conditions + Events (reason RolloutQueued)
```
Count how many are rolling out at once across the watched scope:
```sh
kubectl get dm -A -o custom-columns=NS:.metadata.namespace,NAME:.metadata.name,PHASE:.status.phase \
  | grep -E 'Resolving|Caching|Starting|Evaluating|Promoting'
```

## What holds the slots
A slot is held by every DecisionModel in a non-terminal rollout phase (`Resolving`,
`Caching`, `Starting`, `Evaluating`, `Promoting`). The cap is `--max-concurrent-rollouts`
(chart value `manager.maxConcurrentRollouts`, `0` = unlimited). It exists to avoid a
fleet-wide stampede — for example when the default runtime image changes under
`FollowOperator` and every DecisionModel wants to roll at once. A slot frees as soon as a
rollout reaches a terminal phase (`Ready`, `Failed`, `RolledBack`).

## Common causes
- The cap is set deliberately low (e.g. 2-3) and several DecisionModels changed at once;
  the queue is draining normally — no action needed if slots are turning over.
- A rollout holding a slot is itself stuck (see DecisionModelRolloutStuck): it never
  reaches a terminal phase, so it never releases its slot and the queue stalls.

## Fix
- If slots are turning over, wait — the queued revision starts when one frees.
- If the queue is stalled, find the rollout that is stuck holding a slot and clear its
  blocker (DecisionModelRolloutStuck). A permanently bad revision releases its slot on its
  progress-deadline timeout.
- To let more roll out at once, raise the cap: set `manager.maxConcurrentRollouts` to a
  higher value (or `0` for unlimited) and upgrade the release. Size it to the capacity you
  can afford to roll simultaneously — blue-green runs the stable and candidate side by
  side, so each in-flight rollout needs room for a second revision's Pods and store PVC.
```
