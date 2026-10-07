# Runbook: DecisionModelRolloutFailures

## What it means
One or more rollouts ended in `rolled_back`, `rolled_back_after_promotion`, or `failed`
in the last 15 minutes (`decisionmodel_rollouts_total`).

## How to confirm
```sh
kubectl get dm -A
kubectl describe dm <name> -n <ns>    # failedRevision, Ready/Degraded conditions, Events
```
`rolled_back` means a candidate failed but a stable revision kept serving;
`rolled_back_after_promotion` means a just-promoted revision was reverted;
`failed` means a candidate failed with no stable revision to fall back to (user-facing outage risk).

## Common causes
- Bad candidate image/model (digest/device mismatch) — see DecisionModelModelMismatch.
- Eval-gated promotion rejected the candidate (accuracy below `minAccuracy` or a drop
  beyond `maxAccuracyDrop`) — see DecisionModelEvaluationPoor.
- Prefetch failing permanently (missing tag, digest mismatch, or an upstream tag move).

## Fix
Inspect `status.failedRevision` and the `Evaluated`/`ModelReady` conditions to see which
gate rejected the candidate. Fix the model/tag/dataset and re-apply. For a `failed`
(no stable) case, prioritise: there is no healthy revision serving.
