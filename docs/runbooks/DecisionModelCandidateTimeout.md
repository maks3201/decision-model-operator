# Runbook: DecisionModelCandidateTimeout

## What it means
A candidate revision has stayed in `Starting` or `Evaluating` past the (longer)
candidate window: the model-ready gate or the evaluation is not completing.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>    # candidateRevision, ModelReady / Evaluated conditions
kubectl get pods -n <ns> -l decisionmodel.io/name=<name>
kubectl logs -n <ns> <candidate-pod>
```
Look at the `ModelReady` condition reason: `DigestMismatch`, `DeviceMismatch`,
`NotPinned`, or a probe `error`.

## Common causes
- Serving container healthy to Kubernetes but `/api/ps` never shows the expected
  digest/device (silent CPU fallback on a CUDA request, or a store with the wrong
  model). See DecisionModelModelMismatch.
- The runtime keeps dropping the pin (`NotPinned`): warmup succeeds but the model is
  evicted before the gate flips.
- Evaluation runs but the candidate Pod is slow/unhealthy, or the dataset is very large.

## Fix
- Fix the image/device so `/api/ps` reports the pinned digest on the expected device.
- For `NotPinned`, check the runtime's `keep_alive` handling and node memory pressure.
- The candidate rolls back to the stable revision on its progress deadline; the stable
  keeps serving throughout. Correct the cause and re-apply the spec change.
