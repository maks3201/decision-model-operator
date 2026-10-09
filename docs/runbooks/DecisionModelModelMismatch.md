# Runbook: DecisionModelModelMismatch

## What it means
Readiness probes report a `digest_mismatch` or `device_mismatch`
(`decisionmodel_probe_results_total`): serving Pods look healthy to Kubernetes
(`ContainersReady`) but `/api/ps` shows the wrong model digest or the wrong device.
This is the operator's model-aware readiness signal — it catches a silent CPU fallback.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>    # ModelReady condition reason: DigestMismatch / DeviceMismatch
kubectl get pods -n <ns> -l decisionmodel.io/name=<name>
# From inside the cluster, inspect the runtime directly:
kubectl exec -n <ns> <pod> -- wget -qO- http://localhost:11435/api/ps   # if a shell/tool is available
```
The `ModelReady` condition reason names which mismatch; the probe-result counter shows
the rate.

## Common causes
- `device_mismatch`: `spec.device: cuda` but the GPU was not allocated / the CUDA image
  silently fell back to CPU (`/api/ps` device is `cpu`, not `cuda:<n>`).
- `digest_mismatch`: the store holds a different digest than recorded — a tag moved
  upstream, or the wrong store was mounted. See the prefetch runbook / spike 009.

## Fix
- GPU: ensure the node has `nvidia.com/gpu` capacity and the device plugin is healthy;
  for blue-green, a second GPU is needed for the candidate. Consider MIG / time-slicing.
- Digest: confirm the prefetch Job pulled the recorded digest; a permanent
  `UpstreamTagMoved`/`DigestMismatch` prefetch failure explains a store that cannot match. `UpstreamTagMoved`
  on a lost store cannot be repaired by the operator today (the registry has no pull by digest): restore the
  store volume from a snapshot, or move the DecisionModel to the tag's current digest deliberately.
- The gate holds mismatched Pods out of the Service, so traffic is not served by a wrong
  model; fix the device/store and the gate flips to ready on the next probe.
