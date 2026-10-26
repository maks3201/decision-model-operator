# Autoscaling

The operator does not scale models itself. A DecisionModel is a valid **scale target**: KEDA
and the Kubernetes HPA can write `spec.replicas` through its `scale` subresource, and the operator
then reconciles the serving Deployment from it. Point the autoscaler at the **DecisionModel**,
never at the Deployment the operator creates for each revision.

Status: KEDA 2.21.0 was verified by hand against a DecisionModel CRD with the scale subresource
(spike `docs/spikes/013-keda.md`). KEDA integration is **not covered by CI**.

## Wire it up

1. Install the operator (the CRD carries the scale subresource).
2. Give the DecisionModel a CPU request. The CPU trigger is utilisation relative to the request:
   `config/samples/decisionmodel_v1alpha1_decisionmodel.yaml` sets `resources.requests.cpu: "1"`.
3. Install KEDA and apply `config/samples/decisionmodel_keda_scaledobject.yaml`
   (not in the kustomize sample list, because it needs the KEDA CRDs). The sample sets
   `minReplicaCount: 2`: on a ReadWriteOnce store a scale between 1 and 2 recreates the serving Pods
   (see Limits), so use `minReplicaCount: 1` only with a ReadWriteMany store.
4. Wait until the DecisionModel is `Ready`. Autoscaling starts only once a stable revision exists:
   until then the HPA reports `InvalidSelector` (the scale has no `status.selector`). This is expected.
5. If the DecisionModel is managed by Argo CD or Flux, make them ignore `spec.replicas`
   (see [GitOps](gitops.md#autoscaling-and-specreplicas)).

`minReplicaCount` must be **1 or more**. The operator does not scale to zero: the model store and the
readiness gate need a serving Pod. A scale write of 0 is rejected by the API server. KEDA itself accepts
`minReplicaCount: 0` at admission, so this is a rule for you to enforce.

## Trigger: CPU utilisation

The signal was measured in spike 013 with one `ollaya serve` container (`--cpus=2`, warm `laya:en`, the
`/v1/systemone` demo question), N concurrent clients for 60 s each:

| Concurrency | Responses / 60 s | p50 | p95 | CPU mean (of 200% limit) |
|---|---|---|---|---|
| 1 | 276 | 0.19 s | 0.27 s | 49% |
| 2 | 281 | 0.39 s | 0.47 s | 209% |
| 4 | 284 | 0.82 s | 0.89 s | 209% |
| 8 | 282 | 1.68 s | 2.01 s | 211% |

What this means:

- One Pod serves about 4.7 requests/s on two CPUs whatever the concurrency. More load shows up as
  latency, not throughput, so more replicas are the only way to add capacity.
- CPU follows the load up to saturation and then stays flat. Pick a target of **60-70%** so that the HPA
  reacts before the Pod is saturated. Latency rises before the HPA acts; the trigger is not a queue depth.
- `503 QUEUE_FULL` did not appear up to 8 concurrent clients. Its threshold was not measured.
- The numbers are for one CPU model and one question shape. Measure your own model and workload.

GPU replicas are not covered by a measured signal here.

## Limits

- **ReadWriteOnce store, replicas 1 <-> N.** With a ReadWriteOnce model store, replicas above 1 are
  co-located on the node that holds the volume (a pod-affinity term in the Pod template). Crossing
  between 1 and more than 1 adds or removes that term, which changes the template. The serving
  Deployment uses the Recreate strategy for such a store, so the serving Pods are **recreated**: the
  model is unavailable for its load time. The operator emits a Warning event `ServingPodsRecreated`
  and increments the counter `decisionmodel_serving_pods_recreated_total` on each crossing, at the
  recreation itself (not deferred to the status write), so the signal is recorded even if that
  reconcile's status write fails. For autoscaling either keep `minReplicaCount` at 2 or more (no
  crossing after the first start), or use a ReadWriteMany store (`spec.cache.accessModes`). Changing
  scale between 2 and N does not restart anything.
- **Candidate sizing.** A new revision (a rollout) starts `spec.replicas` Pods, not the autoscaled count.
  With an autoscaled stable at 8 replicas, a candidate also starts 8 Pods, and on CUDA 8 GPUs. Size the
  cluster for the peak of the stable plus one candidate. The operator does not size candidates from the
  stable's current count yet.
- **Scale-up looks like `Degraded` for a while.** A new replica is not model-ready until it has loaded
  the model. The phase is `Degraded` with the `Degraded` condition (reason `ReplicasNotModelReady`);
  `Ready` stays `True` and the Service keeps serving. The `DecisionModelDegraded` alert in
  [metrics](metrics.md) uses `for: 10m`, which a normal model load does not reach. The
  "replicas short" PromQL variant in the same page fires on every scale-up; do not use it as a paging alert.
- **Scale-down and the PodDisruptionBudget.** The PDB (`maxUnavailable: 1`) does not slow an
  autoscaler scale-down: a ReplicaSet scale-down is not an eviction (checked on kind, spike 013).
- **Selector.** `status.selector` is derived from the stable revision and is empty before the first
  stable revision. It selects only the stable revision's serving Pods, not prefetch or candidate Pods.

## Not shipped

No metric beyond CPU is shipped or verified. Scaling on request rate or queue depth needs a metric
source the operator does not provide.
