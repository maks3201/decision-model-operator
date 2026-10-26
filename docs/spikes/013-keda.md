# Spike 013 - KEDA / HPA as a scale target for DecisionModel

Date: 2026-10-25 (macOS arm64, OrbStack, Docker 29.8.2, kind node `kindest/node:v1.37.0`).
Question: can a user's KEDA `ScaledObject` or HPA drive `spec.replicas` of a `DecisionModel`, and
which signal can drive it?

## 1. Versions

| Component | Version | How |
|---|---|---|
| KEDA chart | `kedacore/keda` 2.21.0 (app 2.21.0) | `helm install keda kedacore/keda --version 2.21.0 -n keda --wait` |
| Ollaya | `ghcr.io/ollaya-dev/ollaya:0.18.0` | CPU image, `docker run --cpus=2 --memory=6g` for the load test |
| Model | `laya:en` | pulled by tag, store `c305a9276531` (sha256 `c305a927…5e9d`) |

## 2. Does KEDA accept a custom resource as a scale target?

Probe, not the operator: a throwaway CRD `probemodels.probe.example.io` (not part of the repository)
with the same scale shape the operator will use:

```yaml
subresources:
  status: {}
  scale:
    specReplicasPath: .spec.replicas
    statusReplicasPath: .status.replicas.desired
    labelSelectorPath: .status.selector
```

A `ScaledObject` with `scaleTargetRef` to that kind and a `cron` trigger (`desiredReplicas: "3"`):

```text
Ready=True   ScaledObjectReady: ScaledObject is defined correctly and is ready for scaling
Active=True  ScalerActive: Scaling is performed because triggers are active
HPAActive=True HPAActive: HPA is actively scaling
NAME                  REFERENCE         TARGETS     MINPODS   MAXPODS   REPLICAS
keda-hpa-probe-cron   ProbeModel/demo   3/1 (avg)   1         4         3
spec.replicas after KEDA: 3
```

KEDA and the HPA read the scale subresource and KEDA writes `spec.replicas` through it. The HPA
reads `status.replicas.desired` for the current count and `status.selector` for the Pod set.

RBAC: the KEDA operator ClusterRole (chart 2.21.0) grants `get,list,patch,update,watch` on
`apiGroups: ["*"], resources: ["*/scale"]`, so no extra rule is needed for `decisionmodels/scale`.
`kubectl auth can-i patch probemodels.probe.example.io --subresource=scale --as=system:serviceaccount:keda:keda-operator`
returned `yes`. (A check without `--subresource` returned `no`, which is the wrong resource form for
a subresource, not a denial; KEDA actually wrote the scale.)

Schema on `/scale`: `spec.replicas` with `minimum: 1` rejects a write of 0 at the API server:

```text
kubectl scale probemodel demo --replicas=0
The ProbeModel "demo" is invalid: spec.replicas: Invalid value: 0: spec.replicas in body should be greater than or equal to 1
```

Conclusion: a custom resource with a scale subresource is a valid KEDA/HPA target. The approach
can work. The operator's own CRD still needs the marker (done in the API change, see the report).

## 3. Which signal can drive scaling?

Ollaya has no `/metrics` endpoint (no operator-visible inference metrics), so the only candidate
on the CPU image is container CPU. Measured with one `ollaya serve` container (`--cpus=2`),
warm `laya:en`, `/v1/systemone` demo question shape, N concurrent closed-loop clients for 60 s each.
CPU is `docker stats` CPUPerc (percent of one core, so the limit is 200%).

| Concurrency | 200 responses / 60 s | p50 latency | p95 latency | CPU mean | CPU max | 503 QUEUE_FULL |
|---|---|---|---|---|---|---|
| 1 | 276 | 0.190 s | 0.270 s | 49% | 218% | 0 |
| 2 | 281 | 0.394 s | 0.472 s | 209% | 222% | 0 |
| 4 | 284 | 0.816 s | 0.886 s | 209% | 215% | 0 |
| 8 | 282 | 1.683 s | 2.013 s | 211% | 218% | 0 |

Reading the numbers:

- Throughput is flat at about 4.7 requests/s per Pod from N=1 to N=8. A Pod is capacity-bound;
  latency grows with N (queueing inside the Pod), not throughput.
- CPU tracks load only up to saturation. At N=1 the mean is about 49% of the limit (with peaks at
  218%, the ONNX runtime uses more than one thread per request). From N=2 the Pod sits at its limit.
  Above saturation CPU does not rise further, so a CPU trigger is a "busy or not" signal, not a
  queue-length signal.
- `503 QUEUE_FULL` did not occur up to 8 concurrent clients in 60 s. Its threshold is not measured.
- `docker stats` is slow under load: 124 samples at N=1, 81 at N=2, about 20 at N=4 and N=8.

Decision: CPU utilisation is usable as the trigger, with a target in the 60-70% range, and it works
on the CPU image. It is not a queue-depth signal, so latency can exceed the target before the HPA
reacts. A GPU signal is out of scope (no GPU host). Kubernetes HPA computes utilisation against the
Pod's CPU **request**, not the limit used here; the docs must say to set `resources.requests.cpu`.

## 4. Not verified in this spike

- The operator with the scale subresource installed in a live cluster and KEDA driving a real
  `DecisionModel` (the probe used a CRD with no controller). Covered by envtest in the change, not
  by this spike.
- RWO co-location on a live multi-node cluster (single-node kind here). See the report, section 3a.
- `mkdocs build --strict`: `mkdocs` is not installed on this machine.
- A `/metrics` endpoint on Ollaya 0.18.0 was not probed; the spike relies on the existing docs
  (`docs/api.md`: no `/metrics`).

## 5. Cleanup

The kind cluster `dmo-keda`, the Ollaya container and the `dmo-keda-models` Docker volume are deleted
at the end of the task. The KUBECONFIG was a temporary file, not the user's `~/.kube/config`.

## 3a. Scale-down and the PodDisruptionBudget (section 3c of the task)

Kind, one namespace: a Deployment of 4 pause Pods with a PDB `maxUnavailable: 1`
(the operator's PDB shape). `kubectl scale deploy/web --replicas=2`:

```text
before: 4 pods
after scale to 2: 2 running (non-terminating), 2 status.replicas
PDB disruptionsAllowed: 1
```

A PDB only guards voluntary evictions. A ReplicaSet scale-down is not an eviction, so the PDB does
not block it. The operator's PDB does not slow a KEDA scale-down.

## 6. The real DecisionModel CRD with the scale subresource (after the API change)

The CRD from `config/crd/bases` (`make install`, scale subresource and `status.selector` added) was
installed in the spike cluster, without the operator running:

```text
{"spec":{"replicas":1},"status":{"replicas":0}}        # GET .../decisionmodels/<name>/scale, no status yet
KEDA ScaledObject (cron, desiredReplicas 3): Ready=True Active=True HPAActive=False
HPA: ScalingActive=False InvalidSelector: the HPA target's scale is missing a selector
```

With `status.selector` set by hand (what the operator writes once a stable revision exists), the HPA
scales the real DecisionModel: `spec.replicas` 1 -> 3, `ScalingActive=True ValidMetricFound`.

Consequences:

- Autoscaling is inactive until the first stable revision is recorded (`status.selector` empty). The
  HPA reports `InvalidSelector`. This is expected and is documented.
- A `kubectl scale` or KEDA write of 0 is rejected by the CRD schema (`minimum: 1`). KEDA itself
  accepts `minReplicaCount: 0` at admission (server dry-run passed), so the docs must say `>= 1`.
- The sample ScaledObject (`config/samples/decisionmodel_keda_scaledobject.yaml`) passes KEDA server
  dry-run; the `cooldownPeriod` field was removed because KEDA warns it is irrelevant at `minReplicaCount >= 1`.

## 7. Restart of a Recreate Deployment on a Pod-template change (kind)

A Deployment with `strategy: Recreate` and one Pod: adding a required pod-affinity term to the template
(the co-location term, no replica change) replaces the Pod (UID changed):

```text
before: e85a3816-f261-4d39-b4b6-8279053a5249
after template change (affinity added): a0c5aa9a-0330-4978-a925-7b505e0c2f44
```

This is why `replicas` crossing 1 on a ReadWriteOnce store is a restart, not an in-place scale
(see the Warning event `ServingPodsRecreated` in the report).

## 8. Not verified in this spike

- A DecisionModel driven by KEDA with a **running operator** (the manager cannot probe Pod IPs from the
  macOS host; an in-cluster deployment was not built). The probes above used the real CRD and the
  real KEDA/HPA, but not the operator's reconcile.
- The CPU trigger with KEDA against the real operator's Pods (only the signal was measured, in Docker).
- Scale-to-zero through KEDA `minReplicaCount: 0` (accepted at admission, not run).
