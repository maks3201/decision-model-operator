# Quickstart

Deploy a `laya:en` decision model and send it a request. About 10 minutes on a
laptop-scale cluster.

## Prerequisites

- A Kubernetes cluster (v1.29+; CI tests 1.35, 1.36 and 1.37). [kind](https://kind.sigs.k8s.io/) works:
  `kind create cluster`.
- `kubectl` pointed at that cluster.
- A default StorageClass that supports `ReadWriteOnce` (kind ships one). For
  `replicas > 1` across nodes you need `ReadWriteMany` — see [Scaling](#scaling).
- Egress from the prefetch Job to `https://ollaya.dev` (the model registry:
  manifests) and to `https://huggingface.co` plus the Hugging Face CDN it
  redirects to (`*.hf.co`: the weights). Air-gapped clusters can mirror both, see
  [Supported models](#supported-models).

## Install the operator

### From a release (recommended)

Replace `<tag>` with a released version (e.g. `v0.3.0`<!-- x-release-please-version -->):

```sh
kubectl apply -f https://github.com/maks3201/decision-model-operator/releases/download/<tag>/install.yaml
```

This installs the CRD, RBAC, and the operator Deployment into the
`decision-model-operator-system` namespace.

### From source

```sh
make deploy IMG=ghcr.io/maks3201/decision-model-operator:<tag>
```

Verify the operator is running:

```sh
kubectl -n decision-model-operator-system get deploy
kubectl -n decision-model-operator-system logs deploy/decision-model-operator-controller-manager
```

### With Helm

The chart is published on GHCR as an OCI artifact (and listed on
[Artifact Hub](https://artifacthub.io/packages/search?repo=decision-model-operator)).
Replace `<version>` with a chart version without the `v` (e.g. `0.3.0`<!-- x-release-please-version -->); the operator
image defaults to the matching `v<version>` tag:

```sh
helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator --version <version> \
  --namespace decision-model-operator-system --create-namespace
```

From a source checkout, use `charts/decision-model-operator` instead of the OCI reference.

The chart installs the CRD (from its `crds/` directory), RBAC, the manager
Deployment and the metrics Service. Common overrides:

```sh
helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator --version <version> \
  --namespace decision-model-operator-system --create-namespace \
  --set replicaCount=1 \
  --set manager.allowedRegistries="ollaya.dev\,registry.example.com" \
  --set manager.maxConcurrentReconciles=4
```

Values of note: `image.repository/tag/pullPolicy`, `replicaCount`, `resources`,
`nodeSelector`/`tolerations`/`affinity`, and the manager policy flags
`manager.allowedRegistries`, `manager.allowInsecureRegistries`,
`manager.allowImageOverride`, `manager.maxConcurrentReconciles`,
`manager.ollayaRegistry`. See `charts/decision-model-operator/values.yaml`.

Helm does not upgrade or delete CRDs in `crds/`; manage CRD upgrades out of band.
Upgrade the operator with `helm upgrade`, uninstall with
`helm uninstall dmo -n decision-model-operator-system` (the CRD stays).

### Behind an HTTP proxy

Corporate clusters often reach the model registry only through an HTTP proxy.
Set the proxy on the operator and it is applied to the operator's own egress and
copied into every prefetch Job (the only workload that pulls weights):

```sh
helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator --version <version> \
  --namespace decision-model-operator-system --create-namespace \
  --set proxy.httpProxy=http://proxy.corp:3128 \
  --set proxy.httpsProxy=http://proxy.corp:3128 \
  --set 'proxy.noProxy=10.96.0.0/12\,.svc\,.cluster.local'
```

These render as `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` env on the manager
container (nothing is rendered for an empty value).

The proxy applies to the operator's **registry** calls (resolving a tag's digest)
and is copied to the **prefetch Job** that pulls weights. It
does **not** apply to the operator's in-cluster calls to serving Pods: the
readiness prober and evaluator connect to **Pod IPs** with a separate, proxy-less
HTTP client (so the `Authorization: Bearer <api key>` header never traverses the
proxy and readiness no longer depends on `noProxy`). You therefore do **not**
need the pod CIDR in `noProxy`. Still set `noProxy` for the registry/API path:

- the **service CIDR** — so in-cluster Services (e.g. the Kubernetes API server)
  are reached directly, not via the proxy;
- `.svc,.cluster.local` — in-cluster DNS names.

The service CIDR is the API server's `--service-cluster-ip-range` (often the
ClusterIP range of the `kubernetes` Service in `default`). Example:
`--set 'proxy.noProxy=10.96.0.0/12\,.svc\,.cluster.local'`.

If your proxy URL carries credentials, keep it out of the Helm values: create a
Secret with keys `httpProxy` / `httpsProxy` / `noProxy` and set
`--set proxy.existingSecret=<name>`. The manager env then sources each var from
the Secret (`valueFrom.secretKeyRef`, each key optional); the plaintext
`proxy.*` values are ignored when `existingSecret` is set.

A **credentialed** proxy URL (`user:pass@…`) is used by the **operator only**.
The operator does not copy a credentialed proxy URL into the prefetch Jobs —
a Job's spec is readable by anyone who can `get jobs` in that (tenant) namespace,
so the password would leak. It copies `NO_PROXY`/`no_proxy` to the Jobs **always**,
and logs **one startup warning** naming the skipped variables (names only, never
the value). A value it cannot parse, or one containing `@`, is treated as
credentialed and skipped (fail-closed).

Consequence: with a **credentialed-only** proxy configuration the prefetch pull
has no proxy, so in a proxy-only network it fails **visibly** as `PrefetchFailed`
(phase `Failed`/`RolledBack`, a `Degraded` condition) rather than leaking the
credential. For the Job's own egress to the registry, use an **IP-allowlisted
proxy** that needs no credentials, or a **node-level / transparent proxy**, set
via a plain (credential-free) `HTTP_PROXY` the operator can forward. A proxy URL
without credentials is copied to the prefetch Jobs as normal.

Serving Pods need no proxy — they do not pull.

## Create a DecisionModel

```yaml
# dm.yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  engine: ollaya
  model: laya:en        # an explicit ":tag" is required (a bare name resolves to :latest, a heavier artifact)
  device: cpu
  replicas: 1
  resources:
    requests:
      cpu: "1"
      memory: 3500Mi
    limits:
      memory: 4Gi
```

```sh
kubectl apply -f dm.yaml
kubectl get dm -w
```

`dm` is the short name for `DecisionModel`. The columns show `MODEL`, `DEVICE`,
`PHASE`, and `READY` (model-ready replicas).

## What the phases mean

The operator drives a level-triggered state machine
(`status.phase`):

| Phase        | Meaning |
|--------------|---------|
| `Pending`    | Accepted, not yet processed. |
| `Resolving`  | Resolving the tag to an immutable digest from the registry. |
| `Caching`    | A prefetch Job is downloading the weights into the model-store PVC and verifying the digest. |
| `Starting`   | Serving Pods are created; waiting for the model to load and pass the readiness gate. |
| `Evaluating` | (rollout with `spec.rollout.evaluation`) scoring the candidate against a golden dataset. |
| `Promoting`  | Switching the Service to the new revision. |
| `Ready`      | The stable revision is serving and **all** desired replicas are model-ready. |
| `Degraded`   | The stable revision is serving but not all desired replicas are model-ready: `0 < modelReady < desired` (`Ready=True`, `ModelReady=False`, reason `ReplicasNotModelReady`), or `modelReady == 0` (`Ready=False`, reason `NoModelReadyPods`). The model keeps serving on whatever replicas are ready. |
| `Failed`     | The rollout failed with no healthy stable revision (e.g. tag not found on a first rollout, resolve failure, or a policy violation). |
| `RolledBack` | A candidate failed and traffic stayed on the previous stable revision, which keeps serving. |

`Failed` vs `RolledBack`: both mean the latest revision did not roll out. If a
stable revision was already serving, its traffic is preserved and the phase is
`RolledBack`; if there was nothing healthy to fall back to, it is `Failed`.

If you change the spec rapidly back and forth so a revision's model-store PVC is
being re-created while its **previous** PVC is still `Terminating`, the operator
**waits** (requeues every ~5 s in `Caching`) for the old PVC to finish deleting
rather than failing — the rollout resumes once the store PVC is available.

`Degraded` is a **phase**, not just a condition: since a healthy stable revision
keeps serving, partial (or zero) replica readiness moves `status.phase` to
`Degraded` rather than `Ready`. The `Ready` condition can still be `True` in the
partial case (`0 < modelReady < desired`), because the model is answering on the
ready replicas; when `modelReady == 0` the `Ready` condition is `False`. A
separate `Degraded=True` condition (see below) also accompanies cache-sharing
warnings and rejected candidates without necessarily changing the phase.

Key conditions (`kubectl describe dm support-router` → Conditions):

- `Resolved` — the tag resolved to a digest (recorded in `status.stableRevision.digest`).
- `Cached` — weights are present in the PVC and the digest matched.
- `ModelReady` — enough Pods report the expected digest loaded on the expected device.
- `Ready` — the model is serving.
- `Degraded` — a policy or runtime problem; read the reason and message.

### Condition reasons you may see

Each reason resolves to a specific phase and condition. The **Phase / condition**
column states where the reason actually lands (`kubectl describe dm` → Conditions,
or `-o jsonpath='{.status.conditions}'`):

| Reason | Phase / condition | Meaning | Fix |
|--------|-------------------|---------|-----|
| `NoModelReadyPods` | phase `Degraded`; `Ready=False`, `ModelReady=False`, `Degraded=True` | No serving Pod reports the expected digest/device (`modelReady == 0`). | Check Pod logs / `device`; a CPU pod set to `device: cuda` never loads. |
| `ReplicasNotModelReady` | phase `Degraded`; `Ready=True`, `ModelReady=False`, `Degraded=True` | Some, but not all, replicas are model-ready (`0 < modelReady < desired`). | Wait, or check node capacity / the store PVC access mode. |
| `DigestMismatch` | Pod readiness gate `decisionmodel.io/model-ready`=False (keeps the Pod out of the Service; feeds the counts above) | A Pod loaded a different digest than expected. | The tag moved upstream mid-rollout; re-resolve (change spec) or pin `spec.digest`. |
| `DeviceMismatch` | Pod readiness gate `decisionmodel.io/model-ready`=False | A Pod loaded on the wrong device. | Fix `spec.device` / node GPU availability. |
| `NotPinned` | Pod readiness gate `decisionmodel.io/model-ready`=False | The right model on the right device, but it is **not pinned** in the runtime (`keep_alive`), so the runtime may evict it and silently stop serving. | Usually transient — the operator's warmup pins it (`keep_alive=-1`); the gate recovers once `/api/ps` shows it pinned. Persisting suggests the runtime is dropping the pin. |
| `CacheNotShareable` | `Degraded=True` condition; phase unchanged (the model still serves) | `replicas > 1` with a `ReadWriteOnce` store. | Set `spec.cache.accessModes: ["ReadWriteMany"]` and an RWX storageClass. |
| `CacheSpecImmutable` | `Degraded=True` condition; phase unchanged | A `spec.cache` change the operator cannot apply to the existing PVC: an `accessModes` change (access modes are immutable), a **size shrink** (PVCs cannot shrink), a `storageClassName` change, or a **size grow** the StorageClass refused (it does not allow volume expansion). The message names which. A size **grow** on an expansion-capable StorageClass is applied in place and does **not** set this. | For a shrink / class / access-mode change, delete the DM (and its model-store PVC) and recreate it, or trigger a new revision with a model-affecting change (which provisions a fresh PVC from the current `spec.cache`); for a refused grow, use a StorageClass with `allowVolumeExpansion: true`. |
| `RegistryNotAllowed` | phase `Failed`; `Ready=False` (permanent — not retried) | `spec.model` host is not in `--allowed-registries` (or is `http://` without `--allow-insecure-registries`). | Use an allowed registry, or have the operator configured to allow it. |
| `ImageOverrideNotAllowed` | phase `Failed`; `Ready=False` (permanent) | `spec.image` set but the operator forbids overrides. | Remove `spec.image`, or run the operator with `--allow-image-override`. |
| `UnpinnedRuntimeImage` | `Resolved=False`; the stable keeps serving (with no stable: phase `Degraded`, `Ready=False`) | A new revision would run a mutable-tag runtime image: a `runtimeVersion` this operator build has no digest for, or a `spec.image` without `@sha256:`. | Use a known `runtimeVersion` or pin `spec.image` with `@sha256:`, or run the operator with `--allow-unpinned-runtime-images`. |
| `SecretNotAllowed` | phase `Degraded`; `Ready=False` | The API-key Secret lacks the required label. | Label it `decisionmodel.io/api-key: "true"` (see below); the operator picks it up within ~60 s. |
| `InvalidModelName` | phase `Failed`; `Ready=False` (permanent) | `spec.model` did not parse. | Use `[host/][namespace/]model:tag` with an explicit tag. |
| `ModelNotFound` | phase `Failed`; `Resolved=False` (requeued and re-resolved) | The tag does not exist in the registry. | Fix the tag; a bare name resolves to `:latest`. |
| `DatasetInvalid` | phase `RolledBack` (stable exists) or `Failed`; `Degraded=True` | Eval dataset missing or malformed JSONL. | See [evaluation.md](evaluation.md). |
| `EvaluationFailed` | phase `RolledBack` (stable exists) or `Failed`; `Degraded=True` | Candidate did not meet the eval gates. | Inspect `status.evaluation`; see [evaluation.md](evaluation.md). |
| `EvaluationTimeout` | phase `RolledBack` (stable exists) or `Failed`; `Degraded=True` | Evaluation exceeded its deadline. | Reduce `maxCases` or check the model. |
| `ResourceConflict` | `Ready=False`, `Degraded=True` reason `ResourceConflict` | An object the operator needs (Service, Deployment, Job or PVC) already exists under the DecisionModel's name but is **not owned by it** (different or absent controller owner). The operator never modifies or deletes a foreign object; a foreign **Service** blocks promotion, so traffic cannot reach the model. | Rename or remove the conflicting object (it was not created by this operator), then the next reconcile proceeds. |
| `StoreLost` | `Degraded=True` reason `StoreLost`; stable keeps whatever Pods are ready | The stable revision's model-store PVC went missing (e.g. deleted out of band). The operator **recreates** it (annotated `decisionmodel.io/store-recovering`) and **re-runs the prefetch** before letting Pods serve on it; it will not serve an empty store. | Usually none — recovery is automatic. Check StorageClass/quota if it stays here. |
| `StorePrefetchFailed` | `Degraded=True` reason `StorePrefetchFailed`; held without churn | The recovery prefetch Job exhausted its retries (bad registry, digest mismatch, disk). | Fix the underlying cause, then bump `decisionmodel.io/retry` to re-run recovery. |
| `StoreTerminating` | `Degraded=True` reason `StoreTerminating`; the stable **keeps serving** | The stable store PVC was deleted but is still `Terminating` because the stable Pods still mount it (the `kubernetes.io/pvc-protection` finalizer). The operator does **not** stop the stable automatically (a PVC delete cannot be undone). | Delete the stable Pods so the PVC can finish deleting and recovery can run (`StoreLost` → recreate + prefetch). Expect a short serving gap while the Pods restart. |

For candidate failures (`DatasetInvalid`, `EvaluationFailed`, `EvaluationTimeout`,
and `SecretNotAllowed` when reading an eval dataset), the phase is `RolledBack`
when a stable revision is still serving, and `Failed` only when there is no
stable revision to fall back to.

The `StoreLost` / `StorePrefetchFailed` / `StoreTerminating` reasons concern the
**stable** revision's model store; the stable keeps serving on whatever Pods are
ready throughout (`Ready` can stay `True`), and the operator recovers the store in
place. `StoreTerminating` is the only one that needs a human action — see its row.
While a **candidate** is mid-rollout the operator also keeps the stable healthy:
it restores a deleted stable Deployment, re-applies an API-key rotation, keeps the
stable's PodDisruptionBudget, and re-checks the gate on the stable Pods — all
rendered from `status.stableRevision`, never the in-flight candidate's spec.

### Retrying a failed rollout

A failed revision is not retried automatically (a transient registry blip should
not loop). To retry without changing the spec, set/change the
`decisionmodel.io/retry` annotation to any new value:

```sh
kubectl annotate dm support-router decisionmodel.io/retry="$(date +%s)" --overwrite
```

The controller clears the failed revision once when this value differs from the
one it last consumed. Returning the spec to the last stable revision also clears
it.

### Prefetch failures: transient vs permanent

The prefetch Job tells a **transient** download failure from a **permanent** one
and reacts differently:

- **Transient** (network error, registry/Hugging Face `5xx`, timeout): the Job
  retries up to its `backoffLimit` (4) within the Caching deadline, so a short
  outage recovers on its own.
- **Permanent** — the Job fails **immediately** (no wasted retries):
  - **`ModelNotFound`** — the registry has no such tag (`spec.model` is wrong, or
    a bare name resolved to a different artifact). Fix the tag; a digest pin
    (`spec.digest`) protects against a tag moving.
  - **`DigestMismatch`** — the pulled manifest's sha256 does not match the pinned
    digest (the tag moved upstream mid-rollout, or the content is corrupt).
    Re-resolve by changing the spec, or pin `spec.digest`.

The reason is shown on the failed revision's condition/Event. A permanent failure
will not clear on its own — fix the cause, then retry as above.

### Secret capability labels

A Secret the operator reads must carry the label for the capability it is used
for — one label grants exactly one capability:

| Capability | Label | Granted to |
|---|---|---|
| Engine API key | `decisionmodel.io/api-key: "true"` | `spec.auth.apiKeySecretRef` |
| Eval dataset | `decisionmodel.io/eval-dataset: "true"` | a `datasetRef.secretRef` dataset |
| Download token | `decisionmodel.io/download-token: "true"` | `spec.cache.downloadTokenSecretRef` |

A Secret without the required label is refused: the API key and download token
report phase `Degraded`, `Ready=False`, reason `SecretNotAllowed`; a dataset
Secret is **held** in `Evaluating` (`Evaluated=False`, reason `SecretNotAllowed`)
until the label is added. This guards against a DecisionModel editor pointing the
operator at an arbitrary Secret.

```sh
kubectl label secret my-ollaya-key decisionmodel.io/api-key=true
kubectl label secret my-golden-dataset decisionmodel.io/eval-dataset=true
```

Adding the label to an existing Secret is enough to recover: the operator re-reads
the Secret on its next reconcile and clears the condition within ~60 s — no spec
change and no annotation are required.

> **Transition.** A dataset Secret labelled only with the older
> `decisionmodel.io/api-key: "true"` still works for one release but emits a
> `DeprecatedSecretLabel` Warning; switch it to `decisionmodel.io/eval-dataset`.

The API-key Secret must contain the referenced key: `apiKeySecretRef.optional` is
rejected (CEL), and a labelled Secret missing the key is reported as `Degraded`,
reason `APIKeyInvalid`, with no workloads changed.

### Rotating the API key

You can rotate the key **in place** by changing the value inside the same Secret
(no `spec` change). The operator does **not** watch Secrets (they stay `get`-only
RBAC); instead it reads the Secret on each reconcile and records a hash of the
value in the `decisionmodel.io/apikey-checksum` annotation on the serving
Deployment. When the value changes, the new checksum lands on the Pod template and
the Deployment rolls per the [rollout strategy](#rollout-strategy-and-downtime).

What to expect after you update the Secret value:

- **Pickup is on the next reconcile, within ~60 s** (a `Ready` DecisionModel
  re-checks about once a minute; there is no Secret watch, so it is not instant).
- **A brief 401 window.** The still-running old Pods keep the previous key in their
  env until they are replaced, so the operator's readiness prober gets `401` from
  them. The gate tolerates this for a bounded window (up to ~3 min: 3 re-checks at
  ~60 s) rather than flapping the Pod out immediately; the roll normally completes
  within it. A runtime that keeps returning `401` past that window has its gate
  flipped `False` (reason `ProbeError`).
- **Downtime follows the strategy table.** On the default `ReadWriteOnce` store with
  `replicas: 1` the roll is `Recreate`, so there is a short outage while the new Pod
  starts and reloads the model. With an RWX store and `replicas > 1` the rolling
  update keeps other replicas serving.
- **On upgrade, the first reconcile only records the checksum** (it does not roll a
  Pod that is otherwise unchanged); a roll happens only when the value actually
  changes afterwards.

Model-aware readiness is the point of this operator: a Pod only joins the
Service once the runtime's `/api/ps` reports the **expected digest** on the
**expected device**, via the Pod readiness gate `decisionmodel.io/model-ready`.
A Pod that silently fell back to CPU, or loaded the wrong weights, never gets
traffic.

The gate is also **re-verified every 60 s** (an `/api/ps` Inspect only, no
re-warm): if a Pod later unloads the model, loads a different digest, ends up on
the wrong device, or loses its pin, the gate flips back to `False` (reason
`ProbeError` / `DigestMismatch` / `DeviceMismatch` / `NotPinned`) and the Pod
leaves the Service. A `Ready` DecisionModel therefore reconciles about once a
minute; when nothing changed the re-check writes nothing (no Pod or status
update), so it is cheap.

Traffic moves only after the candidate's Pods are genuinely **Ready** — the
model-ready gate is `True` **and** the Pod's `Ready` condition (kubelet /
`PodReady`) is `True`, so the Pod is actually in the Service's EndpointSlice. The
operator **persists `status.stableRevision` before it switches the Service
selector**, so a crash mid-promotion cannot leave the Service pointing at a
revision the status does not record (and GC never deletes the revision the live
Service selects).

## Operator flags

The manager (controller) accepts these policy flags (set via `make deploy`
args, or Helm `manager.*` values):

| Flag | Default | Meaning |
|------|---------|---------|
| `--allowed-registries` | `ollaya.dev` | Comma-separated registry hosts a `spec.model` may resolve from (SSRF guard). |
| `--allow-insecure-registries` | `false` | Permit `http://` model registries (dev only). |
| `--allow-image-override` | `false` | Permit `spec.image` to override the engine's default image. The override is the serving image and part of the revision. |
| `--allow-unpinned-runtime-images` | `false` | Admit a new revision whose runtime image is a mutable tag (a `runtimeVersion` this build has no digest for, or a `spec.image` without `@sha256:`). It is recorded with `imagePinned: false` and a Warning Event. Running stables are never re-checked. |
| `--max-concurrent-reconciles` | `4` | Max DecisionModels reconciled concurrently. |
| `--runtime-version-policy` | `Pinned` | How an unset `spec.runtimeVersion` resolves: `Pinned` reuses the stable revision's recorded runtime version (an operator upgrade starts no rollout) or `FollowOperator` uses the engine default. |
| `--max-concurrent-rollouts` | `0` | Max DecisionModels rolling out at once across the watched scope (0 = unlimited). Others wait in phase `Pending` (reason `RolloutQueued`), FIFO. |
| `--ollaya-registry` | (empty) | Default registry base URL for host-less model names; empty = `OLLAYA_REGISTRY` env, else `https://ollaya.dev`. |
| `--ollaya-hf-endpoint` | (empty) | Base URL for model-weight downloads (a Hugging Face mirror or enterprise endpoint; the registry still serves manifests). Empty = `OLLAYA_HF_ENDPOINT` env, else Hugging Face. Needs the 0.10.0+ runtime. |

The security defaults are deliberately strict: only `ollaya.dev`, no `http://`,
and no image override unless you opt in.

## Upgrading the operator

A new operator release may ship a newer default engine runtime image. What
happens to your DecisionModels on `helm upgrade` depends on
`--runtime-version-policy`:

- **Pinned (default).** A DecisionModel that does not set `spec.runtimeVersion`
  keeps the runtime version its stable revision was promoted with, so an operator
  upgrade alone starts **no** rollout. The new default is surfaced as the
  `RuntimeUpdateAvailable` condition and a one-off Event; adopt it deliberately by
  setting `spec.runtimeVersion` (e.g. `0.10.0`) on each DecisionModel, which
  starts a normal blue-green rollout for that one.
- **FollowOperator.** An unset `spec.runtimeVersion` tracks the operator default,
  so an upgrade rolls every DecisionModel onto the new runtime at once. Bound the
  blast radius with `--max-concurrent-rollouts` (e.g. `2`–`3`): DecisionModels
  beyond the budget wait in phase `Pending` (reason `RolloutQueued`) and roll out
  in FIFO order as slots free up.

Pin a specific runtime regardless of policy with `spec.runtimeVersion` (mutually
exclusive with `spec.image`):

```yaml
spec:
  model: laya:en
  runtimeVersion: "0.10.0"   # exact engine release; survives operator upgrades
```

## Eval-gated rollout

For blue-green promotion gated on a golden-dataset accuracy **and calibration**
check, set `spec.rollout.evaluation` — see [evaluation.md](evaluation.md).

## Inspect


```sh
kubectl describe dm support-router      # spec, status, conditions, recent Events
kubectl get dm support-router -o yaml   # full status, including status.stableRevision.digest
kubectl get events --field-selector involvedObject.name=support-router
```

## Send a request

The operator creates a Service named after the DecisionModel
(`support-router`) in the same namespace, exposing port `http` (11435).
Port-forward it and POST to `/v1/systemone`:

```sh
kubectl port-forward svc/support-router 11435:11435 &
```

```sh
curl -sS -X POST http://127.0.0.1:11435/v1/systemone \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "model": "laya:en",
    "state": "I was charged twice for my subscription this month. Please refund the second charge.",
    "questions": {
      "department": {
        "type": "choice",
        "criteria": {
          "billing": "billing, payments, charges, refunds",
          "technical": "technical bugs and errors",
          "sales": "pricing and plans",
          "other": "anything else"
        }
      }
    }
  }'
```

The response contains one answer per question id. For a `choice` question you
get `choice`, `confidence` and `probabilities`:

```json
{"answers": {"department": {"type": "choice", "choice": "billing",
  "confidence": 0.97, "probabilities": {"billing": 0.98, "technical": 0.01, "sales": 0.005, "other": 0.005}}}}
```

Question types: `choice` (pick one label), `noul` (probability a statement
holds), `score` (expected level over an ordered scale).

## Supported models

There is no built-in allow-list of model names: any model in an Ollaya registry
can be referenced. What has been verified so far:

| Models | Status |
|---|---|
| `laya:en` | E2E on every change (kind, CPU), and once on a T4 GPU (EKS, runtime 0.7.3) |
| `laya:multilingual`, `nli:latest`, `gliclass:latest` | Pulled, served and measured (see [sizing](sizing.md)) |
| `gliclass:latest`, `nli:latest` | Nightly E2E (kind, CPU) |
| `jevk5:latest` (GGUF, Q8_0, llama.cpp runner) | Reached `Ready` on kind; the gate checks digest, device and pinning, not precision, so quantized models pass ([spike 007](https://github.com/maks3201/decision-model-operator/blob/main/docs/spikes/007-gguf-models.md)) |
| Other GGUF models (`winnow`, `jeb`, `cygnet`, …) | Same runner as `jevk5`, not run individually; 8–13 GB, raise `spec.cache.size` |
| Large models (`nimble`, `clef`, `jeeves`, ~18–19 GB) | Not verified: size `spec.cache.size` and the GPU yourself |

Reference a model by `name:tag`:

- **An explicit tag is required.** A bare name (`laya`) resolves to `:latest`, a
  different and heavier artifact; the operator rejects a model without a tag
  (`InvalidModelName`). Pin `laya:en`, `nli:latest`, etc.
- **Namespaced and host-qualified names** are supported:
  `[host/][namespace/]model:tag`, e.g. `acme/triage:v3` or
  `registry.example.com/acme/triage:v3`.
- **Internal mirror / air-gapped:** set the operator's default registry for
  host-less names with `--ollaya-registry` (Helm `manager.ollayaRegistry`, or the
  `OLLAYA_REGISTRY` env). This covers **both** steps that reach a registry:
  the operator's digest resolve **and** the prefetch Job's `ollaya pull` (the Job
  is given `OLLAYA_REGISTRY` so the pull targets the same mirror). The mirror must speak the Ollaya/Ollama
  registry API — `GET <base>/v2/<namespace>/<model>/manifests/<tag>` returning the
  v2 manifest whose `sha256(manifest bytes)` equals the pinned digest, plus the
  blob endpoints `ollaya pull` fetches; the default namespace is `library`
  (e.g. `library/laya` for `laya:en`). An `http://` mirror base (dev only) makes
  the pull non-TLS automatically. Alternatively, write the host directly into the
  model name (`mirror.corp/library/laya:en`); an explicit host in the name
  overrides `--ollaya-registry` for that model, in both resolve and pull.
  When `--ollaya-registry` points at a mirror, the operator also sets
  `OLLAYA_REGISTRY` on the **serving** Pods (not just the prefetch Job), so
  `ollaya serve` finds the model in the store it was pulled into. The on-disk store
  path is keyed by the registry authority with the port's `:` written as `_` —
  a mirror at `mirror.corp:8080` stores the manifest under
  `manifests/mirror.corp_8080/library/laya/en` — which the operator computes
  consistently for the pull, the serve lookup and the Job's digest check.
- **Registry allow-list:** the operator only resolves from hosts in
  `--allowed-registries` (Helm `manager.allowedRegistries`, default `ollaya.dev`)
  as an SSRF guard; add your mirror there. `http://` registries need
  `--allow-insecure-registries` (dev only).

- **Weight-download mirror (Hugging Face endpoint):** the prefetch Job contacts
  **two** distinct hosts: the **model registry** (default `ollaya.dev`, set via
  `--ollaya-registry`) for manifests and blob digests, and the **weight host**
  (default Hugging Face) for the model-weight blobs themselves. In an air-gapped
  cluster you may need mirrors for both. Set the weight-download mirror with
  `--ollaya-hf-endpoint` (Helm `manager.ollayaHFEndpoint`); the runtime reads it
  as `OLLAYA_HF_ENDPOINT` (requires runtime v0.10.0+; older runtimes ignore it
  harmlessly). Serving Pods never receive this variable — they mount the store
  read-only and never download. When both mirrors are behind a corporate proxy,
  the proxy env already covers both: the controller copies
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` to the prefetch container, so any request
  from the CLI — registry or weight host — goes through the proxy.

  ```sh
  helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator --version <version> \
    --namespace decision-model-operator-system --create-namespace \
    --set manager.ollayaRegistry=https://registry.internal \
    --set manager.ollayaHFEndpoint=https://hf-mirror.internal \
    --set 'manager.allowedRegistries=registry.internal'
  ```

- **Download token (private / gated models):** if the weight host (Hugging Face
  or the mirror) requires authentication, create a Secret with the token and
  reference it from the DecisionModel:

  ```yaml
  spec:
    cache:
      downloadTokenSecretRef:
        name: my-hf-token
        key: token
  ```

  The Secret **must** carry the label `decisionmodel.io/download-token: "true"`,
  or the operator refuses to read it (phase `Degraded`, reason
  `SecretNotAllowed`):

  ```sh
  kubectl create secret generic my-hf-token --from-literal=token=hf_...
  kubectl label secret my-hf-token decisionmodel.io/download-token=true
  ```

  The token is injected into the prefetch Job as `OLLAYA_HF_TOKEN` and sent
  only to the weight-download endpoint, never to the model registry and never
  to serving Pods.

See [sizing.md](sizing.md) for measured per-model `resources` and PVC sizes.

## What changes trigger a new revision

The operator rolls out some spec changes blue-green (a new **revision**: a second
Deployment and its own model-store PVC, verified by the readiness gate before
traffic switches) and applies others **in place** on the running Deployment.

| Spec change | How it is applied |
|-------------|-------------------|
| `engine`, `model`, `digest`, `device`, `resources`, engine image | **New revision** (re-resolve/prefetch as needed). |
| `scheduling` (`nodeSelector`, `tolerations`, `affinity`, `runtimeClassName`) | **New revision.** A placement change re-downloads into a second PVC and, for `device: cuda`, needs a **second GPU** for the duration of the rollout. An unschedulable candidate ends as `RolledBack` (reason `StartTimeout`) with the stable still serving — it does not hang silently. |
| `replicas` | **In place** (scale the stable Deployment). |
| `cache` (size / accessModes / class) | **In place.** A `size` **grow** is applied by expanding the PVC when the StorageClass allows volume expansion; a size **shrink**, a `storageClassName` change, or an `accessModes` change is rejected with `CacheSpecImmutable` (`Degraded=True`) and the PVC is left as is (delete the DM and its PVC to change those). `accessModes: ["ReadOnlyMany"]` is rejected at admission — the prefetch Job must write the store. Note: because each revision gets its own store PVC (named by revision hash) provisioned from the *current* `spec.cache`, a changed class or a shrink also takes effect automatically the next time a **model-affecting** change (model, digest, device, resources, image, scheduling) rolls a new revision — not only via delete-and-recreate. |
| `auth.apiKeySecretRef` | **In place** — changing which Secret/key is referenced updates the stable Deployment's env and it rolls (see the downtime note below). Rotating the *value* inside the **same** Secret is also picked up — see [Rotating the API key](#rotating-the-api-key). |

Engine **resource defaults** (the built-in per-model memory/cpu requests, see
[sizing.md](sizing.md#built-in-defaults)) only apply to a **new** revision. A
running stable keeps the resources it was created with; upgrading the operator
does **not** re-request a running stable.

### Rollout strategy and downtime

The serving Deployment's update strategy depends on the store's access mode and
the device, so an in-place change (or an unavoidable operator-driven roll) does
not deadlock on the volume:

| Store access mode | Device | Strategy |
|-------------------|--------|----------|
| `ReadWriteOnce` (default) | any | `Recreate` (old Pod stops before the new one starts) |
| `ReadWriteMany` | `cuda` | `RollingUpdate`, `maxSurge: 0`, `maxUnavailable: 1` (no second GPU needed) |
| `ReadWriteMany` | `cpu` | `RollingUpdate`, `maxSurge: 25%`, `maxUnavailable: 25%` |

For **`replicas: 1`** on an `RWO` store (the default) or on `cuda`, any in-place
change that alters the Pod template causes a **short outage**: the old Pod stops,
the new Pod starts, loads the model, and passes the gate (for a large model that
can be minutes). For `replicas > 1` on an RWX store the no-surge rolling update
keeps `N-1` Pods serving; on RWX + `cpu` the default surge means no downtime (the
gate still protects the Service). After the placement/resource freeze, the only
things that roll a running stable in place are an `auth.apiKeySecretRef` change
and an operator upgrade that changes a non-placement/non-resource field of the
rendered Pod (rare).

## Scaling

- Always pin an explicit tag (`laya:en`), never a bare name — a bare name
  resolves to `:latest`, a different and heavier artifact.
- For `replicas > 1` spread across nodes, the model-store PVC must be
  `ReadWriteMany`; the default `ReadWriteOnce` binds the PVC to one node.
  (`ReadOnlyMany` is **not** accepted — the prefetch Job must write the store.)
  Set it via `spec.cache`:

  ```yaml
  spec:
    replicas: 3
    cache:
      accessModes: ["ReadWriteMany"]
      storageClassName: my-rwx-class
      size: 4Gi
  ```

  PVC access modes are immutable once created.

See [sizing.md](sizing.md) for per-model `resources` and PVC sizing.

## Uninstall

```sh
kubectl delete -f dm.yaml
# release install:
kubectl delete -f https://github.com/maks3201/decision-model-operator/releases/download/<tag>/install.yaml
# from source:
make undeploy
```
