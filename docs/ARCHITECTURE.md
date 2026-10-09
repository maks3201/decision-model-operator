# decision-model-operator — architecture

Status: alpha (v0.3). Module `github.com/maks3201/decision-model-operator`,
API group `decisionmodel.io/v1alpha1` (the domain is not registered; the group may change before v1).
Engine contract: `internal/engine/engine.go`.

## 1. Goal

Safe production rollout of decision models: a new model version is evaluated against a golden
dataset (and against production) **before** it receives traffic; a regression is blocked or rolled
back. Running the model server is a means, not the product.

```text
DecisionModel CRD
       │
       ▼
Lifecycle / rollout controller   (internal/controller: state machine, revisions, status)
       ├── Runtime adapter       (internal/engine: contract; internal/engine/ollaya: Ollaya)
       ├── Evaluation engine     (internal/controller/evaluator.go, evaluate_flow.go; internal/eval/calibration)
       └── Promotion / rollback  (internal/controller/promotion.go: Service switch, failed revisions)
```

The runtime adapter serves the model (workloads, health, what is loaded). The controller owns the
lifecycle. The evaluation engine decides quality. Promotion decides whether a candidate becomes
active. Only the adapter knows the runtime.

Non-goals: generic model serving (KServe, KubeAI, Ollama operators), GPU scheduling, autoscaling
(stay compatible with HPA/KEDA), a model registry, experiment tracking, LLM text generation,
multi-cluster, distributed inference.

## 2. Facts the design is built on

Verified against code, docs and local runs on 2026-09-27 (details in `docs/spikes/`).

| Fact | Source | Design consequence |
|---|---|---|
| `laya-serve`: FastAPI, `/v1/systemone`, `/health`; one inference at a time per Pod | `laya/serve.py` | concurrency 1 → scale out; autoscale on in-flight/queue |
| Laya does not pin the HF revision (`snapshot_download` without `revision`) | `laya/agent.py` | the operator must resolve and pin versions itself |
| Laya silently falls back to CPU on OOM / missing CUDA; `/health` does not show it | `laya/agent.py`, `serve.py` | the operator needs its own check of the actual device |
| **Ollaya** (`ollaya-dev/ollaya`, 0.12.0, Rust): "Ollama for decision models". Laya, Kev, JevK5, decider, NLI, GLiClass behind one API; weights pinned to commit + sha256; `/v1/systemone`, `/api/pull`, `/api/ps`, `/api/decide` (load/unload, `keep_alive`), `503 QUEUE_FULL` | README, `docs/api.md` | **primary runtime**: the multi-model abstraction already exists |
| Ollaya has no `/metrics`; liveness is `GET /` (200 even with an API key); no implicit pull | `docs/api.md` | readiness and prefetch are the operator's job |
| Image `ghcr.io/ollaya-dev/ollaya:0.12.0` (native arm64 + amd64; `:0.12.0-cuda` amd64 only), rendered as `repo:tag@sha256:<index>`: `ollaya serve`, `0.0.0.0:11435`, UID 1000 | spikes 001, 006 | near-default PodSpec |
| `/api/ps` reports `name`, `digest`, `device`, precision, `expires_at` | spike 001 | `Inspect()` for the readiness gate |
| `POST /api/decide {"model":…,"keep_alive":-1}` loads and pins a model | spike 001 | `Warmup()` is one request |
| Serving from a **read-only** store works; `pull` on a RO store fails | spike 001 | serving Pods mount the PVC read-only; only the prefetch Job writes |
| Model digest = `sha256(manifest bytes)` from `GET https://ollaya.dev/v2/library/<name>/manifests/<tag>`; equals `/api/ps` digest | spike 001 | resolver needs no daemon, plain HTTP |
| `pull` accepts a tag only, never `@sha256:` | spike 001 | pull by tag, then verify the manifest sha256 in the Job |
| A bare name (`laya`) resolves to `latest`, a different, heavier artifact | spike 001 | the API requires an explicit tag |
| `OLLAYA_DEVICE=cuda` on the CPU image fails at model load, not at startup; the port is open | spike 001 | a port/HTTP probe is not enough; gate on `/api/ps` |
| Sizes (CPU, F32, cgroup `anon`, re-measured 2026-10-02): `laya:en` 3.09 GiB, `laya:multilingual` 1.85 GiB, `nli` 3.62 GiB, `gliclass` 2.42 GiB | spike 002 (corrected), `docs/sizing.md` | built-in default requests per measured `name:tag` |
| Ollama 0.35 and llama.cpp also serve `/v1/systemone`. Ollama: `/api/ps` digest = sha256 of the registry manifest, no pull by digest, RO store works; but no `device` field (only `size_vram`), no server-side API key, different request schema | spike 004 | second engine possible later, not in the next stage |
| OCI image volumes (KEP-4639, GA in k8s 1.36) can carry an Ollaya store; gate unaffected; Ollaya registry is not OCI, so the model must be repackaged | spike 005 | opt-in cache backend candidate (later) |
| No existing Kubernetes operator for Laya / Ollaya / Jev-style models found | GitHub + web search | the niche is open (re-check before release) |

## 3. Positioning

| | Helm chart | KServe | ollama-operator-style | **this operator** |
|---|---|---|---|---|
| Deployment / Service / GPU | yes | yes | yes | yes |
| Model pinned to a digest | manual | storageUri | partial | yes, in status |
| Prefetch / cache | no | LocalModelCache | pull at start | Job → PVC before rollout |
| "Model loaded on the expected device" | no | no | no | **yes (readiness gate)** |
| Eval-gated rollout | no | no | no | **yes** |
| Knows the Jev / System-1 contract | no | no | no | **yes** |

The value is in the last three rows. Without them the project is a Helm chart.

## 4. Components

```text
                        Kubernetes API
                              │ watch DecisionModel, owned objects, Pods
                              ▼
┌──────────────────────── controller-manager ────────────────────────┐
│  DecisionModel reconciler (state machine, §6)                      │
│     ├── Resolver    name:tag → pinned digest (registry HTTP)       │
│     ├── Cache       PVC + prefetch Job (`ollaya pull` + sha256)    │
│     ├── Engine      ollaya → PodSpec, JobSpec, HTTP to Pods        │
│     ├── Revisions   one Deployment per revision (stable/candidate) │
│     ├── Prober      warmup + /api/ps → Pod readiness gate          │
│     ├── Evaluator   golden set via /v1/systemone (async)           │
│     └── Status      conditions, events, observedGeneration         │
└────────────────────────────────────────────────────────────────────┘
          │ owns
          ▼
  PVC <name>-store-<rev>   Job (prefetch)   Deployment <name>-<rev>   PDB   Service <name>
                                        │  Pod: ollaya serve
                                        │  readinessGate: decisionmodel.io/model-ready
                                        ▼
                              POST /v1/systemone (Jev-compatible)
```

### Engine contract

`internal/engine/engine.go` is normative and does not import CRD types: the controller maps
`DecisionModel.spec` → `engine.Params`; the engine maps `Params` → PodSpec / JobSpec and talks
to Pods over HTTP. Methods: `Resolve`, `ServingPodSpec` (store RO), `PrefetchJobSpec` (store RW,
digest check), `ServicePort`, `Inspect` (`/api/ps`), `Warmup` (`keep_alive=-1`). Optional
capabilities detected by type assertion: `Decider` (`/v1/systemone`, used by the evaluator) and
`CanonicalName`.

Implementations: `ollaya`. A `laya-serve` engine is possible later; no other abstractions until
a second real engine exists.

## 5. API: `DecisionModel` (v1alpha1)

```yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  engine: ollaya                 # only ollaya today
  model: laya:en                 # explicit tag required (CEL)
  # digest: <64 hex>             # optional hard pin; otherwise resolved and recorded in status
  replicas: 2
  device: cpu                    # cpu | cuda
  resources: {}                  # PodSpec resources; cuda adds nvidia.com/gpu: 1 if absent
  scheduling: {}                 # nodeSelector / tolerations / affinity passthrough
  cache:
    size: 10Gi
    accessModes: [ReadWriteOnce] # RWX needed for replicas > 1 across nodes (§7)
  auth:
    apiKeySecretRef: { name: support-router-key, key: token }
  rollout:
    evaluation:                  # optional; without it promotion follows ModelReady
      datasetRef: { configMapRef: { name: support-router-golden, key: cases.jsonl } }
      minAccuracy: "0.90"
      maxAccuracyDrop: "0.02"    # vs the stable revision on the same dataset
status:
  phase: Ready
  phaseTransitionTime: "…"
  observedGeneration: 3
  endpoint: http://support-router.default.svc:11435/v1/systemone
  stableRevision:    { hash: 7c9f…, model: laya:en, digest: c305…, device: cpu, precision: F32 }
  candidateRevision: null
  failedRevision: null
  evaluation: { accuracy: "0.9400", baselineAccuracy: "0.9300", cases: 500 }
  replicas: { desired: 2, modelReady: 2 }
  conditions: [Resolved, Cached, ModelReady, Evaluated, Promoted, Ready, Degraded]
```

Golden dataset: JSONL, one case per line:
`{"state": …, "questions": {…}, "expected": {"<qid>": "<choice>" | true | false}}`.

Revision hash = hash of engine, model, digest, device, resources, engine image and placement
(`scheduling`; omitted when empty so older hashes are unchanged). A scheduling change is a
blue-green candidate. `replicas`, `cache` and `auth` are in-place updates of the stable Deployment.

Rollout strategy (`rollout.strategy`, default `BlueGreen`):
- **BlueGreen** (default): the candidate runs alongside the stable and traffic switches only after
  it passes its gate. Needs capacity for both revisions at once (e.g. a second GPU).
- **Recreate**: for clusters without spare capacity (a single GPU). The candidate is resolved and
  prefetched **while the stable still serves** (the prefetch Job needs no GPU); only once the model
  is `Cached` is the stable scaled to 0 (recorded in `status.stableStoppedForRevision`, persisted
  first) and, once its Pods are gone, the candidate started. There is a traffic gap (documented
  downtime) until promotion. A candidate failure scales the stable back up (`StableRestored`).
  Relative eval gates (`maxAccuracyDrop`, `maxECEIncrease`, `maxMacroF1Drop`) are skipped under
  Recreate — the stopped stable cannot be measured as a baseline — with a note in
  `status.evaluation.reason` and a Warning; the absolute gates still apply. Recreate cannot be
  combined with `promotion: Manual` (rejected by CEL: production would be down while a human
  approves). Changing the strategy is not a revision-hash change.
  - **Recreate rollback downtime.** After a Recreate promotion the old stable becomes the
    `previousRevision` and stays scaled to 0 for the whole stabilization window (it was stopped
    for the rollout and must not take the single GPU back). If the new stable turns unhealthy in
    the window, the rollback is staged on one GPU — record the decision, scale the unhealthy
    stable to 0, wait until its Pods are gone, scale the previous revision back up, wait until it
    is model-ready, then switch the Service to it (durable `status.recreateRollback`, restart-safe
    at every step). Serving is therefore **down from the moment the new stable is declared
    unhealthy until the restored revision is model-ready** — the price of a single-GPU rollback.
    With BlueGreen the previous revision keeps running through the window, so its rollback is
    instant (just a Service switch).

In-place rules:
- A running stable is rendered from its recorded identity; its placement and resources are frozen
  from the live Deployment, so an operator upgrade (new default resources, GPU toleration, arch
  pin) does not roll it. Stables recorded by an older operator are adopted without a roll.
- Every serving Deployment has an explicit strategy: RWO store → `Recreate`; shareable store +
  `cuda` → `RollingUpdate{maxSurge: 0, maxUnavailable: 1}`; otherwise 25%/25%. For `replicas: 1`
  any in-place change is a short outage; that is the price of no Multi-Attach / GPU deadlock.
- Known limit: an `auth` secret-ref change rolls the stable in place, and the prober uses the
  current key for every revision; a per-revision key is a v0.2 item.

Manual promotion: `spec.rollout.manualPromotion: true` parks a candidate that passed its
gate in `AwaitingPromotion` until `decisionmodel.io/promote=<candidate hash>` is set; no timeout
while waiting; the first revision is never held.

## 6. Reconcile state machine

```text
             spec changed (new revision)
                        │
                        ▼
  ┌──────────┐    ┌──────────┐    ┌─────────────┐    ┌────────────┐
  │ Resolving├───►│ Caching  ├───►│ Starting    ├───►│ Evaluating │
  └────┬─────┘    └────┬─────┘    │ (candidate  │    └──┬──────┬──┘
       │ fail          │ fail     │  Deployment,│   pass│      │fail
       ▼               ▼          │  model-ready│       ▼      ▼
    Failed /        Failed /      │  gate)      │   Promoting RolledBack
    RolledBack      RolledBack    └──────┬──────┘       │   (candidate deleted,
                                         │ timeout      ▼    stable keeps serving)
                                         ▼            Ready
                                     RolledBack   (old revision deleted)
```

- **Resolving**: `model` → digest via the registry. Stored in `status.candidateRevision`.
- **Caching**: the prefetch Job pulls the tag into the PVC and verifies the manifest sha256
  against the resolved digest. Idempotent.
- **Starting**: `Deployment <name>-<rev>` is created without traffic (the Service selects
  `decisionmodel.io/revision=<stable>`). Pods carry the readiness gate
  `decisionmodel.io/model-ready`; the prober runs `Warmup` → `Inspect` and sets the gate True
  only if digest and device match. Mismatch (e.g. silent CPU fallback) → gate False with reason
  `DigestMismatch` / `DeviceMismatch`, an Event, and no promotion.
- **Evaluating**: the golden set runs asynchronously against one candidate Pod (and the stable
  revision for a baseline, if missing). Skipped without `rollout.evaluation`.
- **Promoting**: record the candidate as `status.stableRevision`, switch the Service selector, then
  delete the old revision after a short grace. Blue-green, not
  rolling: evaluation must pass before traffic moves.
- **Failure**: rollback if a stable revision exists, otherwise `Failed`. The failed revision is
  recorded in `status.failedRevision` and not retried automatically. Retry with the
  `decisionmodel.io/retry` annotation.

Other phases: `AwaitingPromotion` (manual promotion, §5) and `Degraded` (not all stable replicas
are model-ready, or a referenced Secret is not allowed). The `Degraded` *condition* additionally
flags issues that do not stop serving, e.g. `StoreTerminating`. `Promoting`
is reserved and never observed: promotion persists `status.stableRevision` and then moves the
Service in one reconcile.

Rules:
- Everything is owned via `OwnerReferences`; no finalizers (no external resources).
- Level-triggered: phase is derived from cluster state on every reconcile.
- Progress timeouts on Caching / Starting / Evaluating, measured from `phaseTransitionTime`.
- `metav1.Condition` everywhere, `observedGeneration` always set.
- Watches: `DecisionModel` (generation changes), owned Deployment / Job / Service / PVC / PDB, Pods by
  label `decisionmodel.io/name`.

## 7. Model cache

One store PVC **per revision**, `<dm>-store-<rev>`, mounted at `OLLAYA_MODELS=/models`. The
prefetch Job mounts it RW, that revision's serving Pods RO; serving Pods therefore cannot pull or
delete models. `fsGroup: 1000` (`fsGroupChangePolicy: OnRootMismatch`) on both.

- Why per revision: blue-green runs stable and candidate at the same time. With one shared RWO PVC a
  candidate scheduled on another node (e.g. one GPU per node) cannot attach it → Multi-Attach
  deadlock (reproduced on EKS). A revision's PVC is garbage-collected with its Deployment/Job (never
  the stable or candidate; the just-demoted previous revision only within the promotion grace). Two
  PVCs exist during a rollout, so the store footprint ≈ 2× the model while it runs. Use a
  StorageClass with `reclaimPolicy: Delete`, or deleted stores keep their volumes.
- Migration: a stable promoted before per-revision stores keeps mounting the shared `<dm>-store` until its next
  promotion; the shared PVC is deleted once no workload references it.
- The stable revision is rendered from its recorded identity in `status.stableRevision` (engine,
  model, digest, device, image, resources), never from the live spec, so starting — or failing — a
  candidate never rewrites the running stable Pod template.
- SELinux: on enforcing nodes whose CSI driver lacks SELinux mount support the runtime relabels the
  volume to the mounting Pod's MCS level; the operator sets one stable level per DecisionModel on
  the prefetch and serving Pods so they can share the store.
- `replicas: 1` or single node: RWO is enough. `replicas > 1` on an RWO store: the operator
  co-locates all replicas on the node holding the volume (required host pod-affinity;
  informational Event `ReplicasCoLocated`, a node failure then takes all replicas down). Use RWX
  (`spec.cache.accessModes`) to spread replicas across nodes.
- Later: node-local cache or OCI image volumes (model as an image), similar to KServe
  LocalModelCache / modelcars.
- Air-gapped: `--ollaya-registry` / `OLLAYA_REGISTRY` → internal mirror.

### Namespace-scoped mode

By default the operator watches all namespaces. `--watch-namespaces=ns1,ns2` (chart value
`watchNamespaces`) scopes the manager cache with `cache.Options.DefaultNamespaces`, and the
reconciler refuses any DecisionModel outside the set (defence in depth). The operator then makes no
cluster-scoped API calls at runtime and runs with one Role per watched namespace; the CRD remains
cluster-scoped and is installed by a cluster admin.

## 8. Resources and scheduling

- `device: cuda` → `nvidia.com/gpu: 1`, `OLLAYA_DEVICE=cuda`, `:0.12.0-cuda` image. GPU behaviour verified on EKS
  (g4dn.xlarge, T4, Bottlerocket, 0.7.3-cuda; not yet re-verified on 0.12.0-cuda): `/api/ps` reports `cuda:0`, which the gate treats as class `cuda`
  (only the exact `cuda:<n>` form; anything else fails the gate). A blue-green rollout needs a
  second GPU for the candidate; on a single GPU set `rollout.strategy: Recreate` so the stable is
  stopped before the candidate starts (documented downtime, no second GPU — see §5). Models are
  small (hundreds of MB to a few GB); recommend GPU
  time-slicing / MIG in the docs, the operator does not automate it.
- `device: cpu` → CPU image; requests from the sizing table in spike 002.
- `scheduling` is a passthrough.

## 9. Security

- Pods: non-root (UID 1000), `readOnlyRootFilesystem`, drop ALL, `RuntimeDefault` seccomp,
  no service account token.
- Engine API key from a Secret (`OLLAYA_API_KEY`); the prober and evaluator use the same key.
  Secrets and user ConfigMaps (golden datasets) are read through the uncached API reader with `get`-only RBAC.
  The operator also writes one owned ConfigMap per revision, `<dm>-manifest-<rev>`, holding the raw model
  manifest so a lost store is rebuilt to the recorded digest. It is written in the reconcile that resolves the
  digest, before the digest is recorded in status, and is deleted by name with its revision. ConfigMap RBAC is
  `get`, `create` and `delete` only: no `list`, `watch` or informer.
- Multi-tenant guards: `--allowed-registries` (default `ollaya.dev`),
  `--allow-insecure-registries` (default false), `--allow-image-override` (default false), and
  Secrets referenced by a DecisionModel must carry the label `decisionmodel.io/api-key: "true"`.
- Trust boundary is the namespace: anyone who can create Pods in it can mount any Secret there.
  Ownership checks before probing/GC are defence in depth, not a tenant boundary.
- Calls to serving Pods (`Inspect`, `Warmup`, `Decide`, which carry the API key) use a client
  without a proxy; only the registry client honours `HTTP(S)_PROXY`. Proxy URLs with
  credentials are never copied into tenant prefetch Jobs.
- Service is `ClusterIP` only; an example NetworkPolicy ships in the docs.
- The prefetch Job is the only component with egress: to the registry (manifests, graphs) and to
  Hugging Face (`huggingface.co`, redirected to `*.hf.co`) for the weights. Both can be mirrored
  (`--ollaya-registry`, `--ollaya-hf-endpoint`); a download token comes from a labelled Secret and
  reaches the prefetch Job only.
- A golden dataset may contain real data; store it in a Secret if sensitive.
- Repository: gitleaks pre-commit hook and CI scan.

## 10. Observability

- Conditions and Events (events.k8s.io/v1) on the CR are the primary UX; printer columns:
  Model, Device, Phase, Ready, Age (Digest with `-o wide`).
- Operator metrics: controller-runtime defaults plus `decisionmodel_*` series (phase, ready replicas,
  rollouts, phase durations, eval accuracy / ECE, probe results, registry latency); see
  `docs/metrics.md`.
- Ollaya has no inference metrics → later: upstream `/metrics` or a sidecar.

## 11. Stack

- Go (module minimum 1.26, built with 1.27), controller-runtime v0.25, k8s libraries v0.37,
  operator-sdk / kubebuilder layout.
- Tests: envtest (reconcile), kind e2e with the CPU Ollaya image and `laya:en` (GitHub Actions,
  amd64).
- Distribution: multi-arch image in GHCR and `install.yaml` attached to a GitHub Release on tag
  `v*`; Helm chart as an OCI artifact (`oci://ghcr.io/maks3201/charts/decision-model-operator`).
  OLM later.

## 12. Roadmap

Stages, not version numbers; the [CHANGELOG](https://github.com/maks3201/decision-model-operator/blob/main/CHANGELOG.md) records what each release ships.

| Stage | Scope |
|---|---|
| **Released** | `DecisionModel`, `ollaya` engine, CPU + CUDA, per-revision PVC store + prefetch Job, readiness gate (digest + device + pinned), blue-green switch, eval-gated and manual promotion (accuracy, ECE/Brier), security guards, namespace-scoped mode, conditions, Events, e2e on kind |
| Next | per-revision API key, autoscaling (KEDA on in-flight / queue), metrics, node-local cache, GPU e2e |
| Later | shadow traffic (agreement rate), confidence cascade / fallback to an LLM |

## 13. Open questions

1. CUDA on a real GPU: `/api/ps` reports `cuda:0` (EKS T4, answered); behaviour when the GPU
   disappears at runtime is still unverified.
2. GGUF / llama.cpp runner models (`jevk5`, `winnow`): not verified (smallest is 4.5 GB).
3. Model licenses differ per model: what to state in the docs.
4. Final project name and API group domain.
