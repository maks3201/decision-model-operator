# Decision Model Operator

A Kubernetes operator that runs open-source System-1 decision models (Laya, Kev, JevK5, …)
served by [Ollaya](https://github.com/ollaya-dev/ollaya), and manages their model lifecycle:
**pin → prefetch → load → verify → evaluate → promote → roll back**.

!!! warning "Alpha"
    The API is `decisionmodel.io/v1alpha1` and may change between minor versions.
    Not affiliated with TypeSafe, Convai Innovations or Ollaya.

## Why

A plain Deployment of a model server tells you the process is up. It does not tell you
*which* model is loaded, *where* it runs, or whether the new version is any good.
This operator closes those gaps:

| Feature | What it does |
|---|---|
| **Digest pinning** | Resolves `model: laya:en` to an immutable sha256 digest recorded in `status`. A moved tag never changes a running revision. |
| **Prefetch** | A Job pulls and verifies the weights into a per-revision PVC *before* any serving Pod starts. Serving Pods mount the store read-only. |
| **Model-aware readiness** | A Pod readiness gate (`decisionmodel.io/model-ready`) turns True only when the runtime reports the expected digest on the expected device, so a silent CUDA→CPU fallback keeps the Pod out of the Service. |
| **Blue-green rollout** | Every model-affecting change creates a candidate revision next to the stable one; traffic switches only when the candidate is ready, and failures roll back automatically. |
| **Eval-gated promotion** | Optionally runs a golden dataset against the candidate and promotes only if accuracy, accuracy drop and calibration (ECE) gates pass. |
| **Secure defaults** | Registry allow-list, no `http://` registries, no image override, labelled API-key Secrets, restricted Pod security — each guard relaxable explicitly. |

## How it works

```text
DecisionModel ──► Resolving ──► Caching ──► Starting ──► Evaluating ──► Promoting ──► Ready
                  (digest)      (Job→PVC)   (candidate,   (golden set,   (Service
                                            readiness     optional)      switch)
                                            gate)
                       any failure ──► RolledBack (stable keeps serving) or Failed
```

The operator owns everything it creates (PVC, Job, Deployments, Service) through owner
references; deleting the `DecisionModel` cleans up.

## Next steps

- [Quickstart](quickstart.md) — install the operator and deploy your first model.
- [Evaluation](evaluation.md) — golden datasets and eval-gated promotion.
- [Sizing](sizing.md) — measured model footprints and resource requests.
- [GitOps](gitops.md) — managing `DecisionModel` resources declaratively.
- [Metrics](metrics.md) — the `decisionmodel_*` series and conditions.
- [GPU CI](gpu-ci.md) — running the GPU end-to-end suite.
- [API reference](api-reference.md) — the full `DecisionModel` schema.
- [Architecture](ARCHITECTURE.md) — the complete design.
