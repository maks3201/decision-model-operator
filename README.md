# decision-model-operator

[![Tests](https://github.com/maks3201/decision-model-operator/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/maks3201/decision-model-operator/actions/workflows/test.yml)
[![E2E](https://github.com/maks3201/decision-model-operator/actions/workflows/test-e2e.yml/badge.svg?branch=main)](https://github.com/maks3201/decision-model-operator/actions/workflows/test-e2e.yml)
[![codecov](https://codecov.io/gh/maks3201/decision-model-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/maks3201/decision-model-operator)
[![Release](https://img.shields.io/github/v/release/maks3201/decision-model-operator?sort=semver)](https://github.com/maks3201/decision-model-operator/releases)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/decision-model-operator)](https://artifacthub.io/packages/search?repo=decision-model-operator)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=maks3201_decision-model-operator&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=maks3201_decision-model-operator)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/maks3201/decision-model-operator/badge)](https://scorecard.dev/viewer/?uri=github.com/maks3201/decision-model-operator)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/15233/badge)](https://www.bestpractices.dev/projects/15233)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Kubernetes operator that runs open-source System-1 decision models (Laya, Kev, JevK5, …)
served by [Ollaya](https://github.com/ollaya-dev/ollaya), and manages their model lifecycle:
**pin → prefetch → load → verify → evaluate → promote → roll back**.

> **Status: alpha.** The API is `decisionmodel.io/v1alpha1` and may change between minor
> versions. Not affiliated with TypeSafe, Convai Innovations or Ollaya.

📖 **Documentation:** <https://maks3201.github.io/decision-model-operator/>

## Why

A plain Deployment of a model server tells you the process is up. It does not tell you
*which* model is loaded, *where* it runs, or whether the new version is any good.
decision-model-operator closes those gaps:

| Feature | What it does |
|---|---|
| **Digest pinning** | Resolves `model: laya:en` to an immutable sha256 digest and records it in `status`. A moved tag never changes a running revision. |
| **Prefetch** | A Job pulls and verifies the weights into a per-revision PVC *before* any serving Pod starts. Serving Pods mount the store read-only. |
| **Model-aware readiness** | A Pod readiness gate (`decisionmodel.io/model-ready`) turns True only when the runtime reports the expected digest on the expected device — a silent CUDA→CPU fallback keeps the Pod out of the Service. |
| **Blue-green rollout** | Every model-affecting change creates a candidate revision next to the stable one; traffic switches only when the candidate is ready. Failures roll back automatically. |
| **Eval-gated promotion** | Optionally runs a golden dataset against the candidate and promotes only if accuracy, accuracy drop and calibration (ECE) gates pass. |
| **Secure defaults** | Registry allow-list, no `http://` registries, no image override, labelled API-key Secrets, restricted Pod security. Each guard can be relaxed explicitly. |

## How it works

```text
DecisionModel ──► Resolving ──► Caching ──► Starting ──► Evaluating ──► Promoting ──► Ready
                  (digest)      (Job→PVC)   (candidate,   (golden set,   (Service
                                            readiness     optional)      switch)
                                            gate)
                       any failure ──► RolledBack (stable keeps serving) or Failed
```

The operator owns everything it creates (PVC, Job, Deployments, Service) through owner
references; deleting the `DecisionModel` cleans up. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
for the full design.

## Requirements

- Kubernetes **1.29+**
- A default StorageClass with `ReadWriteOnce` (`ReadWriteMany` for `replicas > 1` across nodes)
- Egress from the cluster to `https://ollaya.dev`, or an [internal mirror](docs/quickstart.md)
- For `device: cuda`: amd64 nodes with the NVIDIA device plugin

## Installation

### Helm (recommended)

```sh
helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator \
  --version 0.1.0 \
  --namespace decision-model-operator-system --create-namespace
```

Chart values are documented in the [chart README](charts/decision-model-operator/README.md)
and on [Artifact Hub](https://artifacthub.io/packages/search?repo=decision-model-operator).

### kubectl

```sh
kubectl apply -f https://github.com/maks3201/decision-model-operator/releases/download/v0.1.0/install.yaml
```

Both install the CRD, RBAC and the controller into `decision-model-operator-system`.

### Verify the release

From v0.2.0 the image, the Helm chart and the release files are signed with
[cosign](https://github.com/sigstore/cosign) (keyless, GitHub OIDC), and the image carries an
SPDX SBOM and SLSA provenance:

```sh
ID='^https://github.com/maks3201/decision-model-operator/.github/workflows/release.yml@refs/'
ISSUER=https://token.actions.githubusercontent.com

cosign verify ghcr.io/maks3201/decision-model-operator:v0.2.0 \
  --certificate-identity-regexp "$ID" --certificate-oidc-issuer "$ISSUER"
cosign verify ghcr.io/maks3201/charts/decision-model-operator:0.2.0 \
  --certificate-identity-regexp "$ID" --certificate-oidc-issuer "$ISSUER"
cosign verify-blob install.yaml --bundle install.yaml.sigstore.json \
  --certificate-identity-regexp "$ID" --certificate-oidc-issuer "$ISSUER"
gh attestation verify install.yaml --repo maks3201/decision-model-operator   # SLSA provenance (from v0.3.0)

docker buildx imagetools inspect ghcr.io/maks3201/decision-model-operator:v0.2.0 --format '{{json .SBOM}}'
```

## Usage

```yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  model: laya:en        # explicit tag required
  device: cpu           # cpu | cuda
  replicas: 2
```

```sh
kubectl apply -f config/samples/decisionmodel_v1alpha1_decisionmodel.yaml
kubectl get dm -w
```

```text
NAME             MODEL     DEVICE   PHASE   READY   AGE
support-router   laya:en   cpu      Ready   2       3m
```

The model is then served at `http://support-router.<namespace>.svc:11435/v1/systemone`.
Changing `spec.model`, `spec.device` or `spec.resources` starts a blue-green rollout;
`replicas` and scheduling changes are applied in place.

An eval-gated example is in
[config/samples/decisionmodel_v1alpha1_decisionmodel_eval.yaml](config/samples/decisionmodel_v1alpha1_decisionmodel_eval.yaml).

## Documentation

| Guide | |
|---|---|
| [Quickstart](docs/quickstart.md) | Install, first model, request, scaling, proxy and mirror setup, uninstall |
| [API reference](docs/api-reference.md) | All `DecisionModel` fields |
| [Eval-gated rollout](docs/evaluation.md) | Golden datasets, accuracy and calibration gates |
| [Sizing](docs/sizing.md) | Per-model CPU/memory and PVC sizes |
| [GitOps](docs/gitops.md) | Argo CD / Flux notes |
| [Metrics](docs/metrics.md) | Operator metrics |
| [Architecture](docs/ARCHITECTURE.md) | Design, state machine, security model |

## Roadmap

| Version | Scope |
|---|---|
| v0.1 | Digest pinning, prefetch, model-aware readiness, blue-green, eval-gated promotion, security guards; signed releases with SBOM and provenance (v0.2.0) |
| v0.2 | Per-revision API key, autoscaling (KEDA on in-flight / queue), runtime metrics, node-local cache, GPU E2E in CI |
| v0.3 | Shadow traffic, confidence cascade with LLM fallback |

## Development

```sh
make generate manifests   # after changing api/
make build lint test      # build, golangci-lint, unit + envtest
make test-e2e             # kind cluster with the CPU Ollaya image
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow and [RELEASING.md](RELEASING.md)
for how releases are cut.

## Security

Please report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).
