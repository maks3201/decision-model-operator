# decision-model-operator

Kubernetes Operator for Ollaya-served System-1 decision models (DecisionModel CRD).

**Homepage:** <https://maks3201.github.io/decision-model-operator/>

An open-source Kubernetes Operator for Ollaya-served System-1 decision models
(Laya, Kev, JevK5, …). It pins a model to an immutable digest, prefetches the
weights into a PVC, and gates Pod readiness on the runtime actually serving the
expected digest on the expected device (catching a silent CPU fallback). See the
[project docs](https://github.com/maks3201/decision-model-operator/tree/main/docs)
for the DecisionModel CRD and a quickstart.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| maks3201 | <eumaxpl@gmail.com> | <https://github.com/maks3201> |

## Source Code

* <https://github.com/maks3201/decision-model-operator>

## Requirements

Kubernetes: `>=1.29.0-0`

## Install

The chart is published as an OCI artifact on GHCR:

```sh
helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator \
  --version <version> \
  --namespace decision-model-operator-system --create-namespace
```

The operator image tag defaults to `v<appVersion>`; override it with `--set image.tag=vX.Y.Z`.
From a source checkout use `charts/decision-model-operator` instead of the OCI reference.

The chart installs the CRD (from its `crds/` directory), RBAC, the manager
Deployment and — when `metrics.enabled` — the metrics Service.

## Quick start

Install the operator (above), then apply a minimal `DecisionModel` and send it a
request. `laya:en` serves on CPU in a few GB of RAM.

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
```

```sh
kubectl apply -f dm.yaml
kubectl get dm -w          # wait for PHASE=Ready (dm is the short name)
```

Once `Ready`, port-forward the Service (named after the DecisionModel, port
`11435`) and POST a decision to `/v1/systemone`:

```sh
kubectl port-forward svc/support-router 11435:11435 &

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

The response has one answer per question id (for `choice`: `choice`, `confidence`
and `probabilities`). See the
[quickstart](https://maks3201.github.io/decision-model-operator/quickstart/) for
the full walkthrough and
[sizing](https://maks3201.github.io/decision-model-operator/sizing/) for per-model
`resources`.

## Examples

Each snippet below is a `values.yaml` for `helm install -f values.yaml` unless it
is a `DecisionModel` (applied with `kubectl`).

### Behind a corporate proxy

The operator's registry egress and every prefetch Job go through the proxy; the
in-cluster calls to serving Pods stay direct. Set the service CIDR and in-cluster
DNS in `noProxy` (not the pod CIDR).

```yaml
# values.yaml
proxy:
  httpProxy: http://proxy.corp:3128
  httpsProxy: http://proxy.corp:3128
  noProxy: 10.96.0.0/12,.svc,.cluster.local
```

If the proxy URL carries credentials, put them in a Secret with keys
`httpProxy`/`httpsProxy`/`noProxy` and set `proxy.existingSecret: <name>` instead.

### Air-gapped (registry mirror + weight mirror)

Point the operator at an internal registry mirror (manifests) and a Hugging Face
mirror (weights), and allow the mirror host:

```yaml
# values.yaml
manager:
  ollayaRegistry: https://registry.internal        # manifests + digests
  ollayaHFEndpoint: https://hf-mirror.internal      # model-weight blobs
  allowedRegistries: registry.internal              # SSRF allow-list
```

For a private or gated model, add a download token. The Secret **must** carry the
label `decisionmodel.io/download-token: "true"`, and the token is referenced from
the DecisionModel's `spec.cache`:

```sh
kubectl create secret generic my-hf-token --from-literal=token=hf_...
kubectl label secret my-hf-token decisionmodel.io/download-token=true
```

```yaml
# dm.yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  engine: ollaya
  model: laya:en
  device: cpu
  cache:
    downloadTokenSecretRef:
      name: my-hf-token
      key: token
```

### Namespace-scoped

Watch only a fixed set of namespaces; the chart then renders a namespaced
`Role`+`RoleBinding` in each instead of a ClusterRole. The namespaces must
already exist, and the cluster-scoped CRD is still installed once by an admin.

```yaml
# values.yaml
watchNamespaces:
  - team-a
  - team-b
```

### GPU (`device: cuda`)

GPU serving needs the amd64 CUDA runtime image (`:<appVersion>-cuda`, amd64-only)
and the NVIDIA device plugin on the cluster. GPU nodes are usually tainted
`nvidia.com/gpu`; the operator adds the matching toleration automatically. A
blue-green rollout runs two revisions at once, so size for a second GPU (or use
time-slicing / MIG).

```yaml
# dm.yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  engine: ollaya
  model: laya:en
  device: cuda          # selects the :<appVersion>-cuda image and adds nvidia.com/gpu: 1
  replicas: 1
```

See
[GPU CI](https://maks3201.github.io/decision-model-operator/gpu-ci/) for a
GPU-enabled kind setup and the Pascal/Volta `:cuda12` note.

### Metrics, alerts and dashboard

`metrics.enabled` (default `true`) exposes controller-runtime metrics on
`:8443`/HTTPS and creates a metrics Service. `metrics.serviceMonitor.enabled`
(default off) adds a Prometheus Operator `ServiceMonitor` for it (HTTPS, bearer-token
auth; keep `tlsVerify: false` with the self-signed metrics cert, or set `tlsVerify: true`
with `tlsServerName`+`caSecret` when you front it with a cert-manager cert). Without the
Prometheus Operator, scrape the Service directly.

Optional (both default **off**): `prometheusRule.enabled` ships a `PrometheusRule`
with symptom-based alerts (each links a runbook under `docs/runbooks/`), and
`grafanaDashboard.enabled` ships the operator dashboard as a sidecar-discovered
ConfigMap (`grafana_dashboard: "1"`):

```sh
helm upgrade dmo ... \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=kube-prometheus-stack \
  --set grafanaDashboard.enabled=true
```

The rules alert only on metrics the operator actually exports. See
[metrics](https://maks3201.github.io/decision-model-operator/metrics/).

## Verify the chart

The OCI chart is signed with [cosign](https://github.com/sigstore/cosign) (verify with v3+)
(keyless, GitHub OIDC) from v0.2.0:

```sh
ID='^https://github.com/maks3201/decision-model-operator/.github/workflows/release.yml@refs/'
ISSUER=https://token.actions.githubusercontent.com

cosign verify ghcr.io/maks3201/charts/decision-model-operator:<version> \
  --certificate-identity-regexp "$ID" --certificate-oidc-issuer "$ISSUER"
```

## Support

- **Questions / help:**
  [GitHub Discussions](https://github.com/maks3201/decision-model-operator/discussions).
- **Bugs / feature requests:**
  [GitHub Issues](https://github.com/maks3201/decision-model-operator/issues).
- **Security vulnerabilities:** see
  [SECURITY.md](https://github.com/maks3201/decision-model-operator/blob/main/SECURITY.md)
  (do not open a public issue).
- **Documentation:**
  [docs site](https://maks3201.github.io/decision-model-operator/).

## Upgrade

```sh
helm upgrade dmo oci://ghcr.io/maks3201/charts/decision-model-operator \
  --version <version> \
  --namespace decision-model-operator-system
```

### CRD upgrade caveat

Helm installs the CRD in `crds/` on first install but **never upgrades or
deletes it** on `helm upgrade`/`helm uninstall`. To pick up a CRD change, apply
it out of band:

```sh
kubectl apply -f charts/decision-model-operator/crds/decisionmodel.io_decisionmodels.yaml
```

Set `installCRDs: false` if you manage the CRD entirely out of band.

### Upgrading the operator (runtime version)

Each DecisionModel records the Ollaya **runtime version** its running revision was
built with. What a `helm upgrade` that ships a newer default runtime does to a
running model depends on `manager.runtimeVersionPolicy`:

- **`Pinned` (default):** a model with no `spec.runtimeVersion` keeps serving on
  the runtime version its stable revision already recorded. An operator upgrade
  starts **no** rollout; the model is nudged with a `RuntimeUpdateAvailable`
  condition instead. Roll a model forward when you choose by setting
  `spec.runtimeVersion` (e.g. `"0.10.0"`), which triggers one blue-green rollout
  for that model only.
- **`FollowOperator`:** a model with no `spec.runtimeVersion` adopts the operator's
  new default runtime automatically, so the upgrade rolls **every** such model
  blue-green onto the new version.

Because a rollout runs the old and new revisions at once (and, on GPU, needs a
second GPU for the candidate), `FollowOperator` can start a fleet-wide stampede.
Cap it with `manager.maxConcurrentRollouts` (`0` = unlimited; others wait in phase
`Pending`). On GPU clusters a small budget — **2-3** — is recommended so only a
few extra GPUs are needed at any moment:

```yaml
# values.yaml
manager:
  runtimeVersionPolicy: FollowOperator
  maxConcurrentRollouts: 2
```

A model that pins `spec.runtimeVersion` or `spec.image` opts out of both policies
(it already fixes its own runtime).

## Uninstall

```sh
helm uninstall dmo --namespace decision-model-operator-system
```

The CRD is intentionally left in place (see the caveat above); remove it
explicitly if you no longer need any DecisionModel resources:

```sh
kubectl delete crd decisionmodels.decisionmodel.io
```

## Namespace-scoped install

By default the operator runs cluster-wide: it is granted a ClusterRole and
watches DecisionModels in every namespace. To restrict it to a fixed set of
namespaces, set `watchNamespaces`:

```sh
helm install dmo charts/decision-model-operator \
  --namespace decision-model-operator-system --create-namespace \
  --set 'watchNamespaces={team-a,team-b}'
```

In this mode the chart passes `--watch-namespaces=team-a,team-b` to the manager
and, instead of the cluster-wide ClusterRole/ClusterRoleBinding, renders a
namespaced `Role` + `RoleBinding` in each listed namespace (with identical
rules). The leader-election Role stays in the release namespace.

The DecisionModel CRD is **cluster-scoped** regardless of this setting, so it is
not covered by the namespaced Roles and must be installed once by a cluster
admin (via the chart's `crds/` dir on first install, or out of band — see the
CRD upgrade caveat above). The listed namespaces must already exist.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.repository | string | `"ghcr.io/maks3201/decision-model-operator"` | Manager image repository. |
| image.tag | string | `""` | Manager image tag. Empty defaults to `v<Chart.appVersion>` (release images are tagged `vX.Y.Z`). |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy for the manager container. |
| imagePullSecrets | list | `[]` | Secrets for pulling the manager image (e.g. `[{name: ghcr-pull}]` while the GHCR package is private). |
| replicaCount | int | `1` | Number of controller-manager replicas (leader election picks one active). |
| resources | object | `{"limits":{"cpu":"500m","memory":"128Mi"},"requests":{"cpu":"10m","ephemeral-storage":"64Mi","memory":"64Mi"}}` | Resource requests/limits for the manager container. |
| nodeSelector | object | `{}` | Node selector for the manager Pod. |
| tolerations | list | `[]` | Tolerations for the manager Pod. |
| affinity | object | `{}` | Affinity for the manager Pod. |
| leaderElection | bool | `true` | Enable leader election (recommended for HA / `replicaCount` > 1). |
| metrics.enabled | bool | `true` | Expose the metrics endpoint and create a metrics Service (8443/HTTPS). |
| metrics.bindAddress | string | `":8443"` | Address the metrics server binds to. |
| metrics.secure | bool | `true` | Serve metrics over HTTPS. |
| metrics.serviceMonitor.enabled | bool | `false` | Create a `ServiceMonitor` for the metrics Service. Requires `metrics.enabled` and the Prometheus Operator CRDs. |
| metrics.serviceMonitor.namespace | string | `""` | Namespace for the `ServiceMonitor` (empty = release namespace). |
| metrics.serviceMonitor.labels | object | `{}` | Extra labels on the `ServiceMonitor` (e.g. the label your Prometheus `serviceMonitorSelector` matches). |
| metrics.serviceMonitor.interval | string | `""` | Scrape interval (empty = Prometheus default). |
| metrics.serviceMonitor.scrapeTimeout | string | `""` | Per-scrape timeout (empty = Prometheus default). |
| metrics.serviceMonitor.tlsVerify | bool | `false` | Verify the metrics server certificate. Keep false with the operator's self-signed metrics cert; set true and provide `tlsServerName`+`caSecret` when you front it with a cert-manager cert (Secret `metrics-server-cert`). |
| metrics.serviceMonitor.tlsServerName | string | `""` | `tlsConfig.serverName` when `tlsVerify` is true (e.g. `<release>-metrics.<ns>.svc`). |
| metrics.serviceMonitor.caSecret | string | `""` | Secret name holding `ca.crt` for `tlsConfig.ca` when `tlsVerify` is true (cert-manager writes `metrics-server-cert`). |
| healthProbeBindAddress | string | `":8081"` | Health/readiness probe bind address. |
| manager.allowedRegistries | string | `"ollaya.dev"` | `--allowed-registries`: comma-separated registry hosts a `spec.model` may resolve from (SSRF guard). Add your mirror here if you use one. |
| manager.allowInsecureRegistries | bool | `false` | `--allow-insecure-registries`: permit `http://` model registries (dev only). |
| manager.allowImageOverride | bool | `false` | `--allow-image-override`: permit `spec.image` to override the engine default image (lets DM editors run arbitrary images; keep false in multi-tenant). |
| manager.allowUnpinnedRuntimeImages | bool | `false` | `--allow-unpinned-runtime-images`: permit a candidate whose runtime image is a mutable tag (a `spec.runtimeVersion` this operator build has no digest for). Keep false so one revision hash always means one set of runtime bytes; the operator's own default image is always digest-pinned regardless. |
| manager.maxConcurrentReconciles | int | `4` | `--max-concurrent-reconciles`: max DecisionModels reconciled concurrently. |
| manager.ollayaRegistry | string | `""` | `--ollaya-registry`: default registry base URL for host-less model names. Empty = the operator default (`OLLAYA_REGISTRY` env, else `https://ollaya.dev`); not rendered when empty. |
| manager.ollayaHFEndpoint | string | `""` | `--ollaya-hf-endpoint`: base URL for model-weight downloads (a Hugging Face mirror or enterprise endpoint; the model registry still serves manifests). Empty = the runtime default (`OLLAYA_HF_ENDPOINT` env, else Hugging Face); not rendered when empty. Needs the 0.10.0+ runtime; older runtimes ignore it. |
| manager.runtimeVersionPolicy | string | `""` | `--runtime-version-policy`: how an unset `spec.runtimeVersion` resolves. `Pinned` (default) reuses the stable revision's runtime version, so an operator upgrade starts no rollout; `FollowOperator` uses the engine default, so an upgrade rolls every DecisionModel onto the new runtime. Empty = the binary default (`Pinned`); not rendered when empty. |
| manager.maxConcurrentRollouts | string | `nil` | `--max-concurrent-rollouts`: cap on DecisionModels rolling out at once across the watched scope (`0` = unlimited; others wait in phase `Pending`). Set a small value (e.g. 2-3) to avoid a fleet-wide stampede when the default runtime image changes under `FollowOperator`. `null`/unset = the binary default (`0`, unlimited); not rendered unless set to a positive value. |
| watchNamespaces | list | `[]` | Namespaces to watch (`--watch-namespaces`). Empty = cluster-wide (manager ClusterRole/ClusterRoleBinding). When non-empty, the operator watches only these namespaces and the chart renders a namespaced Role+RoleBinding in each instead of the ClusterRole. The CRD stays cluster-scoped and must be installed by a cluster admin. |
| proxy.httpProxy | string | `""` | `HTTP_PROXY` for the manager (and, via the operator, the prefetch Job). Empty = not set. |
| proxy.httpsProxy | string | `""` | `HTTPS_PROXY` for the manager (and the prefetch Job). Empty = not set. |
| proxy.noProxy | string | `""` | `NO_PROXY` for the manager (and the prefetch Job). Set the service CIDR and `.svc,.cluster.local` so the operator's registry/API-server calls go direct, not via the proxy. The pod CIDR is NOT needed: the readiness prober and evaluator reach serving Pod IPs with a separate proxy-less client (the API-key header never traverses the proxy), so readiness does not depend on this value. Example: `10.96.0.0/12,.svc,.cluster.local`. Empty = not set. |
| proxy.existingSecret | string | `""` | Name of an existing Secret to source the proxy URLs from (keys `httpProxy` / `httpsProxy` / `noProxy`, each optional) instead of the plaintext values above — use this when a proxy URL carries credentials. When set, the plaintext `proxy.httpProxy/httpsProxy/noProxy` values are ignored and the env is rendered with `valueFrom.secretKeyRef` (`optional: true` per key). This only keeps the credential out of the Helm values: a **credentialed** proxy URL is used by the operator for its own registry lookups but is NOT passed to the prefetch Jobs (a Job spec is readable by anyone with `get jobs` in that namespace), and the chart cannot change that. For the Job's egress use an IP-allowlisted or node-level proxy that needs no credentials. `NO_PROXY` is always passed. Empty = use the plaintext values. |
| installCRDs | bool | `true` | Install the CRD via Helm's `crds/` dir. Set false to manage the CRD out of band. NOTE: Helm never upgrades or deletes CRDs in `crds/` (see the CRD upgrade caveat in this README). |
| prometheusRule.enabled | bool | `false` | Create a `PrometheusRule` with the operator's alerting rules. Requires the Prometheus Operator CRDs. |
| prometheusRule.namespace | string | `""` | Namespace for the `PrometheusRule` (empty = release namespace). |
| prometheusRule.labels | object | `{}` | Extra labels on the `PrometheusRule` (e.g. the `release:` label your Prometheus selects on). |
| prometheusRule.runbookUrl | string | `"https://github.com/maks3201/decision-model-operator/blob/main/docs/runbooks"` | Base URL prepended to each alert's `runbook_url` annotation (points at the docs site or your fork). |
| prometheusRule.windows | object | `{"candidateTimeout":"20m","degraded":"10m","operatorDown":"5m","rolloutStuck":"15m"}` | Per-alert `for` windows (how long the symptom must hold before firing). |
| grafanaDashboard.enabled | bool | `false` | Create a ConfigMap holding the operator Grafana dashboard, labelled for the Grafana sidecar to auto-import. |
| grafanaDashboard.namespace | string | `""` | Namespace for the dashboard ConfigMap (empty = release namespace; set to where the Grafana sidecar watches). |
| grafanaDashboard.labels | object | `{}` | Extra labels on the dashboard ConfigMap (the sidecar label `grafana_dashboard: "1"` is always added). |
| grafanaDashboard.folder | string | `""` | Value of the Grafana sidecar folder annotation (`k8s-sidecar-target-directory`); empty = the sidecar default. |

