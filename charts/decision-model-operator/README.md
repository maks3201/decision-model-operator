# decision-model-operator

Kubernetes Operator for Ollaya-served System-1 decision models (DecisionModel CRD).

**Homepage:** <https://github.com/maks3201/decision-model-operator>

An open-source Kubernetes Operator for Ollaya-served System-1 decision models
(Laya, Kev, JevK5, …). It pins a model to an immutable digest, prefetches the
weights into a PVC, and gates Pod readiness on the runtime actually serving the
expected digest on the expected device (catching a silent CPU fallback). See the
[project docs](https://github.com/maks3201/decision-model-operator/tree/main/docs)
for the DecisionModel CRD and a quickstart.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| maks3201 |  | <https://github.com/maks3201> |

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
| resources | object | `{"limits":{"cpu":"500m","memory":"128Mi"},"requests":{"cpu":"10m","memory":"64Mi"}}` | Resource requests/limits for the manager container. |
| nodeSelector | object | `{}` | Node selector for the manager Pod. |
| tolerations | list | `[]` | Tolerations for the manager Pod. |
| affinity | object | `{}` | Affinity for the manager Pod. |
| leaderElection | bool | `true` | Enable leader election (recommended for HA / `replicaCount` > 1). |
| metrics.enabled | bool | `true` | Expose the metrics endpoint and create a metrics Service (8443/HTTPS). |
| metrics.bindAddress | string | `":8443"` | Address the metrics server binds to. |
| metrics.secure | bool | `true` | Serve metrics over HTTPS. |
| healthProbeBindAddress | string | `":8081"` | Health/readiness probe bind address. |
| manager.allowedRegistries | string | `"ollaya.dev"` | `--allowed-registries`: comma-separated registry hosts a `spec.model` may resolve from (SSRF guard). Add your mirror here if you use one. |
| manager.allowInsecureRegistries | bool | `false` | `--allow-insecure-registries`: permit `http://` model registries (dev only). |
| manager.allowImageOverride | bool | `false` | `--allow-image-override`: permit `spec.image` to override the engine default image (lets DM editors run arbitrary images; keep false in multi-tenant). |
| manager.maxConcurrentReconciles | int | `4` | `--max-concurrent-reconciles`: max DecisionModels reconciled concurrently. |
| manager.ollayaRegistry | string | `""` | `--ollaya-registry`: default registry base URL for host-less model names. Empty = the operator default (`OLLAYA_REGISTRY` env, else `https://ollaya.dev`); not rendered when empty. |
| watchNamespaces | list | `[]` | Namespaces to watch (`--watch-namespaces`). Empty = cluster-wide (manager ClusterRole/ClusterRoleBinding). When non-empty, the operator watches only these namespaces and the chart renders a namespaced Role+RoleBinding in each instead of the ClusterRole. The CRD stays cluster-scoped and must be installed by a cluster admin. |
| proxy.httpProxy | string | `""` | `HTTP_PROXY` for the manager (and, via the operator, the prefetch Job). Empty = not set. |
| proxy.httpsProxy | string | `""` | `HTTPS_PROXY` for the manager (and the prefetch Job). Empty = not set. |
| proxy.noProxy | string | `""` | `NO_PROXY` for the manager (and the prefetch Job). Set the service CIDR and `.svc,.cluster.local` so the operator's registry/API-server calls go direct, not via the proxy. Since v0.2.0-rc.1 the pod CIDR is NOT needed: the readiness prober and evaluator reach serving Pod IPs with a separate proxy-less client (the API-key header never traverses the proxy), so readiness does not depend on this value. Example: `10.96.0.0/12,.svc,.cluster.local`. Empty = not set. |
| proxy.existingSecret | string | `""` | Name of an existing Secret to source the proxy URLs from (keys `httpProxy` / `httpsProxy` / `noProxy`, each optional) instead of the plaintext values above — use this when a proxy URL carries credentials. When set, the plaintext `proxy.httpProxy/httpsProxy/noProxy` values are ignored and the env is rendered with `valueFrom.secretKeyRef` (`optional: true` per key). This only keeps the credential out of the Helm values: a **credentialed** proxy URL is used by the operator for its own registry lookups but is NOT passed to the prefetch Jobs (a Job spec is readable by anyone with `get jobs` in that namespace), and the chart cannot change that. For the Job's egress use an IP-allowlisted or node-level proxy that needs no credentials. `NO_PROXY` is always passed. Empty = use the plaintext values. |
| installCRDs | bool | `true` | Install the CRD via Helm's `crds/` dir. Set false to manage the CRD out of band. NOTE: Helm never upgrades or deletes CRDs in `crds/` (see the CRD upgrade caveat in this README). |

