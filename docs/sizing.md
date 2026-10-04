# Model sizing

Measured numbers for picking `spec.resources` and `spec.cache.size`. From
spikes [001](spikes/001-ollaya-container.md) and
[002](spikes/002-model-families.md).

All figures are **CPU / F32**, measured under OrbStack on Apple Silicon. GPU
(`device: cuda`) and F16 are **unverified** — no GPU on the test hosts. The
Ollaya runtime image used is `ghcr.io/ollaya-dev/ollaya:0.7.3`.

## Measured per model

Memory is the container's cgroup **`anon`** (anonymous RSS) after the model is
loaded and pinned (`keep_alive:-1`), read from `/sys/fs/cgroup/memory.stat`
inside the container — not `docker stats`, which also counts the page cache left
by the pull and over-reports by ~0.8–1.7 GiB. Re-measured **2026-10-02**
(ghcr.io/ollaya-dev/ollaya:0.7.3, OrbStack arm64, F32 / cpu confirmed via
`/api/ps`).

| Model               | Family   | Download | Memory (`anon`, loaded) |
|---------------------|----------|----------|-------------------------|
| `laya:en`           | laya     | ~853 MB  | ~3.09 GiB (3167 MiB)    |
| `laya:multilingual` | laya     | ~683 MB  | ~1.85 GiB (1894 MiB)    |
| `gliclass:latest`   | gliclass | ~1.77 GB | ~2.42 GiB (2474 MiB)    |
| `nli:latest`        | nli      | ~884 MB  | ~3.62 GiB (3711 MiB)    |

Reproduce (host with Docker; the image has no curl, so port-map and call from the
host):

```sh
docker run -d --name m -p 127.0.0.1:11435:11435 ghcr.io/ollaya-dev/ollaya:0.7.3 serve
docker exec m ollaya pull laya:en
curl -sS -X POST localhost:11435/api/decide -d '{"model":"laya:en","keep_alive":-1}'
curl -sS localhost:11435/api/ps                       # confirm device cpu, precision F32
docker exec m awk '/^anon /{print $2}' /sys/fs/cgroup/memory.stat   # bytes
docker rm -f m
```

Notes:
- `anon` does **not** track download size: `nli` downloads less than `gliclass`
  but loads a larger graph, so measure per model rather than inferring from the
  download.
- `docker stats` MemUsage and cgroup `memory.peak`/`memory.current` include the
  page cache from writing the store during the pull (hundreds of MB to >1 GiB);
  size requests from `anon`, not those.

## Recommended `resources`

Rule of thumb for a CPU / F32 model: memory request ≈ measured `anon`, memory
limit ≈ `anon` × 1.4 (load transient plus request working set), `cpu` request 1 /
limit 2 for low-latency single-request serving. Size the PVC to the download plus
a little headroom.

### Built-in defaults

You do not have to copy these by hand for the measured models. When
`spec.resources` leaves a memory or cpu **request** unset and does not constrain
that key with a limit either, the Ollaya engine fills the request from a built-in
table. The table is keyed by the **exact** model `name:tag` on the default Ollaya
registry and `library` namespace — only the tags actually measured get a default:

| model (CPU / F32) | cpu request | memory request |
|-------------------|-------------|----------------|
| `laya:en`           | 1 | 3584Mi (3.5Gi) |
| `laya:multilingual` | 1 | 2048Mi (2Gi)   |
| `gliclass:latest`   | 1 | 2560Mi (2.5Gi) |
| `nli:latest`        | 1 | 4096Mi (4Gi)   |

The memory request is the measured `anon` rounded **up** to the next 512Mi. The
table lives in `internal/engine/ollaya/resources.go` with a source comment per
row. Rules:

- Anything you set in `spec.resources` wins, per key. If you set a key in
  **either** requests **or** limits, the engine does not default that key (so a
  limit-only spec never ends up with a request above its limit).
- The operator never sets a cpu limit and never sets a memory limit — set your
  own limit from the rule of thumb above.
- An **unmeasured tag** (e.g. `laya:latest`, which is a different, heavier
  artifact than `laya:en`), a **host-qualified** name (`mirror.corp/library/...`),
  a **non-`library` namespace** (`acme/laya:en`), or any unknown model gets **no**
  memory/cpu default — set `spec.resources` yourself.
- On `device: cuda` only a modest cpu request is defaulted and memory is left
  unset (host RAM on CUDA is unmeasured); the GPU limit `nvidia.com/gpu: 1` is
  still added automatically.
- Defaults apply only to a **new** revision. A running stable keeps the resources
  it was created with — upgrading the operator does not re-request it (see
  [quickstart.md](quickstart.md#what-changes-trigger-a-new-revision)).

```yaml
# laya:en  (anon ~3.09 GiB)
resources:
  requests: { cpu: "1", memory: "3584Mi" }
  limits:   { cpu: "2", memory: "4500Mi" }
cache:
  size: 2Gi

# laya:multilingual  (anon ~1.85 GiB)
resources:
  requests: { cpu: "1", memory: "2Gi" }
  limits:   { cpu: "2", memory: "3Gi" }
cache:
  size: 2Gi

# gliclass:latest  (download ~1.77 GB, anon ~2.42 GiB)
resources:
  requests: { cpu: "1", memory: "2560Mi" }
  limits:   { cpu: "2", memory: "3584Mi" }
cache:
  size: 4Gi

# nli:latest  (anon ~3.62 GiB)
resources:
  requests: { cpu: "1", memory: "4Gi" }
  limits:   { cpu: "2", memory: "5632Mi" }
cache:
  size: 2Gi
```

## Disk during rollout

A rollout keeps the stable and candidate revisions in separate sub-paths of the
model-store PVC, so plan for roughly **2× the model size** on disk while a new
revision is rolling out. The prefetch Job prunes old revision sub-paths once the
new one is promoted.

## Rollout timeouts for large models

The default phase timeouts (Caching 30m, Starting 10m, Evaluating 10m) assume
small models. A large model (tens of GB) can exceed them on a cold node — the
pull alone, or loading the weights into memory, may take longer than 10–30m.
Raise them under `spec.rollout.timeouts`; see
[evaluation.md](evaluation.md#rollout-timeouts) for the fields, bounds (1m–24h),
and the caveat that an in-flight prefetch Job keeps its original deadline. Rough
estimates for a ~19 GB model: `caching: 2h`, `starting: 30m`, `evaluating: 30m`
(tune to your registry bandwidth and node disk/CPU).

## New models

For a model not in the table: pull it once (or read the manifest layer sizes),
warm it up (`/api/decide` with `keep_alive:-1`), and read the container's cgroup
`anon` from `/sys/fs/cgroup/memory.stat` (see the reproduce block above) — not
`docker stats`, which includes the pull's page cache. Then apply the rule of
thumb above. Never rely on the implicit `:latest` tag — pin an explicit
`model:tag`.

## GPU and rollout operations

Findings from an EKS GPU test cluster (`g4dn.xlarge`, NVIDIA T4,
Bottlerocket). These are capacity and storage notes for running on GPU, not new
per-model sizes.

- **Blue-green needs stable + candidate at once.** A rollout runs the old and new
  revisions simultaneously (traffic only switches after the candidate is ready
  and, if configured, passes evaluation). On GPU models that means **2× GPU
  capacity during a rollout** — a second GPU for the candidate. The laya / nli
  class models here are small (hundreds of MB to a few GB), so a single GPU with
  time-slicing or MIG serves both revisions; the operator does not automate GPU
  partitioning. The larger 9B-class models Ollaya now ships (~18–19 GB, e.g.
  `nimble:9b`, `jeeves:9b`, `clef:flash`) each need a **24 GB GPU**, so a rollout
  needs **two** such GPUs — size GPU capacity for 2× the model's VRAM.
- **Two model-store PVCs exist during a rollout.** Stores are per revision:
  the candidate's prefetch Job writes a new store PVC while the stable
  PVC is still mounted. Size the StorageClass quota for **≈ 2× the model size**
  while a rollout is in flight.
- **Use a StorageClass with `reclaimPolicy: Delete` for stores.** A revision's
  store PVC is garbage-collected with its Deployment/Job, but with
  `reclaimPolicy: Retain` the backing EBS volume (and its cost) is kept after the
  PVC is deleted. Use `Delete` for store StorageClasses unless you deliberately
  want to retain old revisions' volumes.
- **SELinux relabel cost on multi-GB stores.** On SELinux-enforcing nodes whose
  CSI driver lacks SELinux mount support, the runtime relabels the whole volume
  to the mounting Pod's MCS level on every mount — slow for multi-GB GGUF stores.
  The operator sets **one stable MCS level per DecisionModel** on the prefetch and
  serving Pods so both can *access* the same store (without a shared level the
  second Pod's mount is relabeled to a different MCS level and the other Pod then
  gets `EACCES`). This does **not** remove the per-mount relabel walk itself; it
  only keeps prefetch and serving Pods on a consistent level so they can share one
  store.
- **GPU nodes are usually tainted `nvidia.com/gpu`.** For `device: cuda` the
  operator adds the matching toleration automatically, so GPU workloads
  schedule onto tainted GPU nodes without a manual `spec.scheduling` toleration.
- **Measured on T4:** `laya:en` serves the 5-question triage preset in ~50 ms per
  eval (warm), using ~1 GiB host RSS plus the model resident in VRAM. The per-GPU
  memory footprint is well within a T4's 16 GiB, which is why time-slicing works.

### Private GHCR image (`imagePullSecrets`)

The runtime image (`ghcr.io/ollaya-dev/ollaya`) and, during install, the manager
image may live in a private GHCR package. Create a pull Secret from a token with
the `read:packages` scope and pass it to the chart:

```sh
kubectl create secret docker-registry ghcr-pull \
  --namespace decision-model-operator-system \
  --docker-server=ghcr.io \
  --docker-username=<github-user> \
  --docker-password=<token-with-read:packages>

helm install dmo oci://ghcr.io/maks3201/charts/decision-model-operator --version <version> \
  --namespace decision-model-operator-system --create-namespace \
  --set 'imagePullSecrets[0].name=ghcr-pull'
```

The chart's `imagePullSecrets` value is attached to the manager Pod. The runtime
(serving / prefetch) Pods run in the DecisionModel's own namespace; put an
equivalent pull Secret there (and reference it on the workload's ServiceAccount
or via a namespace default) when the runtime image is private.
