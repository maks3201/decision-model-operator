# GPU CI (manual e2e on a self-hosted GPU runner)

The GPU end-to-end run (`.github/workflows/test-e2e-gpu.yml`) is
`workflow_dispatch`-only and targets a self-hosted runner labelled
`gpu`. It exercises the same e2e suite as the CPU job but with
`E2E_DEVICE=cuda`, so it needs a real NVIDIA GPU. No GPU provider is chosen yet;
this doc is everything needed to wire one up.

## Why it is manual and self-hosted

GitHub-hosted runners have no GPU. The run therefore needs a self-hosted runner
with an NVIDIA GPU. Self-hosted runners on a repository are a security-sensitive
surface, so the workflow is `workflow_dispatch` only — never `push`/`pull_request`.

## 1. Prepare the GPU host

Ubuntu 22.04+ with a recent NVIDIA driver and Docker. Install the NVIDIA
Container Toolkit and make the NVIDIA runtime the Docker default (per the NVIDIA
Container Toolkit install guide and the community kind+GPU guides
[[1]](#refs)[[2]](#refs)):

```sh
# NVIDIA Container Toolkit installed per:
# https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
sudo nvidia-ctk runtime configure --runtime=docker --set-as-default
sudo systemctl restart docker

# kind mounts GPUs as volumes, so the runtime must accept visible devices as
# volume mounts (community requirement for kind + GPUs [1][2]):
sudo sed -i \
  's/^#\s*accept-nvidia-visible-devices-as-volume-mounts.*/accept-nvidia-visible-devices-as-volume-mounts = true/' \
  /etc/nvidia-container-runtime/config.toml
sudo systemctl restart docker

nvidia-smi                                   # driver + GPU visible
docker info --format '{{.DefaultRuntime}}'   # should print: nvidia
```

## 2. kind with GPUs

kind has **no native GPU support**; there is no standard way to inject GPUs into
a kind node [[3]](#refs). Two established options:

- **Manual (what the workflow uses):** with the NVIDIA runtime as the Docker
  default and `accept-nvidia-visible-devices-as-volume-mounts = true`, a kind
  node can run CUDA containers; then install the NVIDIA
  [`k8s-device-plugin`](https://github.com/NVIDIA/k8s-device-plugin) DaemonSet so
  the node advertises `nvidia.com/gpu`. The workflow pins
  `k8s-device-plugin` `v0.17.0` and labels the node. This mirrors the
  substratus.ai / mproffitt kind-with-GPUs guides [[1]](#refs)[[2]](#refs).
- **`nvkind` (recommended tool):** `klueska/kind-with-gpus-examples` wraps the
  above hacks and adds per-node GPU isolation [[3]](#refs). If you prefer it,
  replace the "Create a GPU-enabled kind cluster" + "Install the NVIDIA device
  plugin" steps with `nvkind cluster create` and its device-plugin install. It is
  the cleaner path when a runner has multiple GPUs.

The operator selects the CUDA runtime image (`ghcr.io/ollaya-dev/ollaya:0.18.0-cuda`)
automatically from `device: cuda` (see the engine `imageFor`), so the workflow
only needs to **load that image into kind** and set `E2E_DEVICE=cuda`. `0.18.0-cuda` is not verified on a GPU yet.

> **Pascal / Volta GPUs (compute capability < 7.5):** the default `:0.18.0-cuda`
> image targets newer CUDA runtimes. On older cards (e.g. GTX 10-series Pascal, or
> some Volta setups) use the `-cuda12` image instead. The operator does **not** pick
> it automatically (there is no device-level GPU-architecture detection), so set it
> yourself:
>
> - run the operator with `--allow-image-override` (without it `spec.image` is refused
>   with `ImageOverrideNotAllowed`);
> - set `spec.image` to the digest-pinned reference
>   `ghcr.io/ollaya-dev/ollaya:0.18.0-cuda12@sha256:da484c2c85f86771d27463253d914d4513651d44be976f059f6a0369a350208c`.
>   A `spec.image` containing `@sha256:` is admitted without `--allow-unpinned-runtime-images`;
>   an unpinned tag is refused (`UnpinnedRuntimeImage`) unless that flag is also set.
> - a user-set `spec.image` has no `runtimeVersion`, so its version is unknown and the
>   prefetch Job pulls the tag, not the digest. A moved tag is then reported as
>   `UpstreamTagMoved` instead of being restored by digest.
>
> The `-cuda12` image is not verified on a GPU yet.

## 3. Register the runner (private repo)

1. Repo → Settings → Actions → Runners → New self-hosted runner (Linux x64).
2. Follow the shown `./config.sh` / `./run.sh` steps on the GPU host.
3. Add the labels the workflow selects: `gpu` (plus the default
   `self-hosted, linux, x64`). The workflow uses
   `runs-on: [self-hosted, linux, x64, gpu]`.

### Security notes (self-hosted on a private repo)

- **Never enable it for fork/`pull_request` runs.** The workflow is
  `workflow_dispatch` only; keep it that way. A self-hosted runner will happily
  run whatever code a triggering ref contains, so untrusted refs must not reach
  it. (GitHub explicitly warns against self-hosted runners on public repos for
  this reason; on a private repo, still restrict who can dispatch.)
- **Prefer an ephemeral runner:** register with `--ephemeral` so the runner
  process exits after one job and the host is re-provisioned, preventing state
  bleed between runs.
- **Least privilege:** run the runner as a non-root user in a throwaway VM;
  don't mount host secrets. The workflow needs only `contents: read` and no
  repo secrets.
- **Pin actions and images** (already done: actions at major/SHA-safe versions,
  `k8s-device-plugin` and the Ollaya image pinned by tag).

## 4. Cost-bounded cloud option (spot T4, started per run)

To avoid paying for an always-on GPU box, start a spot GPU VM only for the run:

1. A small always-on controller (or a scheduled workflow) launches a spot
   instance with a single T4 (e.g. AWS `g4dn.xlarge` spot, GCP `n1` + `nvidia-tesla-t4`
   spot/preemptible) from an image that already has the driver + toolkit + an
   **ephemeral** Actions runner baked in and auto-registering with label `gpu`.
2. Dispatch `test-e2e-gpu`; the ephemeral runner picks it up.
3. The instance self-terminates after the job (ephemeral runner exit → shutdown
   hook), so you pay only for the ~15–30 min run.

This keeps GPU cost to a few cents per run and leaves no long-lived GPU runner
attached to the repo. No secrets are stored in the repo; the launcher holds the
cloud credentials.

## <a name="refs"></a>References

1. mproffitt, "kind + CAPI vclusters + GPU" (kind + NVIDIA container toolkit setup),
   https://gist.github.com/mproffitt/a828c074b09bbf65dae184790baacb41
2. substratus.ai, "Using GPUs with kind", https://www.substratus.ai/blog/kind-with-gpus/
3. klueska, `kind-with-gpus-examples` / `nvkind`,
   https://github.com/klueska/kind-with-gpus-examples
4. NVIDIA `k8s-device-plugin`, https://github.com/NVIDIA/k8s-device-plugin

Content from external sources was rephrased for compliance with licensing restrictions.
