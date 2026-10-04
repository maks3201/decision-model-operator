# Spike 005 — Model as an OCI image volume (KEP-4639) instead of PVC + prefetch Job

Date: 2026-10-02.

Question: Kubernetes 1.36 made `volumes[].image` (OCI image as a read-only volume, KEP-4639) GA.
Could an Ollaya model store ship as an OCI image a serving Pod mounts read-only — digest pinning
native in the Pod spec, node-level cache/dedupe, no PVC, no prefetch Job, no RWO/Multi-Attach
(so the per-revision PVCs and `CacheNotShareable` would disappear)? Decided with facts on a
local kind 1.36 cluster. Facts only; anything not run here is marked **unverified**. No operator
code changed.

## Environment

- Host: macOS arm64, Docker via OrbStack.
- kind v0.33.0; cluster node image **`kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`**
  (the digest kind v0.33.0 embeds for 1.36.1). Server `v1.36.1`, **containerd v2.3.1**,
  runtime `containerd://2.3.1`.
- Ollaya CPU image `ghcr.io/ollaya-dev/ollaya:0.7.3` (spike 001), model `laya:en`
  (digest `c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d`).
- All temp pods, the kind cluster, the two store images and the `/tmp` working dir were removed
  at the end. (No local registry was needed — `kind load docker-image` placed the store image on
  the node and `volumes[].image … pullPolicy: Never` consumed it; see §2.)

## 1. Feature availability on kind 1.36

GA in 1.36 → **no feature gate** needed. Confirmed by mounting an image as a volume and reading
it (below). containerd 2.3.1 on the node supports image volumes (the feature needs containerd
≥ 2.1.0 for alpha, with beta/GA support in later 2.x; 2.3.1 is well past that). Version history
(public docs): **alpha 1.31, beta 1.33** (added `subPath`/`subPathExpr`, gate off by default),
**GA 1.36** (April 2026, gate removed). CRI-O has supported it since 1.31.

**EKS 1.36 / Bottlerocket — unverified.** EKS tracks upstream GA features, and Bottlerocket uses
containerd, so image volumes are *expected* to work on EKS 1.36 nodes with a recent containerd,
but this was not run on EKS/Bottlerocket here. A real-GPU + EKS check belongs with the e2e/EKS
lane before relying on it.

## 2. Building the store as an OCI image

Pulled `laya:en` into a host dir with the Ollaya CPU image (spike-001 one-shot:
`ollaya pull laya:en` with `OLLAYA_MODELS=/store/models`), giving an on-disk store of
`blobs/` + `manifests/` (~813 MiB). Packaged it **`FROM scratch`**:

```dockerfile
FROM scratch
COPY models/ /          # store root (blobs/, manifests/) at the IMAGE root — see the gotcha
```

- Image size **853 MB** vs store **813 MiB** (negligible overhead; one layer, arch arm64).
- Loaded onto the node with `kind load docker-image dmo-laya-store:en2 --name dmo-imgvol`.
- `oras push` of an OCI *artifact* was **not** needed and is a dead end for this use case: the
  kubelet image-volume puller unpacks an **OCI image's** layers into a rootfs. A plain artifact
  with arbitrary media-type layers is not mounted as a filesystem of files the same way (the
  image-volume path expects image layers). A `FROM scratch` image is the simplest correct form.

**Gotcha that cost one iteration (document it):** an image volume mounts the image's **root** at
`mountPath`. First build used `COPY models /models`, so the image had `/models/{blobs,manifests}`
and the Pod saw the store one level deep at `/models/models`; `OLLAYA_MODELS=/models` then found
no `blobs/manifests` and Ollaya tried to create them → write to the RO volume → `Error: Read-only
file system (os error 30)` and the container exited. Fix: put `blobs/`+`manifests/` at the image
root (`COPY models/ /`) and point `OLLAYA_MODELS` at the mount path. **The store layout in the
image must match what `OLLAYA_MODELS` expects.**

## 3. Serving Pod from an image volume

```yaml
securityContext: {runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000}
containers:
- name: ollaya
  image: ghcr.io/ollaya-dev/ollaya:0.7.3
  env: [{name: OLLAYA_MODELS, value: /models}, {name: OLLAYA_HOST, value: "0.0.0.0:11435"}]
  securityContext: {readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
  volumeMounts:
  - {name: store, mountPath: /models, readOnly: true}
  - {name: ollaya-state, mountPath: /home/ollaya/.ollaya}   # writable (emptyDir)
volumes:
- name: store
  image: {reference: docker.io/library/dmo-laya-store:en2, pullPolicy: Never}
- name: ollaya-state
  emptyDir: {}
```

Result: **Pod Running, `ollaya serve` loads the model from the RO image volume.** With UID 1000,
`readOnlyRootFilesystem: true`, store RO, and a writable `emptyDir` at `/home/ollaya/.ollaya`,
serving works (same minimal writable set as spike 001; the pid-write warning is harmless).

Mount facts (from a debug Pod):
- `/models` is an **overlay mounted `ro`** (`overlay … (ro,relatime,lowerdir=…containerd…snapshots…)`).
- Files are **root-owned, mode 0644, world-readable**, so a UID 1000 process reads them fine; a
  write to `/models` fails `Read-only file system` (as required — a serving Pod cannot mutate the
  cache, replacing the RO-PVC guarantee from spike 001).
- `/home/ollaya/.ollaya` emptyDir with `fsGroup: 1000` is writable.

Digest check (spike-001 method) — all three agree:

```
registry  GET https://ollaya.dev/v2/library/laya/manifests/en  (3089 B)
          sha256(bytes) = c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d
/api/tags digest        = c305a9276531…e9d
/api/ps   digest        = c305a9276531…e9d   device=cpu  size_vram=0  expires_at=null (pinned)
```

So the **model-ready gate still works unchanged** against an image-volume store: `/api/ps` reports
the same digest + device. Nothing about the gate depends on how the store got onto the node.

## 4. Measurements

- **Store image size** 853 MB vs **on-disk store** 813 MiB — ~5% packaging overhead.
- **First Pod** (image already `kind load`ed onto the node): container started immediately; model
  cold-load into memory ~4 s (`/api/decide {keep_alive:-1}` → `done_reason: load`,
  `load_duration ≈ 4.0 s`). The image-volume mount itself is not the bottleneck — it is a local
  overlay mount.
- **Second Pod on the same node:** reached `Running` in **2 s**; events show
  `Container image "…dmo-laya-store:en2" already present on machine` — **node-level cache + dedupe,
  no re-pull.** This is the KServe-LocalModelCache-style win for free.
- **Not measured (unverified):** cold pull of the store image from a remote registry to a fresh
  node (network-bound, same as any image pull), and eviction/GC under node disk pressure.

## 5. subPath, ownership, downsides

- **`subPath` works** on an image volume (beta+ feature): `volumeMounts[].subPath: blobs` mounted
  the `blobs/` subdir RO and listed it. Non-existent subdirs cannot be mounted (expected).
- **Ownership:** image-volume files are **root-owned and read-only**; `fsGroup` does not change
  the image volume (it is not a writable volume), but world-readable 0644 means a UID 1000 reader
  is fine. A Pod that needs to *write* (prefetch/pull) still needs a separate writable volume.
- **The Ollaya registry is NOT an OCI image registry.** Its manifests use the Docker v2 envelope
  but `config = application/vnd.ollaya.config.v1+json` and layers
  `vnd.ollaya.graph.onnx / weights / tokenizer / decision / calibration / license / arch`. The
  kubelet image-volume puller cannot mount those as a filesystem. **Someone must repackage** the
  store into a real OCI image (`FROM scratch` + the pulled store). So the prefetch Job does not
  disappear — it moves "left" into an **image-build/mirror pipeline** (CI builds `laya-en:<digest>`
  images, or an operator-run build Job pushes to the user's registry).
- **Registry auth:** a private store image needs `imagePullSecrets` on the Pod/ServiceAccount
  (image volumes honor the Pod's pull secrets); the operator would thread an image-pull secret the
  way it threads the model today. (Not exercised here — local images only.)
- **Air-gapped:** fine *if* the store image is mirrored into the internal registry — but that is a
  new build+mirror step the user must run, replacing today's single `--ollaya-registry` pull.
- **Node disk / GC:** store images live in the node image store and are subject to kubelet image
  GC under disk pressure (could evict a store image a Pod will need); PVCs are not. **Unverified**
  behaviour under pressure.
- **Minimum versions:** GA needs **k8s ≥ 1.36** and a runtime with image-volume support
  (containerd ≥ 2.1, CRI-O ≥ 1.31). Clusters below 1.36 (or 1.33–1.35 without the gate) cannot use
  it — a hard floor far above our current support.
- **Digest pinning in the Pod:** `image.reference` can be `repo@sha256:…`, so the Pod spec itself
  pins the store by digest (native, no status round-trip). KEP-5365 further hardens image-digest
  semantics. This is the single strongest upside.

## 6. Conclusion

### Today (PVC + prefetch Job) vs image volume

| Aspect | PVC + prefetch Job | OCI image volume (KEP-4639) |
|---|---|---|
| Digest pinning | resolved → recorded in status; Job verifies | native in Pod spec (`@sha256:`) + still visible in `/api/ps` |
| Prefetch | prefetch Job pulls into PVC | **moves to an image build/mirror pipeline** (someone must repackage) |
| Node cache / dedupe | none (per-revision PVC) | **yes, node-level, automatic** (2 s second Pod) |
| RWO / Multi-Attach / `CacheNotShareable` | real problem → per-revision PVCs | **gone** (RO overlay, any node, any replica count) |
| RO serving guarantee | RO PVC mount | RO overlay (writes fail) — same guarantee |
| Store source | Ollaya registry pull | **must repackage** (Ollaya registry is not an OCI image registry) |
| Air-gapped | mirror via `--ollaya-registry` | mirror the store **image** (new build step) |
| Min versions | any supported k8s | **k8s ≥ 1.36 + containerd ≥ 2.1 / CRI-O ≥ 1.31** |
| Node disk pressure | PVC survives | store image can be GC'd (unverified) |
| gate (`/api/ps`) | unchanged | unchanged |

### Recommendation

**Feasible and attractive as an opt-in `cache.source: image` mode for v0.3+, not a replacement.**
It genuinely removes the RWO/Multi-Attach pain and gives node-level cache/dedupe with
Pod-spec-native digest pinning, and the readiness gate is unaffected. But it is **not free**: the
Ollaya registry is not an OCI image registry, so the model must be repackaged into a `FROM scratch`
image (the prefetch work moves into a build/mirror pipeline, with pull secrets and node-GC
considerations), and it hard-requires k8s ≥ 1.36. Keep PVC + prefetch Job as the default
(works on every supported cluster); add an image-volume store as an alternative `cache` backend
behind a clear version precondition. It can also serve as the planned node-local cache.

### Open questions

1. Who builds the store image — the operator (a build Job that pulls from the Ollaya registry and
   pushes `FROM scratch` to the user's registry) or the user/CI? This is the main cost and shapes
   the API (`cache.source: {image: {reference, pullSecretRef}}` vs `pvc`).
2. EKS 1.36 / Bottlerocket + a real GPU: confirm image volumes mount and that `/api/ps` reports
   `cuda` from an image-volume store (both **unverified** here).
3. Node image GC under disk pressure evicting a store image a running/queued Pod needs — risk vs
   PVC durability. **Unverified.**
4. Interaction with blue-green: two revisions = two store images on the node (cheap, deduped by
   shared blobs?) vs today's two PVCs — likely strictly better, but blob-level dedupe across two
   `FROM scratch` images is **unverified**.
5. Minimum-version policy: are we willing to gate an image-volume cache on k8s ≥ 1.36?

### Caveats / unverified (collected)

- EKS/Bottlerocket, and any GPU/`device:cuda` image-volume path — unverified (local arm64 kind, CPU).
- Remote cold-pull time, cross-image blob dedupe, node image-GC eviction — unverified.
- `imagePullSecrets` for a private store image — not exercised (local images, `pullPolicy: Never`).
- Only `laya:en` (onnx) and the Ollaya CPU runtime exercised.
