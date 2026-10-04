# Spike 001 — Ollaya container facts for PodSpec / prefetch Job + kind setup

## Environment

- Host: macOS (arm64 / Apple Silicon), Docker via OrbStack (`docker info` OK, `orb` present).
- Image: `ghcr.io/ollaya-dev/ollaya` — **the tag is `0.7.3` (no `v` prefix); `v0.7.3` does not exist.**
  The registry tag is `0.7.3` (no `v` prefix). Used `ghcr.io/ollaya-dev/ollaya:0.7.3`
  throughout. `0.7.3` and `latest` share the same digest today
  (`sha256:3e3ad48f93baa7d98d43a9655646b6264bd8322493393a6eb7a899b5d231b4a9`).
- Fresh named volume `dmo-spike-c` used for the store (NOT `ollaya-models`). All temp
  containers/volumes removed at the end.

Image config (`docker image inspect`):

```
arch=arm64 os=linux
entrypoint=["/usr/bin/ollaya"]  cmd=["serve"]
user=1000:1000  workingdir=/home/ollaya  exposed=11435/tcp
env=[HOME=/home/ollaya, OLLAYA_HOST=0.0.0.0:11435,
     OLLAYA_MODELS=/home/ollaya/.ollaya/models]
volumes={"/home/ollaya/.ollaya":{}}
```

So the container runs `ollaya serve` as UID/GID 1000, listens on `0.0.0.0:11435`,
store defaults to `$OLLAYA_MODELS=/home/ollaya/.ollaya/models`.

---

## 1. Image contents (shell? curl/wget? CLI help)

```console
$ docker run --rm --entrypoint /bin/sh IMG -c 'ls -la /bin/sh'
lrwxrwxrwx ... /bin/sh -> dash
$ ... 'for b in sh bash curl wget cat ls chmod id; do command -v $b || echo MISSING; done'
/usr/bin/sh   /usr/bin/bash   curl: MISSING   wget: MISSING
/usr/bin/cat  /usr/bin/ls     /usr/bin/chmod  /usr/bin/id
```

- `/bin/sh` (dash) **and** `/usr/bin/bash` are present, plus coreutils (`cat`, `ls`,
  `chmod`, `id`).
- **No `curl`, no `wget`.**

`ollaya --help` (top-level commands): `serve, run, pull, list (ls), ps, show, rm, cp,
stop, mcp, create, help`. Version: `0.7.3` (`ollaya --version` → "client version is 0.7.3").

`ollaya pull --help`:

```
Usage: ollaya pull [OPTIONS] <MODEL>
Arguments: <MODEL>
Options: --insecure  (Ollama compat; use http:// host in the name for dev registries)
```

**Every `ollaya` invocation prints a harmless stderr line first:**
`onnxruntime cpuid_info warning: Unknown CPU vendor. cpuinfo_vendor value: 0`
(artifact of running the amd64-tuned onnxruntime under arm64 emulation; ignore it).

**Verdict for the prefetch Job:** a shell script *is* possible (`/bin/sh` exists), but there
is no HTTP client, so any registry interaction must go through the `ollaya` CLI. The Job is
cleanest as **pure `ollaya` CLI** (`ollaya pull ...` then `ollaya list`), optionally wrapped
in `sh -c` for digest verification. No need to install curl.

---

## 2. `ollaya pull laya:en` one-shot (no daemon running)

```console
$ docker run --rm -v dmo-spike-c:/home/ollaya/.ollaya \
    --entrypoint /usr/bin/ollaya IMG pull laya:en   # ~48s first time
EXIT=0
```

- **Auto-starts an embedded daemon**: after the pull the volume contains
  `.ollaya/logs/server.log` and `.ollaya/server.11435.pid`, and `server.log` shows two
  "Ollaya is running ... version=0.7.3" lines (one per CLI call). So `pull`/`list`/`show`
  each spin up an in-process server against the store.
- **Exit 0 on success.** The container process exits when the CLI command returns; it does
  **not** leave a daemon running in the background (the pid file is just a record — the
  process is gone once the one-shot container exits).
- stdout is quiet on success (only the onnxruntime warning). No progress bar in
  non-TTY / piped mode.

Failure modes (see also Q6):

```console
# bad model name (fresh volume)
$ ... pull nonexistent-xyz:doesnotexist
EXIT=1   Error: model "nonexistent-xyz:doesnotexist" not found in registry ollaya.dev

# no network (--network none, fresh volume)
$ docker run --rm --network none ... pull kev:en
EXIT=1   Error: error sending request for url (https://ollaya.dev/v2/library/kev/manifests/en)
```

**Verdict:** `ollaya pull` is a self-contained one-shot suitable for a Job container
(`command: ["ollaya","pull","laya:en"]`). Exit 0 = success, non-zero = failure with a clear
stderr message. Job `restartPolicy: OnFailure` + `backoffLimit` handles transient network
errors.

---

## 3. Idempotency (second pull of an already-present model)

```console
$ ... pull laya:en        # model already present
EXIT=0    real ~0.46s
```

Second pull returns **exit 0 in ~0.5s** with no re-download. Safe to re-run the prefetch Job;
it is effectively a no-op when the model is already in the store.

---

## 4. Pull by digest / verifying the pulled digest

**Pull by digest is NOT supported.** Every digest syntax tried is rejected before any network
call:

```console
$ ... pull "laya:en@sha256:c305a9276531...e9d"
EXIT=1  Error: model: invalid model name "..."; expected [host/][namespace/]model[:tag]
$ ... pull "laya@sha256:c305a9276531...e9d"
EXIT=1  (same error)
```

`ollaya pull` only accepts `[host/][namespace/]model[:tag]`. **The operator must pull by tag
and then verify the resulting digest**, not pull by digest.

How to verify the digest after a tag pull — three consistent sources, all agreeing on the
same value:

1. **Manifest file on disk** — stored at
   `/home/ollaya/.ollaya/models/manifests/ollaya.dev/library/laya/en`
   (path = `<registry>/library/<model>/<tag>`). Its bytes are the manifest; `sha256(bytes)`
   is the digest.

   ```console
   $ sha256sum .../manifests/ollaya.dev/library/laya/en
   c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d   (3089 bytes)
   ```

2. **`ollaya list`** — the `ID` column is the **first 12 hex chars** of that digest:

   ```
   NAME      ID             SIZE     MODIFIED
   laya:en   c305a9276531   853 MB   ...
   ```

3. **HTTP API `/api/tags`** (from a running `serve`) — returns the **full 64-char digest**
   as machine-readable JSON (best source for the operator):

   ```json
   {"models":[{"name":"laya:en","digest":
     "c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d",
     "details":{"format":"onnx","family":"laya","parameter_size":"421M", ...}}]}
   ```

**Cross-check against the registry** (matches the product spec: digest = sha256 of the raw
manifest bytes, bare hex):

```console
$ curl -s -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    https://ollaya.dev/v2/library/laya/manifests/en -o m.json   # HTTP 200, 3089 bytes
$ shasum -a 256 m.json
c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d
```

Same value from all four places. `ollaya show laya:en` prints human info (architecture,
parameters=421M, precision F16/F32, format onnx, languages, capabilities, license) but **no
digest** and has no `--json`; `list`/`show` have no JSON flag, so for machine parsing prefer
`/api/tags` (running server) or `sha256sum` of the manifest file (Job with just the store).

> Note: the registry did **not** return a `Docker-Content-Digest` header (only `ETag`), so
> compute the digest from the manifest bytes rather than trusting a header.

---

## 5. `readOnlyRootFilesystem`

Tested `docker run --read-only` (equivalent to `securityContext.readOnlyRootFilesystem:true`)
in several layouts:

| Layout | Result |
|---|---|
| `--read-only`, store as **RW** volume at `/home/ollaya/.ollaya` | serve OK, `run` loads model, `/api/ps` shows it. Writes land in the RW volume. |
| `--read-only`, store as **RO** volume at `/home/ollaya/.ollaya` | serve still starts (exit 0) but WARN: `could not write /home/ollaya/.ollaya/server.11435.pid: Read-only file system` → `ollaya stop` won't find the server (irrelevant under k8s SIGTERM). |
| `--read-only`, models RO at `/models` + `OLLAYA_MODELS=/models`, **writable emptyDir at `/home/ollaya/.ollaya`** + writable `/tmp` | serve OK, `list` sees models, `run` loads, `/api/ps` shows `device:cpu`. |
| Same but **without** the `/tmp` tmpfs (only `/home/ollaya/.ollaya` writable) | **still OK** — `/tmp` is not required. |

**Minimal writable set for read-only-rootfs serving: a single writable mount at
`/home/ollaya/.ollaya`** (holds `logs/server.log` and `server.<port>.pid`). `/tmp` was not
needed for serving `laya:en`.

Because the default store is `/home/ollaya/.ollaya/models`, mounting the model PVC read-only
*and* keeping `.ollaya` writable is easiest by **decoupling the store**: set
`OLLAYA_MODELS=/models`, mount the PVC read-only at `/models`, and give `/home/ollaya/.ollaya`
its own writable `emptyDir`. (Alternatively use a `subPath` mount for `models` under a
writable `.ollaya`, but the `OLLAYA_MODELS` split is cleaner.)

---

## 6. UID 1000 + root-owned store → `fsGroup`

Simulated a **root-owned** store directory (`chown 0:0`, mode 755) and pulled as UID 1000
with no fsGroup:

```console
$ # dir = 0:0 mode=755, container --user 1000:1000
$ ... pull laya:en
EXIT=1
Error: creating /home/ollaya/.ollaya/logs
Caused by: Permission denied (os error 13)
```

Then simulated `securityContext.fsGroup: 1000` (dir group = 1000, group-writable, setgid:
`chown 0:1000 && chmod 2775`) and pulled as UID/GID 1000:

```console
$ ... pull laya:en
EXIT=0
```

**Verdict:** running as UID 1000 against a root-owned volume **fails with EACCES** on the very
first write. **`securityContext.fsGroup: 1000`** (kubelet chgrps the volume to GID 1000 and
adds group-write) fixes it. Set `fsGroup: 1000` on both the **prefetch Job** and the
**serving Pod** whenever they write to the store (the Job always writes; the serving Pod
writes `.ollaya` logs/pid).

> Caveat: on OrbStack, `chown` on a bind/volume from inside a container is remapped, so I
> could not reproduce a *bind mount* that stayed root-owned. The result above uses a Docker
> **named volume** whose top dir I explicitly `chown`ed as root, which does persist — this is
> a faithful stand-in for a root-provisioned PVC. Recommend re-confirming once on a real kind
> PVC (e2e).
>
> **Re-confirmed on a real kind PVC:** kind's default `standard` StorageClass
> (local-path-provisioner) creates the PVC directory **world-writable** (`drwxrwxrwx`, mode
> 0777, root-owned), so a UID 1000 Pod can write **even without** `fsGroup` (verified: two
> throwaway Pods writing to fresh PVCs, one with and one without `fsGroup: 1000` — both
> `WRITE_OK`). So the `fsGroup` requirement is **provisioner-dependent**: on kind local-path
> it is not strictly needed, but the Docker spike's root-owned 0755 case (EACCES without
> fsGroup) is the conservative case for stricter provisioners. Keep `fsGroup: 1000` as the
> portable default.

---

## 7. `OLLAYA_DEVICE=cuda` on the CPU image (no GPU)

**Not a silent CPU fallback — it fails loudly at model *load* time**, not at server startup:

```console
$ docker run -d -e OLLAYA_DEVICE=cuda ... IMG            # server starts fine, exit 0
  server.log: "Ollaya is running ... version=0.7.3"      # no cuda error at startup
$ docker exec <c> ollaya run laya:en --keepalive 5m "test"
EXIT=1
Error: laya:en failed to load: onnxruntime cpuid_info warning ... |
  Error: model files: this build of ollaya has no CUDA support
$ curl -s /api/ps
{"models":[]}      # model never loaded → never appears in /api/ps
```

- The server **starts** (readiness on a TCP/HTTP `/api/tags` probe would pass), but the model
  **never loads**, so `/api/ps` stays `{"models":[]}` and any `run` returns exit 1.
- Control (`OLLAYA_DEVICE=cpu`): `run` succeeds and `/api/ps` shows the model with
  `"device":"cpu"`, `"size_vram":0`, `"context_length":512`, and the full 64-char `digest`.

This is exactly why the **model-aware readiness gate must check `/api/ps`** (digest **and**
`device`), not just that the server port is open: a cuda-misconfigured CPU pod would pass a
naive TCP probe but never actually serve.

**`:cuda` image on this Mac:** `ghcr.io/ollaya-dev/ollaya:0.7.3-cuda` is **amd64-only**
(`docker manifest inspect` → `architecture: amd64`; `docker pull` → *no matching manifest for
linux/arm64/v8*). Could not run it here (no arm64 variant, no GPU). **Unverified on this host.**

`/api/ps` device field, loaded on CPU (verbatim, trimmed):

```json
{"models":[{"name":"laya:en",
  "digest":"c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d",
  "details":{"format":"onnx","family":"laya","parameter_size":"421M",
             "quantization_level":"F32"},
  "size_vram":0,"context_length":512,"device":"cpu"}]}
```

---

## 8. Graceful shutdown (`docker stop`)

```console
$ docker run -d ... IMG   # serving
$ time docker stop <c>
real ~0.20s
$ docker inspect <c> --format '{{.State.ExitCode}}'
0
  server.log: "... INFO ollaya_server::http: shutting down"
```

`ollaya serve` handles **SIGTERM cleanly**: exits **~0.2s**, **exit code 0**, logs
`shutting down`. Well under Docker's / k8s' default 10s / 30s grace period. A modest
`terminationGracePeriodSeconds` (e.g. 30, the default) is more than enough.

---

## 9. Default tag resolution

- `ollaya pull laya` (no tag) starts a **large download** — it resolves to `latest`, which is
  a *different, bigger* artifact than `laya:en` (the pull was still going at 120s and was
  aborted; the manifests differ):

  ```console
  $ curl ... /v2/library/laya/manifests/latest → 1036 bytes, sha256 89aa53ce17e1...  (HTTP 200)
  $ curl ... /v2/library/laya/manifests/en     → 3089 bytes, sha256 c305a9276531...  (HTTP 200)
  # latest != en  (different manifests)
  ```

- Registry probes: `laya:latest` → **200**, `laya:en` → **200**, `laya:default` → 404,
  `laya:laya` → 404.
- **Tag discovery is not available publicly:** `GET https://ollaya.dev/v2/library/laya/tags/list`
  returns an HTML 404 page (the registry does not expose the catalog/tags endpoint without
  auth). Individual tags are only discoverable by probing `.../manifests/<tag>`.
- For reference, the **ghcr image** tags (the runtime, not models) are discoverable via a
  ghcr pull-token and include `0.7.3`, `0.7.3-cuda`, `0.7.3-cuda12`, `latest`, `cuda`, etc.
  (relevant only to Q pinning of the runtime image tag).

**Verdict:** never rely on the implicit default tag — it points at `latest` (a heavier model).
The operator must always specify an explicit `model:tag` (e.g. `laya:en`).

---

## Recommendations for PodSpec / prefetch Job

Concrete, evidence-backed. Model/tag are examples (`laya:en`, digest
`c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d`).

### Prefetch Job (populate the PVC before rollout)

```yaml
spec:
  backoffLimit: 4                 # retries transient network failures (Q2: exit 1 on net error)
  template:
    spec:
      restartPolicy: OnFailure
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000             # REQUIRED: root-owned PVC otherwise EACCES (Q6)
      containers:
        - name: prefetch
          image: ghcr.io/ollaya-dev/ollaya:0.7.3   # tag is 0.7.3, not v0.7.3
          # pure CLI: pull by tag (no digest-pull support, Q4), then verify digest.
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              ollaya pull "$MODEL"                       # exit 0 ok / non-0 fail (Q2)
              got="$(sha256sum "$OLLAYA_MODELS/manifests/ollaya.dev/library/${MODEL%%:*}/${MODEL##*:}" | cut -d' ' -f1)"
              echo "pulled digest: $got"
              [ "$got" = "$EXPECT_DIGEST" ] || { echo "digest mismatch: got $got want $EXPECT_DIGEST" >&2; exit 1; }
          env:
            - {name: MODEL,          value: "laya:en"}
            - {name: EXPECT_DIGEST,  value: "c305a9276531...e9d"}   # full 64 hex
            - {name: OLLAYA_MODELS,  value: "/models"}
          volumeMounts:
            - {name: models, mountPath: /models}      # RW here — the Job writes the store
      volumes:
        - name: models
          persistentVolumeClaim: {claimName: <model-pvc>}
```

Notes:
- No `curl`/`wget` in the image, but `/bin/sh` + `sha256sum` + `ollaya` are all present, so a
  tiny shell wrapper for pull-then-verify works (Q1). A pure-`ollaya` Job (no verification) is
  `command: ["ollaya","pull","laya:en"]`.
- Idempotent: re-running the Job is ~0.5s no-op when already present (Q3).

### Serving Pod

```yaml
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000                 # serving also writes .ollaya logs/pid (Q6)
  containers:
    - name: ollaya
      image: ghcr.io/ollaya-dev/ollaya:0.7.3
      # entrypoint already = ["/usr/bin/ollaya"], cmd = ["serve"]; no command needed.
      ports: [{containerPort: 11435, name: http}]
      env:
        - {name: OLLAYA_HOST,   value: "0.0.0.0:11435"}
        - {name: OLLAYA_MODELS, value: "/models"}       # decouple store from HOME (Q5)
        # For CPU pods do NOT set OLLAYA_DEVICE=cuda on the CPU image — model load fails (Q7).
      securityContext:
        readOnlyRootFilesystem: true                    # verified working (Q5)
        allowPrivilegeEscalation: false
        capabilities: {drop: ["ALL"]}
      volumeMounts:
        - {name: models,   mountPath: /models, readOnly: true}   # prefetched, RO
        - {name: ollaya-state, mountPath: /home/ollaya/.ollaya}  # writable emptyDir (logs/pid)
      startupProbe:                 # port is up in ~0.1s; model load is separate
        httpGet: {path: /api/tags, port: 11435}
        periodSeconds: 2
        failureThreshold: 30
      livenessProbe:
        httpGet: {path: /api/tags, port: 11435}
        periodSeconds: 10
      # readinessProbe: do NOT rely on /api/tags alone — see readiness gate below.
  volumes:
    - name: models
      persistentVolumeClaim: {claimName: <model-pvc>}
    - name: ollaya-state
      emptyDir: {}                  # only writable path needed (Q5: /tmp not required)
  terminationGracePeriodSeconds: 30 # SIGTERM handled in ~0.2s (Q8)
```

### Model-aware readiness (product value #3)

- A plain TCP/`/api/tags` probe passes even when the model failed to load on a
  cuda-misconfigured CPU pod (Q7). The readiness gate `decisionmodel.io/model-ready` must poll
  **`GET /api/ps`** and require a loaded model whose `digest` == expected **and** `device` ==
  expected (e.g. `cpu`).
- A loaded model only appears in `/api/ps` while kept alive — trigger a load and keep it warm
  with `ollaya run <model> --keepalive <dur>` (or an equivalent API load call), or the operator
  can issue one warm-up decision, before asserting readiness. Empty `/api/ps` = not ready.
- `/api/ps` gives everything the gate needs: `digest` (full 64 hex), `device`, `size_vram`,
  `context_length`.

### Digest facts (for status)

- `pull` accepts **tag only**, never `@sha256:` (Q4). Pin by tag in the CR; record the
  observed digest in `status`.
- Digest = `sha256(manifest bytes)`, bare hex; identical across the on-disk manifest file,
  `ollaya list` (first 12 chars), `/api/tags` (full), and the registry manifest bytes.
- Manifest path on disk: `<OLLAYA_MODELS>/manifests/ollaya.dev/library/<model>/<tag>`.

### Caveats / unverified

- `:cuda` image and any GPU/`device:cuda` path are **unverified** (amd64-only image, no GPU on
  this arm64 Mac).
- `fsGroup` behaviour reproduced with a Docker named volume, not a real kind PVC — re-confirm
  on a kind PVC during e2e.
- The onnxruntime `cpuid_info` stderr warning appears on every CLI call under arm64 emulation;
  it is noise, not an error. May differ on amd64 nodes.

---

## kind scripts

`hack/kind-up.sh` (idempotent): checks `docker info`, creates cluster `dmo` if absent,
`kind load docker-image ghcr.io/ollaya-dev/ollaya:0.7.3`, prints `kubectl cluster-info`.
`hack/kind-down.sh` (idempotent): deletes cluster `dmo` if present.

Both run with `set -euo pipefail`. **shellcheck is not installed on this host**, so I could
not run it; scripts were written to be shellcheck-clean (quoted vars, `grep -qx`, no unquoted
expansions).

Proof (run once, then torn down — cluster left DOWN):

```console
$ ./hack/kind-up.sh
creating kind cluster 'dmo'...
 ✓ Starting control-plane 🕹️
loading ghcr.io/ollaya-dev/ollaya:0.7.3 into kind cluster 'dmo'...
Kubernetes control plane is running at https://127.0.0.1:57165
$ ./hack/kind-up.sh            # idempotent re-run
kind cluster 'dmo' already exists; reusing it.
Image ... found to be already present on all nodes.
$ ./hack/kind-down.sh
Deleting cluster "dmo" ...
$ ./hack/kind-down.sh          # idempotent
kind cluster 'dmo' does not exist; nothing to do.
$ kind get clusters
No kind clusters found.
```
