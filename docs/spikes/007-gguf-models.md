# Spike 007 — GGUF (llama.cpp runner) models under the operator

Tested 2026-10-06 on macOS arm64 (OrbStack, 11.7 GiB Docker) with
`ghcr.io/ollaya-dev/ollaya:0.10.0` (CPU). Resolves ARCHITECTURE §13 open question 2:
do ONNX-based facts (readiness gate, digest, device) hold for GGUF models served by
Ollaya's llama.cpp runner?

## Model chosen

`jevk5:latest` — the **smallest GGUF model** in the default registry. Manifest layer sizes
probed via `GET https://ollaya.dev/v2/library/<model>/manifests/<tag>`:

| model | GGUF weights layer | note |
|---|---|---|
| **jevk5:latest** | **4.48 GB** | smallest; chosen |
| winnow:e4b | 8.01 GB | |
| jeb:latest | 9.79 GB | |
| cygnet:latest | 12.67 GB | |

No smaller GGUF tag exists (probed `winnow:e2b/e1b/1b/q4`, `jevk5:1b/2b/3b/mini/small/q4/e2b`,
`jeb:mini/1b/small/q4/e2b`, `cygnet:small` — all 404; the registry does not expose a tag list).
All GGUF manifests carry the same layer set: `application/vnd.ollaya.weights.gguf` plus tiny
`.decision`, `.calibration`, `.license` layers. jevk5:latest at 4.48 GB fits a 16 GB CI runner
(ubuntu-latest) with room for the operator and control plane.

## Facts (jevk5:latest, Q8_0, CPU, 0.10.0)

| Fact | Result |
|---|---|
| Pull into RW store | exit 0, ~374 s (4.48 GB), 4.2 GB on disk |
| Manifest digest on disk == registry sha256 | yes — `58b5d6516410070d5f165d77357ed7bfefe5ffedde63e6ae75ab5101d4fe0221` (1767-byte manifest) |
| Serve from **RO** store + writable `.ollaya` emptyDir + `readOnlyRootFilesystem` | works, `GET /` → 200 (same layout as ONNX; spike 006) |
| Warmup `/api/decide keep_alive:-1` | `done_reason:"load"`, cold load ~18 s |
| `/api/ps` `name` | `jevk5:latest` |
| `/api/ps` `digest` | `58b5d651…0221` (== manifest sha256, bare hex) |
| `/api/ps` `device` | `"cpu"` |
| `/api/ps` `precision` (`details.quantization_level`) | **`"Q8_0"`** (quantization, not F32/F16) |
| `/api/ps` `expires_at` after keep_alive:-1 | `null` → `Pinned=True` |
| `/api/ps` `details.format` | `"gguf"` |
| Memory (cgroup `anon`, loaded) | 5 207 502 848 B = **4966 MiB ≈ 4.85 GiB** |
| `/v1/systemone` (evaluator path) | works — a `choice` question returned `choice:"cancel"`, confidence 0.9785, full probabilities |

## Does our gate accept it?

The readiness gate (`internal/controller/readiness.go`, with `Inspect` in
`internal/engine/ollaya/ollaya.go`) checks exactly three things against the recorded revision:

1. `loaded.Digest != rev.Digest` → `DigestMismatch`. GGUF digest = sha256(manifest), same scheme
   as ONNX → **passes**.
2. `loaded.Device != rev.Device` → `DeviceMismatch`. GGUF reports `"cpu"` (normalized by
   `normalizeDevice`, which only collapses `cuda:<n>` → `cuda`) → **passes** on a CPU revision.
3. `!loaded.Pinned` (i.e. `expires_at != null`) → `NotPinned`. keep_alive:-1 sets `expires_at:null`
   → **passes**.

**Precision is recorded but NOT gated.** `Inspect` copies `details.quantization_level` into
`Loaded.Precision` and the controller stores it in `status.stableRevision.precision`, but no
condition compares it. So `Q8_0` (or any GGUF quant) never fails the gate. This is the key
finding: the gate is precision-agnostic and therefore correct for GGUF out of the box.

## CUDA fallback (CPU image, `OLLAYA_DEVICE=cuda`)

`OLLAYA_DEVICE=cuda` on the CPU image with jevk5:latest: server starts (`GET /` → 200), but the
model **fails to load** — `MODEL_LOAD_FAILED: llama.cpp finds no device CUDA0 (devices: []); the
CUDA libraries are not installed`. `/api/ps` → `{"models":[]}`. The model never appears, so the
gate never sees a loaded entry → the Pod's readiness gate stays False and the probe eventually
times out. Same safe outcome as the ONNX CUDA-misconfig case (spike 001): a naive TCP probe
would pass, the `/api/ps` gate does not. (The failure here is a hard load error rather than a
silent CPU fallback, so there is no wrong-device entry to flag; the empty `/api/ps` is enough.)

## End-to-end on kind

Deployed the operator (kustomize, default image now `0.10.0` from the runtime-bump merge) and
applied a `DecisionModel` `jevk5:latest`, `device: cpu`, `replicas: 1`, `resources.requests
.memory: 5120Mi`/`limits.memory: 6Gi`, `cache.size: 10Gi`:

- phase: Resolving → **Caching ~4 min** (prefetch Job pulled 4.48 GB, completed in 4m14s) →
  **Starting ~1.5 min** (cold load) → **Ready** at ~5m18s total.
- conditions: `Resolved=True, Cached=True, ModelReady=True, Ready=True, Degraded=False`.
- `status.stableRevision`: digest `58b5d651…0221`, device `cpu`, **precision `Q8_0`**, image
  `ghcr.io/ollaya-dev/ollaya:0.10.0`.
- owned objects: Deployment 1/1, Service :11435, PVC 10Gi Bound, prefetch Job Complete.
- Default phase timeouts (Caching 30m, Starting 10m) were comfortably sufficient for this
  4.48 GB model; a larger GGUF (winnow/jeb/cygnet at 8–13 GB, or the 9B-class ~18–19 GB models)
  would need the raised `spec.rollout.timeouts` already documented in sizing.md.

## Verdict

| Aspect | Verdict |
|---|---|
| Readiness gate (digest/device/pinned) | **works as-is** — no change needed; the gate is precision-agnostic |
| Digest scheme (sha256 of manifest) | **works as-is** — identical to ONNX |
| Serving from RO store, `readOnlyRootFilesystem` | **works as-is** |
| `/v1/systemone` evaluator path | **works as-is** |
| CUDA misconfig on CPU image | **safe as-is** — model fails to load, `/api/ps` empty, gate stays False |
| Memory default in `resources.go` | **docs only** — jevk5 is not in the measured table; add a sizing row (below). No code change required: an unmeasured model simply gets no built-in default, and the user sets `spec.resources` (as this spike did). A `jevk5:latest` row could optionally be added to `cpuDefaults`. |

**GGUF models work under the operator with no gate or engine change.** The only follow-up is a
documentation sizing row (and optionally a built-in `resources.go` default entry).

### Proposed `docs/sizing.md` row

| Model | Family | Download | Memory (`anon`, loaded) |
|---|---|---|---|
| `jevk5:latest` | jevk5 (GGUF, Q8_0) | ~4.48 GB | ~4.85 GiB (4966 MiB) |

Recommended `resources` (rule of thumb: request ≈ anon rounded up to 512Mi, limit ≈ anon × 1.4):

```yaml
# jevk5:latest  (GGUF Q8_0, anon ~4.85 GiB)
resources:
  requests: { cpu: "1", memory: "5120Mi" }
  limits:   { cpu: "2", memory: "7Gi" }
cache:
  size: 6Gi      # 4.48 GB download + headroom
```

## Environment

- macOS arm64, OrbStack (11.7 GiB Docker).
- `ghcr.io/ollaya-dev/ollaya:0.10.0` (arm64, CPU).
- kind v0.33.0, k8s v1.37.0 for the end-to-end run.
- All containers, volumes, images, and the kind cluster removed after testing;
  `config/manager/kustomization.yaml` (mutated by `make deploy`) restored.
