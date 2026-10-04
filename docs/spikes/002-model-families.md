# Spike 002 — Second/third model families + resource sizing

Goal: prove the Ollaya engine (`internal/engine/ollaya`) is not Laya-specific, and give users
sizing numbers per model.

## Environment

- Host: macOS (arm64 / Apple Silicon), Docker via OrbStack. Image
  `ghcr.io/ollaya-dev/ollaya:0.7.3` (native arm64). Serving env identical to `ServingPodSpec`
  (`OLLAYA_HOST=0.0.0.0:11435`, `OLLAYA_MODELS=/models`, `OLLAYA_DEVICE=cpu`,
  `OLLAYA_KEEP_ALIVE=-1`), read-only model store at `/models`, writable emptyDir at
  `/home/ollaya/.ollaya`, `--user 1000:1000`, `fsGroup:1000` simulated by chowning the volumes.
- Each model: prefetch (the `PrefetchJobSpec` script — `ollaya pull` then `sha256sum` of
  `$OLLAYA_MODELS/$MANIFEST_PATH` vs the registry digest), then serve read-only, then
  `Resolve` / `Inspect` / `Decide` via the engine, then `docker stats`, then cleanup.
- Device: CPU only (no GPU on this host); precision therefore F32 for all (matches spike 001).

## Model selection (all < 3 GB download; sizes = sum of manifest layer bytes)

Probed the registry manifests before pulling (no wasted downloads):

| Model                | Family   | Runner (layer types)        | Download |
|----------------------|----------|-----------------------------|----------|
| `laya:multilingual`  | laya     | ONNX (`graph.onnx`)         | 683 MB   |
| `nli:latest`         | nli      | ONNX (`graph.onnx`)         | 884 MB   |
| `gliclass:latest`    | gliclass | ONNX (`graph.onnx`)         | 1768 MB  |

Picked these three (distinct families, all under the 3 GB cap). The only GGUF / llama.cpp-runner
candidate found, `jevk5:latest` (layer `graph.gguf`), is **4482 MB > 3 GB**, so it was skipped per
the task; `kev:0.8b`/`decider:0.8b` turned out to be ONNX, not GGUF. So the GGUF runner path
remains unverified here (no small GGUF model exists in the public registry today) — flagged for a
later spike if a small GGUF model ships.

## Results (per model, CPU / F32)

All three worked with the engine **unchanged**. `/api/ps` field shapes are identical across
families (`name`, `digest`, `device`, `details.quantization_level`, `expires_at`); no per-family
differences.

> **Data correction (2026-10-02):** the "RSS (docker stats)" column
> below over-reports memory because `docker stats` counts the page cache left by
> the model pull. Re-measured by cgroup `anon` (`/sys/fs/cgroup/memory.stat`,
> loaded with `keep_alive:-1`): `laya:en` ~3.09 GiB, `laya:multilingual`
> ~1.85 GiB, `nli:latest` ~3.62 GiB, `gliclass:latest` ~2.42 GiB. Use
> [docs/sizing.md](../sizing.md) for the authoritative numbers and the method;
> the figures in the table below are kept as originally recorded.

| Model               | Prefetch digest verify | Resolve == on-disk | /api/ps name / device / precision / pinned | Decide (choice+noul) | Load time | RSS (docker stats) | Decide latency (avg of 5, warm) |
|---------------------|------------------------|--------------------|--------------------------------------------|----------------------|-----------|--------------------|---------------------------------|
| `laya:multilingual` | ok (`2840…84eb`)       | match              | `laya:multilingual` / cpu / F32 / true     | mapped ok            | ~2.8 s    | ~1.85 GiB          | ~72 ms                          |
| `nli:latest`        | ok (`b331…e205`)       | match              | `nli:latest` / cpu / F32 / true            | mapped ok            | ~5.8 s    | ~3.75 GiB          | ~242 ms                         |
| `gliclass:latest`   | ok (`8c4c…e5a6`)       | match              | `gliclass:latest` / cpu / F32 / true       | mapped ok            | ~4.9 s    | ~2.66 GiB          | ~131 ms                         |

Notes:
- `MANIFEST_PATH` computed by the engine (`manifests/ollaya.dev/library/<model>/<tag>`) matched
  the on-disk path for every model; the prefetch digest check passed each time.
- Decide mapped `choice` (Choice + Probabilities + Confidence) and `noul` (Noul, no confidence)
  correctly for all three; the answer JSON shape is the same across families.
- RSS is peak resident set while a single model is loaded and warm (CPU, F32).

## Engine incompatibilities found

None. Resolve, Inspect (`/api/ps` mapping), and Decide (`/v1/systemone` mapping) all work
across the laya / nli / gliclass families with no code change. No new unit test was required
(the existing table-driven tests already cover the field shapes these models return).

## Recommended `resources` per model

Guidance for a CPU / F32 serving Pod. Memory limit ≈ observed RSS + ~40% headroom (model load
plus request working set); requests set to the observed RSS. GPU is out of scope here (unverified
on this host). PVC size ≥ download size with margin for the writable emptyDir logs.

```yaml
# laya:multilingual (download ~683 MB, RSS ~1.85 GiB)
resources:
  requests: { cpu: "1",   memory: "2Gi" }
  limits:   { cpu: "2",   memory: "3Gi" }
# PVC: 2Gi

# gliclass:latest (download ~1.77 GB, RSS ~2.66 GiB)
resources:
  requests: { cpu: "1",   memory: "3Gi" }
  limits:   { cpu: "2",   memory: "4Gi" }
# PVC: 4Gi

# nli:latest (download ~884 MB, RSS ~3.75 GiB)
resources:
  requests: { cpu: "1",   memory: "4Gi" }
  limits:   { cpu: "2",   memory: "6Gi" }
# PVC: 2Gi
```

Rule of thumb for a new ONNX model: `memory request ≈ RSS`, `memory limit ≈ RSS × 1.4`,
`cpu request 1 / limit 2` for low-latency single-request serving; size the PVC to the download
plus a little for logs. RSS does not track download size — `nli` downloads less than `gliclass`
but loads a larger graph into memory, so measure RSS per model rather than inferring it from the
download.

## Caveats / unverified

- CPU / F32 only; GPU and F16 unverified (no GPU on this arm64 host, as in spike 001).
- GGUF / llama.cpp runner unverified: the only GGUF model in the registry (`jevk5:latest`) exceeds
  the 3 GB cap. Re-run this spike for a small GGUF model when one is available to confirm the
  engine handles a non-ONNX runner (expected to be transparent — the engine only speaks HTTP).
- RSS/latency measured under OrbStack on Apple Silicon; absolute numbers will differ on cluster
  nodes, but the relative ordering and the sizing rule of thumb should hold.
