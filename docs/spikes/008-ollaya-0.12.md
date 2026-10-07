# Spike 008 — Ollaya 0.12.0 compatibility + decima:small as a demo/E2E model

Tested 2026-10-07 on macOS arm64 (OrbStack, Docker-compatible) against
`ghcr.io/ollaya-dev/ollaya:0.12.0`, comparing with the 0.10.0 baseline in spike 006
and the current default (`internal/engine/ollaya/ollaya.go` `DefaultRuntimeVersion = "0.10.0"`).
Only the CPU path is tested (the `-cuda`/`-cuda12` images are amd64-only; no GPU host).
Upstream shipped 0.11.0 (decima models, `OLLAYA_THREADS`) and 0.12.0 (Vulkan on Windows,
arbiter, snap q4_k_m) on 2026-10-06.

## 1. Compatibility fact table (0.10.0 → 0.12.0)

Every row re-checked against `:0.12.0`; commands and trimmed output below the table.

| Fact | 0.10.0 (spike 006) | 0.12.0 | Consequence for the operator |
|---|---|---|---|
| Image tags that exist | `0.12.0`, `0.12.0-cuda` | `0.12.0`, `0.12.0-cuda`, **`0.12.0-cuda12` (new)** | no change; CUDA default (`-cuda`) still exists |
| Arches | `0.12.0` amd64+arm64; cuda amd64-only | same (`0.12.0` multi-arch; `-cuda`/`-cuda12` amd64-only) | arm64 CPU image still native; no change |
| Entrypoint / cmd | `/usr/bin/ollaya` / `serve` | same | no change |
| UID / GID | 1000:1000 | same | no change |
| Port | 11435 | same | no change |
| `OLLAYA_MODELS` default | `/home/ollaya/.ollaya/models` | same | no change |
| `OLLAYA_HOST` default | `0.0.0.0:11435` | same | no change |
| `GET /` liveness | 200 without API key | 200 | no change |
| `--read-only` root + RO store serve | works (writable `.ollaya` emptyDir) | **works** (`version="0.12.0" models=/models`; warmup loads on a RO store) | no change |
| `ollaya pull laya:en` | exit 0, idempotent | exit 0 (~87 s cold), second pull 0.26 s | no change |
| `pull @sha256:` | rejected, exit 1 | **rejected, exit 1** (`invalid model name … expected [host/][namespace/]model[:tag]`) | pull by tag + verify digest, no change |
| Manifest disk path | `$OLLAYA_MODELS/manifests/ollaya.dev/library/<model>/<tag>` | same | no change |
| Digest = sha256(manifest) for `laya:en` | `c305a927…5e9d` (3089 B) | **identical** (`c305a927…5e9d`, 3089 B) | resolver computes the same digest; no change |
| `/api/ps` digest == pulled digest | equal | **equal** | `Inspect()` gate, no change |
| `/api/ps` fields | `name`,`model`,`size`,`digest`,`device`,`details`,`expires_at` | all present, same; **new `context_length` (int)** and `size_vram` (already in 0.10.0) | `Inspect` reads `name`/`digest`/`device` — all unchanged; no change |
| `/api/ps` device value | `"cpu"` | `"cpu"` | readiness gate matches `cpu` exactly; no change |
| `/api/decide {keep_alive:-1}` warmup | `done_reason:"load"` | same (`done_reason:"load"`) | `Warmup()` works; no change |
| `/v1/systemone` response shape | `answers.<q>.{type,choice,confidence,probabilities}` | **identical** | evaluator/`Decide` unaffected; no change |
| Prefetch 404-tag class (#43) | `ollaya pull` stderr contains `not found in registry`, exit 1 | **`model "…" not found in registry ollaya.dev`, exit 1** | prefetch script matches `*"not found in registry"*` → `ModelNotFound` (permanent, exit 3); mapping holds, no change |
| Upgrade path: store pulled by 0.10.0 served RO by 0.12.0 | n/a | **works** — same digest `c305…5e9d`, device `cpu`, precision `F32` | existing per-revision PVCs serve on 0.12.0 without re-pull; no change |
| Memory (RSS, `laya:en`, CPU/F32, `--cpus=2`) | 3139 MiB | **~3.08 GiB** (`docker stats`) | within the `laya:en` request; no change to `resources.go` |
| New env vars | `OLLAYA_HF_ENDPOINT`, `OLLAYA_HF_TOKEN` | plus **`OLLAYA_THREADS`** (0.11.0) — see §2 | optional; see §2 |
| Linux Vulkan llama.cpp build | n/a | **no** — `ollaya llama-devices` on the Linux arm64 image lists only a CPU device, `cuda_backend: null`, `llama_cpp: 0.5.0-dev` | Vulkan is the Windows feature; nothing to use on Linux; no design |

### Commands (trimmed)

```
$ docker image inspect ghcr.io/ollaya-dev/ollaya:0.12.0 --format '{{.Config.Entrypoint}} {{.Config.Cmd}} {{.Config.User}}'
[/usr/bin/ollaya] [serve] 1000:1000
# Env: OLLAYA_HOST=0.0.0.0:11435  OLLAYA_MODELS=/home/ollaya/.ollaya/models  ExposedPorts: 11435/tcp

$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.0   # linux/amd64 + linux/arm64
$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.0-cuda     # linux/amd64 only
$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.0-cuda12   # linux/amd64 only (new)

# serve (RO root + RO store), warmup, /api/ps:
INFO ollaya_server::http: Ollaya is running address=0.0.0.0:11435 version="0.12.0" models=/models
$ curl -s .../api/decide -d '{"model":"laya:en","keep_alive":-1}'   → "done_reason":"load"
$ curl -s .../api/ps | jq '.models[0] | {name,digest,device,prec:.details.quantization_level,context_length}'
  name=laya:en digest=c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d device=cpu prec=F32 context_length=512
$ sha256sum <store>/manifests/ollaya.dev/library/laya/en   # 3089 bytes
  c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d

# prefetch failure classes:
$ ollaya pull laya:does-not-exist-xyz → Error: model "laya:does-not-exist-xyz" not found in registry ollaya.dev  (exit 1)
$ ollaya pull laya@sha256:c305…      → Error: model: invalid model name …; expected [host/][namespace/]model[:tag]  (exit 1)

# upgrade path (store pulled by 0.10.0, served by 0.12.0, RO):
  UPGRADE digest c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d device cpu prec F32

# Vulkan check:
$ ollaya llama-devices → {"llama_cpp":"0.5.0-dev","cuda_backend":null,"devices":[{"name":"CPU","type":"cpu",...}]}
```

**Verdict (§1): 0.12.0 is drop-in compatible with 0.10.0** for everything the operator depends on
(serve, RO store, pull, sha256(manifest)==/api/ps digest, warmup, `/v1/systemone` shape, prefetch
404 classification, upgrade path, device gate). The only additive `/api/ps` field is
`context_length`, which `Inspect` does not read. Moving `DefaultRuntimeVersion` 0.10.0 → 0.12.0 would
need no code change beyond the constant and the sizing/CUDA constants if desired; existing
per-revision PVCs serve without re-pull. (Not retested, CPU-only host: `OLLAYA_DEVICE=cuda` behaviour,
SIGTERM shutdown — no release note suggests a change.)

## 2. `OLLAYA_THREADS` (0.11.0)

`laya:en` (ONNX, 421M, F32) in a CPU-limited container (`docker run --cpus=2`), warm, median of
10 `/v1/systemone` calls (demo question shape), peak RSS from `docker stats`:

| `OLLAYA_THREADS` | median latency | min–max | peak RSS |
|---|---|---|---|
| unset | 0.177 s | 0.116–0.200 | 3.08 GiB |
| 2 | 0.139 s | 0.136–0.144 | 3.07 GiB |
| 4 | 0.136 s | 0.114–0.154 | 3.04 GiB |

Setting `OLLAYA_THREADS` to the CPU limit on `laya:en` is **not noise**: it both lowers the median
(~0.177 → ~0.139 s, ~20 %) and tightens the spread (unset ranged 0.116–0.200; `=2` was 0.136–0.144).
Memory is flat. The effect is small in absolute terms (~40 ms) for this ONNX model.

Not measured: a GGUF/llama.cpp model (the runner `OLLAYA_THREADS` targets most). The smallest GGUF
in the registry is `jevk5:latest` at 4.48 GB (spike 007) and none was cached; a 4.48 GB pull was not
justified for this micro-benchmark. **Recommendation:** if the operator ever sets `OLLAYA_THREADS`,
set it to the container CPU limit (`resources.limits.cpu`, rounded down) — it is a safe, small win on
ONNX and expected to matter more for GGUF; leaving it unset is also acceptable. This is a follow-up
decision, not required to adopt 0.12.0.

## 3. decima:small (and decima:base) as a lighter demo / E2E model

Measured against the demo golden set (`examples/demo/dataset.yaml`: 40 English support tickets, one
choice question routing to billing/technical/sales/account, 10 each). Served on 0.12.0, `--cpus=2`.

| metric | laya:en (today) | decima:small | decima:base |
|---|---|---|---|
| params / base | 421M, laya | 122M, multilingual-e5-small 1.1 | 321M, mmBERT-base 2.0 |
| manifest digest | c305…5e9d | `d0cdad85…52db3` | `56ad2542…2c25f1` |
| download size | ~850 MB | **510 MB** | 1324 MB |
| store on disk | ~814 MiB | ~520 MiB | ~1.3 GiB |
| RSS after load | ~3.08 GiB | **520 MiB** | (not isolated) |
| cold load time | ~4.2 s | **0.70 s** | — |
| `/api/ps` device/precision | cpu / F32 | cpu / F32 | cpu / F32 |
| `/v1/systemone` median latency (demo shape) | ~0.14–0.19 s | **0.058 s** | — |
| **accuracy on the demo golden set** | **37/40 = 0.925** | **40/40 = 1.000** | **40/40 = 1.000** |
| deterministic across runs | yes (greedy) | **yes** (identical choices, 2 runs) | — |
| license | — | **Apache-2.0** | Apache-2.0 |

(Upstream library-page benchmark, a *different* dataset: decima-small 0.432, decima-base 0.495,
decima-agent 0.486 — do not confuse with the 1.000 our 4-class golden set yields.)

`decima:small` is a far lighter model: ~6× less RSS (520 MiB vs 3.08 GiB), ~6× faster cold load
(0.70 s vs ~4.2 s), ~3× faster per call, 510 MB download, Apache-2.0, deterministic. For a
2-vCPU CI runner this removes almost all of the model-load cost that dominates E2E and the demo.

**But it does not give a pass/fail pair.** The demo (and the eval-gated E2E) need a *good* candidate
that passes `minAccuracy: 0.90` and a *bad* candidate that fails it. On this golden set both
`decima:small` and `decima:base` score a perfect 1.000, and no decima tag fails. So decima:small is
an excellent **good/pass model** (and a great general demo/E2E serving model for speed), but a failing
candidate still has to come from elsewhere — today that is `laya:multilingual` (0.80). A future task
that adopts decima for the demo should pair `decima:small` (pass) with a model that genuinely fails
the 0.90 gate on this dataset (e.g. keep `laya:multilingual`, or craft/relabel a harder dataset).
Changing the demo/E2E is explicitly out of scope here.

## Environment

- macOS arm64, OrbStack (Docker 29.4.0).
- `ghcr.io/ollaya-dev/ollaya:0.12.0` (arm64), client 0.12.0; `:0.10.0` (arm64) for the upgrade-path
  test. Both already cached.
- No kind cluster used (container-level facts only).
- All spike containers (`dmo-spike008*`, `dmo-decima`, `dmo-thr*`) and volumes
  (`dmo-spike008-store`, `dmo-spike008-store010`) removed after testing; the `:0.12.0`/`:0.10.0`
  images are kept (also used by kind).

## Could not verify

- CUDA (`-cuda`/`-cuda12`): amd64-only, no GPU host.
- Windows Vulkan / arbiter / snap q4_k_m (0.12.0 Windows features): not applicable to the Linux image.
- GGUF `OLLAYA_THREADS` effect: no cached GGUF; smallest is 4.48 GB.
- amd64 runtime behaviour: tested on arm64 only (0.12.0 is multi-arch; the amd64 pull self-check
  quirk noted in earlier spikes is unchanged in principle but not re-exercised here).
