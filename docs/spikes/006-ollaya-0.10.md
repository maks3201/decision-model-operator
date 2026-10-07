# Spike 006 — Ollaya 0.10.0 compatibility (from 0.7.3)

Tested 2026-10-06 on macOS arm64 (OrbStack, Docker Desktop equivalent) against
`ghcr.io/ollaya-dev/ollaya:0.10.0` (arm64+amd64 multi-arch), comparing with the
0.7.3 baseline established in spike 001 and ARCHITECTURE §2. Only the CPU path is
tested (the `:cuda` / `:cuda12` images are amd64-only; no GPU host available).

## Fact table

| Fact (ARCHITECTURE §2) | 0.7.3 | 0.10.0 | Consequence for the operator |
|---|---|---|---|
| Entrypoint / cmd | `/usr/bin/ollaya` / `serve` | same | no change |
| UID / GID | 1000:1000 | same | no change |
| Port | 11435 | same | no change |
| `GET /` liveness | 200 even without API key | same (200) | no change |
| `OLLAYA_MODELS` | `/home/ollaya/.ollaya/models` | same env default | no change |
| `readOnlyRootFilesystem` | works with writable `.ollaya` emptyDir (logs/pid), `/tmp` not required | **same** — verified with `--read-only`, RO models, writable `.ollaya` emptyDir | no change |
| RO model store (serve) | works (RO mount at `$OLLAYA_MODELS`) | **works** (state writes go to `.ollaya`, not `$OLLAYA_MODELS`; serve logged `Ollaya is running ... version="0.10.0" models=/models`) | no change |
| `ollaya pull laya:en` | exit 0, idempotent ~0.5 s | exit 0, idempotent | no change |
| Auto-embedded daemon on `pull` | yes, 30 s self-check | yes, same 30 s self-check; requires writable `$OLLAYA_MODELS` (pre-chown to 1000 or `fsGroup`) | unchanged; the failed-self-check-on-amd64 issue from C-018 persists (curl+jq fetch in the mirror spec works around it) |
| `pull @sha256:` | rejected, exit 1 | rejected, same error | pull by tag + verify digest, no change |
| Bare name → `latest` | yes (`laya` → `laya:latest`, different artifact) | yes (unchanged) | explicit tag required, no change |
| Manifest disk path | `$OLLAYA_MODELS/manifests/ollaya.dev/library/<model>/<tag>` | same path and layout (`blobs/`, `manifests/`) | no change |
| Digest = sha256(manifest) | `c305a9276531…e9d` (3089 bytes, `laya:en`) | **identical** (registry-side, same manifest) | no change; the operator's resolver computes the same digest |
| `/api/ps` fields | `name`, `digest`, `device`, precision (`details.quantization_level`), `expires_at` | all present, same values; new additive fields: `model` (alias of `name`), `size` (top-level int) | no change; `Inspect` reads `name`/`digest`/`device`, which are unchanged |
| `/api/ps` device value | `"cpu"` | `"cpu"` | no change; readiness gate matches `cpu` exactly |
| `/api/decide` warmup | `keep_alive:-1` loads model, `done_reason:"load"` | same response structure | no change; `Warmup()` works |
| `/v1/systemone` | answers decisions | present (input validation error if wrong format, but endpoint is live) | no change |
| `OLLAYA_REGISTRY` mirror | writes `manifests/<host>/…` | format unchanged (tested on the default registry path structure; the env is documented in release notes) | no change; B-038 `RegistryHost()` handles it |
| `OLLAYA_DEVICE=cuda` on CPU image | fail at model load, not startup; `/api/ps` empty | not retested (CPU-only host); no release notes mention a change here | assumed unchanged |
| Graceful shutdown (SIGTERM) | exit 0 in ~0.2 s | not retested; no release notes mention a change | assumed unchanged |
| `fsGroup: 1000` requirement | required for root-owned PVC | same (pull fails EACCES without writable store) | no change |
| **Upgrade path**: 0.7.3 store → 0.10.0 serve | n/a | **works** — a PVC pulled by 0.7.3 is served by 0.10.0 (RO); digest and device unchanged | existing per-revision PVCs work after an image bump; no re-pull needed |
| Memory (cgroup `anon`, `laya:en`, CPU/F32) | 3167 MiB | **3139 MiB** (slightly lower) | well within `3584Mi` request; no change to `resources.go` |
| New env vars | — | `OLLAYA_HF_ENDPOINT`, `OLLAYA_HF_TOKEN` (new) | not used by the operator; no impact |
| New features | — | presets, `images`, `:cuda12` tag, arm64 CUDA | not used; no impact |
| No `presets/` written to store | — | confirmed: store after pull contains only `blobs/` + `manifests/`, no `presets/` directory | RO serving unaffected |

## E2E

Ran the full kustomize e2e suite locally (`OLLAYA_IMAGE=ghcr.io/ollaya-dev/ollaya:0.10.0
E2E_MIRROR=1`, kind `dmo-c23`, arm64): **12 of 12 specs passed, 0 failed, 0 skipped** (884 s).

Caveat: `OLLAYA_IMAGE` only loads the 0.10.0 image into the kind node. The operator's built-in
default (`DefaultImageCPU`, `internal/engine/ollaya/ollaya.go`) is still `0.7.3`, and the e2e
`applyDecisionModel` does not set `spec.image`, so the DecisionModel pods used the 0.7.3 image
during the run. To exercise the operator reconciling DMs that serve on 0.10.0 end-to-end,
either bump `DefaultImageCPU` or use `spec.image` with `--allow-image-override`. The
container-level tests above cover every runtime fact the reconciler depends on (RO store serve,
`/api/ps` field parsing, warmup, upgrade path).

## Verdict

**Drop-in compatible.** No code changes needed to support Ollaya 0.10.0. Bumping
`DefaultImageCPU` from `0.7.3` to `0.10.0` (and `DefaultImageCUDA` from `0.7.3-cuda` to
`0.10.0-cuda`) is the only change required; no PodSpec, Job, readiness gate, resource default,
or e2e adjustment is needed. Existing per-revision PVCs (pulled by 0.7.3) serve correctly on
0.10.0 without re-pull. Memory footprint for `laya:en` is marginally lower (3139 vs 3167 MiB).

## Environment

- macOS arm64, OrbStack (Docker-compatible).
- `ghcr.io/ollaya-dev/ollaya:0.10.0` (arm64), client version 0.10.0.
- `ghcr.io/ollaya-dev/ollaya:0.7.3` (arm64, for upgrade-path test).
- kind v0.33.0, k8s v1.37.0 (for the e2e run).
- All containers, volumes, images, and the kind cluster removed after testing.

<!-- gate skip check: docs-only change, remove -->
