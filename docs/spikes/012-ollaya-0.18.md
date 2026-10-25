# Spike 012 - Ollaya 0.18.0 as the default runtime

Tested 2026-10-25 on macOS arm64 (OrbStack, Docker 29.8.2) against
`ghcr.io/ollaya-dev/ollaya:0.18.0`. Same checklist as spike 011 (0.13.0). CPU path only: the
`-cuda`/`-cuda12` images are amd64-only and there is no GPU host, so nothing here is a GPU result.
The 0.13.0 column is taken from spike 011 (not re-run in this spike).

## Verdict

**0.18.0 keeps the contract the engine reads.** Every fact the readiness gate, the prefetch Job and
the serving Pods depend on was re-checked and is unchanged: manifest bytes and digest, pull by tag,
pull by pinned digest, unknown-digest error text, `/api/decide` warmup, `/api/ps` field set and
device/precision values, `/v1/systemone` response shape, and serving from a read-only store with a
read-only root. No contract change was found, so the default bump (`DefaultRuntimeVersion` 0.13.0 ->
0.18.0) went ahead. Existing revisions are not affected: their runtime is recorded in `status`.

## Image index digests

| Tag | Index digest | Platforms (`imagetools inspect`) |
|---|---|---|
| `0.18.0` | `sha256:a1baf26a5cad5510a5f426bf98097f24b01d96e0751c5202d5d382603b51a656` | linux/amd64, linux/arm64 (+ attestation manifests) |
| `0.18.0-cuda` | `sha256:82f118ce5549ad8a1e437c03f471bfb94c134bef7522664e5d66a3ddd5bab0b4` | linux/amd64 (+ attestation) |
| `0.18.0-cuda12` | `sha256:da484c2c85f86771d27463253d914d4513651d44be976f059f6a0369a350208c` | linux/amd64 (+ attestation) |

The `0.18.0-cuda12` digest is recorded in the `ollaya.go` comment only, like 0.13.0-cuda12: the engine
renders `:tag` and `:tag-cuda`, not `-cuda12`.

## Measured on the CPU image

| Check | Command / source | Result |
|---|---|---|
| Version | `GET /`, `ollaya --version` | `ollaya version is 0.18.0`, `Ollaya is running` |
| Manifest bytes of `laya:en` | `GET https://ollaya.dev/v2/library/laya/manifests/en` | HTTP 200, 3089 B, sha256 `c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d` (identical to 0.13.0) |
| Pull by tag | `ollaya pull laya:en` | exit 0 (cold ~52 s, host network) |
| Pull by pinned name | `ollaya pull laya:en@sha256:<hex>` | exit 0 (same digest as above) |
| Pull by digest, no tag | `ollaya pull laya@sha256:<hex>` | exit 0 |
| `/api/pull` pinned | `{"model":"laya:en@sha256:<hex>"}` | HTTP 200, `{"status":"success"}` |
| Unknown digest | `ollaya pull laya:en@sha256:000...0` | `Error: model "laya:en@sha256:000..." not found in registry ollaya.dev: it has no manifest with that digest` (exit code not captured); `/api/pull` -> HTTP 404 `MODEL_NOT_FOUND` |
| Unknown tag | `ollaya pull laya:nosuchtag` | `Error: model "laya:nosuchtag" not found in registry ollaya.dev` (exit code not captured) |
| Warmup | `POST /api/decide {"model":"laya:en","keep_alive":-1}` | HTTP 200, `done_reason":"load"` |
| `/api/ps` (tag load) | `GET /api/ps` | `name":"laya:en"`, `digest` bare hex = manifest sha256, `device":"cpu"`, `details.quantization_level":"F32"`, `expires_at":null`, `size_vram":0`, `context_length":512` |
| `/api/ps` (pinned load, after restart) | `POST /api/decide {"model":"laya:en@sha256:<hex>","keep_alive":-1}` | HTTP 200; `name` and `model` echo `laya:en@sha256:<hex>`; `digest` bare hex; `device":"cpu"` |
| `/v1/systemone` | `POST` with the `type:"choice"` / `criteria` schema from `docs/evaluation.md` | HTTP 200, `answers.dept.{type,choice,confidence,probabilities}` |
| Read-only store + read-only root | `--read-only --cap-drop ALL --user 1000:1000`, store volume mounted `:ro` | `Ollaya is running`; decide HTTP 200; `/api/ps` shows the digest; `/v1/systemone` HTTP 200 |
| Pull on the read-only store | `POST /api/pull` | HTTP 200 with body `{"error":"Read-only file system (os error 30)","code":"REGISTRY_ERROR"}` (the error is in the body; the status is 200). Serving Pods never pull, so this only matters for the prefetch Job, which writes to a RW store. Not compared with 0.13.0 in this run. |

Field set of `/api/ps` is the same as spike 011: `name, model, size, digest, details, expires_at,
context_length, size_vram, device`. The readiness gate reads `digest`, `device` and
`details.quantization_level`; all keep their 0.13.0 shape.

## Verbatim outputs

`POST /api/decide {"model":"laya:en","keep_alive":-1}` on a tag load:

```json
{"model":"laya:en","answers":{},"usage":{"input_tokens":0,"output_tokens":0},"routing":null,"state_truncated":false,"done_reason":"load","created_at":"2026-10-25T15:58:08.399213632Z","total_duration":3205731793,"load_duration":3205505418,"eval_duration":0}
```

`GET /api/ps` after the tag load:

```json
{"models":[{"name":"laya:en","model":"laya:en","size":852602879,"digest":"c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d","details":{"parent_model":"","format":"onnx","family":"laya","families":["laya"],"parameter_size":"421M","quantization_level":"F32"},"expires_at":null,"size_vram":0,"context_length":512,"device":"cpu"}]}
```

`GET /api/ps` after a pinned load (`{"model":"laya:en@sha256:c305a927...5e9d","keep_alive":-1}`, container restarted first):

```json
{"models":[{"name":"laya:en@sha256:c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d","model":"laya:en@sha256:c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d","size":852602879,"digest":"c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d","details":{"parent_model":"","format":"onnx","family":"laya","families":["laya"],"parameter_size":"421M","quantization_level":"F32"},"expires_at":null,"size_vram":0,"context_length":512,"device":"cpu"}]}
```

Unknown digest on `/api/pull` (HTTP 404):

```json
{"error":"model \"laya:en@sha256:0000000000000000000000000000000000000000000000000000000000000000\" not found in registry ollaya.dev: it has no manifest with that digest","code":"MODEL_NOT_FOUND"}
```

## Consequences for the operator

- The unknown-digest text matches the existing classifier cases in `prefetch_pull_classify_test.go`
  (`not found in registry` -> ModelNotFound, exit 3). No classifier change.
- The pinned-load `name` echo (`laya:en@sha256:...`) is the same as 0.13.0 and already handled by
  `parseName` / `Inspect` (the gate keys on `digest`).
- `device: cuda` on 0.18.0: the `-cuda` image is amd64-only (as before). Not verified on a GPU; the
  last GPU run is on 0.12.1-cuda (see `docs/ARCHITECTURE.md` §8).

## Not verified

- No GPU host: `0.18.0-cuda` and `0.18.0-cuda12` were only checked as image indexes, not run.
- Moved-tag detection (`UpstreamTagMoved`) cannot be produced on demand; the digest path does not use it.
- A registry digest mismatch (`DIGEST_MISMATCH`) was not provoked; the classifier cases come from spike 011.
- amd64 runtime: tested on arm64 only (CI runs amd64 e2e).
