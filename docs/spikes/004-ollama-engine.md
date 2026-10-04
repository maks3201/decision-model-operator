# Spike 004 — Ollama as a second engine (readiness-gate feasibility)

Date: 2026-10-02.

Question this spike answers: Ollama 0.35 added a Jev-style decision API (`/v1/systemone`).
If an `ollama` engine is to be the v0.3 candidate, Ollama must expose enough for our
`decisionmodel.io/model-ready` gate — the expected **digest** on the expected **device**.
This reproduces an earlier quick check properly, in the style of spike 001. Facts only;
anything not run here is marked **unverified**.

## Environment

- Host: macOS (arm64 / Apple Silicon), Docker via OrbStack (`docker info` OK).
- Image pinned: **`ollama/ollama:0.35.1`** (newest non-rc 0.35.x on Docker Hub; the 0.35.x
  tags present were `0.35.0`, `0.35.1`, plus `-rc*`/`-rocm` variants). `latest` was **not**
  used (per the task: `latest` is not guaranteed to be 0.35). Image digest:

  ```
  ollama/ollama@sha256:292ee7945dfc3d5840a181f3ab86fedb1e66703e02c8af98b50f4da56b7e278c
  arch=arm64 os=linux
  entrypoint=["/bin/ollama"] cmd=["serve"] user=<empty→root> workdir=<empty> exposed=11434/tcp
  ```

  **The default image runs as root with `HOME=/root`** (`id` inside the running container =
  `uid=0(root)`), store defaults to `/root/.ollama/models` (`OLLAMA_MODELS` unset). Contrast
  Ollaya (spike 001), which ships `USER 1000` and `OLLAYA_MODELS` preset.
- Port: user's own homebrew Ollama holds `127.0.0.1:11434` — **not touched**. The spike
  published containers on `127.0.0.1:11500` (serve), `:11501` (hardened RO), `:11502` (auth).
- Model: `tev1:0.8b` (~0.8 GB download, 811 MB model layer). All temp containers, the named
  volume and the copied store were removed at the end; the `ollama/ollama:0.35.1` image is
  **kept** (per the task).

---

## 1. `/api/ps` after a `keep_alive: -1` load

Loaded by `POST /v1/systemone` with `"keep_alive": -1` (see §6 for the request shape), then:

```json
// GET /api/ps
{"models":[{
  "name":"tev1:0.8b","model":"tev1:0.8b","size":1167138486,
  "digest":"8d11b3146b7f3f4f4d5e9a64665ab2bdaf8b46e4ec42a5880b60a716ae50e3fb",
  "details":{"parent_model":"","format":"gguf","family":"qwen35","families":["qwen35"],
             "parameter_size":"752.39M","quantization_level":"Q8_0"},
  "expires_at":"2319-01-12T17:04:59Z","size_vram":0,"context_length":2050}]}
```

Fields present: `name`, `model`, `size`, `digest` (full 64-hex, **no `sha256:` prefix**),
`details` (`format`, `family`, `parameter_size`, `quantization_level`), `expires_at`,
`size_vram`, `context_length`.

- `digest` ✅ present (task expected it) and equals the registry/`/api/tags` digest (§2).
- `size` ✅ and `size_vram` ✅ present (task expected them).
- `expires_at` ✅ present; with `keep_alive: -1` it is a far-future timestamp (year 2319),
  i.e. effectively pinned. (Ollama does **not** report `expires_at: null` for a pinned model
  the way Ollaya does — it uses a sentinel far-future time instead. The Ollaya `Inspect`
  treats `expires_at == null` as `Pinned`; that mapping would read Ollama as *not* pinned.)
- **There is NO `device` field.** This is the central finding — see §4 and §9. Device is only
  observable indirectly through `size_vram` (0 on CPU).

---

## 2. Digest = sha256(manifest bytes); tag can move

`GET https://registry.ollama.ai/v2/library/tev1/manifests/0.8b`
with `Accept: application/vnd.docker.distribution.manifest.v2+json` → HTTP 200, 1067 bytes.

```
sha256(manifest bytes) = 8d11b3146b7f3f4f4d5e9a64665ab2bdaf8b46e4ec42a5880b60a716ae50e3fb
/api/ps  digest         = 8d11b3146b7f3f4f4d5e9a64665ab2bdaf8b46e4ec42a5880b60a716ae50e3fb
/api/tags digest        = 8d11b3146b7f3f4f4d5e9a64665ab2bdaf8b46e4ec42a5880b60a716ae50e3fb
```

**All three agree** — same mechanism as Ollaya (digest = sha256 of the raw manifest bytes,
bare hex). The resolver needs no daemon, just HTTP + sha256 of the body.

Manifest shape (no `runner` field; "format" is implied by layer media types):

```
mediaType        = application/vnd.docker.distribution.manifest.v2+json
config.mediaType = application/vnd.docker.container.image.v1+json
layers:
  application/vnd.ollama.image.model    811843424
  application/vnd.ollama.image.system   171
  application/vnd.ollama.image.license  11345
  application/vnd.ollama.image.license  1079
  application/vnd.ollama.image.params   17
```

- There is **no `runner` field** in the manifest; the model format is `gguf`
  (`/api/ps.details.format` and the `vnd.ollama.image.model` layer), i.e. a llama.cpp runner.
  Contrast Ollaya's `laya:en`, which is `onnx` (spike 001).
- The response carried **no `Docker-Content-Digest` header** (only `content-type`); compute
  the digest from the body bytes, do not trust a header (same caveat as spike 001).
- **A tag can move.** `tev1` was updated ~1 day before the check; this
  spike did not re-pull to observe a move, but by construction a tag → manifest mapping is
  mutable upstream, so the operator must pin to the resolved digest and re-verify, exactly as
  for Ollaya. **Unverified here:** an actual tag move within the spike window.

---

## 3. Pull by digest → `invalid model name`

Confirmed, three ways:

```console
$ ollama pull "tev1@sha256:8d11b3146b7f…e3fb"       → Error: 400 Bad Request: invalid model name
$ ollama pull "tev1:0.8b@sha256:8d11b3146b7f…e3fb"  → Error: 400 Bad Request: invalid model name
$ POST /api/pull {"model":"tev1@sha256:…"}          → {"error":"invalid model name"}
```

Same conclusion as Ollaya (spike 001 §4): **pull by tag, then verify the manifest sha256
against the resolved digest** in the prefetch Job. `@sha256:` is rejected before any network
call.

---

## 4. Device semantics

On this CPU-only host:

- `/api/ps` → `size_vram: 0` and **no `device` field**.
- Server log (startup + load):

  ```
  msg="discovering available GPUs..."
  msg="inference compute" id=cpu library=cpu compute="" name=cpu description=cpu
        libdirs=ollama total="7.8 GiB" available="7.7 GiB"
  msg="vram-based default context" total_vram="0 B" default_num_ctx=4096
  msg="system memory" total="7.8 GiB" free="7.2 GiB"
  load_tensors:          CPU model buffer size = 259.56 MiB
  load_tensors:   CPU_REPACK model buffer size = 761.88 MiB
  msg="loaded runners" count=1
  ```

  The device signal in the log is the **`inference compute … id=cpu library=cpu`** line and
  the `CPU …` buffer lines; `total_vram="0 B"`.

- **GPU semantics — UNVERIFIED** (no GPU on this arm64 Mac; the `-rocm` tag is AMD/amd64,
  there is no CUDA tag tried here). Ollama's model is **partial offload**: it reports
  `size` (total) and `size_vram` (bytes resident on GPU). Expected, but not run:
  - full GPU offload → `size_vram == size` (and the `inference compute` log line names the
    GPU library, e.g. `library=cuda`/`rocm`, with a non-zero `total_vram`);
  - partial offload → `0 < size_vram < size`;
  - CPU → `size_vram == 0` (verified).
- **What a GPU run must check for the gate:** because `/api/ps` has **no `device` string**,
  the gate cannot do Ollaya's exact-string match (`cuda:0`). For Ollama it must assert
  `size_vram > 0` **and** ideally `size_vram == size` (fully on GPU) to count as `device:cuda`,
  and `size_vram == 0` as `device:cpu`. This catches the silent-CPU-fallback case the product
  is built around, but by a different field than Ollaya. The exact GPU `size_vram`/`size`
  relationship, and whether a model can load with `size_vram` between 0 and `size` on these
  decision models, is **unverified**.

---

## 5. Hardened read-only run

Pulled the store in a default container, copied `/root/.ollama/.../models` out, then ran:

```
--user 1000:1000 --read-only --tmpfs /tmp -e HOME=/tmp -e OLLAMA_MODELS=/models
-v <models>:/models:ro
```

Result: **serving works.** `/api/version` → `{"version":"0.35.1"}`, `/api/tags` lists
`tev1:0.8b`, and `/v1/systemone` returns a correct decision from the RO store. Writes are
confined to the `/tmp` tmpfs (`HOME=/tmp`).

`pull` and `delete` on the RO store **fail** (as required so a serving Pod cannot mutate the
cache):

```console
POST /api/pull   → {"error":"open /models/blobs/sha256-…-partial-0: read-only file system"}
DELETE /api/delete → {"error":"remove /models/manifests/registry.ollama.ai/library/tev1/0.8b:
                      read-only file system"}  (HTTP 500)
ollama rm tev1:0.8b → Error: remove /models/manifests/.../tev1/0.8b: read-only file system
```

Notes vs Ollaya:
- Manifest path on disk: `/models/manifests/registry.ollama.ai/library/<model>/<tag>`
  (Ollaya: `.../manifests/ollaya.dev/library/<model>/<tag>`).
- The default image is **root + `HOME=/root`**, so a hardened Pod must override both
  `runAsUser: 1000` and `HOME` (to a writable path) and set `OLLAMA_MODELS` to the RO mount —
  more PodSpec surgery than Ollaya needs. `fsGroup` for a root-provisioned RW prefetch PVC is
  **unverified** here (only the RO serving path was hardened); expect the spike-001 finding to
  apply (needs `fsGroup: 1000` on stricter provisioners, not on kind local-path).

---

## 6. `/v1/systemone` request + response shape vs Ollaya

**Request — different question schema.** Ollama **requires** a per-question `instructions`
string and takes `criteria` flat on the question (no `choice:{…}` wrapper). The working body:

```json
{"model":"tev1:0.8b","keep_alive":-1,
 "state":"I was charged twice this month, please refund the extra charge.",
 "questions":{"department":{
    "type":"choice",
    "instructions":"Which department should handle this ticket?",
    "criteria":{"billing":"…","technical":"…","sales":"…","other":"anything else"}}}}
```

Rejections observed while finding the shape (useful for the engine/evaluator):
- no `instructions` → `question "department": instructions must be a nonempty string, object, or array`
- `criteria` nested under `choice:{criteria:…}` → `choice criteria must map option keys to descriptions or null`
- missing `type` → `type must be choice, noul, or score`

So Ollaya's request (`{type, criteria}` with no required `instructions`) is **not** accepted
verbatim by Ollama — the engine would have to synthesize an `instructions` string per question.

**Response — answer object matches; envelope differs.**

```json
{"model":"tev1:0.8b",
 "answers":{"department":{
    "type":"choice","choice":"billing",
    "probabilities":{"billing":0.9998,"technical":0.0002,"sales":7e-6,"other":9e-6},
    "confidence":0.9983}},
 "usage":{"input_tokens":154,"output_tokens":1}}
```

Against our decoder in `internal/engine/ollaya/decide.go` (read, not changed):
- `systemOneResponse` = `{answers: map[string]rawAnswer, error, code}`.
- `rawAnswer` = `{type, choice, probabilities, noul, score, confidence}`.

Verdict: **the per-answer decoding is reusable as-is.** Ollama's answer objects carry exactly
`type` / `choice` / `probabilities` / `confidence` (and would carry `noul`/`score` for those
types), which `mapAnswers` already handles. Two envelope differences, neither fatal:
1. Ollama adds top-level `model` and `usage{input_tokens,output_tokens}` — our struct ignores
   unknown fields, so decoding still works; `usage` is simply dropped (Ollaya has no `usage`).
2. **Error shape differs.** Ollama returns `{"error":"…"}` with an HTTP status (404 for an
   unloaded model, 400 for a bad request) and **no `code`** field; our decoder keys off `code`
   for a structured message and falls back to the status otherwise — so Ollama errors would be
   reported by status + `error` text, never by `code`. `503 QUEUE_FULL` (which our retry keys
   on) was **not** observed/!tested for Ollama (unverified whether Ollama emits that code).

Precision: Ollama reports `details.quantization_level` = `Q8_0` (gguf), **not** `F32`/`F16`.
`engine.Loaded.Precision` is a free-form string, so it would carry `Q8_0`; any logic expecting
`F32`/`F16` (Ollaya/onnx) would not match.

---

## 7. Auth: no server-side API key

Ran the server with `-e OLLAMA_API_KEY=s3cr3t-test` (mirroring how we set `OLLAYA_API_KEY`):

```console
GET  /api/tags       (no key)      → 200
POST /v1/systemone   (no key)      → 200
GET  /api/tags       (wrong Bearer)→ 200
server log: "security: no API key is set and CORS allows all origins"
```

**Ollama has no built-in server-side API-key gate.** `OLLAMA_API_KEY` is a *client* credential
for Ollama **Cloud** (`Authorization: Bearer` to `ollama.com`), not a local server auth switch;
the docs state plainly "Local requests do not need an API key." Setting it on the server does
nothing — unauthenticated and wrong-key requests still return 200.

Consequence for `spec.auth`: we today map `spec.auth.apiKeySecretRef` → `OLLAYA_API_KEY` and the
prober/evaluator send the key, and the auth e2e asserts 401-without / 200-with. **An `ollama`
engine cannot honor `spec.auth`** — there is no server-side key to enforce. The engine should
**reject `spec.auth` for `engine: ollama`** (surface `Ready=False`, a clear reason) rather than
silently accept a key that provides no protection. (Engine-contract implication under Requests.)

---

## 8. Liveness / readiness endpoints and load failure

- `GET /` → `200`, body `Ollama is running`. (Same idea as Ollaya's `GET /`.)
- `GET /api/version` → `{"version":"0.35.1"}`.
- `GET /api/tags` → 200 with the catalog (store contents), independent of whether a model is
  loaded — so, like Ollaya, a port/`/api/tags` probe is **not** sufficient for model-readiness.
- Load failure: `POST /v1/systemone` with a model not in the store → **HTTP 404**
  `{"error":"model \"…\" not found, try pulling it first"}`. A successfully-loaded model is the
  only thing that appears in `/api/ps`; a failed load never shows up there. So the gate logic
  "poll `/api/ps`, require the expected model present" transfers directly.

---

## 9. Conclusion

### Gate requirement → Ollama field → verified?

| Gate needs | Ollaya (today) | Ollama 0.35.1 | Verified? |
|---|---|---|---|
| Model is actually loaded | `/api/ps` entry exists | `/api/ps` entry exists | ✅ yes |
| Expected **digest** | `/api/ps.digest` (64-hex) | `/api/ps.digest` (64-hex) = sha256(manifest) | ✅ yes |
| Digest resolvable w/o daemon | registry HTTP + sha256 | `registry.ollama.ai` HTTP + sha256 | ✅ yes |
| Expected **device** | `/api/ps.device` (`cpu`/`cuda:0`) | **no `device` field**; infer from `size_vram` (0 = CPU) | ⚠️ CPU yes, **GPU unverified** |
| Pinned (won't evict) | `expires_at == null` | `expires_at` = far-future sentinel (not null) | ✅ observed, mapping differs |
| Pull-by-digest guard | rejected → pull by tag | rejected (`invalid model name`) → pull by tag | ✅ yes |
| RO serving store | works; pull/delete fail | works; pull/delete fail (read-only FS) | ✅ yes |
| Decide for the evaluator | `/v1/systemone` | `/v1/systemone`, answer object identical | ✅ answer yes; request schema differs |
| Server-side API key | `OLLAYA_API_KEY` enforced | **none** (local has no auth) | ✅ (confirmed absent) |

**Bottom line:** the gate is **feasible** on Ollama for CPU — digest matching works end to end,
and the "loaded on the expected device" check can be done via `size_vram` instead of a `device`
string. The product's core value (pin-to-digest, prefetch to a RO store, model-aware readiness)
survives. The gaps are a **different device signal** (`size_vram`, GPU behaviour unverified),
**no server-side auth** (so `spec.auth` must be rejected for this engine), and a **different
`/v1/systemone` request schema** (`instructions` required, flat `criteria`). None of these block
an engine; they are engine-specific mappings behind the existing contract.

### Open questions

1. **GPU/`device` on Ollama (highest priority):** verify on a real GPU what `/api/ps` reports
   (`size_vram` vs `size`, and whether partial offload `0 < size_vram < size` is possible for
   decision models). The gate's device check for Ollama hinges on this. The CUDA image/tag was
   not exercised here.
2. `expires_at` sentinel vs null: confirm Ollama never returns `null`, so an engine maps
   "pinned" from a far-future `expires_at` rather than `== null`.
3. `503 QUEUE_FULL` (or any structured `code`): does Ollama emit it under load? Our decider's
   queue-retry keys on 503; unverified for Ollama.
4. Model licenses: `tev1` layers include two `vnd.ollama.image.license` blobs — per-model
   license text, as with Laya; what to state in the docs.

### Engine-contract implications (NOT changing `engine.go`; see Requests in the report)

- `engine.Loaded.Device` is a device **string**; an Ollama engine has no such string and would
  derive the device class from `size_vram`/`size`. The contract's `Inspect` → `Loaded.Device`
  is fine as the *output* type, but confirm the gate compares device
  **classes** (`cpu`/`cuda`), not raw strings, so an Ollama engine can report `cuda`/`cpu`
  synthesised from `size_vram` (today `normalizeDevice` only parses `cuda:<n>` strings).
- `spec.auth` has no meaning for Ollama; the contract/validation should allow an engine to
  **reject** auth (there is no `Decider`/auth capability for a no-auth runtime).
- `Loaded.Precision` would carry gguf quant levels (`Q8_0`), not `F32`/`F16`.

### Caveats / unverified (collected)

- GPU / `device:cuda` / CUDA image path — **unverified** (arm64 Mac, no GPU; no CUDA tag run).
- A tag actually moving within the spike window — **unverified** (not re-pulled).
- `fsGroup` on a root-provisioned RW prefetch PVC for Ollama — **unverified** (only RO serving
  hardened here); expect spike-001's provisioner-dependent result to apply.
- `503 QUEUE_FULL` / structured error `code` for Ollama — **unverified**.
- Only `tev1:0.8b` (gguf/qwen35) exercised; `nimble` and larger `tev1` sizes not run.
