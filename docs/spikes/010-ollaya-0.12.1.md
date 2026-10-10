# Spike 010 — Ollaya 0.12.1 is a drop-in for 0.12.0

Tested 2026-10-10 on macOS arm64 (OrbStack, Docker 29.4.0) against
`ghcr.io/ollaya-dev/ollaya:0.12.1`, re-running the spike 008 checklist and comparing with the
0.12.0 baseline there. CPU path only (the `-cuda`/`-cuda12` images are amd64-only; no GPU host).
Upstream v0.12.1 (2026-10-09) adds the Credence model family and fixes weights downloads from
mirrors that ignore byte ranges (#67) ([release notes](https://github.com/ollaya-dev/ollaya/releases/tag/v0.12.1)).

## Verdict

**0.12.1 is drop-in compatible with 0.12.0** for everything the operator depends on. Every checked
fact is identical to 0.12.0 (digest, `/api/ps` fields and device, warmup, `/v1/systemone` shape,
RO-store serve, pull/idempotency, `pull @sha256:` rejection, moved-tag sha256 mismatch, `ollaya
list` with the `.seed-tmp` sibling, and `OLLAYA_REGISTRY` to a local mirror). No code change is
needed beyond the version→digest table and the default constant (done in the engine; chart/README
appVersion follow).

## 0.12.0 vs 0.12.1 table

| Fact | 0.12.0 (spike 008) | 0.12.1 | Difference |
|---|---|---|---|
| Image tags that exist | `0.12.0`, `-cuda`, `-cuda12` | `0.12.1`, `-cuda`, `-cuda12` | none (all three exist) |
| Arches | cpu amd64+arm64; `-cuda`/`-cuda12` amd64-only | **same** (cpu amd64+arm64; `-cuda` amd64-only; `-cuda12` amd64-only) | none |
| Entrypoint / cmd / UID | `/usr/bin/ollaya serve`, 1000:1000 | `[/usr/bin/ollaya] [serve] 1000:1000` | none |
| Port / `OLLAYA_HOST` / `OLLAYA_MODELS` | 11435 / `0.0.0.0:11435` / `/home/ollaya/.ollaya/models` | same | none |
| `GET /` liveness | 200 | 200 (up in ~1 s) | none |
| `--read-only` root + RO store serve | works | **works** (`version="0.12.1" models=/models`; warmup loads on a RO store, `/api/ps` shows the digest) | none |
| `ollaya pull laya:en` | exit 0, idempotent | cold ~47.9 s, second pull **0.29 s** | none (cold time is host/emulation noise) |
| `pull @sha256:` / `:tag@sha256:` | rejected, exit 1 | **rejected** (`invalid model name …; expected [host/][namespace/]model[:tag]`) — both forms | none (pull-by-digest #64 still unshipped) |
| Manifest for `laya:en` | `c305a927…5e9d`, 3089 B | **identical** (`c305a927…5e9d`, 3089 B) | none |
| sha256(manifest) == `/api/ps` digest | equal | **equal** (`c305…5e9d`) | none |
| `/api/ps` fields | `name,model,size,digest,device,details,expires_at,context_length,size_vram` | **identical set** | none |
| `/api/ps` device / precision | `cpu` / `F32` | `cpu` / `F32` | none |
| `/api/decide {keep_alive:-1}` warmup | `done_reason:"load"` | `done_reason:"load"` | none |
| `/v1/systemone` response shape | `answers.<q>.{type,choice,confidence,probabilities}` | **identical** (`type=choice, choice=billing, confidence=0.9357, probabilities present`) | none |
| `ollaya list` with a `.seed-tmp` sibling dir | lists the model (sibling ignored) | **lists `laya:en`, exit 0**; the hidden `.seed-tmp/` is not read as a tag | none |
| Moved-tag detection (overwrite manifest, re-check sha256) | mismatch → `UpstreamTagMoved` | **MISMATCH as expected** (new bytes `bafebd36…` ≠ recorded `c305…`); the prefetch verify-after-pull catches it | none |
| `OLLAYA_REGISTRY` → local mirror (`hack/mirror`) | resolve+pull offline through the mirror | **works**: pulled `library/laya:en` fully from the mirror (manifest + every blob, parallel byte-range on the 842 MB blob); `/api/ps` digest == the mirror's served (urls-stripped) manifest sha256 `5342f940…b0f1` | none |
| RSS (laya:en, warm, CPU) | ~3.08 GiB | **3.094 GiB** (`docker stats`) | none (noise) |
| `ollaya llama-devices` (Linux arm64) | `llama_cpp:0.5.0-dev`, `cuda_backend:null`, cpu only | **same** | none |

### Commands (trimmed)

```
$ docker image inspect ghcr.io/ollaya-dev/ollaya:0.12.1 --format '{{.Config.Entrypoint}} {{.Config.Cmd}} {{.Config.User}}'
[/usr/bin/ollaya] [serve] 1000:1000
# Env: OLLAYA_HOST=0.0.0.0:11435  OLLAYA_MODELS=/home/ollaya/.ollaya/models

$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.1          # linux/amd64 + linux/arm64
$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.1-cuda     # linux/amd64 only
$ docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:0.12.1-cuda12   # linux/amd64 only

# serve (RW store), pull, warmup, /api/ps:
INFO ollaya_server::http: Ollaya is running address=0.0.0.0:11435 version="0.12.1" models=/models
$ docker exec … ollaya pull laya:en            # cold ~47.9 s; second pull 0.29 s
$ curl .../api/decide -d '{"model":"laya:en","keep_alive":-1}'  → "done_reason":"load"
$ curl .../api/ps | jq '.models[0]|{name,digest,device,prec:.details.quantization_level,context_length,expires_at}'
  name=laya:en digest=c305a9276531…5e9d device=cpu prec=F32 context_length=512 expires_at=null
$ wc -c <store>/manifests/ollaya.dev/library/laya/en   → 3089
$ sha256sum …/laya/en                                  → c305a9276531…5e9d   # == /api/ps digest

# /v1/systemone (demo criteria shape):
$ curl .../v1/systemone -d '{"model":"laya:en","state":"…double charged…","questions":{"q":{"type":"choice","criteria":{…}}}}'
  answers.q = {type:choice, choice:billing, confidence:0.9357, probabilities:{…}}

# read-only store serve:
$ docker run --read-only -v store:/models:ro --tmpfs /home/ollaya/.ollaya … :0.12.1
  → GET / 200; warmup done_reason:load; /api/ps digest c305…5e9d device cpu

# pull @sha256: rejected:
$ ollaya pull laya@sha256:c305…       → Error: model: invalid model name …; expected [host/][namespace/]model[:tag]
$ ollaya pull laya:en@sha256:c305…    → same

# ollaya list with a .seed-tmp sibling:
$ mkdir /models/.seed-tmp && echo junk > /models/.seed-tmp/seed.ABC123
$ ollaya list   → NAME=laya:en ID=c305a9276531 SIZE=853 MB   (exit 0; .seed-tmp ignored)

# moved-tag simulation (verify-after-pull catches it):
$ printf '{"schemaVersion":2}' > …/laya/en ; sha256sum …/laya/en
  bafebd36189a… ≠ recorded c305a9276531…   → MISMATCH (operator → UpstreamTagMoved)

# OLLAYA_REGISTRY to a local mirror (hack/mirror serving the exported store on :8099):
$ docker run -e OLLAYA_REGISTRY=http://host.docker.internal:8099 … :0.12.1
$ ollaya pull laya:en
  mirror log: 200 /v2/ ; 200 manifest library/laya/en (urls stripped) ; 200 blob … range="bytes=…" (parallel)
  → ollaya list: host.docker.internal:8099/library/laya:en ; /api/ps digest 5342f940…b0f1
     (== sha256 of the mirror's urls-stripped manifest; resolve↔pull↔ps consistent offline)
```

## Environment

- macOS arm64, OrbStack (Docker 29.4.0).
- `ghcr.io/ollaya-dev/ollaya:0.12.1` (arm64), client 0.12.1.
- `hack/mirror` (`go run`, `MIRROR_STORE=<exported store>`, `MIRROR_ADDR=:8099`) for the
  `OLLAYA_REGISTRY` test; the store was `ollaya pull laya:en`-populated, then exported.
- No kind cluster (container-level facts only).
- All spike containers (`dmo-spike010-serve`, `-ro`, `-mirror`), the `go run` mirror, and volumes
  (`dmo-spike010-store`, `dmo-spike010-mstore`) and the exported store dir removed after testing;
  the `:0.12.1` image is kept (also used by kind/CI).

## Could not verify

- CUDA (`-cuda`/`-cuda12`): amd64-only, no GPU host (same as spike 008).
- amd64 runtime: tested on arm64 only (0.12.1 cpu is multi-arch).
- #67 (mirror byte-range download fix) end to end against a real range-ignoring mirror: the
  `hack/mirror` server supports Range, so the fix path (one-stream fallback) was not forced; the
  parallel byte-range pull through the mirror did succeed.
- `OLLAYA_THREads`/GGUF perf: out of scope here (covered for 0.12.0 in spike 008 §2).
