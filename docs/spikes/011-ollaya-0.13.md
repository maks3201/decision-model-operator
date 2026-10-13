# Spike 011 - Ollaya 0.13.0 ships pull by digest

Tested 2026-10-13 on macOS arm64 (OrbStack, Docker 29.8.2) against
`ghcr.io/ollaya-dev/ollaya:0.13.0`, re-running the spike 010 checklist and focusing on the new
pull-by-digest path. CPU path only (the `-cuda`/`-cuda12` images are amd64-only; no GPU host).
Upstream v0.13.0 (2026-10-13) closes ollaya-dev/ollaya#64: the CLI and `/api/pull` accept
`<name>[:<tag>]@sha256:<hex>`, and ollaya.dev now serves `/v2/<ns>/<model>/manifests/sha256:<hex>`.
Design is in upstream `docs/decisions/0006-manifest-retention.md`.

## Verdict

**0.13.0 is drop-in compatible with 0.12.x** for everything already shipped, and it unlocks the
exact-revision prefetch that spike 009 wanted. Required engine changes are additive:

1. **Version->digest table + default constant**: add `0.13.0`, `0.13.0-cuda`, `0.13.0-cuda12`
   index digests (below). Nothing in the serving/prefetch PodSpec changes.
2. **Prefetch by pinned name (the point of the release)**: the prefetch Job can pull
   `<model>:<tag>@sha256:<digest>` instead of `<tag>` + a verify-after-pull. The manifest is
   verified against the digest **before** anything is stored, and a moved tag no longer silently
   returns the new version. This makes the seeded-manifest workaround (spike 009) unnecessary for
   restoring an exact revision: a lost store rebuilds from the digest alone, as long as the
   author's Hugging Face repo and the retained blobs are still up.
3. **`UpstreamTagMoved` can be retired on the digest path**: a tag pull is no longer how we pin.
   The failure mode becomes `DIGEST_MISMATCH` (502) / `MODEL_NOT_FOUND` (404) from the runtime,
   surfaced before any write.

**One thing to watch for the readiness gate (not a break, but a behaviour change).** `/api/ps`
`name` and `model` echo the name the model was **loaded** under. Loaded by `laya:en` -> `name=laya:en`.
Loaded by `laya:en@sha256:<hex>` (which the prefetch would now use) -> `name=laya:en@sha256:<hex>`.
The bare `digest` field is unchanged (bare hex) in both cases. Our gate compares name + digest +
device; if `Warmup` starts using a pinned name, the engine's `Inspect` name comparison must tolerate
the `@sha256:<hex>` suffix (or key off the `digest` field, which stays bare). Flagged to the engine
owner under Requests - no e2e change forces it yet.

**Compatibility break with 0.12.x: none.** Tags behave exactly as before; the digest syntax is new
surface. Clients older than 0.13.0 keep working against the registry (the `sha256-*` manifest files
are invisible to them). Nothing in 0.12.x stops working on 0.13.0.

## Image index digests (for the version->digest table)

| Tag | Index digest (manifest list) | Arches |
|---|---|---|
| `0.13.0` | `sha256:31f749def61165d7bbb15def4e3d799a403461e0b8b777f359a924bb9b845ce6` | linux/amd64 + linux/arm64 |
| `0.13.0-cuda` | `sha256:03ba657824d300e1cbcf05e5ef67045e52cdfc2b86caebbbc434a63168490d24` | linux/amd64 only |
| `0.13.0-cuda12` | `sha256:e2787b27bfd6e8d611c536fb11f167962fabca2b1fd9f532944fd94e78d6edfd` | linux/amd64 only |

Per-arch manifest digests of `0.13.0`: amd64
`sha256:338a4ffed57cce157195cd3eb99c6f79fd1d9eb91276556e87a712f9112403a3`, arm64
`sha256:c0c2e718b77a5a36df0bb6a3dcd2b6226a75100b136f533b0d59c1d50163fcf3` (the two
`unknown/unknown` entries are the attestation manifests). Entrypoint `[/usr/bin/ollaya] [serve]`,
UID 1000:1000, `OLLAYA_HOST=0.0.0.0:11435`, `OLLAYA_MODELS=/home/ollaya/.ollaya/models` -
unchanged from 0.12.x.

## 0.12.1 vs 0.13.0 table

| Fact | 0.12.1 (spike 010) | 0.13.0 | Difference |
|---|---|---|---|
| Image tags / arches | `:0.12.1` amd64+arm64; `-cuda`/`-cuda12` amd64-only | **same** (`:0.13.0` amd64+arm64; `-cuda`/`-cuda12` amd64-only) | none |
| Entrypoint / UID / port / env | `[/usr/bin/ollaya] [serve]` 1000:1000, 11435, `OLLAYA_MODELS` | **identical** | none |
| `GET /` liveness | 200 | 200 (version `0.13.0`) | none |
| `--read-only` root + RO store serve | works | **works** (`version="0.13.0" models=/models`; warmup loads, `/api/ps` shows the digest; the benign `server.pid` write warning is unchanged from 0.12.x) | none |
| `ollaya pull laya:en` (tag) | cold ~48s, warm 0.29s | cold ~45s, warm 0s | none (host noise) |
| `laya:en` manifest bytes / sha256 | `c305a927...5e9d`, 3089 B | **identical** (`c305...5e9d`, 3089 B) | none |
| sha256(manifest) == `/api/ps` digest | equal | **equal** | none |
| `pull <name>:<tag>@sha256:<hex>` | **rejected** (invalid name) | **accepted**: fetches `/manifests/sha256:<hex>`, verifies, stores under the tag | NEW (#64 shipped) |
| `pull <name>@sha256:<hex>` (no tag) | rejected | **accepted**: stored under tag `sha256-<hex>`, lists as `name@sha256:<hex>` | NEW |
| `/api/pull {"model":"...@sha256:<hex>"}` | n/a | **200 `{"status":"success"}`** | NEW |
| `/api/ps` field set | `name,model,size,digest,device,details,expires_at,context_length,size_vram` | **identical set** | none |
| `/api/ps` `name`/`model` value | the tag (`laya:en`) | **echoes how it was loaded**: bare tag -> `laya:en`; pinned -> `laya:en@sha256:<hex>`. `digest` stays bare hex. | CHANGED on the pinned path (gate note above) |
| `/api/ps` device / precision | `cpu` / `F32` | `cpu` / `F32` | none |
| `/v1/systemone` response shape | `answers.<q>.{type,choice,confidence,probabilities}` | **identical** (`choice=billing, confidence=0.855`); top-level `model` echoes the pinned name | shape none; `model` echo as above |
| Moved-tag detection | tag pull returns new bytes -> operator `UpstreamTagMoved` | tag pull still returns new bytes; **a digest pull restores the exact old revision** (see below) | NEW recovery path |
| `OLLAYA_REGISTRY` -> local mirror | tag pull offline | **digest pull offline** through a mirror that serves the digest route (routes below) | NEW route needed on a mirror |

## Measured detail per checklist item

### 2. Pull by digest - what is stored, timing, idempotency

```
# already-populated store, pull by pinned name: idempotent, instant
$ ollaya pull laya:en@sha256:c305a927...5e9d        -> exit 0, ~0s

# cold (after `ollaya rm laya:en`): fetches by digest, stores under the tag
$ ollaya rm laya:en ; ollaya pull laya:en@sha256:c305...5e9d   -> exit 0, ~42s
$ ollaya list        -> NAME=laya:en  ID=c305a9276531  SIZE=853 MB
  store: manifests/ollaya.dev/library/laya/en          (stored under the TAG)

# no tag: stored under tag sha256-<hex>, lists as name@sha256:<hex>
$ ollaya pull laya@sha256:c305...5e9d  -> exit 0
$ ollaya list        -> laya@sha256:c305a9276531   c305a9276531   853 MB
  store: manifests/ollaya.dev/library/laya/sha256-c305a9276531...5e9d

# HTTP /api/pull by digest
$ curl -s -w '%{http_code}' /api/pull -d '{"model":"laya:en@sha256:c305...5e9d","stream":false}'
  -> {"status":"success"}  HTTP=200
```

Works on an already-populated store (idempotent) and when the local `laya:en` tag holds a different
manifest (the digest pull overwrites the tag with the digest-named manifest - see item 4).

### 3. Errors - explicit, before the stream, nothing written

```
# unknown digest
$ ollaya pull laya:en@sha256:0000...0000               -> CLI exit 1
  Error: model "laya:en@sha256:0000...0000" not found in registry ollaya.dev:
         it has no manifest with that digest
$ curl -s -w '%{http_code}' /api/pull -d '{"model":"laya:en@sha256:0000...0000"}'
  -> {"error":"... it has no manifest with that digest","code":"MODEL_NOT_FOUND"}  HTTP=404
  store: unchanged (no sha256-0000... file written)

# manifest whose bytes do not hash to the requested digest (local mirror serving wrong bytes)
$ ollaya pull laya:en@sha256:aaaa...aaaa               -> CLI exit 1
  Error: the registry served manifest sha256:f6888fde... for "laya:en@sha256:aaaa...aaaa",
         not the one its digest names; nothing was stored
$ curl -s -w '%{http_code}' /api/pull -d '{"model":"laya:en@sha256:aaaa...aaaa"}'
  -> {"error":"... not the one its digest names; nothing was stored","code":"DIGEST_MISMATCH"}  HTTP=502
  store: unchanged
```

`curl --fail` distinguishes both from success (404 / 502 vs 200). Nothing is written in either case.

### 4. Moved tag - digest restores the exact old revision

Local mirror in "tag moved" mode: `/manifests/en` (the tag) serves a different manifest, while
`/manifests/sha256:<old>` still serves the old one.

```
$ ollaya pull laya:en                      -> exit 1 (gets the moved manifest; here it was a different byte string)
$ ollaya pull laya:en@sha256:c305...5e9d   -> exit 0  (restores the OLD revision under the `en` tag)
  store: manifests/.../laya/en  sha256 = c305a9276531...5e9d  (the old digest, byte for byte)
$ curl /api/decide -d '{"model":"laya:en@sha256:c305...5e9d","keep_alive":-1}'
$ curl /api/ps -> name=laya:en@sha256:c305...5e9d  digest=c305a9276531...5e9d  device=cpu
```

So after a tag moves upstream, the operator can rebuild the exact revision from the recorded digest,
which spike 009 could not. The restore depends on the author's Hugging Face repo (weights) and the
retained blobs on ollaya.dev still being available.

### 5. `/api/ps` / `/api/decide` / `/v1/systemone` with a pinned name

- On a store that only ever held the bare tag, loaded by `laya:en`: `/api/ps` -> `name=laya:en`,
  `model=laya:en`, `digest=c305...` (bare), `device=cpu`. No digest suffix.
- Loaded by `laya:en@sha256:<hex>`: `/api/ps` -> `name` **and** `model` =
  `laya:en@sha256:c305...5e9d`; `digest` still bare hex.
- `/v1/systemone` with a pinned `model`: top-level `model` echoes `laya:en@sha256:<hex>`; `answers`
  shape unchanged.

This matches the maintainer's note that "response `model` fields carry the digest". Impact: our gate
(engine `Inspect` -> `Loaded.Name` + `Loaded.Digest` + `Loaded.Device`) must not require
`Loaded.Name == "laya:en"` once the prefetch/warmup uses a pinned name; the robust key is
`Loaded.Digest` (unchanged, bare hex) plus a name prefix match.

### 6. `OLLAYA_REGISTRY` mirror - routes a digest pull needs

Driving `ollaya pull laya:en@sha256:<hex>` through a mirror (`OLLAYA_REGISTRY=http://mirror`), the
client makes:

```
GET /v2/                                             (registry ping; may be skipped if the base is known)
GET /v2/library/laya/manifests/sha256:<hex>          (verified against <hex> before any read)
GET /v2/library/laya/blobs/sha256:<hex>  x N          (only when the manifest's blob "urls" are absent;
                                                       with Range / parallel chunks)
```

The manifest-by-digest route is the new requirement. **Our e2e mirror does not serve it today.**
`hack/mirror/main.go` routes `/manifests/<tag>` by reading the on-disk file
`manifests/<host>/<repo>/<tag>`; a request for `.../manifests/sha256:<hex>` reads a file named
`sha256:<hex>` which does not exist, so it 404s (verified: `/manifests/en` -> 200,
`/manifests/sha256:<hex>` -> 404). To cover a digest pull in e2e the mirror must map the
`sha256:<hex>` path segment to the on-disk `sha256-<hex>` manifest (as it already does for blob
names), or serve a store populated by a digest pull (which writes `manifests/.../sha256-<hex>`).
Because the mirror strips the manifest "urls" (so blobs come from the mirror), the served bytes -
and therefore the digest the client verifies against - differ from ollaya.dev; a digest-pull e2e
must compute the expected digest from the mirror's served bytes (the existing spec already does this
via `mirrorManifestDigest`), not hard-code `c305...`.

### 7. RO store serve / `GET /` / port / UID / entrypoint

Unchanged from 0.12.x: `--read-only` root + RO store mount serves (`GET /` 200, warmup loads,
`/api/ps` reports the digest and `device=cpu`), port 11435, UID 1000, entrypoint
`/usr/bin/ollaya serve`.

## Registry retention (from the maintainer, verified where possible)

`laya:en` has pointed to `sha256:c305a927...5e9d` since 2026-09-27, so the digest the operator
recorded on earlier versions is this one. `curl -fsI https://ollaya.dev/v2/library/laya/manifests/sha256:c305...5e9d`
returns 200 (`cache-control: public, max-age=0, must-revalidate`, an `ETag`, **no** `Content-Type`
and **no** `Docker-Content-Digest` header - the client hashes the body, matching decision 0006).
Retention covers every `laya:en` since the move to ollaya.dev; manifests from 0.3.1 and earlier
(blobs on the old `ollaya.cobanov.dev` origin) are not kept. A store backup remains the only fully
self-controlled copy, because weights are still fetched from the author's Hugging Face repo at the
pinned commit.

Two limits from the maintainer, relevant to us: a digest pins the manifest it names, so for the
router `laya:latest` it pins the router, not its targets - pin `laya:en` / `laya:multilingual`
directly; and weights still depend on the upstream Hugging Face repo staying up.

## Environment

- macOS arm64, OrbStack (Docker 29.8.2).
- `ghcr.io/ollaya-dev/ollaya:0.13.0` (arm64), client 0.13.0.
- A ~80-line throwaway Python registry (`http.server`) served a captured `laya:en` store to test the
  digest route, DIGEST_MISMATCH (wrong bytes at a chosen digest), and the moved-tag restore (tag
  route serves a different manifest while the digest route serves the old one), reachable from the
  ollaya container over a Docker network via `OLLAYA_REGISTRY`. The e2e mirror
  (`hack/mirror/main.go`) was run standalone against the same store to confirm the digest-route 404.
- No kind cluster (container-level facts only).
- All spike containers, volumes, the Docker network and temp files were removed after testing; the
  `:0.13.0` image is kept (also used by kind/CI).

## Could not verify

- CUDA (`-cuda`/`-cuda12`): amd64-only, no GPU host (same as spikes 008/010). The `/api/ps` device
  on a real GPU after a digest pull is unverified.
- amd64 runtime: tested on arm64 only (0.13.0 cpu is multi-arch).
- Retention of older `laya:en` builds end to end: only the current `c305...` digest was fetched from
  ollaya.dev; the claim that 17 older manifests are kept was not exercised against the live site.
- Whether every blob a digest pull needs is still served by ollaya.dev for an older `laya:en`: not
  tested (would need a known older digest and its blobs).
