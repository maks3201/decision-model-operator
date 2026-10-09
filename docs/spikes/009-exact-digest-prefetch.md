# Spike 009 — exact-digest prefetch and moved-tag recovery

> **Correction (2026-10-09).** The Q4 claim below — that `ollaya pull <tag>` trusts an
> on-disk manifest and does **not** overwrite it, so a seed enables moved-tag recovery — is
> **wrong**. A clean reproduction on `ollaya:0.12.0` (see "Correction note" at the end of this
> file) shows `ollaya pull <tag>` **always re-resolves the tag and overwrites the on-disk
> manifest** with the tag's current bytes, both with an empty blobs dir and with the blobs
> already present. This matches upstream (ollaya-dev/ollaya#64). Consequently a manifest seed
> does **not** restore a moved tag: after the pull the store holds whatever the tag points to
> now, and the operator's post-pull digest check fails such a case with `UpstreamTagMoved`.
> Read the Q4 rows, the "moved-tag simulation" evidence, and the "moved-tag recovery WORKS"
> conclusion below as **superseded**; the manifest seed remains useful only as defence in depth
> (fail-fast on a bad recorded value before pulling), not as recovery.

Tested 2026-10-07 on macOS arm64 (OrbStack) against `ghcr.io/ollaya-dev/ollaya:0.10.0`
(the current default runtime) and `:0.12.0`, CPU path only. The question: prefetch
pulls by **tag** and then verifies `sha256(manifest)` against the recorded digest.
That is verify-after-pull, not content-addressed pinning. If a tag moves upstream
(`laya:en` → D'), can a lost store of a stable revision pinned to D still be
re-materialised to exactly D?

Commands use the host `curl`/`shasum` for registry probes (the image ships no
`curl`, confirmed in spike 001) and `docker exec` for the CLI/store probes.

## Fact table

| # | Question | 0.10.0 | 0.12.0 | Consequence |
|---|---|---|---|---|
| Q1 | `GET /v2/library/laya/manifests/sha256:<D>` on ollaya.dev | **307 → URL-encoded path → 404** (HTML page, ~11.8 KB), no manifest | **307 → 404** (same) | the registry has **no manifest-by-digest endpoint**; only by tag |
| Q1 | `GET …/manifests/en` (by tag) | 200, 3089 bytes, `sha256 = c305a927…e9d` | 200, identical bytes/digest | resolver keeps pulling by tag |
| Q2 | `ollaya pull "laya:en@sha256:<D>"` / `laya@sha256:<D>` | rejected: `invalid model name … expected [host/][namespace/]model[:tag]` | rejected (same) | no pull-by-digest, as spike 001 found on 0.7.3 |
| Q2 | `ollaya pull` flags | only `--insecure` (`pull --help`) | only `--insecure` | no digest flag |
| Q3 | manifest references | `config`/`graph.onnx`/`decision`/`calibration` blobs by `sha256` on `https://ollaya.dev/blobs/sha256-…`; **weights + tokenizer by Hugging Face repo+commit+file**, e.g. `https://huggingface.co/convaiinnovations/laya/resolve/aa8c91ca…/model.safetensors` | identical | the manifest is already content-addressed: each layer pins a `sha256` and the HF weights pin an immutable commit, so old content stays fetchable while the commit/blobs exist even after a tag moves |
| Q4 | pre-seed a fresh store with the manifest only (no blobs) at the tag path, then `ollaya pull <tag>` | pull resumes: fetches only the referenced blobs; `ollaya list` shows the model with `ID = c305a92765` (= D); manifest bytes unchanged | same | the store can be materialised from a persisted manifest |
| Q4 | with a manifest already on disk, does `pull <tag>` re-resolve the tag from the registry? | **No** — seeded `laya/en` tag path with `laya:multilingual`'s manifest (`2840506e…`), ran `ollaya pull laya:en` against the LIVE registry; manifest on disk stayed `2840506e…` and was not overwritten with the live `laya:en` manifest | **No** (same) | `pull` **trusts the on-disk manifest**; it does not re-resolve the tag when a manifest is present |

### Key evidence (0.10.0; 0.12.0 identical)

Manifest-by-digest is a 404, by-tag is the real manifest:

```
by-tag    http=200 size=3089
by-digest http=307         # location: /v2/library/laya/manifests/sha256%3Ac305…  -> 404 HTML (11788 bytes)
sha256(by-tag bytes) = c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d   # == D
```

Pull by digest rejected on both:

```
$ ollaya pull "laya:en@sha256:c305a92765…e9d"
Error: model: invalid model name "laya:en@sha256:c305…"; expected [host/][namespace/]model[:tag]
$ ollaya pull --help
Options:
      --insecure  Accepted for Ollama compatibility; use an http:// host in the name for dev registries
```

Manifest layers (abridged) — weights pinned to an immutable HF commit:

```json
"layers": [
  {"mediaType":"application/vnd.ollaya.graph.onnx","digest":"sha256:9dc0bb17…","urls":["https://ollaya.dev/blobs/sha256-9dc0bb17…"]},
  {"mediaType":"application/vnd.ollaya.weights","size":842609210,
   "urls":["https://huggingface.co/convaiinnovations/laya/resolve/aa8c91ca088ec597df95a0d1c76b3063cb2ae5e8/model.safetensors"]},
  {"mediaType":"application/vnd.ollaya.tokenizer",
   "urls":["https://huggingface.co/convaiinnovations/laya/resolve/aa8c91ca…/tokenizer/tokenizer.json"]}
]
```

Pull trusts the on-disk manifest (moved-tag simulation):

```
seeded at <laya/en> path = multilingual manifest BEFORE: 2840506e1f978aeb696a
=== pull laya:en on the seeded store (live registry) ===
manifest at <laya/en> AFTER pull: 2840506e1f978aeb696a
RESULT: TRUSTED on-disk manifest -> moved-tag recovery WORKS (pull did not re-resolve)
```

Pre-seed with manifest only (no blobs) → pull materialises the blobs, keeps D:

```
seed store before pull: (no blobs dir)
=== ollaya pull laya:en on the pre-seeded store ===
NAME      ID             SIZE     MODIFIED
laya:en   c305a9276531   853 MB   Less than a second ago
blobs now present: 9      # manifest sha256 unchanged = c305a927…
```

## Conclusion

- **Pull-by-digest does not exist** (neither a registry manifest-by-digest endpoint
  nor a CLI/name form), unchanged from 0.7.3 through 0.12.0. We cannot content-address
  the pull itself.
- **But a store can be recovered to an exact digest without pull-by-digest**: persist
  the manifest **bytes** for the recorded digest D, write them to the on-disk tag path,
  and run `ollaya pull <tag>` — the CLI trusts the on-disk manifest, fetches the exact
  blobs it references (immutable `sha256` blobs on the registry and an immutable HF
  commit for the weights), and the store ends up pinned to D regardless of where the tag
  points now. This is strictly better than pull-by-digest for recovery.
- The manifest layers are content-addressed; the only non-`sha256` reference (HF
  weights) is pinned to a commit, so old content survives a tag move as long as the
  commit and blobs exist upstream.

## Implemented now (engine)

The recovery mechanism above needs the controller to persist the manifest bytes, which
the engine does not receive today (`engine.Params` carries only `Model.Digest`). That is
a contract/controller change (see the report's Requests). What the engine can do without
it, and does in this change: tell a **moved tag** apart from a **corrupt download** when
the post-pull digest check fails, so the user sees why a lost store cannot be rebuilt by
re-pulling the tag.

- A successful pull whose manifest is well-formed (`schemaVersion: 2`) but whose digest
  ≠ the recorded one → reason `UpstreamTagMoved` (exit 5), message naming both short
  digests (`recorded <12hex>, registry now serves <12hex>`).
- A pull whose manifest is not well-formed → reason `DigestMismatch` (exit 4, corruption).
- Both are permanent (`podFailurePolicy` FailJob), so neither burns the retry budget.

Verified in the image (both reasons and the OK path):

```
case A: well-formed but DIFFERENT manifest  -> UpstreamTagMoved (recorded c305a9276531, now 2840506e1f97) exit 5
case B: corrupt manifest (garbage)          -> DigestMismatch   (recorded c305a9276531, got c7f910be1831) exit 4
case C: the real laya:en manifest           -> OK (digest matches)
```

## Follow-up (for the controller, not this spike)

Persist the resolved manifest bytes alongside the digest so store recovery of a stable
revision can pre-seed the manifest and re-pull by tag to materialise exactly D even after
the tag moved. See the task report's Requests for the proposed capability.

## Implementation results — rebuild from a seeded manifest

The engine now implements the pre-seed recovery the spike identified. `Resolve` fills
`engine.ModelRef.Manifest` with the exact bytes it hashed (`sha256(Manifest) == Digest`).
When the controller passes those bytes back in `Params.Model.Manifest`, `PrefetchJobSpec`
delivers them to the Job as a base64 env var (`MANIFEST_SEED_B64`, capped at 256 KiB — real
manifests are a few KB; a larger one falls back to pull-by-tag). The script verifies
`sha256(seed) == EXPECT_DIGEST` **before** writing, writes the seed to the on-disk tag path
(`manifests/<host>/<ns>/<model>/<tag>`, same layout helpers, mirror hosts included), then
`ollaya pull` (which trusts the on-disk manifest) and the existing post-pull digest check.

Proven on the real image (identical on `:0.10.0` and `:0.12.0`): a FRESH store (no blobs)
seeded with laya:en's recorded manifest, running the rendered prefetch script, then
`ollaya serve` from that store:

```
seeded manifest for c305a9276531…e9d at manifests/ollaya.dev/library/laya/en
pulled digest: c305a9276531…e9d
digest ok: c305a9276531…e9d        # script exit=0, 9 blobs fetched from the seed
# serve from the rebuilt store:
host liveness -> 200
load http=200
/api/ps: {"models":[{"name":"laya:en","digest":"c305a9276531…e9d","device":"cpu"}]}
```

So the store is rebuilt to the recorded digest from the manifest bytes alone — no
pull-by-digest, and no dependency on what the tag points to now. A seed whose sha256 does
not match the expected digest fails permanently BEFORE any write (verified: `reason:
DigestMismatch`, exit 4, manifest not written).

### Failure class: weights/commit gone upstream

The seed rebuilds the manifest, but `ollaya pull` still fetches the referenced blobs — the
registry `sha256` blobs and the HF commit-pinned weights (`.../resolve/<commit>/...`). If
that upstream content is deleted (the HF commit/file removed, or the registry blob gone),
the pull fails to fetch a layer. That surfaces as a pull error, not a digest mismatch, and
the script classifies it as `Transient` (exit 1, retried) — the Job keeps retrying until the
deadline, then the revision fails. This is correct: the content genuinely no longer exists,
and no local action can recover it; the operator cannot invent the weights. (A dedicated
"blobs gone" permanent reason would need the CLI to distinguish a 404-on-blob from a
transient network error, which it does not expose today — noted for a future upstream ask.)

## Correction note (2026-10-09) — Q4 re-check on 0.12.0

Re-tested on macOS arm64 (OrbStack) against `ghcr.io/ollaya-dev/ollaya:0.12.0`
(`client version is 0.12.0`), CPU path, no running daemon (`ollaya pull`/`ollaya list`
run standalone). The question Q4 got wrong: when a manifest is already on disk at the tag
path, does `ollaya pull <tag>` keep it, or overwrite it by re-resolving the tag?

Setup: a host store mounted at `/home/ollaya/.ollaya/models` (the default; `/models` is not
writable by the image's UID 1000). The two manifests, fetched by tag from the live registry:

```
laya:en          sha256 = c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d  (3089 bytes)
laya:multilingual sha256 = 2840506e1f978aeb696a6f84af43ecc7fca95754cb63a67bafe6132532d384eb  (3115 bytes)
```

### Experiment 1 — EMPTY blobs dir

Seed `laya:multilingual`'s manifest at the `laya/en` tag path, blobs/ empty, then
`ollaya pull laya:en`:

```
# seed
cp multilingual.json  <store>/manifests/ollaya.dev/library/laya/en
ls <store>/blobs      # empty

BEFORE pull: on-disk laya/en manifest = 2840506e1f97…  (multilingual)
$ ollaya pull laya:en           # exit 0
AFTER  pull: on-disk laya/en manifest = c305a9276531…  (laya:en)   <-- OVERWRITTEN
blobs now present: 9            # laya:en's blobs, incl. the 842,609,210-byte weights
                                # sha256-891102d3… (model.safetensors)
```

Result: the seeded multilingual manifest was **overwritten** with the live `laya:en`
manifest, and `laya:en`'s 9 blobs were downloaded. The on-disk file is byte-identical to
the live `laya:en` manifest.

### Experiment 2 — blobs PRESENT

Re-seed the multilingual manifest at the same `laya/en` path, this time with `laya:en`'s 9
blobs from Experiment 1 still in blobs/, then `ollaya pull laya:en`:

```
BEFORE pull: on-disk laya/en manifest = 2840506e1f97…  (multilingual), blob count = 9
$ ollaya pull laya:en           # exit 0
AFTER  pull: on-disk laya/en manifest = c305a9276531…  (laya:en)   <-- OVERWRITTEN
blob count after = 9            # nothing new to fetch; all laya:en blobs already present
$ ollaya list
NAME      ID             SIZE     MODIFIED
laya:en   c305a9276531   853 MB   10 seconds ago
```

Result: the manifest was **overwritten again**. The presence of the blobs does not change
the outcome — `pull <tag>` re-resolves the tag and rewrites the manifest regardless.

### Corrected conclusion

- `ollaya pull <tag>` **always re-resolves the tag from the registry and overwrites the
  on-disk manifest** with the tag's current bytes, in both the empty-blobs and
  blobs-present cases on 0.12.0. The original Q4 finding ("pull trusts the on-disk manifest;
  moved-tag recovery WORKS") does **not** reproduce and is withdrawn. The likely cause of the
  earlier result: the laya:en blobs were already present and the on-disk manifest observed
  "after" was in fact already the laya:en manifest (not the seeded one), so no change was
  seen. This re-check controls for that by diffing the exact on-disk sha256 before and after.
- A persisted manifest seed therefore **cannot** rebuild a moved tag: after the pull the
  store holds whatever `<tag>` points at now, so a tag that moved to D' leaves D' on disk,
  which the operator's post-pull digest check catches as `UpstreamTagMoved` (exit 5,
  permanent). The seed's only remaining value is defence in depth: it is verified against the
  recorded digest **before** the pull, so a bad recorded value fails fast (`DigestMismatch`)
  without a network round-trip. It is not a recovery mechanism.
- Exact-digest recovery still requires upstream pull-by-digest
  (`<name>:<tag>@sha256:<hex>` plus retained manifests), which no runtime in our supported
  range (0.7.3 … 0.12.0) provides (ollaya-dev/ollaya#64, still open). Until then the operator
  pins via tag-pull + post-pull sha256 verification, which fails loudly rather than drifting
  when a tag moves.

The "Implemented now (engine)", "Implementation results — rebuild from a seeded manifest",
and "Follow-up" sections above describe the seed as enabling moved-tag recovery; that
specific capability does not hold. The engine's moved-vs-corrupt classification
(`UpstreamTagMoved` vs `DigestMismatch`) and the fail-fast seed verification are correct and
unaffected.
