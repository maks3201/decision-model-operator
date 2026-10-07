# Spike 009 — exact-digest prefetch and moved-tag recovery

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
