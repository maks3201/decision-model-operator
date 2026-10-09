# Releasing

How decision-model-operator is versioned, tested and published. Only the maintainer merges the release PR and pushes tags.

Modeled on what mature operators do: cert-manager (supported-releases policy, tested vs supported Kubernetes
versions), Flux / external-secrets (signed images, SBOM), and the kubebuilder compatibility policy.

## 1. Versioning

- SemVer tags `vX.Y.Z`, no component prefix. One version for the image, `install.yaml` and the Helm chart
  (`Chart.yaml` `version` = `appVersion`, bumped by Release Please).
- Pre-1.0 (`0.y.z`):
  - **minor** (`0.2.0`): features, and any change a user must act on — CRD schema tightening (new validation
    that can reject existing objects), renamed/removed fields or flags, changed defaults, RBAC changes.
    Each needs an entry under "Upgrade notes" in the release notes.
  - **patch** (`0.2.1`): bug fixes only; no new fields, no new validation, no RBAC changes.
- API `decisionmodel.io/v1alpha1` may change between minors (it is alpha). Never in a patch.
- Commit messages are Conventional Commits (`feat:`, `fix:`, `feat!:` / `BREAKING CHANGE:`); Release Please
  derives the version and CHANGELOG from them, so the commit type must match the rule above.

## 2. Release candidates

Every minor release starts as a release candidate; patches may skip it when the fix is covered by E2E.

1. Tag `vX.Y.0-rc.N` from `main` (GitHub pre-release, image `:vX.Y.0-rc.N`).
2. Validate the RC against the checklist in §3 using **the published artifacts**, not a source build.
3. Problems → fix on `main`, next `rc.N+1`. No final release from code that was not an RC (for minors).
4. Final `vX.Y.0` = the Release Please PR rebased onto the last RC's commit (`gh pr update-branch <N> --rebase`):
   the release commit adds only the changelog and version bumps on top of that RC, so the application code is
   identical to the RC. Any other commit between the RC and the release PR means a new RC.

## 3. Go / no-go checklist

Automated (must be green on the exact release commit):

- [ ] `Lint`, `Tests`, `E2E Tests` (kustomize + helm legs) on `main` at that SHA.
- [ ] Upgrade E2E: install the previous release from its published artifacts, create a DecisionModel, upgrade
      to the candidate, the DecisionModel stays `Ready` and is **not** re-rolled (no new revision).
- [ ] Release-artifact smoke: `install.yaml` and the chart `.tgz` from the RC apply on a clean kind cluster.
- [ ] Kubernetes versions: E2E on the oldest and newest *tested* versions (§5).

Manual (the maintainer ticks them in the release PR description):

- [ ] GPU smoke (`device: cuda`, one model, gate True on `cuda:<n>`) when the release touches the engine,
      prober, device handling or scheduling. Record cluster, GPU and result in the release PR.
- [ ] Release notes: "Upgrade notes" section present (or "none"), breaking changes called out.
- [ ] Docs match the release (quickstart commands, flags, chart values; `make api-docs` drift green).
- [ ] No open issue labelled `release-blocker`.

## 4. Publishing

- Release Please keeps a release PR open on `main`. **Merge it only when §3 is complete.** It is fine for it
  to stay open for days.
- `main` is protected: changes land only through pull requests with green Tests and Lint. The release PR is
  opened by the GitHub Actions bot, so its workflow runs wait for approval: approve them (Actions → run →
  "Approve and run", or `gh api -X POST repos/<owner>/<repo>/actions/runs/<id>/approve`), then merge with squash.
- On merge: tag + GitHub Release, then `publish-assets` builds the multi-arch image and uploads
  `install.yaml` and the chart — only after E2E on that commit is green.
- The image, the OCI chart and the release files are signed keyless with cosign (GitHub OIDC); the image carries
  an SPDX SBOM and SLSA provenance, and the release files get a SLSA provenance attestation
  (`*.intoto.jsonl`). Verification commands are in the README ("Verify the release").

## 5. Support policy

- Pre-1.0: only the **latest minor** gets fixes (as patch releases). Users upgrade to get fixes.
- Kubernetes: *tested* = versions E2E runs on every release; *supported* = we fix reported bugs.
  Target: tested = the three most recent Kubernetes minors kind supports; supported = the same.
  The current list lives in the README [Compatibility](README.md#compatibility) table and is updated each minor.
- Upgrades: from the previous minor (N-1 → N) is tested in CI. Skipping minors is not tested; upgrade one minor
  at a time. Downgrades are not supported.
- Deprecation: a field, flag, default or reason string that users may rely on is deprecated for at least one
  minor (Warning Event or log, release-notes entry) before it is removed or changed.
- Runtime: each release states the Ollaya image version it pins (`ghcr.io/ollaya-dev/ollaya:<ver>`) and was
  tested with. Bumping it is a minor change (new runtime behaviour), unless it is a pure security patch.

## 6. Bad releases

- Never delete or move a published tag, never re-push an image tag.
- Fix forward with a patch release. Edit the bad release's notes: "Do not use — superseded by vX.Y.Z" and why.
- Security issue: follow `SECURITY.md`; patch release first, advisory after.

## 7. Cadence

No fixed schedule. A minor when a roadmap milestone is done (`docs/ARCHITECTURE.md` §12), patches as needed.
Do not use releases to debug a cluster: iterate with RCs or source builds, release what passed §3.
