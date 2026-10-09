# Testing

This project runs its tests in three tiers so a pull request gets fast, decisive
signal while the slow, environment-heavy suites run on a schedule or before a
release. This page is the source of truth for which suite runs when; the GitHub
Actions workflows under `.github/workflows/` implement it.

## Tiers

| Tier | When | What it must catch |
|---|---|---|
| **PR** | every pull request and push to `main` | compile/type errors, unit and controller (envtest) regressions, the core end-to-end lifecycle, lint, security, RBAC widening, docs build |
| **Nightly** | scheduled (daily, ~03:00-04:00 UTC) and `workflow_dispatch` | things too slow or too broad for a PR: other model families, the Kubernetes version matrix, HA, chaos, chart-upgrade |
| **Release** | `release-please` -> `release.yml` on a `v*` tag | upgrade from the previous release, Helm-chart upgrade, OLM bundle, artifact smoke; GPU smoke is manual |

A suite is placed in the lowest tier that still gives useful signal without
blowing the PR time budget. Moving a suite between tiers is a change to the
workflows and to this table.

## PR tier (blocking)

Runs on every `pull_request` (and on push to `main`). These are the required
checks for merge.

| Suite | Workflow | Notes |
|---|---|---|
| unit + envtest (+ `-race`) | `test.yml` (`unit + envtest`) | `make test` over every package except `/e2e`; the gate check is `tests-required` |
| RBAC guard | `test.yml` (`RBAC guard` step) | `go test ./test/rbac/...`; fails on an over-broad or unreviewed ClusterRole rule (see below) |
| core E2E shards | `test-e2e.yml` | `!nightly` kustomize shards (lifecycle, mirror, eval, auth, queue) + the helm lifecycle shard + the multi-node `recreate` shard; gate check `e2e-required` |
| lint | `lint.yml` | golangci-lint, gofmt, go vet, actionlint, shellcheck |
| security | `security.yml`, `codeql.yml` | gitleaks, govulncheck, Trivy, CodeQL |
| chart lint | `test-e2e.yml` (`helm lint + template`) | -- |
| chart upgrade | `test-e2e-upgrade-chart.yml` | also runs nightly; on a PR it guards chart schema/values regressions |
| docs build | `docs.yml` | `mkdocs build --strict` on docs changes |

A docs-only change (only `docs/**` or `*.md`) skips the heavy jobs; the gate
jobs (`tests-required`, `e2e-required`) still report success so a required check
never hangs.

### E2E shards

The per-PR E2E set is split by Ginkgo `Describe` label into parallel shards, each
on its own kind cluster, so the serial model-loading cost runs in parallel rather
than as one long job:

| Shard | Label filter | Cluster |
|---|---|---|
| lifecycle | `!nightly && lifecycle` | single node |
| mirror | `!nightly && mirror` | single node (free-disk) |
| eval | `!nightly && eval` | single node |
| auth | `!nightly && auth` | single node |
| queue | `!nightly && queue` | single node (free-disk) |
| recreate | `recreate` | **two nodes** (`hack/kind-recreate.yaml`, free-disk) |
| helm | `!nightly` | single node, Helm install |

The `recreate` container (RWO co-location + the Recreate rollout strategy) needs
more than one node, so it runs on its own cluster built from
`hack/kind-recreate.yaml` via the `KIND_CONFIG` passthrough in `hack/kind-up.sh`.
It carries both the `recreate` and `nightly` labels, so the single-node `!nightly`
shards never select it; it is excluded from the nightly model-matrix leg
(`!ha && !chaos && !recreate`), which runs on a single node.

## Nightly tier

Scheduled daily and on `workflow_dispatch`. A failure turns the run red; it does
not open issues.

| Suite | Workflow | Scope |
|---|---|---|
| model matrix | `test-e2e-nightly.yml` (`test-e2e`) | the `!nightly` E2E set against non-laya models (`gliclass:latest`, `nli:latest`), single node, `!ha && !chaos && !recreate` |
| Kubernetes version matrix | `test-e2e-nightly.yml` (`k8s-versions`) | the per-PR laya:en set on each supported Kubernetes minor (`hack/k8s-versions.env`) |
| HA | `test-e2e-ha.yml` | the `ha`-labelled specs (replica disruption, co-location failure modes) |
| chaos | `test-e2e-chaos.yml` | the `chaos`-labelled specs (fault injection) |
| chart upgrade | `test-e2e-upgrade-chart.yml` | nightly cron (also a PR check) |

Fleet / scale stress specs, when added, belong here (label them so they are
excluded from the PR set and from the single-node nightly legs that cannot run
them).

## Release tier

Driven by `release-please.yml`, which -- after it creates the GitHub Release --
gates the release on these before invoking `release.yml`:

| Suite | Workflow | Scope |
|---|---|---|
| full E2E of the release commit | `test-e2e.yml` (awaited via `wait-for-e2e`) | the exact release SHA's E2E run must be green |
| previous-version upgrade + artifact smoke | `test-e2e-upgrade.yml` (`workflow_call`) | upgrade from the previous **final** release to the release commit; skipped for the first release |
| OLM bundle | `release.yml` | bundle build/validate |
| GPU smoke | `test-e2e-gpu.yml` | **manual** (`workflow_dispatch`); CUDA runners are not in the automated gate |

Helm-chart upgrade is covered both as a PR/nightly check (`test-e2e-upgrade-chart.yml`)
and implicitly by the release upgrade path.

## The RBAC guard

`test/rbac/role_guard_test.go` is a cluster-free `go test` that parses the two
committed copies of the operator ClusterRole -- `config/rbac/role.yaml` (kustomize)
and `charts/decision-model-operator/files/role.yaml` (Helm) -- and fails on:

- any `*` apiGroup, resource or verb;
- `secrets` with any verb other than `get`;
- `configmaps` with `list` or `watch`;
- any rule not in the allow-list committed in the test (and any allow-list rule
  missing from a role file);
- the two role files drifting out of sync.

The ClusterRole is generated (`make manifests`), so widening the operator's
access shows up as a new rule the guard rejects. Granting a new permission is
therefore a deliberate, reviewable edit of `allowedRules` in the test -- it cannot
slip in with a controller change. The guard runs both as its own fast PR step and
inside `make test`.
