# E2E tests

End-to-end tests run on a local [kind](https://kind.sigs.k8s.io/) cluster against the real Ollaya
runtime image. The suite builds and loads the operator image, installs the CRDs and manager, then
exercises the reconcile lifecycle, eval-gated rollout, API-key auth, and the in-cluster registry
mirror.

## Running locally

```sh
# Whole !nightly suite on one cluster (what a single kustomize run used to do):
make test-e2e E2E_LABEL_FILTER='!nightly'

# One shard / one container while iterating (fast):
make test-e2e E2E_LABEL_FILTER='!nightly && eval'      KIND_CLUSTER=dmo-e2e-eval
make test-e2e E2E_LABEL_FILTER='!nightly && auth'      KIND_CLUSTER=dmo-e2e-auth
make test-e2e E2E_LABEL_FILTER='!nightly && (lifecycle || mirror)' E2E_MIRROR=1 KIND_CLUSTER=dmo-e2e-life
```

`make test-e2e` leaves the cluster **down** afterwards. Run one cluster at a time on a laptop
(memory). Ginkgo Describe labels: `lifecycle`, `auth`, `eval`, `mirror`, and `nightly` (the two
rollout-policy containers). On CI the `!nightly` set is split into parallel shards by these labels;
the full serial suite still runs nightly to catch cross-spec interactions a split can hide.

## CI process rules (keep CI fast and cheap)

- Run `actionlint` and `shellcheck` locally before every push; batch fixes into one push per
  iteration rather than pushing per-fix.
- While a run is in flight, iterate on a single workflow with
  `gh workflow run <file> --ref <branch>` instead of re-running every workflow.
- Do not block on `gh run watch` for 20 minutes: start the watch in the background and work on the
  local part of the next task meanwhile; report when the run finishes.
- Iterate E2E locally on kind first (focus the specs you touch); push to CI once they pass. CI on
  amd64 stays the release gate — a local arm64 pass is not evidence for amd64/runner-specific issues
  (disk, CUDA).
