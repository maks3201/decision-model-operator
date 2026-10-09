# Eval-gated rollout

When `spec.rollout.evaluation` is set, a new revision is not promoted just
because its Pods became model-ready — it must first pass an evaluation against a
golden dataset. This catches a model that is accurate on average but
mis-calibrated (over-confident), and a candidate that regressed against the
current stable revision.

## Golden dataset (JSONL)

The dataset is JSON Lines: one JSON object per line, each a case with `state`,
`questions` (the same shapes you POST to `/v1/systemone`), and `expected` (the
correct answer per question id).

```json
{"state": "I was charged twice, please refund the second charge.", "questions": {"dept": {"type": "choice", "criteria": {"billing": "payments and refunds", "technical": "bugs"}}}, "expected": {"dept": "billing"}}
{"state": "The app crashes on login.", "questions": {"dept": {"type": "choice", "criteria": {"billing": "payments and refunds", "technical": "bugs"}}}, "expected": {"dept": "technical"}}
```

- `expected` values match the answer type: a label string for `choice`, a
  boolean for `noul`, an **integer level** for `score`.
- **Score questions.** Ollaya answers a `score` question with an expected-value
  level `score = Σ i·pᵢ` (a float, usually between integer levels) plus a
  per-level `probabilities` map and a `confidence` (docs/api.md). A case counts as
  correct when `|predicted − expected| ≤ tolerance` (levels). The tolerance is, in
  order: a per-case `"tolerance": {"<qid>": 0.5}` map, else
  `spec.rollout.evaluation.scoreTolerance`, else the default `0.5` (which means
  "rounds to the expected level"). A score answer contributes to **ECE/Brier only
  when** the runtime returns a usable probability distribution (keys `0..n-1`
  summing to ~1); otherwise it counts toward **accuracy only**. A score question
  is no longer silently scored as wrong (it was before this release).

  ```jsonl
  {"state": "…", "questions": {"sev": {"type": "score"}}, "expected": {"sev": 3}, "tolerance": {"sev": 1}}
  ```
- A malformed line invalidates the whole dataset (reason `DatasetInvalid`; phase
  `RolledBack` if a stable revision is serving, else `Failed`). An invalid dataset
  is a **permanent** failure: the revision is recorded in `status.failedRevision`
  and not retried automatically. The operator does **not** watch the ConfigMap /
  Secret holding the dataset, so after you fix it you must either change a
  model-affecting spec field or bump the `decisionmodel.io/retry` annotation (see
  [Retrying a failed rollout](#retrying-a-failed-rollout)) for the operator to pick
  up the corrected dataset. (A dataset object that is simply **missing** is treated
  as transient and retried on its own, in case it is created shortly after.)
- Only the first `maxCases` lines are used (default 500).

Store it in a ConfigMap (or Secret) and point `datasetRef` at the key:

### Secret capability labels

A Secret the operator reads must carry the label for the capability it is used
for — one label grants exactly one capability:

| Capability | Label | Allows |
|---|---|---|
| Engine API key | `decisionmodel.io/api-key: "true"` | `spec.auth.apiKeySecretRef` |
| Eval dataset | `decisionmodel.io/eval-dataset: "true"` | a `datasetRef.secretRef` dataset |
| Download token | `decisionmodel.io/download-token: "true"` | `spec.cache.downloadTokenSecretRef` |

A ConfigMap dataset needs no label. A dataset **Secret** must carry
`decisionmodel.io/eval-dataset: "true"`.

> **Transition (one release).** A dataset Secret labelled only with the old
> `decisionmodel.io/api-key: "true"` is still accepted, but the operator emits a
> `DeprecatedSecretLabel` Warning Event naming the label to add. Add
> `decisionmodel.io/eval-dataset: "true"` — it will become required. A dataset
> Secret with neither label is **held** (not failed): phase `Evaluating`,
> `Evaluated=False` reason `SecretNotAllowed`, until the label is added or the
> evaluation timeout expires.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: support-router-eval
data:
  cases.jsonl: |
    {"state": "I was charged twice, please refund.", "questions": {"dept": {"type": "choice", "criteria": {"billing": "payments and refunds", "technical": "bugs"}}}, "expected": {"dept": "billing"}}
    {"state": "The app crashes on login.", "questions": {"dept": {"type": "choice", "criteria": {"billing": "payments and refunds", "technical": "bugs"}}}, "expected": {"dept": "technical"}}
```

## Configure the gate

```yaml
apiVersion: decisionmodel.io/v1alpha1
kind: DecisionModel
metadata:
  name: support-router
spec:
  model: laya:en
  device: cpu
  rollout:
    evaluation:
      datasetRef:
        configMapRef:
          name: support-router-eval
          key: cases.jsonl
      minAccuracy: "0.90"       # candidate must reach at least this accuracy
      maxAccuracyDrop: "0.02"   # and not drop more than this vs the stable baseline (optional)
      minMacroF1: "0.80"        # macro-averaged F1 floor over choice/bool questions (optional)
      maxMacroF1Drop: "0.05"    # macro-F1 must not drop more than this vs baseline (optional)
      maxECE: "0.10"            # absolute calibration gate (optional)
      maxECEIncrease: "0.05"    # calibration must not worsen this much vs baseline (optional)
      maxCases: 500             # cap on dataset lines used (optional, default 500)
      scoreTolerance: "0.5"     # correctness band for score questions, in levels (optional, default 0.5)
```

Gate fields (all decimal strings in [0,1]):

| Field             | Meaning | If omitted |
|-------------------|---------|------------|
| `minAccuracy`     | Minimum candidate accuracy to promote. | required |
| `maxAccuracyDrop` | Max accuracy drop vs the stable baseline. | no drop constraint |
| `minMacroF1`      | Minimum candidate macro-F1 (choice/bool questions). | absolute macro-F1 gate off |
| `maxMacroF1Drop`  | Max macro-F1 drop vs the stable baseline. | relative macro-F1 gate off |
| `maxECE`          | Max absolute Expected Calibration Error. | absolute ECE gate off |
| `maxECEIncrease`  | Max ECE increase vs the stable baseline. | relative ECE gate off |
| `maxCases`        | First N dataset lines scored. | 500 |
| `scoreTolerance`  | Correctness band for `score` questions, in levels (not in [0,1]). | 0.5 |

**Baseline** is the current stable revision scored on the same dataset; the
`*Drop` / `*Increase` gates compare the candidate against it. The baseline is run
**only when a relative gate is configured** (`maxAccuracyDrop` or
`maxECEIncrease`); with only absolute gates (`minAccuracy`, `maxECE`) the baseline
is not run at all, so it never delays promotion or causes an `EvaluationTimeout`.
On a first rollout (no stable revision) there is no baseline either, so only the
absolute gates apply.

> **Known limitation — one API key per DecisionModel.** The operator uses a single
> `spec.auth.apiKeySecretRef` for every revision, including the baseline run against
> the **stable** revision. If you change `spec.model` (a new candidate) **and** the
> API-key value in the *same* edit, the stable revision's Pods still run with the
> old key, so the baseline scoring request gets `401` and the baseline run fails.
> With a relative gate configured (`maxAccuracyDrop` / `maxECEIncrease`) that
> missing baseline rolls the candidate back. **Change the model and the API key in
> two separate steps** (rotate the key first, let it roll — see
> [Rotating the API key](quickstart.md#rotating-the-api-key) — then change the
> model), or temporarily drop the relative gates for the key-rotation rollout.
> Per-revision keys are planned for a later version (ARCHITECTURE §9).

The API-key Secret must contain the referenced key: `apiKeySecretRef.optional` is
not supported (rejected by CEL), and a labelled Secret missing the key is reported
as Degraded (reason `APIKeyInvalid`) with no workloads changed, so the prober and
the serving Pod never read different keys.

**ECE** (expected calibration error) and the **Brier** score come from the
calibration metrics: they measure whether the model's stated confidence matches
how often it is right. Accuracy alone does not catch an over-confident model;
the ECE gates do.

## Macro-F1: catching class imbalance

Accuracy hides class imbalance. If 90% of the golden cases expect `billing`, a
router that answers `billing` every time scores 0.90 accuracy while never getting
a single `refund` or `sales` case right. **Macro-F1** averages the per-class F1
with every class weighted equally, so a class the model never predicts drags the
score down regardless of how rare it is.

```yaml
spec:
  rollout:
    evaluation:
      datasetRef: { configMapRef: { name: support-router-eval, key: cases.jsonl } }
      minAccuracy: "0.90"
      minMacroF1: "0.80"      # a majority-only predictor fails here even at 0.90 accuracy
      maxMacroF1Drop: "0.05"  # or require it not to regress vs the stable baseline
```

- Macro-F1 is computed over **choice and bool (`noul`) questions only** — the ones
  with a class label to average. `score` questions have no class and are excluded.
  The overall macro-F1 is the **unweighted mean of the per-question macro-F1**: a
  question with one case weighs the same as one with 500.
- `maxMacroF1Drop` is a relative gate: like `maxAccuracyDrop` it is enforced only
  when a stable revision exists to provide a baseline. When it is the only relative
  gate configured, the operator **does run a baseline evaluation of the stable** on
  the same dataset to compare against (the macro-F1 comes from that same baseline
  run as `baselineAccuracy`, no second pass).
- **No classifiable question → the gate fails** — for `minMacroF1`. If `minMacroF1`
  is set but the dataset has no choice or bool question (nothing to average), the
  candidate is **rejected** with reason `ClassificationUnavailable`, never promoted
  on a silent macro-F1 of 0 — the same fail-closed rule the calibration gate uses
  when no case produces a probability distribution. `maxMacroF1Drop` without a
  stable revision is simply **skipped** (like `maxAccuracyDrop`): there is no
  baseline to compare against, so it does not fail or block the first rollout.
- A missing, empty or unscorable answer still counts as a **miss for its expected
  class** (it does not silently vanish), so a candidate that skips the minority-class
  cases cannot score macro-F1 1.0 on the classes it did answer.
- `status.evaluation` records `macroF1`, `baselineMacroF1` (when a baseline ran),
  `classifiableCases`, and a bounded `questions` list (at most 20 entries, worst
  macro-F1 first, with `truncated: true` when more existed) for per-question
  visibility. A gate message names the failing comparison, e.g.
  `macroF1 0.7100 < minMacroF1 0.8000` or
  `macroF1 dropped 0.0600 (baseline 0.8400) > maxMacroF1Drop 0.0500`.


## Reading the result

The evaluation outcome is recorded in `status.evaluation`:

```sh
kubectl get dm support-router -o jsonpath='{.status.evaluation}' | jq
```

```json
{
  "revision": "a1b2c3d4e5",
  "accuracy": "0.9400",
  "ece": "0.0700",
  "brier": "0.0500",
  "baselineAccuracy": "0.9300",
  "baselineEce": "0.0600",
  "cases": 500,
  "failedCases": 30,
  "minAccuracy": "0.90",
  "maxAccuracyDrop": "0.02",
  "maxEce": "0.10",
  "maxEceIncrease": "0.02",
  "result": "Passed",
  "reason": "",
  "datasetDigest": "9f2c…",
  "approvalId": "a1b2c3d4e5f6",
  "completedAt": "2026-09-28T00:00:00Z"
}
```

`result` is `Passed` or `Failed`; on a failure `reason` carries the gate message
(e.g. `accuracy 0.8700 < minAccuracy 0.9200`). The `minAccuracy` / `maxAccuracyDrop`
/ `maxEce` / `maxEceIncrease` fields echo the gate that was applied, so the
recorded result is self-describing.

- Pass -> the candidate is promoted (phase goes `Evaluating` -> `Ready`;
  `status.lastPromotionTime` is stamped and the `Promoted` condition is
  `True`/`Promoted`).
- Fail -> the candidate is rejected and the stable revision keeps serving. The
  phase ends `RolledBack` (or `Failed` when there was no stable revision), the
  `Ready` condition stays `True` with reason `CandidateRejected`, and the
  `Degraded` condition is `True` with reason `EvaluationFailed`. The rejected
  revision is recorded in `status.failedRevision` with its `reason`, `message`
  and `failedAt`. A `Warning` event is emitted too.

Other evaluation reasons: `DatasetInvalid` (bad JSONL / missing dataset),
`EvaluationTimeout` (scoring exceeded the deadline), `EvaluationUnsupported`
(the engine has no `Decide` capability). See the condition-reason table below and
the phase table in [quickstart.md](quickstart.md).

### Missing or unreadable dataset

A dataset that is not yet usable does **not** fail the revision immediately — the
candidate **holds** in `Evaluating` (no traffic moves, its Deployment is kept) and
resumes on its own once the dataset becomes readable, with **no spec change or
retry annotation** needed:

| Situation | `Evaluated` reason while held | Resolves when |
|---|---|---|
| dataset ConfigMap/Secret does not exist | `DatasetNotFound` | the object is created |
| the referenced key is absent | `DatasetKeyNotFound` | the key is added |
| dataset Secret missing the `decisionmodel.io/api-key: "true"` label | `SecretNotAllowed` | the label is added |

A `Warning` event is emitted once per cause while held. A hold is bounded by the
`Evaluating` timeout (`spec.rollout.timeouts.evaluating`, default 10m): if the
dataset is still unusable when it expires, the candidate is rolled back (the
stable revision keeps serving). A transient API error on the read (timeout, 5xx,
throttling, RBAC) is retried with workqueue backoff and never records a failed
revision; it is bounded by the same timeout.

Only a genuinely **invalid** dataset (malformed JSONL) is a permanent
`DatasetInvalid` failure. Fixing the content in place is not auto-detected (the
dataset is not watched); set the `decisionmodel.io/retry` annotation to re-run.

## Watching a rollout from the CLI

`kubectl get dm` shows the stable (`Active`) and in-flight (`Candidate`) models,
the last evaluation `Accuracy`, the `Phase`, and the `Ready` condition `Reason`:

```text
$ kubectl get dm
NAME             ACTIVE    CANDIDATE   ACCURACY   PHASE               REASON              AGE
support-router   laya:en   kev:en      0.8700     RolledBack          CandidateRejected   6d
billing-router   laya:en               0.9400     Ready               Promoted            20d
```

`-o wide` adds the active digest (short), device, model-ready replicas and the
last promotion time.

## Conditions and reasons

The condition **types** are stable; the **reasons** tell the story and are part of
the API (safe to alert on):

| Type | Reason | Meaning |
|---|---|---|
| `Resolved` | `Resolved` | tag resolved to a digest |
| `Resolved` | `ResolveFailed` / `ModelNotFound` | registry resolution failed |
| `Cached` | `Cached` | weights present in the store |
| `Cached` | `Caching` / `PrefetchFailed` / `CacheTimeout` | prefetch in progress or failed |
| `ModelReady` | `ModelReady` | all serving Pods report the expected digest on the expected device |
| `ModelReady` | `DigestMismatch` / `DeviceMismatch` / `NotPinned` / `ProbeError` | the readiness gate failed (e.g. silent CPU fallback) |
| `Evaluated` | `EvaluationRunning` | golden-dataset scoring is in progress |
| `Evaluated` | `EvaluationPassed` | the candidate met every configured gate |
| `Evaluated` | `EvaluationSkipped` | no evaluation configured; promotion follows readiness |
| `Evaluated` | `BaselineUnavailable` | a relative gate needs a stable baseline that is not reachable yet |
| `Evaluated` | `DatasetNotFound` / `DatasetKeyNotFound` / `SecretNotAllowed` | the golden dataset is not yet readable; the candidate holds (see "Missing or unreadable dataset") |
| `Evaluated` | `DatasetChanged` | the dataset content changed while awaiting promotion; re-evaluating |
| `Promoted` | `PromotionPending` | a `Manual`-policy candidate passed its gate and waits for approval |
| `Promoted` | `Promoted` | the candidate was promoted and is serving |
| `Stabilizing` | `Stabilizing` | the new stable is in its post-promotion stabilization window (previous revision kept) |
| `Stabilizing` | `StableRolling` | a quorum shortfall is ignored because the stable Deployment is mid-rollout (replicas scale-up, key rotation, scheduling change, restart) |
| `Stabilizing` | `PostPromotionUnhealthy` | the new stable looks unhealthy in the window (debouncing before rollback) |
| `Ready` | `Ready` | the stable revision is serving and healthy |
| `Ready` | `CandidateRejected` | a candidate was rejected (or rolled back after promotion); the surviving revision keeps serving |
| `Degraded` | `EvaluationFailed` | the candidate failed a gate (message names it) |
| `Degraded` | `PostPromotionUnhealthy` | the new stable was rolled back to the previous revision during the stabilization window |
| `Degraded` | `StoreTerminating` / `StoreLost` / `StorePrefetchFailed` | a store issue that does not stop serving |
| `Degraded` | `SecretNotAllowed` / `DownloadTokenInvalid` | a referenced Secret is not usable |

## Events

Events are human-readable and name the model and short digest instead of a bare
revision hash, so `kubectl describe dm` reads like a changelog:

```text
Normal   EvaluationStarted   evaluating revision a1b2c3d4e5f60718 against 500 cases
Warning  EvaluationFailed    candidate kev:en@sha256:1a2b3c4d failed evaluation: accuracy 0.8700 < minAccuracy 0.9200; laya:en@sha256:c305a927 keeps serving
Warning  RolledBack          candidate kev:en@sha256:1a2b3c4d (revision a1b2c3d4e5f60718) rejected (EvaluationFailed): ...; laya:en@sha256:c305a927 keeps serving
Normal   Promoted            promoted laya:en@sha256:c305a927 (revision 7c9f0a1b2c3d4e5f) (accuracy 0.9400, baseline 0.9300)
```

The event **reason** (the first column after the type) is stable and safe to key
alerts on; the message wording may change.

## Promotion policy

`spec.rollout.promotion` selects how a model-ready candidate is promoted:

| Value | Behaviour |
|---|---|
| `Automatic` | promote as soon as the candidate is model-ready (and, when evaluation is set, has passed the gate) |
| `EvaluationGated` | requires `rollout.evaluation`; the gate decides promotion (rejected by CEL without evaluation) |
| `Manual` | hold the passed candidate in `AwaitingPromotion` until a human approves it |

When `promotion` is unset the effective policy is `EvaluationGated` if
`rollout.evaluation` is set, else `Automatic`.

> The boolean `spec.rollout.manualPromotion: true` is **deprecated** but still
> works as an alias for `promotion: Manual`. Setting `promotion` and
> `manualPromotion: true` to disagreeing values is rejected by CEL.

## Manual promotion (hold for approval)

Set `spec.rollout.promotion: Manual` to hold a candidate that passed its gate
until a human approves it, instead of promoting automatically. It works with or
without `spec.rollout.evaluation`: the candidate is built, becomes model-ready,
passes the gate (if configured), and then **waits**.

```yaml
spec:
  rollout:
    promotion: Manual
    evaluation:            # optional; Manual works without it too
      datasetRef: { configMapRef: { name: support-router-eval, key: cases.jsonl } }
      minAccuracy: "0.90"
```

While waiting:

- The phase is `AwaitingPromotion` and the condition `Promoted` is
  `False` with reason `PromotionPending`; its message names the annotation and the
  candidate hash. The **stable revision keeps serving** on the Service the whole
  time.
- There is **no timeout** — a parked candidate waits indefinitely.
- `promotion` is not part of the revision hash, so changing it does not
  create a new revision; switching it to `Automatic` releases a parked candidate
  to promote.

Approve the candidate by annotating the DecisionModel with its **approvalID**
(shown in `status.evaluation.approvalId`, in the `AwaitingPromotion` Event, and in
the `Promoted` condition message):

```sh
id=$(kubectl get dm support-router -o jsonpath='{.status.evaluation.approvalId}')
kubectl annotate dm support-router decisionmodel.io/promote="$id" --overwrite
```

- The approvalID is `sha256(revisionHash + policyHash + datasetDigest)[:12]`, so it
  identifies **this revision with this evaluation result**. It changes whenever the
  revision, the gate policy, or the dataset content changes — an approval cannot
  carry over to a re-evaluation. Without `rollout.evaluation` (a manual-only hold)
  it is derived from the revision hash alone.
- A value that is neither the current approvalID nor (without evaluation) the bare
  revision hash is a **stale approval**: it is ignored with a `StaleApproval`
  Warning that names the current approvalID.
- **With `rollout.evaluation` configured, only the approvalID promotes.** A bare
  `status.candidateRevision.hash` is **not** honoured — it would otherwise carry an
  approval given for an old result across a re-evaluation of the same revision — and
  is ignored with a `DeprecatedApproval` Warning naming the current approvalID.
- **Without evaluation** (a manual-only hold) the bare revision hash still works for
  one release and emits a `DeprecatedApproval` Warning; prefer the approvalID.
- On approval the controller promotes the candidate (`Promoted=True/Promoted`),
  then **removes the annotation** once the promotion is durable.
- Anyone who can `update` the DecisionModel can approve — it is the same trust
  level as editing the spec, not a separate permission.

Caveats:

- The **first** revision of a DecisionModel (no stable yet) is **not** held — it
  promotes without approval, because there is nothing serving to protect.
- While a candidate is parked, changing the **evaluation policy** — any
  `rollout.evaluation` threshold (`minAccuracy`, `maxAccuracyDrop`, `maxECE`,
  `maxECEIncrease`, `scoreTolerance`), the `datasetRef`, or `maxCases` — **or
  editing the dataset content in place** (same ConfigMap/Secret) makes the recorded
  result stale, so the operator **leaves `AwaitingPromotion` and re-evaluates**
  rather than promoting on the old result. A dataset content change is detected by
  re-reading its bytes on each reconcile while parked (reason `DatasetChanged`,
  Warning Event); there is no watch, so the detection delay is at most one regate
  interval (~60s). Re-evaluation rotates the approvalID, so an approval set for the
  old identity does **not** carry over — re-approve with the new approvalID.
- Otherwise, to force a fresh candidate, change a model-affecting field (model,
  digest, device, resources, image, scheduling) or switch `promotion` to
  `Automatic` and back to `Manual`.
- A `promote` annotation whose hash never became stable (e.g. you approved, then
  reverted the spec) **stays on the object** and would approve a later revision
  that happens to resolve to that same hash. Remove a stale approval by hand:
  `kubectl annotate dm support-router decisionmodel.io/promote-`.

How Argo CD and Flux surface `AwaitingPromotion` is described in
[gitops.md](gitops.md) (Argo CD reports it `Suspended`; a Flux `healthCheckExprs`
check can treat it as in-progress).

## Post-promotion stabilization

A promotion is not the end of the risk: a new stable can look model-ready at the
instant of promotion and fall over a minute later. `spec.rollout.stabilization`
(a Go duration, default `5m`, `0` disables it) keeps the **previous** revision's
Deployment running — scaled to its replicas but out of the Service — for a window
after promotion, so the operator can switch traffic back instantly if the new
stable turns out unhealthy.

```yaml
spec:
  rollout:
    stabilization: "5m"   # keep the previous revision this long after promotion (0 = off)
```

During the window (condition `Stabilizing=True`), the operator rolls back to the
previous revision if the new stable:

- drops below the model-ready **quorum** `max(1, ceil(replicas/2))` for longer than
  a short debounce (30s) — a brief dip during a rolling restart does not trip it; or
- reports a readiness-gate **`DigestMismatch` / `DeviceMismatch`** (wrong model
  loaded), or a container in **`CrashLoopBackOff`** — these roll back immediately,
  no debounce.

A quorum shortfall caused by an **intentional in-place rollout** of the stable
(replicas scale-up, API-key rotation, scheduling change, or `kubectl rollout
restart`) is **not** treated as a failure: while the stable Deployment is
mid-rollout — `generation` not yet observed, or the `Progressing` condition is
`True` with a reason other than `NewReplicaSetAvailable` (e.g. `ReplicaSetUpdated`)
— the condition is `Stabilizing=True` with reason **`StableRolling`** and the
debounce does not advance. A completed rollout settles to `Progressing=True,
NewReplicaSetAvailable` and stays there even if its Pods later go unready, so a
post-rollout health failure is **not** masked and still rolls back after the
debounce. Immediate failures (gate mismatch, crash loop) always roll back.
Protection stays bounded: a rollout that exceeds its Deployment
`progressDeadlineSeconds` (`Progressing=False, ProgressDeadlineExceeded`) is counted
as a failure and rolled back. If the stabilization window elapses while a rollout
is still in progress, the window is extended (the previous revision is kept) until
the rollout settles healthy or trips the progress deadline.

On rollback the Service switches back to the previous revision (its Pods are still
running, so there is no cold start), the new revision is recorded in
`status.failedRevision` with reason `PostPromotionUnhealthy`, the phase is
`RolledBack`, a Warning Event `RolledBackAfterPromotion` is emitted, and
`decisionmodel_rollouts_total{result="rolled_back_after_promotion"}` is
incremented. The rolled-back revision is not retried automatically (a spec change
or the `decisionmodel.io/retry` annotation is required).

If the window passes healthy, the operator emits `Stabilized`, drops the
`Stabilizing` condition, and garbage-collects the previous revision as usual.
Notes:

- The check runs on each reconcile from persisted status
  (`status.previousRevision`) and live cluster state, so it is correct across an
  operator restart and uses no in-memory timers.
- A spec change during the window starts a new candidate as usual and **ends** the
  window (the previous revision is then collected normally).
- `stabilization: 0` restores the previous behaviour: the previous revision is
  removed after a short endpoint-gap grace, with no rollback window.
- The previous revision's store PVC lives through the window (so the footprint is
  ~2× the model during it), the same as during a rollout.

## Rollout timeouts

Each rollout phase has a progress timeout; if the phase does not complete in
time the rollout fails (and rolls back to the stable revision if there is one).
Override them under `spec.rollout.timeouts` for large models that need longer on
a cold node:

```yaml
spec:
  rollout:
    timeouts:
      caching: "2h"       # Caching phase (and the prefetch Job's activeDeadlineSeconds)
      starting: "30m"     # Starting phase (and the per-Pod model warmup bound)
      evaluating: "20m"   # Evaluating phase (and each golden-dataset run)
```

| Field        | Bounds | Default |
|--------------|--------|---------|
| `caching`    | The Caching phase. Also set as the prefetch Job's `activeDeadlineSeconds`. | 30m |
| `starting`   | The Starting phase. Also bounds the background model warmup on each Pod. | 10m |
| `evaluating` | The Evaluating phase and each golden-dataset evaluation run. | 10m |

- Values are Go durations (`90m`, `2h`, `1h30m`) and must be between **1m and 24h**.
- `timeouts` is **not** part of the revision hash, so changing it does not create a
  new revision; the new phase timeouts apply on the next reconcile.
- An **existing prefetch Job keeps the `activeDeadlineSeconds` it was created
  with** — a changed `caching` does not retroactively extend a pull that is
  already running. To apply a new caching timeout to an in-flight pull, retry with
  the `decisionmodel.io/retry` annotation (see below) so the Job is recreated.

Suggested starting points for large GGUF models (e.g. the ~19 GB `clef:flash`
/ 9B-class models) — **estimates**, tune to your registry bandwidth and node
disk/CPU:

```yaml
spec:
  rollout:
    timeouts:
      caching: "2h"     # ~19 GB pull + sha256 verify over a slow link
      starting: "30m"   # loading tens of GB into memory on a cold node
      evaluating: "30m" # larger model = slower per-case decisions
```

## Retrying a failed rollout

A failed revision is not retried automatically. Change the spec (a new revision)
or bump the `decisionmodel.io/retry` annotation to clear the failure once — see
[quickstart.md](quickstart.md#retrying-a-failed-rollout).
