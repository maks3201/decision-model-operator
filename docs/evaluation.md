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
  boolean for `noul`, a level for `score`.
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
      maxECE: "0.10"            # absolute calibration gate (optional)
      maxECEIncrease: "0.05"    # calibration must not worsen this much vs baseline (optional)
      maxCases: 500             # cap on dataset lines used (optional, default 500)
```

Gate fields (all decimal strings in [0,1]):

| Field             | Meaning | If omitted |
|-------------------|---------|------------|
| `minAccuracy`     | Minimum candidate accuracy to promote. | required |
| `maxAccuracyDrop` | Max accuracy drop vs the stable baseline. | no drop constraint |
| `maxECE`          | Max absolute Expected Calibration Error. | absolute ECE gate off |
| `maxECEIncrease`  | Max ECE increase vs the stable baseline. | relative ECE gate off |
| `maxCases`        | First N dataset lines scored. | 500 |

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

**ECE** (expected calibration error) and the **Brier** score come from the
calibration metrics: they measure whether the model's stated confidence matches
how often it is right. Accuracy alone does not catch an over-confident model;
the ECE gates do.

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
| `Promoted` | `PromotionPending` | a `Manual`-policy candidate passed its gate and waits for approval |
| `Promoted` | `Promoted` | the candidate was promoted and is serving |
| `Ready` | `Ready` | the stable revision is serving and healthy |
| `Ready` | `CandidateRejected` | a candidate was rejected; the stable revision keeps serving |
| `Degraded` | `EvaluationFailed` | the candidate failed a gate (message names it) |
| `Degraded` | `CacheNotShareable` / `StoreTerminating` / `StoreLost` / `StorePrefetchFailed` | a store issue that does not stop serving |
| `Degraded` | `SecretNotAllowed` / `DownloadTokenInvalid` | a referenced Secret is not usable |

## Events

Events are human-readable and name the model and short digest instead of a bare
revision hash, so `kubectl describe dm` reads like a changelog:

```text
Normal   EvaluationStarted   evaluating revision a1b2c3d4e5 against 500 cases
Warning  EvaluationFailed    candidate kev:en@sha256:1a2b3c4d failed evaluation: accuracy 0.8700 < minAccuracy 0.9200; laya:en@sha256:c305a927 keeps serving
Warning  RolledBack          candidate kev:en@sha256:1a2b3c4d rejected (EvaluationFailed): ...; laya:en@sha256:c305a927 keeps serving
Normal   Promoted            promoted laya:en@sha256:c305a927 (accuracy 0.9400, baseline 0.9300)
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

Approve the candidate by annotating the DecisionModel with the candidate's hash:

```sh
hash=$(kubectl get dm support-router -o jsonpath='{.status.candidateRevision.hash}')
kubectl annotate dm support-router decisionmodel.io/promote="$hash" --overwrite
```

- The annotation value **must equal** `status.candidateRevision.hash`; an approval
  for any other hash is ignored.
- On approval the controller promotes the candidate (`Promoted=True/Promoted`),
  then **removes the annotation** once the promotion is durable
  (`status.stableRevision.hash` matches).
- Anyone who can `update` the DecisionModel can approve — it is the same trust
  level as editing the spec, not a separate permission.

Caveats:

- The **first** revision of a DecisionModel (no stable yet) is **not** held — it
  promotes without approval, because there is nothing serving to protect.
- While a candidate is parked, changing the **evaluation policy** — any
  `rollout.evaluation` threshold (`minAccuracy`, `maxAccuracyDrop`, `maxECE`,
  `maxECEIncrease`), the `datasetRef`, or `maxCases` — makes the recorded result
  stale, so the operator **leaves `AwaitingPromotion` and re-evaluates** under the
  new policy rather than promoting on the old result. A change to `datasetRef` or
  `maxCases` runs a fresh evaluation; a thresholds-only change re-applies the gates
  to a freshly read result. If you set the `decisionmodel.io/promote` approval in
  the **same** edit as a policy change, the approval is not consumed on the
  re-evaluation cycle, but the annotation **stays**: once the candidate re-parks
  under the new policy the **same approval promotes it on the next reconcile**, on
  the freshly recorded result. You do not need to re-approve.
- Editing the dataset **content in place** (same ConfigMap/Secret, same keys) while
  a candidate is parked is **not** detected — the ConfigMap/Secret is not watched
  and the policy hash does not change. Bump the `decisionmodel.io/retry` annotation
  to force a re-evaluation against the new content (or change `datasetRef`).
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
