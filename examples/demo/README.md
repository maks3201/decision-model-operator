# Eval-gated rollout demo

This demo shows the one promise the operator exists for: **a candidate decision
model is evaluated before it receives production traffic — a regression is
blocked, a good model is promoted.**

![eval-gated rollout demo](../../docs/assets/demo.gif)

It runs end to end on a local [kind](https://kind.sigs.k8s.io/) cluster with the
real Ollaya runtime and three real registry models. Nothing is staged: every
number in the output is produced by the operator evaluating the models against
the golden dataset in this directory.

## The scenario

1. Deploy an eval-gated `DecisionModel` named `intent-router` whose stable model
   is `laya:en`. It routes an English support ticket to a department
   (billing / technical / sales / account).
2. Patch `spec.model` to a **weaker** candidate. The operator caches it, starts a
   candidate revision behind the model-ready gate, evaluates it against the golden
   dataset, finds it below the accuracy gate, and rolls the rollout back. The
   Service selector never moves off the stable revision, so the weaker model never
   serves a request.
3. Patch `spec.model` to a **strong** candidate. It passes the gate, the Service
   is switched to it (blue-green), and the old revision's Deployment is
   garbage-collected after the promotion grace.

## The models and the gate

Measured on [`dataset.yaml`](./dataset.yaml) — 40 English support tickets, 4
departments, 10 cases each — with the gate `minAccuracy: 0.90`,
`maxAccuracyDrop: 0.05`:

| role      | model               | accuracy | gate result                              |
|-----------|---------------------|---------:|------------------------------------------|
| stable    | `laya:en`           |   0.9250 | serves                                   |
| candidate | `laya:multilingual` |   0.8000 | **rejected** (0.80 < minAccuracy 0.90)   |
| candidate | `nli:latest`        |   0.9250 | **promoted** (passes floor and drop)     |

"Weaker" here means *weaker on this specific English support-routing task*, not
broken. `laya:multilingual` is a capable model — it is simply less accurate than
`laya:en` on this English-only set (its department routing for account/technical
tickets is the main gap). That is exactly the kind of regression the gate is meant
to catch before it reaches traffic. The scores are deterministic: the Ollaya
runtime decodes greedily, so repeated runs reproduce the same accuracy.

To reproduce the measurement yourself, send each ticket in `dataset.yaml` to a
served model's `/v1/systemone` and compare the returned `choice` to the case's
`expected` label — that is exactly what the operator's evaluator does.

## Prerequisites

- `docker` (OrbStack or Docker Desktop) running
- `kind`, `kubectl`
- `curl` and `jq` (to send and parse the sample `/v1/systemone` request)
- for the default install from source: `make` and a Go toolchain (to build the
  operator image)
- for the chart install (`INSTALL=chart`): `helm`

One kind cluster is created, named `dmo-demo` by default.

## Run it

```sh
make demo          # create the cluster, install the operator, run the scenario
make demo-clean    # delete the cluster
```

`make demo` is interactive: it pauses between steps so you can read each one.
Press Enter to advance. To run unattended (no pauses), set `NO_WAIT=1`.

Equivalent, with more control:

```sh
make demo-prepare             # cluster + operator install + runtime image (slow, once)
hack/demo.sh run              # just the scenario (fast to re-run)
```

### Options (environment variables)

| var            | default                        | meaning                                            |
|----------------|--------------------------------|----------------------------------------------------|
| `KIND_CLUSTER` | `dmo-demo`                     | kind cluster name                                  |
| `INSTALL`      | `local`                        | `local` (build from source) or `chart` (released)  |
| `CHART_VERSION`| latest                         | chart version when `INSTALL=chart`                 |
| `OLLAYA_IMAGE` | `ghcr.io/ollaya-dev/ollaya:0.10.0` | runtime image preloaded into the cluster       |
| `NO_WAIT`      | unset                          | set to any value to skip the between-step pauses   |

Install the released chart instead of building from source:

```sh
INSTALL=chart make demo
```

## What to look at

- **Step 3 (rejection).** The phase advances `Caching -> Starting -> Evaluating ->
  RolledBack`. The `EvaluationFailed` Event states the exact reason
  (`accuracy 0.8000 < minAccuracy 0.9000`). `status.stableRevision` and the
  Service's `decisionmodel.io/revision` selector are unchanged, and the stable
  model answers the same ticket — the weaker candidate never served traffic.
- **Step 4 (promotion).** The phase advances `Caching -> Starting -> Evaluating ->
  Ready`. The `EvaluationPassed` Event records the accuracy, the Service selector
  moves to the new revision, and `status.evaluation` shows the candidate score
  against the stable baseline.
- **Step 5 (cleanup).** Only the promoted revision's Deployment remains; the old
  one is garbage-collected after the promotion grace.

## Timing

- **Cold run** (fresh cluster, weights fetched by the per-revision prefetch Job
  from the registry): about 4–5 minutes for the scenario, plus the one-time
  cluster create and operator build. Each revision owns its own store PVC, so the
  weights are fetched once per model during the run.
- The GIF in this directory is a real cold run, sped up about 8x so it stays under
  2 MB and readable at 800 px.

## Reset

```sh
make demo-clean                                   # delete the whole cluster
# or, to re-run the scenario on the same cluster:
kubectl --context kind-dmo-demo delete ns dmo-demo --ignore-not-found
hack/demo.sh run
```

## Re-recording the GIF

The GIF is produced from [`demo.tape`](./demo.tape) with
[vhs](https://github.com/charmbracelet/vhs):

```sh
make demo-prepare
kubectl --context kind-dmo-demo delete ns dmo-demo --ignore-not-found
vhs examples/demo/demo.tape                       # writes docs/assets/demo.gif
```
