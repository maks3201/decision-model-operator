# Runbook: DecisionModelEvaluationPoor

## What it means
The last completed evaluation has accuracy below 0.9 or Expected Calibration Error
above 0.1 (`decisionmodel_evaluation_accuracy`, `decisionmodel_evaluation_ece`). Tune
these thresholds to your target in the chart.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>    # status.evaluation {accuracy, baselineAccuracy, ece, cases}
```
Compare `accuracy` against the spec's `rollout.evaluation.minAccuracy` and
`baselineAccuracy` against `maxAccuracyDrop`.

## Common causes
- The candidate model genuinely performs worse than the stable one on the golden set.
- The golden dataset drifted from production, or is too small to be representative.
- Calibration regressed (high ECE) even if top-1 accuracy looks acceptable — the model
  is over/under-confident.

## Fix
- If the candidate is worse, keep the stable revision (the gate already blocked promotion)
  and iterate on the model.
- If the dataset is stale, refresh `rollout.evaluation.datasetRef` and re-run.
- A high ECE with acceptable accuracy may still be acceptable for your use; adjust the
  alert threshold if so. Do not relax the promotion gate silently.
