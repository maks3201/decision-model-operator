# Runbook: DecisionModelEvaluationPoor

## What it means
The last completed evaluation is below target on at least one of: accuracy
(`decisionmodel_evaluation_accuracy`), macro-averaged F1
(`decisionmodel_evaluation_macro_f1`), or Expected Calibration Error
(`decisionmodel_evaluation_ece`). Macro-F1 is averaged across classes, so it catches a
model that scores well on the majority class but poorly on the rest even when top-1
accuracy looks fine. All three thresholds are chart values
(`prometheusRule.thresholds.minAccuracy` / `minMacroF1` / `maxEce`); tune them to your
target. Macro-F1 is absent until an evaluation with classifiable (choice/bool) questions
runs, so it never fires before the first such evaluation.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>    # status.evaluation {accuracy, baselineAccuracy, macroF1, ece, cases}
```
Compare `accuracy` against the spec's `rollout.evaluation.minAccuracy` and
`baselineAccuracy` against `maxAccuracyDrop`.

## Common causes
- The candidate model genuinely performs worse than the stable one on the golden set.
- Accuracy looks fine but macro-F1 is low: the model does well on the majority class and
  poorly on the rest (class imbalance in the dataset masks it in top-1 accuracy).
- The golden dataset drifted from production, or is too small to be representative.
- Calibration regressed (high ECE) even if top-1 accuracy looks acceptable — the model
  is over/under-confident.

## Fix
- If the candidate is worse, keep the stable revision (the gate already blocked promotion)
  and iterate on the model.
- If the dataset is stale, refresh `rollout.evaluation.datasetRef` and re-run.
- A high ECE with acceptable accuracy may still be acceptable for your use; adjust the
  alert threshold if so. Do not relax the promotion gate silently.
