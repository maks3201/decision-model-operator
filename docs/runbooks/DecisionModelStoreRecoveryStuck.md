# Runbook: DecisionModelStoreRecoveryStuck

## What it means
The stable revision's model store is `Degraded` with a store/registry reason, and has
stayed that way for the alert window (default 30m): the store-recovery prefetch is not
completing. Unlike `DecisionModelDegraded`, this alert keys on the condition itself
(`decisionmodel_condition{type="Degraded"}`), not the DecisionModel's phase — so it also
fires when the stall happens during a rollout, when the phase is the candidate's. The
stable keeps serving on whatever Pods are still ready throughout.

## How to confirm
```sh
kubectl describe dm <name> -n <ns>     # the Degraded condition reason + message
kubectl get jobs -n <ns> -l decisionmodel.io/name=<name>
kubectl get pvc -n <ns>                # the <name>-store-<rev> PVC
```
```promql
decisionmodel_condition{type="Degraded",status="True",
  reason=~"StoreLost|StorePrefetchFailed|RegistryNotAllowed"}
```

## Reasons and what to do
The behaviour below is what the operator does for each reason.

- **StoreLost** — the stable store PVC went missing (deleted out of band). The operator
  recreates it (annotated `decisionmodel.io/store-recovering`) and re-runs the prefetch
  before serving on it; it will not serve an empty store. This should self-clear once the
  prefetch completes. If it is stuck here for the window, the prefetch is failing or
  pending — check the recovery Job and StorageClass/quota (a PVC that never binds keeps it
  Degraded). No manual action is normally needed; investigate the Job.

- **StorePrefetchFailed** — the recovery prefetch Job exhausted its bounded retries (a bad
  registry, a digest mismatch, or disk). This is a HELD state: the operator stops retrying
  until you intervene. Fix the underlying cause (registry reachable, store has room,
  correct digest), then re-run recovery:
  ```sh
  kubectl annotate dm <name> -n <ns> decisionmodel.io/retry="$(date +%s)" --overwrite
  ```
  Bumping the retry annotation recreates the recovery and resets the attempt counter.

- **RegistryNotAllowed** — the revision's recorded registry (its `--ollaya-registry`
  mirror, or, for an explicit-host model name, the name's own host) is no longer in
  `--allowed-registries`, so recovery refuses to pull from a now-disallowed source. Re-add
  the host to `--allowed-registries` (operator flag / chart `manager.allowedRegistries`), or
  move the model to an allowed registry. Recovery resumes once the host is allowed again;
  the stable keeps serving meanwhile.

## Notes
- The stable revision identity never changes during recovery — a transient registry outage
  does not fail or roll back the stable; it only delays recovery.
- To restore an exact revision whose tag moved upstream (`UpstreamTagMoved`, a permanent
  prefetch failure that surfaces as StorePrefetchFailed), use a volume snapshot of the
  store: the seeded manifest cannot rebuild a moved tag.
