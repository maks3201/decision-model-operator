# Runbook: DecisionModelOperatorDown

## What it means
Prometheus sees no controller-runtime reconcile metric for the DecisionModel controller
(`absent(controller_runtime_reconcile_total{controller="decisionmodel"})`). The operator
is down, crash-looping, or not being scraped. This is `critical`: while it is down, no
DecisionModel reconciles (no rollouts, no readiness updates).

## How to confirm
```sh
kubectl get pods -n <operator-ns> -l control-plane=controller-manager
kubectl describe pod -n <operator-ns> <manager-pod>      # restarts, last state, events
kubectl logs -n <operator-ns> <manager-pod> --previous   # crash reason
```
Also check the metrics endpoint is reachable by Prometheus (ServiceMonitor/scrape config,
the metrics Service on 8443/HTTPS, and authn/authz).

## Common causes
- Manager Pod crash-looping (bad config flag, missing RBAC at startup, panic).
- No leader elected / all replicas unschedulable (resources, node taints).
- Scrape broken: ServiceMonitor missing, TLS/authn misconfigured, wrong namespace —
  the operator may actually be healthy but invisible to Prometheus.

## Fix
- Restore the Pod: fix the crash cause from `--previous` logs, correct flags/RBAC, free
  scheduling capacity.
- If the operator is healthy, fix the scrape (ServiceMonitor selector, metrics Service,
  RBAC for the metrics reader). Confirm with:
  `kubectl port-forward -n <operator-ns> svc/<release>-metrics 8443:8443` and curl `/metrics`.
- Leader election means only one replica is active; that is expected, not an outage.
