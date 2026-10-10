# Migrating a fleet to a different registry (air-gap / mirror switch)

Each DecisionModel revision records the registry it was resolved and prefetched
against in `status.stableRevision.registry` (and `status.candidateRevision.registry`
while a rollout is in flight). The engine default is recorded explicitly
(`https://ollaya.dev`), so the field is only empty on a revision created by an
operator older than the recorded-registry change; the controller then adopts the
registry that revision is actually running with on its first reconcile, without
rolling it.

Because the registry is pinned per revision, changing or removing the operator's
`--ollaya-registry` flag affects **only new revisions**. A running stable keeps its
recorded registry and its serving Pods are **not** re-rendered or rolled by a flag
change, in either direction (adding, changing, or removing the flag). This is by
design: the serving Pod template is rendered from the recorded revision, not from
the live flag.

## Why a per-revision registry

The on-disk store layout depends on the registry authority, and the prefetch Job and
the serving Pods must agree on it. If a flag change silently re-rendered a running
stable, its Pod could look for the model under a store path that was never populated
and serve nothing. Pinning the registry on the revision keeps a running stable intact
and makes the migration explicit: a DecisionModel moves to the new registry only when
it gets a new revision.

## Migrating the fleet

1. Point the operator at the new registry and allow-list it. Keep the **old**
   registry on the allow-list too (see the store-recovery caveat below).

   ```sh
   # flags on the controller-manager (Helm: manager.ollayaRegistry,
   # manager.allowedRegistries, manager.allowInsecureRegistries)
   --ollaya-registry=https://mirror.corp
   --allowed-registries=ollaya.dev,mirror.corp
   # add --allow-insecure-registries only for an http:// mirror (dev only)
   ```

   Existing stables keep serving on their old recorded registry; nothing rolls.

2. Trigger a new revision for each DecisionModel so it re-resolves and re-prefetches
   against the new registry. Any model-affecting spec change creates a new revision
   (for example changing `spec.model` to a new tag, or `spec.resources`). A new
   revision records the new registry and promotes through the normal rollout.

3. Check which DecisionModels are still on the old registry:

   ```sh
   kubectl get dm -A \
     -o custom-columns='NS:.metadata.namespace,NAME:.metadata.name,REGISTRY:.status.stableRevision.registry'
   ```

   Example output:

   ```text
   NS         NAME             REGISTRY
   default    support-router   https://ollaya.dev
   default    triage-router    https://mirror.corp
   ```

   Any row still showing the old registry has not been migrated yet; give it a new
   revision.

## Store-recovery caveat: keep the old registry allowed until everyone has moved

If a stable's model store is lost (its store PVC is deleted or corrupted), the
operator re-prefetches it from the registry recorded on that revision -- the **old**
registry for a not-yet-migrated DecisionModel, not the current `--ollaya-registry`
flag. That recovery pull is re-checked against the current `--allowed-registries` and
`--allow-insecure-registries`. If you have already removed the old registry from the
allow-list, the recovery is refused: the DecisionModel goes `Degraded` with reason
`RegistryNotAllowed` and a Warning Event, and no recovery Job is created (the stable
keeps serving while its Pods are up).

So the safe order is: migrate every DecisionModel to the new registry first (step 2
above, confirmed with the custom-columns check), and only then drop the old registry
from `--allowed-registries`. A DecisionModel that still records the old registry needs
that registry allowed so a store recovery can pull from it.

## Related

- Registry and air-gap setup: [Quickstart - internal mirror / air-gapped](../quickstart.md#supported-models).
- `Degraded` reasons including `RegistryNotAllowed`:
  [DecisionModelDegraded](DecisionModelDegraded.md).
