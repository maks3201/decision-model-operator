/*
Copyright 2026 maks3201.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// garbageCollectRevisions deletes Deployments and Jobs of other revisions.
// gcRevisions deletes Deployments and Jobs of revisions that are neither the
// current stable nor the current candidate. The single revision demoted by the
// most recent promotion (status.previousRevision) is spared for promoteGrace
// after its promotedAt so the new revision's endpoints can populate; it never
// blocks GC of any other stale revision.
func (r *DecisionModelReconciler) gcRevisions(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) error {
	keep := map[string]struct{}{}
	if dm.Status.StableRevision != nil {
		keep[dm.Status.StableRevision.Hash] = struct{}{}
	}
	if dm.Status.CandidateRevision != nil {
		keep[dm.Status.CandidateRevision.Hash] = struct{}{}
	}
	// The just-demoted previous revision lingers through the post-promotion keep
	// window (the stabilization window, at least the endpoint-gap grace) so
	// traffic can be switched back to it if the new stable turns out unhealthy.
	prevHash := ""
	if p := dm.Status.PreviousRevision; p != nil && p.Hash != "" {
		within := p.PromotedAt != nil && r.now().Sub(p.PromotedAt.Time) < previousRevisionKeep(dm)
		if within {
			keep[p.Hash] = struct{}{}
			prevHash = p.Hash
		}
	}

	// Never delete the revision the live Service is selecting, whatever status
	// says: it is what clients are being served from right now. Read from the
	// Service itself so it holds even if a code path forgot to keep status and
	// Service in step.
	svc := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: dm.Name}, svc); err == nil {
		if rev := svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]; rev != "" {
			keep[rev] = struct{}{}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	shouldDelete := func(rev string) bool {
		if rev == "" {
			return false
		}
		_, kept := keep[rev]
		return !kept
	}

	// Delete owned Deployments, Jobs, per-revision store PVCs and PDBs whose
	// revision is no longer kept. Jobs need background
	// propagation so their Pods are collected; the rest use foreground defaults.
	jobPolicy := metav1.DeletePropagationBackground
	if err := r.gcStaleByRevision(ctx, dm, &appsv1.DeploymentList{}, shouldDelete); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &batchv1.JobList{}, shouldDelete,
		&client.DeleteOptions{PropagationPolicy: &jobPolicy}); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &corev1.PersistentVolumeClaimList{}, shouldDelete); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &policyv1.PodDisruptionBudgetList{}, shouldDelete); err != nil {
		return err
	}

	// Legacy migration: the shared <dm>-store PVC carries only LabelName
	// (no revision label), so the loop above never selects it. Delete it once no
	// live Deployment of this DM still references it (i.e. the legacy stable has
	// been superseded by a per-revision revision). Never delete it while a
	// workload still mounts it.
	if err := r.gcLegacyStore(ctx, dm); err != nil {
		return err
	}

	// Once the previous revision's grace has elapsed and it has been removed,
	// clear it so it no longer appears in status.
	if dm.Status.PreviousRevision != nil && prevHash == "" {
		dm.Status.PreviousRevision = nil
	}
	return nil
}

// gcStaleByRevision lists the owned objects of one kind for a DecisionModel and
// deletes those whose decisionmodel.io/revision label is no longer kept. It
// unifies the per-kind GC loops (Deployments, Jobs, PVCs, PDBs) so gcRevisions
// stays simple. A missing object on delete is ignored (already collected).
func (r *DecisionModelReconciler) gcStaleByRevision(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	list client.ObjectList,
	shouldDelete func(rev string) bool,
	deleteOpts ...client.DeleteOption,
) error {
	if err := r.List(ctx, list, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return err
	}
	var delErr error
	if err := meta.EachListItem(list, func(o runtime.Object) error {
		obj, ok := o.(client.Object)
		if !ok {
			return nil
		}
		// Defence in depth: only delete objects this DM actually owns.
		// Labels alone are attacker-settable, so a foreign object with matching
		// labels (same name/revision) must survive GC; delete only when the
		// controller OwnerReference UID is this DM's.
		if c := metav1.GetControllerOf(obj); c == nil || c.UID != dm.UID {
			return nil
		}
		if !shouldDelete(obj.GetLabels()[decisionmodelv1alpha1.LabelRevision]) {
			return nil
		}
		if err := r.Delete(ctx, obj, deleteOpts...); err != nil && !apierrors.IsNotFound(err) {
			delErr = err
		}
		return nil
	}); err != nil {
		return err
	}
	return delErr
}

// gcLegacyStore deletes the legacy shared <dm>-store PVC once no live Deployment
// of this DM still mounts it (legacy store migration). It re-lists Deployments so a
// Deployment just deleted earlier in the same gcRevisions pass is not counted as
// a live reference. It is a no-op when the PVC does not exist or is still
// referenced.
func (r *DecisionModelReconciler) gcLegacyStore(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) error {
	legacy := storeName(dm)
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: legacy}, pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pvc.DeletionTimestamp != nil {
		return nil // already being deleted
	}
	// Only ever delete the shared store if this DM owns it. The operator always set a
	// controller OwnerReference on <dm>-store, so a missing/foreign owner means
	// the PVC is not ours — never delete it by name alone.
	if !ownedBy(pvc, dm) {
		return nil
	}
	var deps appsv1.DeploymentList
	if err := r.List(ctx, &deps, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return err
	}
	for i := range deps.Items {
		if deps.Items[i].DeletionTimestamp != nil {
			continue // being deleted, not a live reference
		}
		if claimNameFromPodSpec(&deps.Items[i].Spec.Template.Spec) == legacy {
			return nil
		}
	}
	if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteRevisionWorkloads removes the Deployment and Job of a specific revision
// (used on rollback of a failed candidate).
func (r *DecisionModelReconciler) deleteRevisionWorkloads(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) error {
	policy := metav1.DeletePropagationBackground
	// Each object is deleted only if it exists AND is controlled by this DM: a
	// name collision with a user's object must never be deleted.
	if err := r.deleteIfOwned(ctx, dm,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: revisionName(dm, rev)}}); err != nil {
		return err
	}
	if err := r.deleteIfOwned(ctx, dm,
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}},
		&client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return err
	}
	// A failed revision's per-revision store PVC is deleted with its workloads.
	// The legacy shared <dm>-store is never per-revision, so this only
	// removes storeNameRev; the live Pod spec no longer references it after the
	// Deployment delete above.
	if err := r.deleteIfOwned(ctx, dm,
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: storeNameRev(dm, rev)}}); err != nil {
		return err
	}
	// A failed candidate never gets a PDB (only the stable revision does), but
	// delete any that might exist for symmetry with the other owned objects.
	if err := r.deleteIfOwned(ctx, dm,
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: pdbName(dm, rev)}}); err != nil {
		return err
	}
	return nil
}

// deleteIfOwned Gets the object by its name/namespace and deletes it only when
// it exists and is controlled by this DM. A NotFound is ignored (already gone);
// a foreign object with the same name is left untouched.
func (r *DecisionModelReconciler) deleteIfOwned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	obj client.Object,
	opts ...client.DeleteOption,
) error {
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !ownedBy(obj, dm) {
		return nil
	}
	if err := r.Delete(ctx, obj, opts...); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
