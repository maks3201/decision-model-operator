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
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// maintainStable keeps the recorded stable revision healthy while a candidate is
// mid-rollout. It restores a deleted stable Deployment, applies an
// API-key rotation (checksum), keeps the PDB, and regates the stable Pods — all
// rendered from status.stableRevision, never the live spec (which describes the
// candidate). It owns no phase/Ready/Degraded conditions and does not switch the
// Service or GC: the candidate flow owns those.
//
// Store recovery is intentionally NOT run here (it drives the stable path once
// the rollout concludes). If the stable store PVC is missing or terminating the
// Deployment is left untouched — never re-rendered against a missing/dying
// volume — and regating is skipped for this round.
func (r *DecisionModelReconciler) maintainStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) error {
	claim, _ := r.storeClaimForStable(ctx, dm, stable)
	serveable, terminating, err := r.stableStoreServeable(ctx, dm, claim)
	if err != nil {
		return err
	}
	if !serveable {
		// Missing/recovering store: do not render the Deployment against it. The
		// stable path will recover it after the rollout concludes.
		return nil
	}
	stableParams := r.stableParams(ctx, dm, stable, claim)
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, stableParams, stable.Hash, true, keyChecksum, legacyChecksum, terminating); err != nil {
		return err
	}
	if err := r.ensurePDB(ctx, dm, stable.Hash); err != nil {
		return err
	}
	// Regate the stable Pods: a stable Pod that lost the model during
	// the rollout must flip its gate False. The readiness count is not used here;
	// the candidate flow owns phase/Ready.
	_, _, probeErr := r.probePods(ctx, dm, eng, stable, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return probeErr
	}
	return nil
}

// stableStoreServeable reports whether the stable store PVC named claim is
// present, owned, not terminating and not marked recovering — i.e. safe to
// render the stable Deployment against. terminating is true when the PVC is
// being deleted but still present (freeze the template). A missing,
// foreign, or recovering store is not serveable.
func (r *DecisionModelReconciler) stableStoreServeable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) (serveable, terminating bool, err error) {
	pvc := &corev1.PersistentVolumeClaim{}
	gerr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc)
	switch {
	case apierrors.IsNotFound(gerr):
		return false, false, nil
	case gerr != nil:
		return false, false, gerr
	case !ownedBy(pvc, dm):
		return false, false, nil
	case pvc.Annotations[storeRecoveringAnnotation] == annotationTrue:
		return false, false, nil
	case pvc.DeletionTimestamp != nil:
		return true, true, nil
	default:
		return true, false, nil
	}
}

// recoverStableStore ensures the stable revision's store PVC exists and is
// populated before the stable Pods are (re)started. It returns handled=true when
// the store is missing or still being (re)prefetched: in that case it has set
// the result/err the caller must return and the stable path stops for this round
// (the Deployment is not re-rendered against a missing/empty volume).
// It returns handled=false (serve) for a healthy store and also for a PVC that is
// Terminating but still mounted, reporting storeTerminating=true so the caller
// keeps the stable serving and preserves Degraded=StoreTerminating.
//
// Recovery is level-triggered from cluster state: a store PVC recreated
// by recovery carries the storeRecoveringAnnotation, set at Create time, so the
// store counts as unpopulated until a fresh prefetch completes — and this
// survives an operator restart (no in-memory "recovering" marker). The store is
// treated as populated once a Complete prefetch Job created at or after the PVC
// exists (job.CreationTimestamp >= pvc.CreationTimestamp); a Complete Job older
// than the PVC filled a previous volume and is stale. The annotation is then
// removed. A PVC without the annotation (normal rollout, or legacy/pruned) is
// healthy.
//
// A failed recovery prefetch is deleted and recreated, bounded by
// maxStoreRecoverAttempts (counted per failed Job UID so a stale cache read is not
// double-counted); past the bound it stops with Degraded=StorePrefetchFailed. To
// avoid reading the informer cache for a Job just deleted in the same reconcile,
// every delete returns immediately with a short requeue.
func (r *DecisionModelReconciler) recoverStableStore(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	legacy bool,
) (handled, storeTerminating bool, res ctrl.Result, err error) {
	// --- 1-2. Inspect the store PVC and the stable prefetch Job. ---
	st, handled, res, err := r.inspectRecoveryState(ctx, dm, stable, claim)
	if handled {
		return true, false, res, err
	}

	// --- 3. Healthy (serve)? A present PVC that is not marked recovering is
	// populated: either a normal rollout or a legacy/pruned store. A PVC still
	// mounted but Terminating also keeps serving; the caller preserves
	// Degraded=StoreTerminating through applyStableReadiness. ---
	if !st.pvcMissing && !st.recovering {
		if !st.terminating {
			clearStoreRecover(dm)
		}
		return false, st.terminating, ctrl.Result{}, nil
	}

	// --- 4. Recovering (PVC missing, or present with the recovering annotation):
	// hold Degraded and keep the store being (re)filled. ---
	// Once recovery is exhausted, StorePrefetchFailed owns the Degraded condition;
	// do not overwrite it with StoreLost on every reconcile (no status churn).
	if !storeRecoverExhausted(dm) {
		r.degradeStoreLost(ctx, dm, claim)
	}

	// Recreate the PVC if it is gone, carrying the recovering annotation, then
	// return (next reconcile observes the new PVC and (re)creates the prefetch;
	// never Get a resource we just wrote in the same reconcile from the cache).
	if st.pvcMissing {
		if cerr := r.createRecoveryPVC(ctx, dm, stable, claim, legacy); cerr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, cerr)
			return true, false, res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return true, false, res, ferr
	}

	res, err = r.runRecoveryPrefetch(ctx, dm, eng, stable, claim, st)
	return true, false, res, err
}

// storeRecoveryState is the inspected PVC/Job state the recovery flow needs.
type storeRecoveryState struct {
	pvc          *corev1.PersistentVolumeClaim
	job          *batchv1.Job
	jobExists    bool
	pvcMissing   bool
	jobOngoing   bool
	jobFailedNow bool
	// recovering is true when the present PVC carries the storeRecoveringAnnotation
	// (recreated by recovery and not yet repopulated).
	recovering bool
	// terminating is true when the present PVC is being deleted but is still
	// mounted by the stable Pods (pvc-protection). The stable keeps serving and
	// surfaces Degraded=StoreTerminating.
	terminating bool
	// populated is true when the PVC is present and a Complete prefetch Job was
	// created at or after it (so it filled THIS volume, not a previous one).
	populated bool
	// staleComplete is true when a Complete Job predates the PVC (it filled an
	// older volume): it must be deleted before a fresh prefetch runs.
	staleComplete bool
}

// inspectRecoveryState reads the stable store PVC and prefetch Job and classifies
// them for recoverStableStore. It returns handled=true (with the result/err the
// caller must return) for the terminal cases — a foreign/terminating PVC, a
// foreign Job, or a non-NotFound Get error.
func (r *DecisionModelReconciler) inspectRecoveryState(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
) (st storeRecoveryState, handled bool, res ctrl.Result, err error) {
	pvc := &corev1.PersistentVolumeClaim{}
	pvcErr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc)
	switch {
	case pvcErr == nil:
		if !ownedBy(pvc, dm) {
			// Persist the ResourceConflict condition/Event and back off via
			// RequeueAfter with a nil error (controller-runtime ignores RequeueAfter
			// when err != nil).
			_ = r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
			res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
			return st, true, res, ferr
		}
		if pvc.DeletionTimestamp != nil {
			// Policy: a stable store PVC stuck Terminating is still
			// mounted by the stable Pods (pvc-protection). Do NOT stop them
			// automatically — surface Degraded=StoreTerminating and let the stable
			// path keep running (regate, key rotation). Once the PVC is actually
			// gone, recovery (pvcMissing) recreates and reprefetches it. handled is
			// false so reconcileStablePath proceeds; the condition is persisted by
			// the stable path's own finish().
			r.degradeStoreTerminating(ctx, dm, claim)
			st.pvc = pvc
			st.recovering = false // present and mounted: keep serving, do not treat as unpopulated
			st.terminating = true
			return st, false, ctrl.Result{}, nil
		}
		st.pvc = pvc
		st.recovering = pvc.Annotations[storeRecoveringAnnotation] == annotationTrue
	case apierrors.IsNotFound(pvcErr):
		st.pvcMissing = true
	default:
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, pvcErr)
		return st, true, res, ferr
	}

	st.job = &batchv1.Job{}
	jobErr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, stable.Hash)}, st.job)
	st.jobExists = jobErr == nil
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, jobErr)
		return st, true, res, ferr
	}
	if st.jobExists && !ownedBy(st.job, dm) {
		// Same as the PVC conflict: persist the condition and requeue with nil err.
		_ = r.conflictIfNotOwned(ctx, dm, st.job, "Job")
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
		return st, true, res, ferr
	}
	live := st.jobExists && st.job.DeletionTimestamp == nil
	st.jobOngoing = live && !jobComplete(st.job) && !jobFailed(st.job)
	st.jobFailedNow = live && jobFailed(st.job)
	if live && jobComplete(st.job) {
		// Complete AND created at/after the PVC => it filled this (recovering)
		// volume; otherwise it filled an older volume and is stale.
		if !st.pvcMissing && !st.job.CreationTimestamp.Before(&st.pvc.CreationTimestamp) {
			st.populated = true
		} else {
			st.staleComplete = true
		}
	}
	return st, false, ctrl.Result{}, nil
}

// runRecoveryPrefetch manages the recovery prefetch Job once the PVC exists: it
// deletes a failed Job (bounded by maxStoreRecoverAttempts, per Job UID, then
// StorePrefetchFailed) or a stale Complete Job, returning immediately after any
// delete (never reading the just-deleted Job from the cache), and otherwise
// (re)creates the prefetch and holds until it completes.
func (r *DecisionModelReconciler) runRecoveryPrefetch(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	st storeRecoveryState,
) (res ctrl.Result, err error) {
	bg := metav1.DeletePropagationBackground
	delJob := func() error {
		return r.deleteIfOwned(ctx, dm,
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: prefetchName(dm, stable.Hash)}},
			&client.DeleteOptions{PropagationPolicy: &bg})
	}

	// Recovery complete: a fresh Complete prefetch Job filled this PVC. Clear the
	// recovering annotation (so the next reconcile serves) and the failure counter.
	if st.populated {
		if cerr := r.clearStoreRecoveringAnnotation(ctx, st.pvc); cerr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, cerr)
			return res, ferr
		}
		clearStoreRecover(dm)
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A failed recovery prefetch: count it once per Job UID, then delete and
	// requeue (recreate next reconcile). Past the bound, give up.
	if st.jobFailedNow {
		// A permanent failure (e.g. the tag does not exist, or the pulled digest
		// does not match) cannot be fixed by retrying: give up at once without
		// burning the bounded attempts, Degraded with the classified reason.
		if permanent, detail := r.prefetchFailureReason(ctx, dm, eng, stable.Hash); permanent {
			r.degradeStorePrefetchReason(ctx, dm, claim, detail)
			res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
			return res, ferr
		}
		if r.countFailedRecovery(dm, st.job.UID) {
			r.degradeStorePrefetchFailed(ctx, dm, claim)
			// Stop churning: slow requeue, no further counting or writes.
			res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
			return res, ferr
		}
		if derr := delJob(); derr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, derr)
			return res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A stale Complete Job (older than the PVC): delete it and requeue; a fresh
	// prefetch is created on the next reconcile, after the delete has settled.
	if st.staleComplete {
		if derr := delJob(); derr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, derr)
			return res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A live ongoing prefetch: hold until it completes.
	if st.jobOngoing {
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		return res, ferr
	}

	// No Job present (PVC exists, recovery active): create a fresh prefetch.
	// Seed the recorded manifest so recovery rebuilds exactly the pinned digest
	// (the tag may have moved since this revision was promoted); a missing
	// manifest falls back to pull-by-tag + verify.
	recoveryParams := r.seedManifest(ctx, dm, r.stableParams(ctx, dm, stable, claim), stable.Hash, stable.Digest)
	_, done, failed, perr := r.ensurePrefetchJob(ctx, dm, eng, recoveryParams, stable.Hash)
	if perr != nil {
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, perr)
		return res, ferr
	}
	if failed || !done {
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		return res, ferr
	}
	// Freshly created and already Complete (unusual): populated next reconcile.
	res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
	return res, ferr
}

// countFailedRecovery records a failed recovery prefetch Job identified by jobUID
// into dm.Status.StoreRecovery (persisted by the surrounding finish, so the bound
// survives an operator restart). It counts a given Job UID at most once (so a
// stale cache read of the same failed Job is not double-counted) and returns
// whether the bounded retries are now exhausted. The caller must persist dm's
// status (every caller returns through finish).
func (r *DecisionModelReconciler) countFailedRecovery(dm *decisionmodelv1alpha1.DecisionModel, jobUID types.UID) (exhausted bool) {
	s := dm.Status.StoreRecovery
	if s == nil {
		s = &decisionmodelv1alpha1.StoreRecoveryStatus{}
		dm.Status.StoreRecovery = s
	}
	if s.Exhausted {
		return true
	}
	if s.LastFailedJob != string(jobUID) {
		s.LastFailedJob = string(jobUID)
		s.Attempts++
	}
	if s.Attempts >= maxStoreRecoverAttempts {
		s.Exhausted = true
	}
	return s.Exhausted
}

// clearStoreRecoveringAnnotation removes the storeRecoveringAnnotation from a now
// repopulated store PVC via a Patch, so later reconciles treat it as healthy.
// A no-op if the annotation is already gone.
func (r *DecisionModelReconciler) clearStoreRecoveringAnnotation(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc == nil || pvc.Annotations[storeRecoveringAnnotation] == "" {
		return nil
	}
	patched := pvc.DeepCopy()
	delete(patched.Annotations, storeRecoveringAnnotation)
	return r.Patch(ctx, patched, client.MergeFrom(pvc))
}

// storeRecoverExhausted reports whether the bounded recovery retries for a
// DecisionModel are exhausted (StorePrefetchFailed owns the Degraded condition).
func storeRecoverExhausted(dm *decisionmodelv1alpha1.DecisionModel) bool {
	return dm.Status.StoreRecovery != nil && dm.Status.StoreRecovery.Exhausted
}

// clearStoreRecover resets the persisted recovery bookkeeping for a
// DecisionModel (on completion, a new revision, or a retry token). The
// restart-safe "recovering" signal lives on the PVC annotation; the attempt
// bound lives in status.storeRecovery, cleared here. The caller persists it.
func clearStoreRecover(dm *decisionmodelv1alpha1.DecisionModel) {
	dm.Status.StoreRecovery = nil
}

// degradeStorePrefetchFailed marks a lost-store recovery as exhausted after
// attempts failed prefetch Jobs.
func (r *DecisionModelReconciler) degradeStorePrefetchFailed(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("stable store %q recovery failed after %d prefetch attempts; "+
		"the store could not be repopulated (manual intervention required)", claim, maxStoreRecoverAttempts)
	// Emit the Event only on the transition into StorePrefetchFailed, and write the
	// condition with a fixed message so repeated reconciles cause no status churn.
	already := meta.IsStatusConditionPresentAndEqual(dm.Status.Conditions,
		decisionmodelv1alpha1.ConditionDegraded, metav1.ConditionTrue) &&
		meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded).Reason == reasonStorePrefetchFailed
	if already {
		return
	}
	r.event(ctx, dm, corev1.EventTypeWarning, eventStorePrefetchFail, "%s", msg)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStorePrefetchFailed,
		Message: msg,
	})
}

// degradeStorePrefetchReason marks a lost-store recovery as failed by a
// permanent prefetch error (e.g. the tag does not exist, or the pulled digest
// does not match) — given up at once, not after maxStoreRecoverAttempts. The
// condition reason stays StorePrefetchFailed (API); the classified reason and
// detail go in the message.
func (r *DecisionModelReconciler) degradeStorePrefetchReason(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim, detail string,
) {
	msg := fmt.Sprintf("stable store %q recovery failed permanently: %s (manual intervention required)",
		claim, detail)
	already := meta.IsStatusConditionPresentAndEqual(dm.Status.Conditions,
		decisionmodelv1alpha1.ConditionDegraded, metav1.ConditionTrue) &&
		meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded).Reason == reasonStorePrefetchFailed &&
		meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded).Message == msg
	if already {
		return
	}
	r.event(ctx, dm, corev1.EventTypeWarning, eventStorePrefetchFail, "%s", msg)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStorePrefetchFailed,
		Message: msg,
	})
}

// degradeStoreLost sets Degraded=True/StoreLost and emits one Event.
func (r *DecisionModelReconciler) degradeStoreLost(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("stable store PVC %q is missing; recreating and prefetching before serving", claim)
	if meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded) == nil ||
		!meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded) {
		r.event(ctx, dm, corev1.EventTypeWarning, eventStoreLost, "%s", msg)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStoreLost,
		Message: msg,
	})
}

// degradeStoreTerminating sets Degraded=True/StoreTerminating while the stable
// store PVC is being deleted but is still mounted by the stable Pods.
// The operator does not stop the stable automatically (a PVC delete cannot be
// undone); the user must delete the Pods to let recovery proceed. The Event fires
// only on the transition into StoreTerminating and the condition carries a fixed
// message, so repeated reconciles on the regate interval cause no status churn.
func (r *DecisionModelReconciler) degradeStoreTerminating(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("the stable store PVC %q is being deleted; it is still mounted by the stable Pods "+
		"— delete them to let recovery proceed", claim)
	deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
	already := deg != nil && deg.Status == metav1.ConditionTrue && deg.Reason == reasonStoreTerminating
	if !already {
		r.event(ctx, dm, corev1.EventTypeWarning, eventStoreTerminating, "%s", msg)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStoreTerminating,
		Message: msg,
	})
}

// storePVCSpec returns the model-store PVC spec (size/class/access modes) from
// the DecisionModel's cache spec, with the operator defaults when unset.
func storePVCSpec(dm *decisionmodelv1alpha1.DecisionModel) corev1.PersistentVolumeClaimSpec {
	size := resource.MustParse("10Gi")
	var storageClass *string
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if dm.Spec.Cache != nil {
		if !dm.Spec.Cache.Size.IsZero() {
			size = dm.Spec.Cache.Size
		}
		storageClass = dm.Spec.Cache.StorageClassName
		if len(dm.Spec.Cache.AccessModes) > 0 {
			accessModes = dm.Spec.Cache.AccessModes
		}
	}
	return corev1.PersistentVolumeClaimSpec{
		AccessModes:      accessModes,
		StorageClassName: storageClass,
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: size},
		},
	}
}

// createRecoveryPVC creates the store PVC for a lost-store recovery, carrying the
// storeRecoveringAnnotation so the store counts as unpopulated (restart-safe)
// until a fresh prefetch completes. legacy uses the shared <dm>-store
// name and labels; otherwise the per-revision name/labels. No-op if it exists.
func (r *DecisionModelReconciler) createRecoveryPVC(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	legacy bool,
) error {
	existing := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	labels := map[string]string{decisionmodelv1alpha1.LabelName: dm.Name}
	if !legacy {
		labels = revisionLabels(dm, stable.Hash)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   dm.Namespace,
			Name:        claim,
			Labels:      labels,
			Annotations: map[string]string{storeRecoveringAnnotation: annotationTrue},
		},
		Spec: storePVCSpec(dm),
	}
	if err := controllerutil.SetControllerReference(dm, pvc, r.Scheme); err != nil {
		return err
	}
	return r.createOrAdopt(ctx, dm, pvc, "PersistentVolumeClaim")
}

// errStoreTerminating reports that the revision's store PVC is being deleted and
// cannot be reused yet. It is a normal, transient condition, not a failure.
var errStoreTerminating = errors.New("model store PVC is terminating")

// errStoreLostRecovering reports that a stable revision's store PVC is missing or
// terminating, so the stable path must enter recovery (recoverStableStore) rather
// than render Pods against a missing/empty/dying volume.
var errStoreLostRecovering = errors.New("stable store lost; recovering")

// storeTerminatingRequeue is how long to wait for a terminating store PVC to go.
const storeTerminatingRequeue = 5 * time.Second

// ensurePVC creates the per-revision model-store PVC and returns it. On
// an existing PVC it does not mutate access modes (immutable). Each revision
// owns its store (storeNameRev) so a blue-green rollout on RWO storage does not
// deadlock on Multi-Attach when the candidate lands on a different node than the
// stable. The PVC carries revisionLabels so gcRevisions can collect it by the
// same rules as the revision's Deployment/Job.
func (r *DecisionModelReconciler) ensurePVC(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: storeNameRev(dm, rev)}
	err := r.Get(ctx, key, pvc)
	if err == nil {
		if !ownedBy(pvc, dm) {
			return nil, r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
		}
		// A claim that is being deleted must not be reused: Pods would mount a
		// volume that is about to disappear, and the prefetch would write into it.
		// This happens after fast churn (a revision is recreated while its store from
		// the previous round is still terminating). Wait until it is gone, then
		// create a fresh one.
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreTerminating
		}
		return pvc, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      storeNameRev(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		Spec: storePVCSpec(dm),
	}
	if err := controllerutil.SetControllerReference(dm, pvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.createOrAdopt(ctx, dm, pvc, "PersistentVolumeClaim"); err != nil {
		return nil, err
	}
	return pvc, nil
}

// storeClaimForStable resolves the store PVC claim name a recorded stable
// revision should mount. A legacy stable (promoted before per-revision stores) still mounts
// the shared <dm>-store; its live Deployment is authoritative, so we read the
// claim from it and keep using it until the next promotion (legacy store migration).
// Otherwise the per-revision PVC storeNameRev(dm, rev.Hash) is used. The second
// return reports whether the resolved claim is the legacy shared PVC.
func (r *DecisionModelReconciler) storeClaimForStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev *decisionmodelv1alpha1.RevisionStatus,
) (claimName string, legacy bool) {
	perRev := storeNameRev(dm, rev.Hash)
	dep := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev.Hash)}
	if err := r.Get(ctx, key, dep); err == nil {
		if live := claimNameFromPodSpec(&dep.Spec.Template.Spec); live != "" {
			return live, live == storeName(dm)
		}
	}
	// The live Deployment is gone (e.g. deleted out of band, or lost before a
	// restart). Do not blindly fall back to a fresh per-revision PVC: a legacy
	// stable's populated shared <dm>-store may still exist, and switching to a new
	// empty per-revision claim would strand the full volume and serve from an
	// empty one. If the legacy shared store exists and is ours, keep
	// using it; otherwise use the per-revision claim.
	legacyPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: storeName(dm)}, legacyPVC); err == nil &&
		ownedBy(legacyPVC, dm) && legacyPVC.DeletionTimestamp == nil {
		return storeName(dm), true
	}
	return perRev, false
}

// claimNameFromPodSpec returns the ClaimName of the first PVC-backed volume in a
// PodSpec (the model store), or "" if none. Used to read a live Deployment's
// store claim for legacy migration.
func claimNameFromPodSpec(spec *corev1.PodSpec) string {
	for i := range spec.Volumes {
		if src := spec.Volumes[i].PersistentVolumeClaim; src != nil {
			return src.ClaimName
		}
	}
	return ""
}

// ensureStoreForStable returns the store PVC the stable revision uses, for the
// cache-sharing guard. It never recreates a missing store here: a lost stable
// store must go through recoverStableStore (annotated create + prefetch) so the
// stable is not served from an empty volume. It therefore returns
// errStoreLostRecovering when the stable store PVC is missing or terminating —
// both for a per-revision PVC and for a legacy shared store — and the caller
// routes straight to reconcileStablePath. For a healthy legacy stable it Gets
// the shared <dm>-store (never recreates it); otherwise it ensures the stable
// revision's own per-revision PVC exists.
func (r *DecisionModelReconciler) ensureStoreForStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
) (*corev1.PersistentVolumeClaim, error) {
	claim, legacy := r.storeClaimForStable(ctx, dm, stable)
	if legacy {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				// Legacy shared store gone: recover it (recoverStableStore recreates
				// the shared PVC annotated and reprefetches) instead of returning a
				// bare NotFound before the stable path.
				return nil, errStoreLostRecovering
			}
			return nil, err
		}
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreLostRecovering
		}
		return pvc, nil
	}
	// Per-revision stable store. A missing or terminating PVC is not re-created
	// here (that would be an unannotated, empty volume); hand it to recovery.
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: storeNameRev(dm, stable.Hash)}, pvc)
	switch {
	case err == nil:
		if !ownedBy(pvc, dm) {
			return nil, r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
		}
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreLostRecovering
		}
		return pvc, nil
	case apierrors.IsNotFound(err):
		return nil, errStoreLostRecovering
	default:
		return nil, err
	}
}

// guardCacheSharing sets the Degraded condition when the model-store PVC cannot
// be shared across the requested replicas, or when the spec asks to change an
// immutable PVC's access modes/storage class, and applies an allowed size grow.
// It never blocks progress for a policy reason (single-node works). Returns
// (true, nil) when it set Degraded=True, and (false, err) for a transient error
// (e.g. an expansion Patch that failed for a non-policy reason) so the caller
// backs off via the workqueue instead of parking a misleading condition.
func (r *DecisionModelReconciler) guardCacheSharing(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pvc *corev1.PersistentVolumeClaim,
) (bool, error) {
	if dm.Spec.Cache != nil {
		// StorageClassName and access modes are immutable on an existing PVC.
		if dm.Spec.Cache.StorageClassName != nil &&
			!storageClassEqual(dm.Spec.Cache.StorageClassName, pvc.Spec.StorageClassName) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: "spec.cache.storageClassName differs from the existing PVC; a PVC's " +
					"storage class is immutable. Delete the PVC to apply a new storage class.",
			})
			return true, nil
		}
		if len(dm.Spec.Cache.AccessModes) > 0 &&
			!accessModesEqual(dm.Spec.Cache.AccessModes, pvc.Spec.AccessModes) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: "spec.cache.accessModes differs from the existing PVC; PVC access modes are " +
					"immutable and will not be changed. Delete the PVC to apply new access modes.",
			})
			return true, nil
		}
		// Size: grow via an expansion Patch (an API Forbidden/Invalid rejection ->
		// CacheSpecImmutable); a shrink is CacheSpecImmutable; a transient Patch
		// error is returned so the caller backs off.
		degraded, serr := r.guardCacheSize(ctx, dm, pvc)
		if serr != nil {
			return false, serr
		}
		if degraded {
			return true, nil
		}
	}

	if desiredReplicas(dm) > 1 && !accessModesShareable(pvc.Spec.AccessModes) {
		setStatusCondition(dm, metav1.Condition{
			Type:   decisionmodelv1alpha1.ConditionDegraded,
			Status: metav1.ConditionTrue,
			Reason: reasonCacheNotShareable,
			Message: "replicas>1 but the model-store PVC is not ReadWriteMany/ReadOnlyMany; " +
				"Pods on different nodes will be stuck in ContainerCreating (Multi-Attach). " +
				"Set spec.cache.accessModes to a shared mode with RWX-capable storage.",
		})
		return true, nil
	}
	return false, nil
}

// guardCacheSize reconciles a spec.cache.size change against an existing PVC: a
// grow is attempted as a volume expansion patch, and the API server's own
// rejection (the StorageClass forbids expansion, or the request is otherwise
// Invalid/Forbidden) is surfaced as CacheSpecImmutable. A shrink is rejected
// without a call. A transient Patch error (anything other than Forbidden/Invalid)
// is returned so the caller backs off via the workqueue instead of parking a
// misleading immutable condition. This never reads the
// (cluster-scoped) StorageClass, so it works unchanged in namespace-scoped mode.
// Returns (true, nil) when it set Degraded.
func (r *DecisionModelReconciler) guardCacheSize(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pvc *corev1.PersistentVolumeClaim,
) (bool, error) {
	want := dm.Spec.Cache.Size
	if want.IsZero() {
		return false, nil
	}
	have := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	cmp := want.Cmp(have)
	if cmp == 0 {
		return false, nil
	}
	if cmp < 0 {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonCacheSpecImmutable,
			Message: "spec.cache.size is smaller than the existing PVC; a PVC cannot be shrunk.",
		})
		return true, nil
	}
	// Grow: attempt the expansion patch and let the API server decide. A
	// StorageClass that forbids expansion rejects it with Forbidden/Invalid.
	patched := pvc.DeepCopy()
	if patched.Spec.Resources.Requests == nil {
		patched.Spec.Resources.Requests = corev1.ResourceList{}
	}
	patched.Spec.Resources.Requests[corev1.ResourceStorage] = want
	if err := r.Patch(ctx, patched, client.MergeFrom(pvc)); err != nil {
		if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: fmt.Sprintf("spec.cache.size increase was rejected (the StorageClass may not "+
					"allow volume expansion); the PVC size is unchanged: %v", err),
			})
			return true, nil
		}
		// A transient error (conflict, server unavailable, ...): return it so the
		// workqueue retries with backoff; do not misreport it as immutable.
		return false, fmt.Errorf("expanding store PVC %q: %w", pvc.Name, err)
	}
	return false, nil
}

// ensurePrefetchJob creates the prefetch Job for a revision and reports its state.
func (r *DecisionModelReconciler) ensurePrefetchJob(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	params engine.Params,
	rev string,
) (created, done, failed bool, err error) {
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}
	getErr := r.Get(ctx, key, job)
	if getErr == nil {
		if !ownedBy(job, dm) {
			return false, false, false, r.conflictIfNotOwned(ctx, dm, job, "Job")
		}
		if job.DeletionTimestamp != nil {
			// Being deleted (e.g. a lost-store recovery is replacing it): report it
			// as neither done nor failed so the caller waits and recreates it once
			// it is gone.
			return false, false, false, nil
		}
		return false, jobComplete(job), jobFailed(job), nil
	}
	if !apierrors.IsNotFound(getErr) {
		return false, false, false, getErr
	}

	// Prefetch only needs to pull weights, never to run inference, so it must not
	// use the resolved serving image. For device: cuda that image is the ~1.7 GB
	// :<version>-cuda tag, which forces the prefetch Pod onto a CUDA-capable node and
	// adds a long pull (seen on EKS). Pass only the user's explicit spec.image
	// override; with none, the engine picks its CPU default. The
	// serving Deployment still renders from the resolved image in params.
	prefetchParams := params
	prefetchParams.Image = dm.Spec.Image

	job = &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      prefetchName(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		Spec: eng.PrefetchJobSpec(prefetchParams),
	}
	// Label the prefetch Pods for the operator's name-scoped Pod cache (LabelName)
	// so a failed prefetch's termination message can be read to classify the
	// failure, plus a dedicated prefetch-revision label the newest-failed lookup
	// keys on. We deliberately do NOT set LabelRevision here: that is the selector
	// of the revision's Service/PDB/Deployment, so a prefetch Pod carrying it
	// would be picked up as a (probe-less, so Ready) Service endpoint with nothing
	// listening and would count against the PDB. The Job adds its own
	// controller-uid/job-name labels on top; extra template labels are allowed.
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = map[string]string{}
	}
	job.Spec.Template.Labels[decisionmodelv1alpha1.LabelName] = dm.Name
	job.Spec.Template.Labels[decisionmodelv1alpha1.LabelPrefetchRevision] = rev
	// The prefetch Pod must land where serving Pods may run: on tainted /
	// dedicated (e.g. GPU) pools, and — with WaitForFirstConsumer storage — it
	// is the first consumer that pins the PVC's zone.
	applyScheduling(&job.Spec.Template.Spec, dm.Spec.Scheduling)
	applyProxyEnv(&job.Spec.Template.Spec, r.PrefetchProxyEnv)
	// A large model needs a longer pull than the engine's default Job deadline:
	// follow an explicit spec.rollout.timeouts.caching. Left alone when
	// unset so the engine's own default stays in force.
	if t := rolloutTimeouts(dm); t != nil && t.Caching != nil {
		secs := int64(t.Caching.Duration / time.Second)
		job.Spec.ActiveDeadlineSeconds = &secs
	}
	applySELinuxLevel(&job.Spec.Template.Spec, dm)
	if err := controllerutil.SetControllerReference(dm, job, r.Scheme); err != nil {
		return false, false, false, err
	}
	if err := r.createOrAdopt(ctx, dm, job, "Job"); err != nil {
		return false, false, false, err
	}
	return true, false, false, nil
}

// prefetchFailureReason classifies why a revision's prefetch Job failed by
// reading the newest failed prefetch Pod's terminated container state and asking
// the engine's PrefetchFailureClassifier capability. It returns whether the
// failure is permanent (retrying cannot help) and a human-readable detail
// beginning with the classified reason, for the condition message/Event. detail
// is "" when nothing could be read (no classifier, no failed Pod/message), so
// the caller keeps its generic message.
func (r *DecisionModelReconciler) prefetchFailureReason(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	rev string,
) (permanent bool, detail string) {
	classifier, ok := eng.(engine.PrefetchFailureClassifier)
	if !ok {
		return false, ""
	}
	msg, exit, found := r.newestFailedPrefetchTermination(ctx, dm, rev)
	if !found {
		return false, ""
	}
	reason, permanent := classifier.ClassifyPrefetchFailure(msg, exit)
	if reason == "" {
		return permanent, ""
	}
	detail = reason
	if msg != "" {
		detail = fmt.Sprintf("%s (%s)", reason, strings.TrimSpace(msg))
	}
	return permanent, detail
}

// newestFailedPrefetchTermination returns the terminated-container message and
// exit code of the most recently started failed Pod of a revision's prefetch
// Job. Prefetch Pods carry LabelName + LabelPrefetchRevision (never
// LabelRevision, which is a serving selector), so they are in the operator's
// name-scoped Pod cache. Those labels can be set by anyone with Pod create
// rights in the namespace, so a Pod is trusted only when its controller
// OwnerReference is the current prefetch Job's UID, and that Job is itself owned
// by this DecisionModel. found is false when the Job is missing or not ours, or
// when no owned Pod has a usable termination.
func (r *DecisionModelReconciler) newestFailedPrefetchTermination(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (message string, exitCode int32, found bool) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}, &job); err != nil {
		return "", 0, false
	}
	if !ownedBy(&job, dm) {
		return "", 0, false
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(dm.Namespace),
		client.MatchingLabels{
			decisionmodelv1alpha1.LabelName:             dm.Name,
			decisionmodelv1alpha1.LabelPrefetchRevision: rev,
		}); err != nil {
		return "", 0, false
	}
	var newest *corev1.Pod
	var newestStart time.Time
	for i := range pods.Items {
		p := &pods.Items[i]
		// Trust ownership, not labels: only Pods controlled by the current Job's
		// UID. A foreign Pod with matching labels/job-name, or a previous Job's
		// Pod reusing the name with a different UID, is ignored.
		if c := metav1.GetControllerOf(p); c == nil || c.Kind != "Job" || c.UID != job.UID {
			continue
		}
		term := terminatedContainer(p)
		if term == nil {
			continue
		}
		start := p.CreationTimestamp.Time
		if p.Status.StartTime != nil {
			start = p.Status.StartTime.Time
		}
		if newest == nil || start.After(newestStart) {
			newest, newestStart = p, start
		}
	}
	if newest == nil {
		return "", 0, false
	}
	t := terminatedContainer(newest)
	return t.Message, t.ExitCode, true
}

// terminatedContainer returns the first container's terminated state for a Pod,
// or nil when its first container has not terminated.
func terminatedContainer(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	if len(pod.Status.ContainerStatuses) == 0 {
		return nil
	}
	return pod.Status.ContainerStatuses[0].State.Terminated
}

// storeAccessModes returns the access modes of the store claim a Deployment
// mounts: the PVC's own (they are immutable and authoritative), else what the
// spec asks for, else ReadWriteOnce (the default the PVC is created with).
func (r *DecisionModelReconciler) storeAccessModes(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) []corev1.PersistentVolumeAccessMode {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc); err == nil &&
		len(pvc.Spec.AccessModes) > 0 {
		return pvc.Spec.AccessModes
	}
	if dm.Spec.Cache != nil && len(dm.Spec.Cache.AccessModes) > 0 {
		return dm.Spec.Cache.AccessModes
	}
	return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
}
