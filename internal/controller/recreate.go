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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// rolloutStrategy returns the effective rollout strategy, defaulting to
// BlueGreen when unset (so existing DecisionModels and an empty spec keep the
// current behaviour).
func rolloutStrategy(dm *decisionmodelv1alpha1.DecisionModel) decisionmodelv1alpha1.RolloutStrategy {
	if dm.Spec.Rollout != nil && dm.Spec.Rollout.Strategy == decisionmodelv1alpha1.RolloutRecreate {
		return decisionmodelv1alpha1.RolloutRecreate
	}
	return decisionmodelv1alpha1.RolloutBlueGreen
}

// rollbackStrategy is the strategy a stabilization-window rollback must stage
// with: the one recorded on the previous revision at promotion time, not the live
// spec (spec.rollout.strategy can change mid-rollout and is not part of the
// revision hash, so reading it live could pick the wrong branch — e.g. try to
// BlueGreen-switch to a previous revision that was scaled to 0 for a Recreate
// rollout). It falls back to the live spec only for a previous revision recorded
// by an older operator (no Strategy field).
func rollbackStrategy(dm *decisionmodelv1alpha1.DecisionModel) decisionmodelv1alpha1.RolloutStrategy {
	if p := dm.Status.PreviousRevision; p != nil && p.Strategy != "" {
		return p.Strategy
	}
	return rolloutStrategy(dm)
}

// stableStopped reports whether the stable revision is currently scaled to 0 for
// a Recreate rollout (status.stableStoppedForRevision set).
func stableStopped(dm *decisionmodelv1alpha1.DecisionModel) bool {
	return dm.Status.StableStoppedForRevision != ""
}

// desiredReplicasForRevision is desiredReplicas, except the stable revision is
// rendered at 0 while it is intentionally stopped for a Recreate rollout
// (status.stableStoppedForRevision set and rev is the stable). This is the single
// place the scaled-down stable's replica count is decided, so ensureDeployment's
// drift repair holds it at 0 instead of reverting it to spec.replicas.
// desiredReplicasForRevision is desiredReplicas, except a revision is rendered at
// 0 while a Recreate rollout intentionally holds it down:
//   - the stable while it is stopped for a candidate (status.stableStoppedForRevision), and
//   - the previous revision during a Recreate stabilization window — it was scaled
//     to 0 for the rollout and must stay at 0 for the whole window so that, on one
//     GPU, it never schedules a Pod that would sit Pending or steal the GPU from the
//     new stable. (Nothing re-renders the previous revision today; this makes the
//     "stays at 0" guarantee explicit and survives a future change that does.)
//
// This is the single place a Recreate-held revision's replica count is decided, so
// ensureDeployment's drift repair holds it at 0 instead of reverting to spec.replicas.
func desiredReplicasForRevision(dm *decisionmodelv1alpha1.DecisionModel, rev string) int32 {
	if stableStopped(dm) &&
		dm.Status.StableRevision != nil && dm.Status.StableRevision.Hash == rev {
		return 0
	}
	if rollbackStrategy(dm) == decisionmodelv1alpha1.RolloutRecreate &&
		dm.Status.PreviousRevision != nil && dm.Status.PreviousRevision.Hash == rev {
		return 0
	}
	return desiredReplicas(dm)
}

// maybeRecreateStop runs the Recreate stop gate: when the strategy is Recreate
// and a stable exists, it stops the stable (persist-then-act) and waits for its
// Pods to be gone before the candidate starts. Returns handled=true (with the
// result/err the caller must finish with) while the stop is in progress or on
// error; handled=false when BlueGreen, no stable, or the stable is confirmed down
// so the caller proceeds to start the candidate.
func (r *DecisionModelReconciler) maybeRecreateStop(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
) (handled bool, res ctrl.Result, err error) {
	if rolloutStrategy(dm) != decisionmodelv1alpha1.RolloutRecreate || dm.Status.StableRevision == nil {
		return false, ctrl.Result{}, nil
	}
	return r.recreateStopStable(ctx, dm, eng, dm.Status.StableRevision, candidate)
}

// recreateStopStable scales the stable revision's Deployment to 0 for a Recreate
// rollout, recording the intent in status FIRST (persist-then-act) so a crash
// after the write still has the stable marked stopped and the next reconcile
// renders it at 0. It is called only once the candidate's model is Cached and
// only when a stable exists. Returns handled=true with the result the caller must
// return while the stop is in progress (status just persisted, or the stable Pods
// are not yet gone); handled=false once the stable is confirmed down so the
// caller proceeds to start the candidate.
func (r *DecisionModelReconciler) recreateStopStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	candidate *decisionmodelv1alpha1.RevisionStatus,
) (handled bool, res ctrl.Result, err error) {
	// Record the intent before scaling anything. If the status write fails we
	// return without touching the Deployment, so the stable keeps serving and the
	// next reconcile retries from a clean state.
	if dm.Status.StableStoppedForRevision != candidate.Hash {
		dm.Status.StableStoppedForRevision = candidate.Hash
		r.event(ctx, dm, corev1.EventTypeNormal, eventStableStopped,
			"stopping stable revision %s to free capacity for candidate revision %s (Recreate strategy); "+
				"serving is paused until the candidate is promoted", stable.Hash, candidate.Hash)
		bufferRollout(ctx, rolloutStableStopped)
		persisted, conflict, perr := r.persistStatus(ctx, dm)
		if conflict {
			return true, ctrl.Result{RequeueAfter: probeRequeue}, nil
		}
		if perr != nil {
			return true, ctrl.Result{}, perr
		}
		if !persisted {
			return true, ctrl.Result{}, nil
		}
	}

	// Scale the stable Deployment to 0 (idempotent via desiredReplicasForRevision)
	// and wait until its Pods are gone before starting the candidate, so the two
	// revisions never contend for the single GPU.
	if err := r.scaleStableToZero(ctx, dm, eng, stable); err != nil {
		return true, ctrl.Result{}, err
	}
	gone, err := r.stableReplicasGone(ctx, dm, stable.Hash)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if !gone {
		return true, ctrl.Result{RequeueAfter: probeRequeue}, nil
	}
	return false, ctrl.Result{}, nil
}

// scaleStableToZero re-renders the stable Deployment, which now resolves to 0
// replicas (desiredReplicasForRevision), without re-probing or re-rendering it
// against anything the live spec changed. It reuses maintainStable, which renders
// from the recorded stable identity.
func (r *DecisionModelReconciler) scaleStableToZero(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
) error {
	// apiKey is not needed to scale down; maintainStable preserves the existing
	// template checksum on an empty key, so no roll. Pass "".
	return r.maintainStable(ctx, dm, eng, stable, "")
}

// stableReplicasGone reports whether the revision has no serving Pods left, so
// the single GPU it held is actually free for another revision to take. A Pod
// that is merely terminating (DeletionTimestamp set) is NOT counted as gone: it
// still holds the device until the kubelet finishes killing the container, and
// bringing a GPU revision up against a still-attached device deadlocks. "Gone"
// therefore means no Pod object for the revision remains. A Pod-list/ownership
// error aborts (returned) rather than being read as "gone".
func (r *DecisionModelReconciler) stableReplicasGone(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, error) {
	pods, err := r.revisionPods(ctx, dm, rev)
	if err != nil {
		return false, err
	}
	return len(pods) == 0, nil
}

// recreateRestoreStable scales the stable revision back up after a Recreate
// candidate failed, clearing the stopped marker. It is persist-then-act: the
// caller (rollbackOrFail) persists the cleared marker as part of the failure
// status write, then the next stable-path reconcile renders the stable at its
// replicas again. This helper only emits the StableRestored Event and clears the
// marker in memory; it does not write status itself.
func recreateRestoreStable(ctx context.Context, r *DecisionModelReconciler,
	dm *decisionmodelv1alpha1.DecisionModel, stable *decisionmodelv1alpha1.RevisionStatus) {
	if dm.Status.StableStoppedForRevision == "" {
		return
	}
	r.event(ctx, dm, corev1.EventTypeNormal, eventStableRestored,
		"restoring stable revision %s after the Recreate candidate failed", stable.Hash)
	bufferRollout(ctx, rolloutStableRestored)
	dm.Status.StableStoppedForRevision = ""
}

// recreateBaselineUnavailable reports whether a Recreate rollout has the stable
// scaled to 0, so a relative eval gate's baseline (which needs a live stable Pod)
// cannot be measured and must be skipped rather than failing closed. It is only
// true while the stable is intentionally stopped for this candidate.
func recreateBaselineUnavailable(dm *decisionmodelv1alpha1.DecisionModel) bool {
	return stableStopped(dm)
}

// dispatchRecreateRollback drives a staged Recreate rollback from Reconcile. It
// returns handled=true when it owns this reconcile (and the caller must return
// res/err). It:
//   - clears a stale marker whose Target no longer matches the recorded stable
//     (it could never be driven by reconcileRecreateRollback, so it would linger
//     forever) and reports handled=false so a normal dispatch proceeds;
//   - routes a missing/terminating target store to the stable store-recovery path
//     instead of bringing the target up against a dead volume;
//   - otherwise runs reconcileRecreateRollback.
//
// storeLost/storeTerminating are the signals already computed for the target
// (which is the recorded stable here) earlier in Reconcile.
func (r *DecisionModelReconciler) dispatchRecreateRollback(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
	storeLost, storeTerminating bool,
) (bool, ctrl.Result, error) {
	rb := dm.Status.RecreateRollback
	if rb == nil {
		return false, ctrl.Result{}, nil
	}
	// Stale marker (Target moved or no stable): drop it so it stops blocking later
	// reconciles. The clear is persisted by whichever normal dispatch runs next
	// (all end in finish()); report not-handled so that dispatch proceeds.
	if stable == nil || rb.Target != stable.Hash {
		dm.Status.RecreateRollback = nil
		return false, ctrl.Result{}, nil
	}
	// A missing/terminating target store must recover (annotated recreate +
	// reprefetch) before the target can serve; otherwise the rollback would hang
	// probing an empty volume. Recovery owns Degraded; a later reconcile re-enters
	// here once the store is healthy.
	if storeLost || storeTerminating {
		claim, legacy := r.storeClaimForStable(ctx, dm, stable)
		handled, _, res, rerr := r.recoverStableStore(ctx, dm, eng, stable, claim, legacy)
		if rerr != nil {
			r2, e2 := r.finish(ctx, dm, ctrl.Result{}, rerr)
			return true, r2, e2
		}
		if handled {
			r2, e2 := r.finish(ctx, dm, res, nil)
			return true, r2, e2
		}
	}
	res, err := r.reconcileRecreateRollback(ctx, dm, eng, stable, apiKey)
	return true, res, err
}

// reconcileRecreateRollback finishes a staged Recreate rollback. The new stable
// turned unhealthy inside its stabilization window; rollbackToPrevious recorded
// the decision (status.stableRevision = the target/previous revision,
// status.failedRevision = the unhealthy one, status.recreateRollback set) and
// deleted the unhealthy revision's workloads. This path brings the target back on
// the single GPU in a capacity-safe order and is restart-safe (all state from
// status + cluster):
//  1. (re)delete the unhealthy revision's workloads — idempotent, so a crash or a
//     delete error between rollbackToPrevious's persist and its own delete cannot
//     leave the failed Pods holding the GPU forever;
//  2. wait until the unhealthy revision's Pods are actually gone (GPU freed) —
//     only THEN render the target Deployment at its replicas, so the target never
//     schedules a Pod that fights the dying one for the single GPU;
//  3. wait until the target is model-ready;
//  4. switch the Service to the target and clear the marker.
//
// If the target never becomes model-ready within the Starting timeout (measured
// from status.recreateRollback.startedAt) the rollback is abandoned: the DM goes
// Failed/Degraded and the marker is cleared, so it does not loop forever with the
// Service down.
func (r *DecisionModelReconciler) reconcileRecreateRollback(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	target *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) (ctrl.Result, error) {
	rb := dm.Status.RecreateRollback
	// 1. (Re)delete the unhealthy revision's workloads. rollbackToPrevious already
	// did this once, but a crash or a delete error in between would leave only the
	// "wait for gone" step, which would never progress. deleteRevisionWorkloads is
	// idempotent (NotFound is ignored), so repeating it here closes that gap.
	if rb.Failed != "" {
		if err := r.deleteRevisionWorkloads(ctx, dm, rb.Failed); err != nil {
			return r.finish(ctx, dm, ctrl.Result{}, err)
		}
	}
	// 2. Wait until the unhealthy revision's Pods are actually gone (a terminating
	// Pod still holds the GPU — stableReplicasGone no longer counts it as gone).
	// The target Deployment is NOT rendered until the GPU is free, so it cannot
	// schedule a Pod that competes with the dying one.
	gone, err := r.stableReplicasGone(ctx, dm, rb.Failed)
	if err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if !gone {
		if timedOut, res, terr := r.recreateRollbackTimedOut(ctx, dm, target); timedOut {
			return res, terr
		}
		// Keep the Service off the target (documented downtime) and wait.
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
	}

	// 3. GPU is free: render the target Deployment from its recorded identity (now
	// at its replicas, since desiredReplicasForRevision no longer holds it at 0 —
	// it is the stable again and the stop marker is cleared) and probe it.
	claim, _ := r.storeClaimForStable(ctx, dm, target)
	params := r.stableParams(ctx, dm, target, claim)
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, params, target.Hash, true, keyChecksum, legacyChecksum, false); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if err := r.ensurePDB(ctx, dm, target.Hash); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	ready, precision, probeErr := r.probePods(ctx, dm, eng, target, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return r.finish(ctx, dm, ctrl.Result{}, probeErr)
	}
	if precision != "" {
		target.Precision = precision
	}
	r.setReplicaStatus(dm, ready)
	if ready < 1 {
		// The GPU is free and the target is rendered, but it has not loaded the
		// model yet. Abandon the rollback if it has run past the Starting timeout
		// (a target that never comes up must not keep the Service down forever);
		// otherwise keep the Service off the target (documented downtime) and wait.
		if timedOut, res, terr := r.recreateRollbackTimedOut(ctx, dm, target); timedOut {
			return res, terr
		}
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
	}
	// 4. Target is up: switch the Service to it, clear the marker, announce.
	if serr := r.ensureService(ctx, dm, eng, target.Hash); serr != nil {
		if errors.Is(serr, errResourceConflict) {
			return r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
		}
		return r.finish(ctx, dm, ctrl.Result{}, serr)
	}
	r.event(ctx, dm, corev1.EventTypeNormal, eventStableRestored,
		"Recreate rollback complete: restored revision %s and switched traffic to it", target.Hash)
	bufferRollout(ctx, rolloutStableRestored)
	dm.Status.RecreateRollback = nil
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonCandidateRejected,
		Message: fmt.Sprintf("rolled back to %s; it is serving again", target.Hash),
	})
	return r.finish(ctx, dm, r.regateRequeue(), nil)
}

// recreateRollbackTimedOut abandons a staged Recreate rollback whose target never
// became model-ready within the Starting timeout, measured from
// status.recreateRollback.startedAt. It returns (false, _) while still within the
// window (the caller keeps waiting). On timeout it marks the DM Failed with
// Degraded=RecreateRollbackTimeout, clears the marker (so the dispatch stops
// re-entering this path), emits a Warning, and persists — the Service stays off
// the never-ready target (the documented single-GPU rollback downtime becomes a
// terminal failure a human must resolve, e.g. with the retry annotation). The
// returned ctrl.Result/error is finish()'s, ready to return from the caller.
func (r *DecisionModelReconciler) recreateRollbackTimedOut(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	target *decisionmodelv1alpha1.RevisionStatus,
) (bool, ctrl.Result, error) {
	rb := dm.Status.RecreateRollback
	if rb == nil || rb.StartedAt == nil {
		return false, ctrl.Result{}, nil
	}
	if r.now().Sub(rb.StartedAt.Time) <= startingTimeout(dm) {
		return false, ctrl.Result{}, nil
	}
	msg := fmt.Sprintf(
		"Recreate rollback to %s did not become model-ready within %s; abandoning the rollback",
		target.Hash, startingTimeout(dm))
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
	dm.Status.RecreateRollback = nil
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonRecreateRollbackTimeout,
		Message: msg,
	})
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reasonRecreateRollbackTimeout,
		Message: msg,
	})
	r.event(ctx, dm, corev1.EventTypeWarning, eventRecreateRollbackTimeout, "%s", msg)
	res, err := r.finish(ctx, dm, ctrl.Result{}, nil)
	return true, res, err
}

// announceRecreateRelativeSkip emits the Warning that the relative eval gates are
// skipped under Recreate because the stable is stopped and cannot be measured.
func (r *DecisionModelReconciler) announceRecreateRelativeSkip(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	candidate *decisionmodelv1alpha1.RevisionStatus,
) {
	r.event(ctx, dm, corev1.EventTypeWarning, eventEvaluationOnHold,
		"candidate %s: relative eval gates (maxAccuracyDrop/maxECEIncrease/maxMacroF1Drop) are skipped "+
			"under the Recreate strategy because the stable is stopped and cannot be measured as a baseline",
		modelRef(candidate))
}

// resolveEvalGates parses the eval thresholds into evalGates and reports whether
// a stable baseline is needed (any relative gate set). Under the Recreate
// strategy with the stable stopped, the relative gates (accuracy drop, ECE
// increase, macro-F1 drop) are dropped — there is no live stable to measure —
// and a Warning is emitted; the absolute gates (minAccuracy, maxECE, minMacroF1)
// are kept.
func (r *DecisionModelReconciler) resolveEvalGates(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	evalSpec *decisionmodelv1alpha1.EvaluationSpec,
	candidate *decisionmodelv1alpha1.RevisionStatus,
) (evalGates, bool) {
	minAcc, _ := parseDecimal(evalSpec.MinAccuracy)
	maxDrop, hasDrop := parseDecimal(evalSpec.MaxAccuracyDrop)
	maxECE, hasMaxECE := parseDecimal(evalSpec.MaxECE)
	maxECEInc, hasMaxECEInc := parseDecimal(evalSpec.MaxECEIncrease)
	minMacroF1, hasMinF1 := parseDecimal(evalSpec.MinMacroF1)
	maxF1Drop, hasF1Drop := parseDecimal(evalSpec.MaxMacroF1Drop)
	needsBaseline := hasDrop || hasMaxECEInc || hasF1Drop

	skipped := needsBaseline && recreateBaselineUnavailable(dm)
	if skipped {
		needsBaseline, hasDrop, hasMaxECEInc, hasF1Drop = false, false, false, false
		r.announceRecreateRelativeSkip(ctx, dm, candidate)
	}
	return evalGates{
		minAcc:  minAcc,
		maxDrop: maxDrop, hasDrop: hasDrop,
		maxECE: maxECE, hasMaxECE: hasMaxECE,
		maxECEInc: maxECEInc, hasMaxECEInc: hasMaxECEInc,
		minMacroF1: minMacroF1, hasMinF1: hasMinF1,
		maxF1Drop: maxF1Drop, hasF1Drop: hasF1Drop,
		recreateSkipped: skipped,
	}, needsBaseline
}
