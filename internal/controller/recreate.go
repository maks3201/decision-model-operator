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
	if rolloutStrategy(dm) == decisionmodelv1alpha1.RolloutRecreate &&
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

// stableReplicasGone reports whether the stable revision has no serving Pods
// left (so the candidate can safely take the GPU). A Pod-list/ownership error
// aborts (returned) rather than being read as "gone".
func (r *DecisionModelReconciler) stableReplicasGone(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, error) {
	pods, err := r.revisionPods(ctx, dm, rev)
	if err != nil {
		return false, err
	}
	for i := range pods {
		if pods[i].DeletionTimestamp == nil {
			return false, nil
		}
	}
	return true, nil
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

// reconcileRecreateRollback finishes a staged Recreate rollback. The new stable
// turned unhealthy inside its stabilization window; rollbackToPrevious recorded
// the decision (status.stableRevision = the target/previous revision,
// status.failedRevision = the unhealthy one, status.recreateRollback set) and
// deleted the unhealthy revision's workloads. This path brings the target back on
// the single GPU in a capacity-safe order and is restart-safe (all state from
// status + cluster):
//  1. render the target Deployment at its replicas (it was scaled to 0 for the rollout);
//  2. wait until the unhealthy revision's Pods are gone (GPU freed);
//  3. wait until the target is model-ready;
//  4. switch the Service to the target and clear the marker.
func (r *DecisionModelReconciler) reconcileRecreateRollback(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	target *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) (ctrl.Result, error) {
	rb := dm.Status.RecreateRollback
	// 1. Render the target Deployment from its recorded identity (now at its
	// replicas, since desiredReplicasForRevision no longer holds it at 0 — it is
	// the stable again and the stop marker is cleared).
	claim, _ := r.storeClaimForStable(ctx, dm, target)
	params := r.stableParams(ctx, dm, target, claim)
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, params, target.Hash, true, keyChecksum, legacyChecksum, false); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if err := r.ensurePDB(ctx, dm, target.Hash); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	// 2. Wait until the unhealthy revision's Pods are gone (free the GPU).
	gone, err := r.stableReplicasGone(ctx, dm, rb.Failed)
	if err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	// 3. Probe the target for model-readiness.
	ready, precision, probeErr := r.probePods(ctx, dm, eng, target, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return r.finish(ctx, dm, ctrl.Result{}, probeErr)
	}
	if precision != "" {
		target.Precision = precision
	}
	r.setReplicaStatus(dm, ready)
	if !gone || ready < 1 {
		// Still stopping the unhealthy revision or waiting for the target to load
		// the model: keep the Service off the target (documented downtime) and wait.
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
