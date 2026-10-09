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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Manual promotion gate.
const (
	reasonPromotionPending = "PromotionPending"
	reasonPromoted         = "Promoted"

	eventAwaitingPromotion  = "AwaitingPromotion"
	eventPromotionApproved  = "PromotionApproved"
	eventDeprecatedApproval = "DeprecatedApproval"
	eventStaleApproval      = "StaleApproval"
)

// manualPromotion reports whether the effective promotion policy holds a
// passed candidate for human approval (promotion: Manual, or the deprecated
// manualPromotion: true alias).
func manualPromotion(dm *decisionmodelv1alpha1.DecisionModel) bool {
	return effectivePromotionPolicy(dm) == decisionmodelv1alpha1.PromotionManual
}

// effectivePromotionPolicy resolves spec.rollout.promotion, honouring the
// deprecated manualPromotion alias and the documented defaults: EvaluationGated
// when rollout.evaluation is set, else Automatic. manualPromotion:true forces
// Manual (CEL rejects a conflicting explicit promotion).
func effectivePromotionPolicy(dm *decisionmodelv1alpha1.DecisionModel) decisionmodelv1alpha1.PromotionPolicy {
	r := dm.Spec.Rollout
	if r == nil {
		return decisionmodelv1alpha1.PromotionAutomatic
	}
	if r.Promotion != "" {
		return r.Promotion
	}
	if r.ManualPromotion { //nolint:staticcheck // deprecated field kept working as an alias for promotion: Manual
		return decisionmodelv1alpha1.PromotionManual
	}
	if r.Evaluation != nil {
		return decisionmodelv1alpha1.PromotionEvaluationGated
	}
	return decisionmodelv1alpha1.PromotionAutomatic
}

// currentApprovalID is the approval identity for this candidate right now:
// the recorded evaluation's approvalID when it belongs to this candidate, else
// (no evaluation, or a manual-only hold) derived from the revision hash alone.
func currentApprovalID(dm *decisionmodelv1alpha1.DecisionModel, candidate *decisionmodelv1alpha1.RevisionStatus) string {
	if ev := dm.Status.Evaluation; ev != nil && ev.Revision == candidate.Hash && ev.ApprovalID != "" {
		return ev.ApprovalID
	}
	return approvalID(candidate.Hash, "", "")
}

// approvalDecision is the outcome of matching the promote annotation.
type approvalDecision int

const (
	approvalNone        approvalDecision = iota // no/stale annotation
	approvalByID                                // matches the current approvalID
	approvalByBareHash                          // deprecated bare revision hash (no eval only)
	approvalBareIgnored                         // bare hash with eval configured: NOT honoured
)

// approvalMatch classifies the promote annotation against this candidate.
// With rollout.evaluation configured the ONLY thing that promotes is the current
// approvalID: a bare revision hash is explicitly NOT honoured (it would otherwise
// carry an approval given for an old result across a re-evaluation of the same
// revision). Without evaluation (manual hold only) the bare hash keeps working
// for one release.
func approvalMatch(dm *decisionmodelv1alpha1.DecisionModel, candidate *decisionmodelv1alpha1.RevisionStatus) approvalDecision {
	tok := dm.Annotations[decisionmodelv1alpha1.AnnotationPromote]
	if tok == "" {
		return approvalNone
	}
	if tok == currentApprovalID(dm, candidate) {
		return approvalByID
	}
	if tok == candidate.Hash && candidate.Hash != "" {
		if evaluationSpec(dm) != nil {
			return approvalBareIgnored
		}
		return approvalByBareHash
	}
	return approvalNone
}

// promoteOrAwait is the single exit of the gate: a candidate that passed
// ModelReady (and evaluation, when configured) either takes traffic right away
// or, with manualPromotion, waits in AwaitingPromotion for an approval.
//
// The first revision of a DecisionModel (no stable yet) is never held: there is
// no traffic to protect and no Service to keep pointing anywhere.
//
// While waiting there is no progress timeout and nothing is requeued: approval
// arrives as an annotation change, which the controller watches, and Pod changes
// still enqueue the DecisionModel.
func (r *DecisionModelReconciler) promoteOrAwait(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	precision string,
	cacheDegraded bool,
	skipApproval bool,
) (ctrl.Result, error) {
	if !manualPromotion(dm) || dm.Status.StableRevision == nil {
		return r.promote(ctx, dm, eng, candidate, precision, cacheDegraded)
	}

	wantID := currentApprovalID(dm, candidate)
	// A re-evaluation triggered by a policy/dataset change must re-park and NOT
	// honour an approval on that cycle (skipApproval): the result is re-recorded
	// under the new identity (new approvalID) first, which the user must approve.
	if !skipApproval {
		decision := approvalMatch(dm, candidate)
		switch decision {
		case approvalByID, approvalByBareHash:
			if decision == approvalByBareHash {
				r.event(ctx, dm, corev1.EventTypeWarning, eventDeprecatedApproval,
					"revision %s approved by bare hash (deprecated); set %s=%s instead",
					candidate.Hash, decisionmodelv1alpha1.AnnotationPromote, wantID)
			}
			r.event(ctx, dm, corev1.EventTypeNormal, eventPromotionApproved,
				"promotion of revision %s approved via %s", candidate.Hash, decisionmodelv1alpha1.AnnotationPromote)
			setStatusCondition(dm, metav1.Condition{
				Type:    decisionmodelv1alpha1.ConditionPromoted,
				Status:  metav1.ConditionTrue,
				Reason:  reasonPromoted,
				Message: fmt.Sprintf("revision %s promoted after approval", candidate.Hash),
			})
			return r.promote(ctx, dm, eng, candidate, precision, cacheDegraded)
		case approvalBareIgnored:
			// A bare revision hash while evaluation is configured is NOT honoured:
			// it could carry an approval given for an old result across a re-eval of
			// the same revision. Warn with the exact command (every reconcile until
			// the user switches to the approvalID; bounded by the regate requeue).
			r.event(ctx, dm, corev1.EventTypeWarning, eventDeprecatedApproval,
				"ignoring bare-hash approval for revision %s; set %s=%s (the approvalID) to promote",
				candidate.Hash, decisionmodelv1alpha1.AnnotationPromote, wantID)
		case approvalNone:
			// A set-but-non-matching approval is stale (identity changed): warn once,
			// naming the current approvalID, and keep waiting.
			if tok := dm.Annotations[decisionmodelv1alpha1.AnnotationPromote]; tok != "" &&
				tok != candidate.Hash && dm.Status.Phase != decisionmodelv1alpha1.PhaseAwaitingPromotion {
				r.event(ctx, dm, corev1.EventTypeWarning, eventStaleApproval,
					"ignoring stale approval %q for revision %s; current approvalID is %s",
					tok, candidate.Hash, wantID)
			}
		}
	}

	// Announce once, on entry (not on every re-reconcile while waiting).
	if dm.Status.Phase != decisionmodelv1alpha1.PhaseAwaitingPromotion {
		r.event(ctx, dm, corev1.EventTypeNormal, eventAwaitingPromotion,
			"candidate %s passed its gate%s; set annotation %s=%s to promote",
			modelRef(candidate), evaluationSummary(dm, candidate.Hash),
			decisionmodelv1alpha1.AnnotationPromote, wantID)
	}
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseAwaitingPromotion)
	setStatusCondition(dm, metav1.Condition{
		Type:   decisionmodelv1alpha1.ConditionPromoted,
		Status: metav1.ConditionFalse,
		Reason: reasonPromotionPending,
		Message: fmt.Sprintf("revision %s is ready; set annotation %s=%s to promote it",
			candidate.Hash, decisionmodelv1alpha1.AnnotationPromote, wantID),
	})
	// Keep re-inspecting the parked candidate's Pods: a model lost in the meantime
	// must not be promoted on the strength of an old probe.
	return r.finish(ctx, dm, r.regateRequeue(), nil)
}

// evaluationSummary renders the recorded evaluation of the candidate for the
// AwaitingPromotion Event: ", accuracy 0.9400, ECE 0.0400" or "" when the
// candidate was not evaluated.
func evaluationSummary(dm *decisionmodelv1alpha1.DecisionModel, hash string) string {
	ev := dm.Status.Evaluation
	if ev == nil || ev.Revision != hash {
		return ""
	}
	s := ", accuracy " + ev.Accuracy
	if ev.ECE != "" {
		s += ", ECE " + ev.ECE
	}
	return s
}

// promotionEvalSummary renders " (accuracy 0.9400, baseline 0.9300)" for the
// Promoted Event, or "" when the promoted revision was not evaluated.
func promotionEvalSummary(dm *decisionmodelv1alpha1.DecisionModel, hash string) string {
	ev := dm.Status.Evaluation
	if ev == nil || ev.Revision != hash || ev.Accuracy == "" {
		return ""
	}
	s := " (accuracy " + ev.Accuracy
	if ev.BaselineAccuracy != "" {
		s += ", baseline " + ev.BaselineAccuracy
	}
	return s + ")"
}

// consumePromoteApproval removes the promote annotation once the revision it
// approved is the persisted stable revision, i.e. the promotion is durable. It
// must not be removed earlier: a status write that conflicts would otherwise
// lose the approval, and leaving it forever would auto-approve a later candidate
// that happens to get the same hash (for example after reverting the spec).
//
// It patches metadata only and refreshes dm from the response, so the caller
// takes its status-patch base afterwards and the optimistic lock stays valid.
func (r *DecisionModelReconciler) consumePromoteApproval(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) error {
	tok, ok := dm.Annotations[decisionmodelv1alpha1.AnnotationPromote]
	if !ok || dm.Status.StableRevision == nil {
		return nil
	}
	// The approval is durable once the revision it named is the persisted stable.
	// Accept both the approvalID (current form) and the bare revision hash (the
	// deprecated form) so whichever the user set is cleared after promotion.
	stable := dm.Status.StableRevision
	if tok != currentApprovalID(dm, stable) && tok != stable.Hash {
		return nil
	}
	base := dm.DeepCopy()
	delete(dm.Annotations, decisionmodelv1alpha1.AnnotationPromote)
	return r.Patch(ctx, dm, client.MergeFrom(base))
}

// promote marks the candidate as the stable revision and moves the Service to it.
//
// Order matters and is the crash-safety invariant: status.stableRevision
// is persisted FIRST, and only when that write is durable is the Service switched.
// The Service selector therefore only ever names a revision that the persisted
// status already records as stable. If the process dies, or the status write
// conflicts or fails, before the switch, the Service still points at the previous
// stable (which is still recorded as stable), and the next reconcile simply
// promotes again. The opposite order left a window in which the Service pointed at
// a revision that status did not know about; a spec change in that window started
// a new candidate whose garbage collection deleted the revision the Service was
// selecting, leaving it with no endpoints.
func (r *DecisionModelReconciler) promote(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	precision string,
	cacheDegraded bool,
) (ctrl.Result, error) {
	rev := candidate.Hash
	prevStable := dm.Status.StableRevision
	// Do not promote onto a foreign Service: traffic would go to someone else's
	// Service, not our Pods. Check through the uncached APIReader (a foreign
	// unlabelled Service is invisible to the cache) BEFORE persisting Ready.
	// The candidate keeps running; this is not a rollout failure.
	if blocked, berr := r.foreignServiceBlocks(ctx, dm); berr != nil {
		return r.finish(ctx, dm, ctrl.Result{}, berr)
	} else if blocked {
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
	}
	switching := prevStable == nil || prevStable.Hash != rev
	candidate.Precision = precision
	dm.Status.StableRevision = candidate
	dm.Status.CandidateRevision = nil
	dm.Status.FailedRevision = nil // successful rollout clears any prior failure
	// Recreate: the candidate is now the stable, so the stop is over — clear the
	// marker so neither revision is held at 0. (No-op under BlueGreen.)
	dm.Status.StableStoppedForRevision = ""
	// A newly promoted stable has a fresh, populated store, so any lost-store
	// recovery bookkeeping from a previous stable no longer applies: reset the
	// bounded-retry count (also the "reset on a new revision" rule).
	if switching {
		clearStoreRecover(dm)
	}

	if switching {
		// Record the demoted revision so its workloads linger for promoteGrace
		// (endpoints of the new revision must populate first), then emit Promoted
		// exactly once for this transition.
		now := metav1.NewTime(r.now())
		dm.Status.LastPromotionTime = &now
		if prevStable != nil {
			dm.Status.PreviousRevision = &decisionmodelv1alpha1.PreviousRevisionStatus{
				Hash: prevStable.Hash, PromotedAt: &now, Revision: prevStable.DeepCopy(),
			}
		}
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionPromoted,
			Status:  metav1.ConditionTrue,
			Reason:  reasonPromoted,
			Message: fmt.Sprintf("revision %s is serving", rev),
		})
		r.event(ctx, dm, corev1.EventTypeNormal, eventPromoted, "promoted %s (revision %s)%s", modelRef(candidate), candidate.Hash, promotionEvalSummary(dm, candidate.Hash))
		bufferRollout(ctx, rolloutPromoted)
	}
	// GC now: everything except the new stable, the just-demoted previous revision
	// (protected within its grace window) and whichever revision the live Service
	// still selects (see gcRevisions): the Service has not moved yet.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseReady)
	setReadyConditions(dm, cacheDegraded)

	// Persist first. Events and metrics are flushed by this write, only on success.
	persisted, conflict, err := r.persistStatus(ctx, dm)
	if conflict {
		// Lost an optimistic-lock race: nothing was stored and the Service was not
		// touched. The next reconcile promotes again from fresh state.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !persisted {
		return ctrl.Result{}, nil
	}
	// With the new stable durably recorded, create its PodDisruptionBudget BEFORE
	// moving traffic to it: with replicas > 1 the Service switch otherwise opens a
	// window where the new stable serves live traffic with no disruption
	// protection (a node drain could take down all replicas at once). A PDB create
	// failure blocks the switch (requeue). On a crash between the status write and
	// here, status already says stable=candidate, so the next reconcile's stable
	// path creates the PDB and re-asserts the Service — convergent either way.
	if err := r.ensurePDB(ctx, dm, rev); err != nil {
		return ctrl.Result{}, err
	}
	// Only now, with the new stable durably recorded, move the Service. If this
	// fails the error requeues; every reconcile path that has a stable re-asserts
	// the Service to it, so it converges.
	if err := r.ensureService(ctx, dm, eng, rev); err != nil {
		return ctrl.Result{}, err
	}
	// Requeue after the grace so the previous revision is collected even if
	// nothing else triggers a reconcile.
	if dm.Status.PreviousRevision != nil {
		return ctrl.Result{RequeueAfter: promoteGrace}, nil
	}
	return ctrl.Result{}, nil
}

// rollbackOrFail handles a failed candidate: RolledBack if a stable revision
// exists (which keeps serving), else Failed. The failed revision is recorded so
// it is not automatically retried; a spec change (new hash) clears it.
//
// Durability-first (same contract as rollbackToPrevious): it records
// failedRevision, phase and conditions and PERSISTS them before deleting the
// candidate's workloads. If the write loses an optimistic-lock race or errors,
// nothing is deleted and the next reconcile redoes the decision from fresh
// state — so a crash can never leave the DM with its candidate workloads gone
// but no failed-revision record, which would restart the same rollout (breaking
// the "not retried automatically" guarantee). The workload delete after a
// durable write is idempotent; a partial delete is finished by gcRevisions /
// the next reconcile.
func (r *DecisionModelReconciler) rollbackOrFail(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	failed *decisionmodelv1alpha1.RevisionStatus,
	reason, message string,
) (ctrl.Result, error) {
	dm.Status.CandidateRevision = nil
	failedAt := metav1.NewTime(r.now())
	failed.Reason = reason
	failed.Message = message
	failed.FailedAt = &failedAt
	dm.Status.FailedRevision = failed
	// Recreate: the stable was scaled to 0 to free capacity for this candidate.
	// Clear the stopped marker as part of this same (persist-then-act) failure
	// write, so the stable scales back to its replicas on the next stable-path
	// reconcile. Emit StableRestored. (No-op under BlueGreen / when not stopped.)
	if dm.Status.StableRevision != nil {
		recreateRestoreStable(ctx, r, dm, dm.Status.StableRevision)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message,
	})
	if dm.Status.StableRevision != nil {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseRolledBack)
		// Ready stays True on a rollback: the stable revision keeps serving. The
		// reason tells the story (a candidate was rejected), the Degraded condition
		// above carries the failure detail.
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  reasonCandidateRejected,
			Message: fmt.Sprintf("candidate %s rejected (%s); %s keeps serving", modelRef(failed), reason, modelRef(dm.Status.StableRevision)),
		})
		r.event(ctx, dm, corev1.EventTypeWarning, eventRolledBack,
			"candidate %s (revision %s) rejected (%s): %s; %s keeps serving",
			modelRef(failed), failed.Hash, reason, message, modelRef(dm.Status.StableRevision))
		bufferRollout(ctx, rolloutRolledBack)
	} else {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
		r.event(ctx, dm, corev1.EventTypeWarning, eventFailed,
			"revision %s failed (%s): %s", modelRef(failed), reason, message)
		bufferRollout(ctx, rolloutFailed)
	}

	// Persist first. On conflict nothing was stored and no Event/metric emitted;
	// requeue and redo the decision from fresh state next reconcile.
	persisted, conflict, err := r.persistStatus(ctx, dm)
	if conflict {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !persisted {
		return ctrl.Result{}, nil
	}

	// Only now, with the failure durably recorded, delete the candidate's
	// workloads. If this fails the failure record stands and gcRevisions / the
	// next reconcile finish the delete; so requeue rather than block.
	if derr := r.deleteRevisionWorkloads(ctx, dm, failed.Hash); derr != nil {
		return ctrl.Result{}, derr
	}
	return ctrl.Result{}, nil
}

// rollbackOrFailPermanent is rollbackOrFail for a permanent failure: after the
// status write it returns a TerminalError so the workqueue stops retrying.
func (r *DecisionModelReconciler) rollbackOrFailPermanent(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	failed *decisionmodelv1alpha1.RevisionStatus,
	reason, message string,
) (ctrl.Result, error) {
	res, err := r.rollbackOrFail(ctx, dm, failed, reason, message)
	if err != nil {
		return res, err // status write failed; let the caller requeue
	}
	if _, ok := permanentReasons[reason]; ok {
		return res, reconcile.TerminalError(fmt.Errorf("%s: %s", reason, message))
	}
	return res, nil
}
