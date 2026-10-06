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

	eventAwaitingPromotion = "AwaitingPromotion"
	eventPromotionApproved = "PromotionApproved"
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

// isApproved reports whether the promote annotation approves exactly this
// candidate revision. An approval for any other hash (a stale one for an older
// candidate) never promotes a newer candidate.
func isApproved(dm *decisionmodelv1alpha1.DecisionModel, candidateHash string) bool {
	return candidateHash != "" && dm.Annotations[decisionmodelv1alpha1.AnnotationPromote] == candidateHash
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

	// A re-evaluation triggered by a policy change must re-park and NOT honour an
	// approval on that cycle: the result is re-recorded under the new policy hash
	// first. The SAME approval then promotes the candidate on the next reconcile,
	// on the freshly recorded result addendum).
	if isApproved(dm, candidate.Hash) && !skipApproval {
		r.event(ctx, dm, corev1.EventTypeNormal, eventPromotionApproved,
			"promotion of revision %s approved via %s", candidate.Hash, decisionmodelv1alpha1.AnnotationPromote)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionPromoted,
			Status:  metav1.ConditionTrue,
			Reason:  reasonPromoted,
			Message: fmt.Sprintf("revision %s promoted after approval", candidate.Hash),
		})
		return r.promote(ctx, dm, eng, candidate, precision, cacheDegraded)
	}

	// Announce once, on entry (not on every re-reconcile while waiting).
	if dm.Status.Phase != decisionmodelv1alpha1.PhaseAwaitingPromotion {
		r.event(ctx, dm, corev1.EventTypeNormal, eventAwaitingPromotion,
			"candidate %s passed its gate%s; set annotation %s=%s to promote",
			modelRef(candidate), evaluationSummary(dm, candidate.Hash),
			decisionmodelv1alpha1.AnnotationPromote, candidate.Hash)
	}
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseAwaitingPromotion)
	setStatusCondition(dm, metav1.Condition{
		Type:   decisionmodelv1alpha1.ConditionPromoted,
		Status: metav1.ConditionFalse,
		Reason: reasonPromotionPending,
		Message: fmt.Sprintf("revision %s is ready; set annotation %s=%s to promote it",
			candidate.Hash, decisionmodelv1alpha1.AnnotationPromote, candidate.Hash),
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
	if !ok || dm.Status.StableRevision == nil || dm.Status.StableRevision.Hash != tok {
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

	if switching {
		// Record the demoted revision so its workloads linger for promoteGrace
		// (endpoints of the new revision must populate first), then emit Promoted
		// exactly once for this transition.
		now := metav1.NewTime(r.now())
		dm.Status.LastPromotionTime = &now
		if prevStable != nil {
			dm.Status.PreviousRevision = &decisionmodelv1alpha1.PreviousRevisionStatus{
				Hash: prevStable.Hash, PromotedAt: &now,
			}
		}
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionPromoted,
			Status:  metav1.ConditionTrue,
			Reason:  reasonPromoted,
			Message: fmt.Sprintf("revision %s is serving", rev),
		})
		r.event(ctx, dm, corev1.EventTypeNormal, eventPromoted, "promoted %s%s", modelRef(candidate), promotionEvalSummary(dm, candidate.Hash))
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
func (r *DecisionModelReconciler) rollbackOrFail(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	failed *decisionmodelv1alpha1.RevisionStatus,
	reason, message string,
) (ctrl.Result, error) {
	if err := r.deleteRevisionWorkloads(ctx, dm, failed.Hash); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	dm.Status.CandidateRevision = nil
	failedAt := metav1.NewTime(r.now())
	failed.Reason = reason
	failed.Message = message
	failed.FailedAt = &failedAt
	dm.Status.FailedRevision = failed
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
			"candidate %s rejected (%s): %s; %s keeps serving",
			modelRef(failed), reason, message, modelRef(dm.Status.StableRevision))
		bufferRollout(ctx, rolloutRolledBack)
	} else {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
		r.event(ctx, dm, corev1.EventTypeWarning, eventFailed,
			"revision %s failed (%s): %s", modelRef(failed), reason, message)
		bufferRollout(ctx, rolloutFailed)
	}
	return r.finish(ctx, dm, ctrl.Result{}, nil)
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
