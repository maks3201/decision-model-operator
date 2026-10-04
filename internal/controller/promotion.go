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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Manual promotion gate.
const (
	reasonAwaitingApproval  = "AwaitingApproval"
	reasonPromotionApproved = "Approved"

	eventAwaitingPromotion = "AwaitingPromotion"
	eventPromotionApproved = "PromotionApproved"
)

// manualPromotion reports whether spec.rollout.manualPromotion is set.
func manualPromotion(dm *decisionmodelv1alpha1.DecisionModel) bool {
	return dm.Spec.Rollout != nil && dm.Spec.Rollout.ManualPromotion
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
			Reason:  reasonPromotionApproved,
			Message: fmt.Sprintf("revision %s promoted after approval", candidate.Hash),
		})
		return r.promote(ctx, dm, eng, candidate, precision, cacheDegraded)
	}

	// Announce once, on entry (not on every re-reconcile while waiting).
	if dm.Status.Phase != decisionmodelv1alpha1.PhaseAwaitingPromotion {
		r.event(ctx, dm, corev1.EventTypeNormal, eventAwaitingPromotion,
			"revision %s passed its gate%s; set annotation %s=%s to promote",
			candidate.Hash, evaluationSummary(dm, candidate.Hash),
			decisionmodelv1alpha1.AnnotationPromote, candidate.Hash)
	}
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseAwaitingPromotion)
	setStatusCondition(dm, metav1.Condition{
		Type:   decisionmodelv1alpha1.ConditionPromoted,
		Status: metav1.ConditionFalse,
		Reason: reasonAwaitingApproval,
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
