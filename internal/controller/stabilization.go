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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// stabilizationResult tells the stable path what the stabilization window did
// this reconcile.
type stabilizationResult struct {
	rolledBack bool        // the new stable was rolled back to the previous revision
	res        ctrl.Result // the result to return when rolledBack
	err        error
}

// reconcileStabilization runs the post-promotion stabilization window on the
// stable path. While the previous revision is still kept (status.previousRevision
// within the window) it watches the new stable's health; if the new stable turns
// out unhealthy it switches the Service back to the previous revision and records
// a rollback (PostPromotionUnhealthy). Once the window passes healthy it emits a
// Stabilized Event/condition and lets gcRevisions collect the previous revision.
//
// It is idempotent and level-triggered: all state comes from status
// (previousRevision.promotedAt, the Stabilizing condition's transition time) and
// cluster state (Pods, Service), never an in-memory timer.
func (r *DecisionModelReconciler) reconcileStabilization(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	ready int32,
) stabilizationResult {
	p := dm.Status.PreviousRevision
	window := stabilizationFor(dm)
	// No window to run: either no previous revision kept, or stabilization is
	// disabled (0 -> previous collected after the endpoint-gap grace by GC).
	if p == nil || p.Hash == "" || p.PromotedAt == nil || window <= 0 {
		meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing)
		return stabilizationResult{}
	}
	elapsed := r.now().Sub(p.PromotedAt.Time)

	unhealthy, immediate, detail := r.newStableUnhealthy(ctx, dm, stable, ready)
	if unhealthy && (immediate || r.unhealthyDebounceElapsed(dm)) {
		return r.rollbackToPrevious(ctx, dm, eng, stable, p, detail)
	}

	if elapsed >= window {
		// Window passed healthy: announce stabilization once, drop the condition,
		// and let gcRevisions (called next on the stable path) collect the
		// previous revision.
		if meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing) != nil {
			r.event(ctx, dm, corev1.EventTypeNormal, eventStabilized,
				"revision %s stable after the %s stabilization window; collecting the previous revision",
				modelRef(stable), window)
		}
		meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing)
		return stabilizationResult{}
	}

	// Still in the window. Record Stabilizing: True (watching) when healthy, False
	// (PostPromotionUnhealthy) when unhealthy-but-not-yet-past-debounce — its
	// LastTransitionTime is the idempotent debounce clock.
	if unhealthy {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionStabilizing,
			Status:  metav1.ConditionFalse,
			Reason:  reasonPostPromotionUnhealthy,
			Message: detail,
		})
	} else {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionStabilizing,
			Status:  metav1.ConditionTrue,
			Reason:  reasonStabilizing,
			Message: fmt.Sprintf("watching the new stable for %s before collecting the previous revision", window),
		})
	}
	return stabilizationResult{}
}

// unhealthyDebounceElapsed reports whether the Stabilizing condition has been
// False (PostPromotionUnhealthy) for longer than the debounce, i.e. the new
// stable has stayed below quorum long enough to roll back. The condition's
// LastTransitionTime is the clock, so it survives restarts.
func (r *DecisionModelReconciler) unhealthyDebounceElapsed(dm *decisionmodelv1alpha1.DecisionModel) bool {
	c := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != reasonPostPromotionUnhealthy {
		return false // not yet marked unhealthy this episode; start the debounce now
	}
	return r.now().Sub(c.LastTransitionTime.Time) >= postPromotionDebounce
}

// newStableUnhealthy inspects the new stable's Pods and reports whether it is
// unhealthy during the stabilization window. immediate is true for a gate
// digest/device mismatch or a CrashLoopBackOff container (roll back now, no
// debounce); a mere shortfall below the model-ready quorum is not immediate (it
// debounces, to tolerate a brief dip during a rolling restart).
func (r *DecisionModelReconciler) newStableUnhealthy(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	ready int32,
) (unhealthy, immediate bool, detail string) {
	pods, err := r.revisionPods(ctx, dm, stable.Hash)
	if err == nil {
		for i := range pods {
			pod := &pods[i]
			if pod.DeletionTimestamp != nil {
				continue
			}
			if reason := gateMismatchReason(pod); reason != "" {
				return true, true, fmt.Sprintf("new stable Pod %s: %s", pod.Name, reason)
			}
			if crashLoopingContainer(pod) {
				return true, true, fmt.Sprintf("new stable Pod %s is in CrashLoopBackOff", pod.Name)
			}
		}
	}
	// Quorum shortfall (debounced): fewer than max(1, ceil(desired/2)) model-ready.
	if quorum := readyQuorum(desiredReplicas(dm)); ready < quorum {
		return true, false, fmt.Sprintf("only %d of %d required model-ready Pods", ready, quorum)
	}
	return false, false, ""
}

// readyQuorum is the minimum model-ready replica count the new stable must keep
// during the window: max(1, ceil(desired/2)).
func readyQuorum(desired int32) int32 {
	if desired <= 1 {
		return 1
	}
	q := (desired + 1) / 2
	if q < 1 {
		q = 1
	}
	return q
}

// gateMismatchReason returns the model-ready gate's reason when it is False for a
// digest/device mismatch (a definitive "wrong model loaded"), else "".
func gateMismatchReason(pod *corev1.Pod) string {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Type != corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate) {
			continue
		}
		if c.Status == corev1.ConditionFalse && (c.Reason == reasonDigestMismatch || c.Reason == reasonDeviceMismatch) {
			return c.Reason
		}
	}
	return ""
}

// crashLoopingContainer reports whether any container of the Pod is waiting in
// CrashLoopBackOff.
func crashLoopingContainer(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		if w := pod.Status.ContainerStatuses[i].State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
			return true
		}
	}
	return false
}

// rollbackToPrevious ends the stabilization window by rolling the new stable
// back to the previous revision. The order is durability-first: it builds the
// new status (stable = previous, failed = new, previousRevision = nil) and
// PERSISTS it before touching the Service or deleting any workload. If the write
// loses an optimistic-lock race or errors, nothing in the cluster is changed and
// the next reconcile redoes the whole decision from fresh state — so a failed
// write can never leave status saying stable=<failed> while the Service and
// workloads have already been rolled back (which would re-create the bad model
// as stable). Only after a durable write does it switch the Service back and
// delete the failed revision; both are idempotent, and if either fails here the
// normal stable path converges them on a later reconcile (ensureService re-points
// to the recorded stable, gcRevisions collects the no-longer-kept failed hash).
func (r *DecisionModelReconciler) rollbackToPrevious(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	failed *decisionmodelv1alpha1.RevisionStatus,
	prev *decisionmodelv1alpha1.PreviousRevisionStatus,
	detail string,
) stabilizationResult {
	msg := fmt.Sprintf("new stable %s unhealthy during the stabilization window (%s); rolling back to %s",
		modelRef(failed), detail, prev.Hash)

	failedAt := metav1.NewTime(r.now())
	rolledBack := failed.DeepCopy()
	rolledBack.Reason = reasonPostPromotionUnhealthy
	rolledBack.Message = detail
	rolledBack.FailedAt = &failedAt

	// Promote the previous revision back to stable from its own recorded identity
	// (retained in previousRevision.revision), so the stable path re-renders its
	// workloads exactly as they were — never from the live spec (ARCHITECTURE §7).
	restored := prev.Revision
	if restored == nil {
		// Defensive: a previousRevision recorded by an older operator had only the
		// hash; fall back to the hash so the stable path's legacy rendering (from
		// the live Deployment) takes over.
		restored = &decisionmodelv1alpha1.RevisionStatus{Hash: prev.Hash}
	}
	dm.Status.StableRevision = restored
	dm.Status.FailedRevision = rolledBack
	dm.Status.PreviousRevision = nil
	meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing)
	meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionPromoted)
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseRolledBack)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonPostPromotionUnhealthy,
		Message: msg,
	})
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonCandidateRejected,
		Message: fmt.Sprintf("rolled back to %s; it keeps serving", prev.Hash),
	})
	// Buffer the Event and metric: finish() flushes them only on a successful
	// status write, so a conflicting/failed write emits nothing and the retry
	// reconcile does not duplicate them.
	r.event(ctx, dm, corev1.EventTypeWarning, eventRolledBackPromo, "%s", msg)
	bufferRollout(ctx, rolloutRolledBackAfterPromotion)

	// Persist first. On conflict nothing was stored and no Event/metric was
	// emitted; requeue and redo the decision from fresh state next reconcile.
	persisted, conflict, err := r.persistStatus(ctx, dm)
	if conflict {
		return stabilizationResult{rolledBack: true, res: ctrl.Result{RequeueAfter: time.Second}}
	}
	if err != nil {
		return stabilizationResult{rolledBack: true, err: err}
	}
	if !persisted {
		return stabilizationResult{rolledBack: true, res: ctrl.Result{}}
	}

	// Only now, with the rollback durably recorded, move the Service back to the
	// previous revision (its Pods are still up — kept through the window) and
	// delete the failed revision's workloads. If either fails, the stable path
	// converges it on the next reconcile; so requeue rather than block.
	if err := r.ensureService(ctx, dm, eng, prev.Hash); err != nil {
		return stabilizationResult{rolledBack: true, err: err}
	}
	if err := r.deleteRevisionWorkloads(ctx, dm, failed.Hash); err != nil {
		return stabilizationResult{rolledBack: true, err: err}
	}
	return stabilizationResult{rolledBack: true, res: ctrl.Result{RequeueAfter: time.Second}}
}
