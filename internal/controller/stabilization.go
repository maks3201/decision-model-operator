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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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

// inStabilizationWindow reports whether a DecisionModel is currently keeping a
// previous revision alive inside its post-promotion stabilization window. While
// true the previous revision's Deployment is still running (the second GPU/disk
// the rollout budget exists to bound) and the DM can still roll back to it, so
// the fleet budget must count it as an active rollout. It mirrors the window
// guard in reconcileStabilization: a previous revision with a promotedAt and a
// positive window whose elapsed time has not passed the window.
func (r *DecisionModelReconciler) inStabilizationWindow(dm *decisionmodelv1alpha1.DecisionModel) bool {
	p := dm.Status.PreviousRevision
	if p == nil || p.Hash == "" || p.PromotedAt == nil {
		return false
	}
	window := stabilizationFor(dm)
	if window <= 0 {
		return false
	}
	return r.now().Sub(p.PromotedAt.Time) < window
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

	// Tell an intentional in-place rollout of the stable (replicas scale-up,
	// API-key rotation, scheduling change, manual restart) from a model-health
	// failure. A non-immediate quorum shortfall during a rollout is expected while
	// new Pods start and load the model, so it must NOT start/advance the rollback
	// debounce. Immediate failures (gate DigestMismatch/DeviceMismatch,
	// CrashLoopBackOff) still roll back regardless. A rollout that blows its
	// progressDeadline (Progressing=False, ProgressDeadlineExceeded) is itself a
	// failure and rolls back.
	rollout := r.stableRolloutState(ctx, dm, stable)
	rollingShortfall := unhealthy && !immediate && rollout == rolloutInProgress
	if unhealthy && !immediate && rollout == rolloutDeadlineExceeded {
		immediate = true
		detail = fmt.Sprintf("%s; the stable rollout exceeded its progress deadline", detail)
	}

	if unhealthy && !rollingShortfall && (immediate || r.unhealthyDebounceElapsed(dm)) {
		return r.rollbackToPrevious(ctx, dm, eng, stable, p, detail)
	}

	if elapsed >= window {
		// Window elapsed.
		if rollingShortfall {
			// The stable is below quorum only because it is mid-rollout, not
			// because the model is unhealthy. Do NOT roll back and do NOT announce
			// Stabilized (which would let GC collect the rollback target while the
			// new stable is not yet proven healthy). Extend the window: keep the
			// previous revision and requeue. A stuck rollout is still bounded —
			// progressDeadline turns it into rolloutDeadlineExceeded above, which
			// rolls back.
			r.markStableRolling(dm, window, detail)
			return stabilizationResult{}
		}
		if unhealthy {
			// Unhealthy at the boundary for a non-rollout reason (e.g. a quorum
			// shortfall with no rollout in flight): the window has no more time to
			// give the new stable, so roll back now rather than serve below quorum.
			return r.rollbackToPrevious(ctx, dm, eng, stable, p, detail)
		}
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

	// Still in the window. Record Stabilizing: True (watching, or StableRolling
	// while a rollout masks the shortfall) when healthy-enough; False
	// (PostPromotionUnhealthy) when a genuine shortfall is debouncing toward a
	// rollback — its LastTransitionTime is the idempotent debounce clock.
	switch {
	case rollingShortfall:
		r.markStableRolling(dm, window, detail)
	case unhealthy:
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionStabilizing,
			Status:  metav1.ConditionFalse,
			Reason:  reasonPostPromotionUnhealthy,
			Message: detail,
		})
	default:
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionStabilizing,
			Status:  metav1.ConditionTrue,
			Reason:  reasonStabilizing,
			Message: fmt.Sprintf("watching the new stable for %s before collecting the previous revision", window),
		})
	}
	return stabilizationResult{}
}

// markStableRolling sets Stabilizing=True/StableRolling, explaining that the
// quorum shortfall is being ignored because the stable Deployment is mid-rollout.
// It clears any prior PostPromotionUnhealthy debounce so a rollout that starts
// after a brief dip does not inherit a stale clock.
func (r *DecisionModelReconciler) markStableRolling(
	dm *decisionmodelv1alpha1.DecisionModel, window time.Duration, detail string,
) {
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionStabilizing,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStableRolling,
		Message: fmt.Sprintf("stable rollout in progress (%s); not counting the quorum shortfall as a failure during the %s window", detail, window),
	})
}

// rolloutState classifies the stable Deployment's rollout progress.
type rolloutState int

const (
	rolloutIdle             rolloutState = iota // fully rolled out (or Deployment absent)
	rolloutInProgress                           // a template/scale change is still being applied
	rolloutDeadlineExceeded                     // Progressing=False, ProgressDeadlineExceeded
)

// stableRolloutState reads the stable serving Deployment and reports whether it
// is mid-rollout, has exceeded its progress deadline, or is idle. It is a
// best-effort signal: a Deployment that cannot be read (missing, or a lookup
// error) is treated as rolloutIdle so a genuine health shortfall is never masked
// by a read failure.
//
// "In progress" means the Deployment controller is actively applying a change,
// NOT merely "fewer Pods are available than desired" (which is true of every
// quorum shortfall, including a new stable whose Pods never become ready). The
// signals are the ones the deployment controller sets on the Progressing
// condition (k8s pkg/controller/deployment/util/deployment_util.go):
//   - generation != status.observedGeneration — the latest spec change (template
//     or replicas) is not yet observed; or
//   - Progressing=True with a reason OTHER than NewReplicaSetAvailable, i.e.
//     NewReplicaSetCreated / FoundNewReplicaSet / ReplicaSetUpdated — a rollout
//     is underway. A completed rollout settles to Progressing=True,
//     NewReplicaSetAvailable and stays there even if Pods later go unready, so a
//     post-rollout health failure is correctly NOT classified as "rolling".
//   - Progressing=False, ProgressDeadlineExceeded — the rollout is stuck; a
//     failure, so protection is bounded.
func (r *DecisionModelReconciler) stableRolloutState(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
) rolloutState {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, stable.Hash)}, &dep); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).V(1).Info("stabilization: could not read stable Deployment", "error", err)
		}
		return rolloutIdle
	}
	if c := metav1.GetControllerOf(&dep); c == nil || c.UID != dm.UID {
		return rolloutIdle // not ours; do not infer rollout state from it
	}
	progressing := findDeploymentCondition(&dep, appsv1.DeploymentProgressing)
	if progressing != nil &&
		progressing.Status == corev1.ConditionFalse && progressing.Reason == deployTimedOutReason {
		return rolloutDeadlineExceeded
	}
	// The spec change has not been observed yet: definitely rolling.
	if dep.Generation != dep.Status.ObservedGeneration {
		return rolloutInProgress
	}
	// A rollout is underway while Progressing=True for any reason other than the
	// terminal NewReplicaSetAvailable. Do NOT use availableReplicas/updatedReplicas:
	// those are below spec for every quorum shortfall, not just a rollout.
	if progressing != nil && progressing.Status == corev1.ConditionTrue &&
		progressing.Reason != deployNewRSAvailableReason {
		return rolloutInProgress
	}
	return rolloutIdle
}

// Deployment Progressing-condition reasons set by the k8s deployment controller.
const (
	deployNewRSAvailableReason = "NewReplicaSetAvailable" // rollout complete
	deployTimedOutReason       = "ProgressDeadlineExceeded"
)

// findDeploymentCondition returns the Deployment condition of the given type, or
// nil.
func findDeploymentCondition(dep *appsv1.Deployment, t appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range dep.Status.Conditions {
		if dep.Status.Conditions[i].Type == t {
			return &dep.Status.Conditions[i]
		}
	}
	return nil
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
