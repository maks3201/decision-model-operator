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
	"sync/atomic"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// probePods probes candidate Pods and patches their readiness gate condition.
// It returns the number of Pods with the gate True, the observed precision, and
// the last probe transport error (if any).
func (r *DecisionModelReconciler) probePods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	rev *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) (ready int32, precision string, probeErr error) {
	pods, err := r.revisionPods(ctx, dm, rev.Hash)
	if err != nil {
		return 0, "", err
	}
	prober := r.prober()
	reinsp, canReinspect := r.canReinspect()
	// Bound each background warmup by spec.rollout.timeouts.starting when set.
	probeCtx := withWarmupTimeout(ctx, dm)

	var counts probeCounts

	for i := range pods {
		pod := &pods[i]
		// A terminating Pod never counts and is not probed: it is on its way out
		// and must not be promoted onto.
		if pod.DeletionTimestamp != nil {
			continue
		}
		// A Pod already gated True is only probed again when needed:
		//  - its containers restarted and became ready again since it was last
		//    probed (the pinned model may be gone): full Probe, which also warms up;
		//  - or regateInterval elapsed: an Inspect-only re-check, which catches a
		//    model the runtime lost without a container restart.
		// Otherwise it only has to still be serving-ready to be counted.
		recheck := false
		if gateTrue(pod) {
			if !r.restartedSinceProbe(pod) {
				if canReinspect && r.regateDue(pod.UID, r.now()) {
					recheck = true
				} else {
					if servingReady(pod) {
						ready++
						if pr := r.knownPrecision(pod.UID); pr != "" {
							precision = pr
						}
					}
					continue
				}
			}
		} else if pod.Status.PodIP == "" || !containersReady(pod) {
			continue
		}
		// The prober warms the model asynchronously (keyed by Pod UID) and does a
		// short synchronous inspect; while warmup is still running it returns
		// errWarmupInProgress, which is neither ready nor a mismatch.
		var (
			loaded engine.Loaded
			perr   error
		)
		if recheck {
			loaded, perr = reinsp.Reinspect(probeCtx, pod, eng, apiKey, rev.Model)
			r.regateMark(pod.UID, r.now())
		} else {
			loaded, perr = prober.Probe(probeCtx, pod, eng, apiKey, rev.Model)
		}
		if errors.Is(perr, errWarmupInProgress) {
			continue
		}
		if perr != nil {
			probeErr = perr
			// A re-inspect of an already-healthy (gate True, PodReady) Pod that
			// hits a *transport/auth* error — e.g. a 401 in the window right after
			// an API key rotation, before the new-key Pods are up — must NOT flip
			// the Pod out of the Service on the first error: that would drop all
			// serving capacity on a transient blip. Keep it counted for a bounded
			// number of consecutive re-inspect failures; past that, a stuck/hung or
			// mis-keyed runtime is flipped False. A definitive model
			// loss (errModelNotLoaded) flips the gate False at once, and a
			// first-time probe error (gate not yet True) is handled as before.
			if recheck && gateTrue(pod) && !errors.Is(perr, errModelNotLoaded) &&
				r.transientReinspectFail(pod.UID) <= maxTransientReinspectFailures {
				if servingReady(pod) {
					ready++
					if pr := r.knownPrecision(pod.UID); pr != "" {
						precision = pr
					}
				}
				continue
			}
			if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonProbeError, perr.Error()); transitioned {
				counts.nErr++
				r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
					"pod %s probe error: %s", pod.Name, perr.Error())
			}
			r.clearTransientReinspectFail(pod.UID)
			continue
		}
		// A conclusive probe/re-inspect succeeded: clear any transient-failure run.
		r.clearTransientReinspectFail(pod.UID)
		// The restart (if any) has been looked at conclusively: stop re-probing it.
		r.markHandled(pod, loaded.Precision)
		if pr := r.applyProbeResult(ctx, dm, pod, rev, loaded, recheck, &counts); pr != "" {
			precision = pr
		}
		if counts.lastReady {
			ready++
		}
	}
	// Count probe outcomes only on a gate transition, so a steady-state Pod is
	// not re-counted on every requeue. Buffered to flush after the status write.
	bufferProbeResult(ctx, probeReady, counts.nReady)
	bufferProbeResult(ctx, probeDigestMismatch, counts.nDigest)
	bufferProbeResult(ctx, probeDeviceMismatch, counts.nDevice)
	bufferProbeResult(ctx, probeNotPinned, counts.nPin)
	bufferProbeResult(ctx, probeError, counts.nErr)
	return ready, precision, probeErr
}

// probeCounts accumulates per-reconcile probe outcome counters.
type probeCounts struct {
	nReady, nDigest, nDevice, nPin, nErr int
	lastReady                            bool // set by applyProbeResult to add one ready
}

// applyProbeResult patches the readiness gate for one successfully-probed Pod
// based on digest/device/pin match, and records the outcome counters. It returns
// a non-empty precision when the Pod matched (to surface to status) and whether
// the Pod should be counted ready this reconcile (via counts.lastReady, read by
// the caller). Extracted from probePods to keep its cyclomatic complexity in
// check.
func (r *DecisionModelReconciler) applyProbeResult(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pod *corev1.Pod,
	rev *decisionmodelv1alpha1.RevisionStatus,
	loaded engine.Loaded,
	recheck bool,
	counts *probeCounts,
) (precision string) {
	counts.lastReady = false
	switch {
	case loaded.Digest != rev.Digest:
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonDigestMismatch,
			fmt.Sprintf("loaded digest %s != expected %s", loaded.Digest, rev.Digest)); transitioned {
			counts.nDigest++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (loaded %s, expected %s)", pod.Name, reasonDigestMismatch, loaded.Digest, rev.Digest)
		}
	case loaded.Device != rev.Device:
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonDeviceMismatch,
			fmt.Sprintf("loaded on device %s, expected %s", loaded.Device, rev.Device)); transitioned {
			counts.nDevice++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (loaded %s, expected %s)", pod.Name, reasonDeviceMismatch, loaded.Device, rev.Device)
		}
	case !loaded.Pinned:
		// The right model on the right device, but the runtime may evict it
		// (keep_alive expiry): it would silently stop serving. Not ready.
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonNotPinned,
			"model is loaded but not pinned (keep_alive); the runtime may evict it"); transitioned {
			counts.nPin++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (model %s is not pinned in the runtime)", pod.Name, reasonNotPinned, rev.Model)
		}
	default:
		if recheck {
			// Still loaded, on the expected device, pinned: the gate is already
			// True, so write nothing (no Pod patch, no status churn).
			if servingReady(pod) {
				counts.lastReady = true
			}
			return loaded.Precision
		}
		// Set the gate, but do not count the Pod in this same reconcile: the
		// Service must only move to Pods that Kubernetes already reports Ready
		// (and that are therefore in the EndpointSlice). PodReady follows the
		// gate asynchronously; the Pod update event brings the next reconcile.
		// An already-True gate (a probe after a restart) counts if PodReady holds.
		transitioned, ok := r.patchGate(ctx, pod, metav1.ConditionTrue, reasonModelReady, "model loaded on expected device")
		if !ok {
			return loaded.Precision
		}
		r.regateMark(pod.UID, r.now())
		if transitioned {
			counts.nReady++
		}
		if !transitioned && servingReady(pod) {
			counts.lastReady = true
		}
		return loaded.Precision
	}
	return ""
}

// patchGate sets the model-ready Pod condition via a status merge-patch of just
// that condition (not a full-object update, avoiding conflicts with the
// kubelet). It returns whether the status transitioned and whether the write
// succeeded; callers count a Pod ready only when the gate write succeeds.
func (r *DecisionModelReconciler) patchGate(
	ctx context.Context,
	pod *corev1.Pod,
	status metav1.ConditionStatus,
	reason, message string,
) (transitioned, ok bool) {
	log := logf.FromContext(ctx)
	transitioned = !podConditionHasStatus(pod, corev1.ConditionStatus(status))
	base := pod.DeepCopy()
	cond := corev1.PodCondition{
		Type:               corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate),
		Status:             corev1.ConditionStatus(status),
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
	}
	setPodCondition(pod, cond)
	// A JSON merge patch (client.MergeFrom) would send the whole status.conditions
	// array and could revert a condition the kubelet set between our Get and this
	// Patch (e.g. Ready/ContainersReady). Pod conditions have a strategic-merge
	// key of `type`, so a strategic merge patches only the ModelReady gate by type
	// and leaves the kubelet's conditions untouched.
	if err := r.Status().Patch(ctx, pod, client.StrategicMergeFrom(base)); err != nil {
		log.V(1).Info("failed to patch pod readiness gate", "pod", pod.Name, "error", err)
		return transitioned, false
	}
	if afterGatePatch != nil && status == metav1.ConditionTrue {
		afterGatePatch(ctx, r.Client, pod)
	}
	return transitioned, true
}

// revisionPods lists Pods for a DecisionModel revision.
func (r *DecisionModelReconciler) revisionPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(dm.Namespace),
		client.MatchingLabels{
			decisionmodelv1alpha1.LabelName:     dm.Name,
			decisionmodelv1alpha1.LabelRevision: rev,
		},
	); err != nil {
		return nil, fmt.Errorf("%w: %v", errPodListFailed, err)
	}
	return r.maybeFilterOwnedPods(ctx, dm, rev, list.Items)
}

// enforcePodOwnership gates the controller-chain ownership check in
// revisionPods. It defaults to true (production always enforces); the envtest
// suite relaxes it for legacy fixtures that create bare labelled Pods, and the
// ownership specs switch it back on. This mirrors the kubelet seam used
// for.
var enforcePodOwnership atomic.Bool

func init() { enforcePodOwnership.Store(true) }

// maybeFilterOwnedPods applies filterOwnedPods only when ownership enforcement
// is on; otherwise it trusts the label selector (envtest legacy fixtures).
func (r *DecisionModelReconciler) maybeFilterOwnedPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
	pods []corev1.Pod,
) ([]corev1.Pod, error) {
	if !enforcePodOwnership.Load() {
		return pods, nil
	}
	return r.filterOwnedPods(ctx, dm, rev, pods)
}

// filterOwnedPods keeps only Pods whose controller chain leads to this DM's
// revision Deployment: Pod -> ReplicaSet (controlled by the expected Deployment
// UID) -> Deployment (controlled by dm.UID). Labels are attacker-controllable
// (anyone who can create Pods in the namespace can set them and, with them, make
// the operator send the API key to a foreign Pod), so selection by label alone
// is not enough. The namespace is the real trust boundary — a Pod creator can
// already mount any Secret in it — so this is defence in depth, not a boundary
// fix.
//
// A genuine lookup error (a non-NotFound Get on the Deployment or a ReplicaSet)
// is returned (wrapped as errPodListFailed) so the caller aborts and the
// workqueue backs off — it must NOT be mistaken for "0 owned Pods", which would
// wrongly flip Degraded / trip progress timeouts. A NotFound (the
// Deployment or RS is simply gone) legitimately drops the Pod(s).
func (r *DecisionModelReconciler) filterOwnedPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
	pods []corev1.Pod,
) ([]corev1.Pod, error) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev)}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil // no Deployment => no owned Pods (not an error)
		}
		return nil, fmt.Errorf("%w: get Deployment: %v", errPodListFailed, err)
	}
	if c := metav1.GetControllerOf(&dep); c == nil || c.UID != dm.UID {
		return nil, nil
	}
	rsOK := map[types.UID]bool{}
	owned := pods[:0]
	for i := range pods {
		pod := &pods[i]
		rsRef := metav1.GetControllerOf(pod)
		if rsRef == nil || rsRef.Kind != "ReplicaSet" {
			continue
		}
		ok, seen := rsOK[rsRef.UID]
		if !seen {
			var rs appsv1.ReplicaSet
			if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: rsRef.Name}, &rs); err != nil {
				if !apierrors.IsNotFound(err) {
					// A transient RS read failure must abort, not silently drop the
					// Pod (which would look like "0 Pods").
					return nil, fmt.Errorf("%w: get ReplicaSet %s: %v", errPodListFailed, rsRef.Name, err)
				}
				rsOK[rsRef.UID] = false
			} else {
				// The fetched RS must be exactly the one the Pod references (a
				// name can be reused with a different UID) and controlled by this
				// revision's Deployment.
				rc := metav1.GetControllerOf(&rs)
				rsOK[rsRef.UID] = rs.UID == rsRef.UID && rc != nil && rc.UID == dep.UID
			}
			ok = rsOK[rsRef.UID]
		}
		if ok {
			owned = append(owned, *pod)
		}
	}
	return owned, nil
}

// setReplicaStatus records desired/modelReady counts.
func (r *DecisionModelReconciler) setReplicaStatus(dm *decisionmodelv1alpha1.DecisionModel, ready int32) {
	dm.Status.Replicas = decisionmodelv1alpha1.ReplicaStatus{
		Desired:    desiredReplicas(dm),
		ModelReady: ready,
	}
}

// applyStableReadiness sets phase + Ready/ModelReady/Degraded conditions for the
// stable revision honestly:
//   - ready == desired      -> Ready, Ready=True, ModelReady=True
//   - 0 < ready < desired   -> Degraded, Ready=True, ModelReady=False, Degraded=True (ReplicasNotModelReady)
//   - ready == 0            -> Degraded, Ready=False (NoModelReadyPods), ModelReady=False
//
// A cache-sharing Degraded (cacheDegraded) is always preserved.
func (r *DecisionModelReconciler) applyStableReadiness(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	ready, desired int32,
	cacheDegraded bool,
) {
	switch {
	case ready >= desired:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseReady)
		setReadyConditions(dm, cacheDegraded)
	case ready > 0:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseDegraded)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  reasonReady,
			Message: "serving on a subset of replicas",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionModelReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonReplicasNotModelReady,
			Message: fmt.Sprintf("%d/%d replicas model-ready", ready, desired),
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonReplicasNotModelReady,
			Message: fmt.Sprintf("%d/%d replicas model-ready", ready, desired),
		})
	default:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseDegraded)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoModelReadyPods,
			Message: "no serving Pod reports the expected digest on the expected device",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionModelReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoModelReadyPods,
			Message: "0 replicas model-ready",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonNoModelReadyPods,
			Message: "0 replicas model-ready",
		})
	}
}

// requeueIfShort requeues while fewer Pods are ready than desired.
func (r *DecisionModelReconciler) requeueIfShort(ready, desired int32) ctrl.Result {
	if ready < desired {
		return ctrl.Result{RequeueAfter: probeRequeue}
	}
	return ctrl.Result{}
}

func (r *DecisionModelReconciler) prober() Prober {
	if r.Prober != nil {
		return r.Prober
	}
	return NewProber(r.bgContext)
}

// pruneProberWarm drops the prober's per-Pod warm-cache entries for Pods of this
// DecisionModel that no longer exist, so the map does not leak across rollouts.
// Best-effort: a list failure skips pruning (never aborts the
// reconcile), and a Prober without the warmPruner capability is a no-op.
func (r *DecisionModelReconciler) pruneProberWarm(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) {
	pruner, ok := r.prober().(warmPruner)
	if !ok {
		return
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return
	}
	live := make(map[types.UID]bool, len(pods.Items))
	for i := range pods.Items {
		live[pods.Items[i].UID] = true
	}
	pruner.PruneWarm(dm.Namespace+"/"+dm.Name, live)
}
