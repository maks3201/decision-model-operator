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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// regateInterval is how often a Pod whose gate is already True is re-inspected.
// A runtime can lose or unload its model without the container restarting (for
// example a GGUF runner that the runtime restarts in-process), which the
// restart-based re-probe cannot see.
const regateInterval = 60 * time.Second

// regateSweepFactor: entries not refreshed for this many intervals belong to Pods
// that no longer exist and are dropped.
const regateSweepFactor = 10

// maxTransientReinspectFailures bounds how many consecutive re-inspect
// transport/auth errors a healthy (gate-True) Pod may absorb before its gate is
// flipped False. Keeping the gate True across a brief API-key rotation window
// (a few 401s until the new-key Pods are up) must not let a permanently hung or
// mis-keyed runtime stay "model-ready" forever. At regateInterval
// (60s) this is ~3 minutes.
const maxTransientReinspectFailures = 3

// reinspector is an optional Prober capability: a cheap Inspect-only check of a
// Pod that is already serving. Unlike Prober.Probe it never warms the model up.
// The controller detects it by type assertion; a Prober without it simply does
// no periodic re-check.
type reinspector interface {
	Reinspect(ctx context.Context, pod *corev1.Pod, eng engine.Engine, apiKey, model string) (engine.Loaded, error)
}

// regateTracker remembers, in memory only, when each Pod was last inspected.
// Nothing is written to the API for it, so a steady-state Ready DecisionModel
// causes no status write.
type regateTracker struct {
	last      map[types.UID]time.Time
	lastSweep time.Time
	// handled is, per Pod, the ContainersReady transition time that was current
	// the last time the Pod was conclusively probed. A container restart changes
	// that value, so "restarted since we looked" is "the value differs". It is
	// compared against the Pod's own timestamp, never against the operator's
	// clock, so operator/node clock skew cannot hide a restart.
	handled map[types.UID]metav1.Time
	// precision is, per Pod, the precision (F32, F16, ...) the runtime reported
	// the last time the Pod was conclusively probed. A Pod that is counted on a
	// later reconcile without being probed again (its gate is already True) still
	// has to contribute it to the revision's recorded precision.
	precision map[types.UID]string
	// transientFails counts, per Pod, consecutive re-inspect transport/auth
	// errors on an already-healthy Pod. Reset to 0 by any successful re-inspect
	// (regateMark) or when the gate is flipped. Bounds the "keep gate True on a
	// transient error" window.
	transientFails map[types.UID]int
}

// regateDue reports whether a gated Pod should be re-inspected at now. A Pod
// never seen before (for example after an operator restart) is due at once.
func (r *DecisionModelReconciler) regateDue(uid types.UID, now time.Time) bool {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	t, ok := r.regate.last[uid]
	return !ok || now.Sub(t) >= regateInterval
}

// regateMark records that a Pod was inspected (or just gated) at now, and drops
// entries of Pods that have not been refreshed for a long time (deleted Pods).
func (r *DecisionModelReconciler) regateMark(uid types.UID, now time.Time) {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	if r.regate.last == nil {
		r.regate.last = map[types.UID]time.Time{}
	}
	r.regate.last[uid] = now
	if now.Sub(r.regate.lastSweep) >= regateInterval*regateSweepFactor {
		for k, t := range r.regate.last {
			if now.Sub(t) >= regateInterval*regateSweepFactor {
				delete(r.regate.last, k)
				delete(r.regate.handled, k)
				delete(r.regate.precision, k)
				delete(r.regate.transientFails, k)
			}
		}
		r.regate.lastSweep = now
	}
}

// restartedSinceProbe reports whether a Pod whose gate is already True has
// restarted its containers since it was last conclusively probed. The gate's own
// LastTransitionTime cannot answer that: setPodCondition rightly keeps it when the
// status does not change, so "ContainersReady newer than the gate" stays true
// forever after one restart and the Pod used to be fully re-probed (Warmup
// included) on every reconcile. A Pod never probed by this process
// (for example after an operator restart) is probed once.
func (r *DecisionModelReconciler) restartedSinceProbe(pod *corev1.Pod) bool {
	if !containersReadyNewerThanGate(pod) {
		return false
	}
	cur, ok := containersReadyTime(pod)
	if !ok {
		return false
	}
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	h, seen := r.regate.handled[pod.UID]
	return !seen || !h.Equal(&cur)
}

// markHandled records the Pod's current ContainersReady transition time as
// handled, ending the re-probe for that restart, and remembers the precision the
// runtime reported. A later restart changes the time and probes again.
func (r *DecisionModelReconciler) markHandled(pod *corev1.Pod, precision string) {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	if r.regate.precision == nil {
		r.regate.precision = map[types.UID]string{}
	}
	r.regate.precision[pod.UID] = precision
	cur, ok := containersReadyTime(pod)
	if !ok {
		return
	}
	if r.regate.handled == nil {
		r.regate.handled = map[types.UID]metav1.Time{}
	}
	r.regate.handled[pod.UID] = cur
}

// knownPrecision returns the precision last reported for a Pod ("" if unknown).
func (r *DecisionModelReconciler) knownPrecision(uid types.UID) string {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	return r.regate.precision[uid]
}

// transientReinspectFail records one more consecutive re-inspect transport/auth
// error for a Pod and returns the new count. The caller keeps the gate True
// while the count is at or below maxTransientReinspectFailures and flips it
// False once it exceeds it.
func (r *DecisionModelReconciler) transientReinspectFail(uid types.UID) int {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	if r.regate.transientFails == nil {
		r.regate.transientFails = map[types.UID]int{}
	}
	r.regate.transientFails[uid]++
	return r.regate.transientFails[uid]
}

// clearTransientReinspectFail resets a Pod's consecutive transient re-inspect
// failure count (after a successful re-inspect or a gate flip).
func (r *DecisionModelReconciler) clearTransientReinspectFail(uid types.UID) {
	r.regateMu.Lock()
	defer r.regateMu.Unlock()
	delete(r.regate.transientFails, uid)
}

// canReinspect reports whether the configured Prober supports periodic
// re-inspection.
func (r *DecisionModelReconciler) canReinspect() (reinspector, bool) {
	ri, ok := r.prober().(reinspector)
	return ri, ok
}

// regateRequeue returns the requeue needed to keep re-inspecting a serving (or
// parked) revision: regateInterval when the Prober supports it, otherwise none.
func (r *DecisionModelReconciler) regateRequeue() ctrl.Result {
	if _, ok := r.canReinspect(); ok {
		return ctrl.Result{RequeueAfter: regateInterval}
	}
	return ctrl.Result{}
}
