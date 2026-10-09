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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// soonest returns the result with the earliest non-zero RequeueAfter (zero means
// "no requeue" and never wins).
func soonest(a, b ctrl.Result) ctrl.Result {
	switch {
	case a.RequeueAfter == 0:
		return b
	case b.RequeueAfter == 0:
		return a
	case b.RequeueAfter < a.RequeueAfter:
		return b
	}
	return a
}

// finish updates status (with observedGeneration + endpoint) and returns.
// reconcileState is per-reconcile context carrying the status-patch merge base
// and buffered Events (flushed only after a successful status write, so a
// conflicting/failed write never emits duplicate Events —.
type reconcileState struct {
	base   *decisionmodelv1alpha1.DecisionModel
	events []bufferedEvent
	// metrics are counter increments / histogram observations accumulated during
	// the reconcile and flushed only after a successful status write, so a
	// conflicting/failed write never double-counts (same rule as Events).
	metrics []bufferedMetric
	// resolvedManifest is the raw model manifest from this reconcile's Resolve
	// (sha256 == the resolved digest), stashed so the manifest-ConfigMap persist
	// reuses it instead of calling Resolve a second time. Empty when Resolve was
	// short-circuited (hard-pinned spec.digest, or a digest reused from status) or
	// the engine returned no manifest.
	resolvedManifest []byte
	// abandonedManifestRevs holds revision hashes whose candidate was cleared this
	// reconcile without being promoted (superseded / abandoned). gcRevisions
	// deletes their <dm>-manifest-<rev> ConfigMap by name even when no store PVC
	// exists to key it to (a candidate queued by the rollout budget never got a
	// PVC). Set before clearing status.candidateRevision; consumed by the GC pass
	// that runs on the same reconcile.
	abandonedManifestRevs []string
}

type bufferedEvent struct {
	eventType, reason, note string
	args                    []interface{}
}

// bufferedMetric is a pending metric mutation. Exactly one of the counter/
// histogram roles applies, selected by kind.
type bufferedMetric struct {
	kind   metricKind
	label  string        // result label (rollout/probe) or phase (duration)
	amount float64       // counter increment (default 1)
	dur    time.Duration // observation for phase-duration histogram
}

type metricKind int

const (
	metricKindRollout metricKind = iota
	metricKindProbe
	metricKindPhaseDuration
)

type reconcileStateKey struct{}

// withPatchBase returns a context carrying base as the status-patch merge base
// and an empty Event buffer.
func withPatchBase(ctx context.Context, base *decisionmodelv1alpha1.DecisionModel) context.Context {
	return context.WithValue(ctx, reconcileStateKey{}, &reconcileState{base: base})
}

// reconcileStateFrom returns the per-reconcile state, or nil when absent.
func reconcileStateFrom(ctx context.Context) *reconcileState {
	if v, ok := ctx.Value(reconcileStateKey{}).(*reconcileState); ok {
		return v
	}
	return nil
}

// markManifestAbandoned records that a candidate revision was cleared this
// reconcile without being promoted, so the GC pass deletes its manifest
// ConfigMap by name even when the revision never got a store PVC to key it to.
// A no-op when there is no reconcile state (direct unit calls).
func markManifestAbandoned(ctx context.Context, rev string) {
	if rev == "" {
		return
	}
	if st := reconcileStateFrom(ctx); st != nil {
		st.abandonedManifestRevs = append(st.abandonedManifestRevs, rev)
	}
}

// finish patches status (observedGeneration + endpoint) with an optimistic-lock
// merge from the reconcile's captured base, then flushes buffered Events. On a
// conflict it requeues without emitting Events or persisting a stale status.
func (r *DecisionModelReconciler) finish(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	res ctrl.Result,
	reconcileErr error,
) (ctrl.Result, error) {
	persisted, conflict, err := r.persistStatus(ctx, dm)
	switch {
	case conflict:
		// Stale object: events and metrics were dropped; requeue for a fresh read.
		return ctrl.Result{RequeueAfter: time.Second}, reconcileErr
	case err != nil:
		if reconcileErr == nil {
			return res, err
		}
		return res, reconcileErr
	case !persisted:
		return res, reconcileErr
	}
	return res, reconcileErr
}

// persistStatus writes dm.Status (observedGeneration + endpoint included) with an
// optimistic-lock merge from the reconcile's captured base and, only on success,
// flushes the buffered Events and metrics. persisted reports whether the
// status is now durably stored; conflict is true when the write lost an
// optimistic-lock race (nothing was stored and nothing was emitted).
//
// promote() relies on persisted: the Service may only be moved to a revision that
// is already recorded as status.stableRevision, so a failed or conflicting write
// can never leave the Service on a revision the cluster state does not know is
// stable.
func (r *DecisionModelReconciler) persistStatus(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) (persisted, conflict bool, err error) {
	dm.Status.ObservedGeneration = dm.Generation
	dm.Status.Endpoint = r.endpoint(dm)
	st := reconcileStateFrom(ctx)
	if st != nil && st.base != nil {
		if perr := r.Status().Patch(ctx, dm, client.MergeFromWithOptions(st.base, client.MergeFromWithOptimisticLock{})); perr != nil {
			return false, apierrors.IsConflict(perr), perr
		}
		r.flushEvents(dm, st)
		flushMetrics(dm, st)
		return true, false, nil
	}
	if uerr := r.Status().Update(ctx, dm); uerr != nil {
		return false, false, uerr
	}
	if st != nil {
		r.flushEvents(dm, st)
		flushMetrics(dm, st)
	}
	return true, false, nil
}

// refreshPatchBase resets the optimistic-lock base of the current reconcile to a
// snapshot of dm, so a SECOND persistStatus/finish later in the same reconcile
// patches from the just-written state instead of the stale start-of-reconcile
// base (which would conflict). Call it right after an intermediate persistStatus
// that must be followed by more status writes in the same reconcile (e.g. the
// rollout admission write, which precedes the candidate flow's own finish).
func refreshPatchBase(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) {
	if st := reconcileStateFrom(ctx); st != nil {
		st.base = dm.DeepCopy()
	}
}

// flushEvents emits all buffered Events after a successful status write.
func (r *DecisionModelReconciler) flushEvents(dm *decisionmodelv1alpha1.DecisionModel, st *reconcileState) {
	if r.Recorder == nil {
		st.events = nil
		return
	}
	for _, e := range st.events {
		r.Recorder.Eventf(dm, nil, e.eventType, e.reason, e.reason, e.note, e.args...)
	}
	st.events = nil
}

// flushMetrics records the just-persisted status into the gauges and applies
// all buffered counter/histogram mutations. Called from finish only after a
// successful status write, so metrics stay consistent with accepted status.
func flushMetrics(dm *decisionmodelv1alpha1.DecisionModel, st *reconcileState) {
	recordStatusMetrics(dm)
	ns, name := dm.Namespace, dm.Name
	for _, m := range st.metrics {
		switch m.kind {
		case metricKindRollout:
			rolloutsTotal.WithLabelValues(ns, name, m.label).Inc()
		case metricKindProbe:
			probeResultsTotal.WithLabelValues(ns, name, m.label).Add(m.amount)
		case metricKindPhaseDuration:
			phaseDurationSeconds.WithLabelValues(ns, name, m.label).Observe(m.dur.Seconds())
		}
	}
	st.metrics = nil
}

// bufferRollout records a rollout outcome to flush after the status write.
func bufferRollout(ctx context.Context, result string) {
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics, bufferedMetric{kind: metricKindRollout, label: result, amount: 1})
	}
}

// bufferProbeResult records n probe outcomes of a given result to flush after
// the status write.
func bufferProbeResult(ctx context.Context, result string, n int) {
	if n <= 0 {
		return
	}
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics,
			bufferedMetric{kind: metricKindProbe, label: result, amount: float64(n)})
	}
}

// bufferPhaseDuration records the time spent in a phase that is now being left,
// to flush after the status write.
func bufferPhaseDuration(ctx context.Context, phase decisionmodelv1alpha1.DecisionModelPhase, d time.Duration) {
	if phase == "" || d < 0 {
		return
	}
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics,
			bufferedMetric{kind: metricKindPhaseDuration, label: string(phase), dur: d})
	}
}

// setPhase updates the phase and records the transition time when it changes.
// When the phase actually changes, it buffers the duration the DecisionModel
// spent in the previous phase for the phase-duration histogram (flushed after
// the status write).
func (r *DecisionModelReconciler) setPhase(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	phase decisionmodelv1alpha1.DecisionModelPhase,
) {
	if dm.Status.Phase == phase && dm.Status.PhaseTransitionTime != nil {
		return
	}
	if dm.Status.Phase != phase {
		// Observe how long we stayed in the phase we are now leaving.
		if dm.Status.Phase != "" && dm.Status.PhaseTransitionTime != nil {
			bufferPhaseDuration(ctx, dm.Status.Phase, r.now().Sub(dm.Status.PhaseTransitionTime.Time))
		}
		t := metav1.NewTime(r.now())
		dm.Status.PhaseTransitionTime = &t
	}
	dm.Status.Phase = phase
}

// phaseExceeded reports whether the current phase has lasted longer than d,
// measured from the explicit phaseTransitionTime.
func (r *DecisionModelReconciler) phaseExceeded(
	dm *decisionmodelv1alpha1.DecisionModel,
	d time.Duration,
) bool {
	if dm.Status.PhaseTransitionTime == nil {
		return false
	}
	return r.now().Sub(dm.Status.PhaseTransitionTime.Time) > d
}

func (r *DecisionModelReconciler) endpoint(dm *decisionmodelv1alpha1.DecisionModel) string {
	port := int32(0)
	if eng, ok := r.Engines[engineOrDefault(dm.Spec.Engine)]; ok {
		port = eng.ServicePort()
	}
	return fmt.Sprintf("http://%s.%s.svc:%d/v1/systemone", dm.Name, dm.Namespace, port) // NOSONAR: Ollaya serves plain HTTP in-cluster (no TLS)
}

func (r *DecisionModelReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// bgContext returns the base context for background evaluation goroutines,
// so they are cancelled on manager shutdown. Falls back to Background.
func (r *DecisionModelReconciler) bgContext() context.Context {
	if r.BaseContext != nil {
		return r.BaseContext
	}
	return context.Background()
}

// event emits an events.k8s.io/v1 Event if a Recorder is configured (nil-safe).
// The reason doubles as the action (the verb describing what happened to the DM).
// event buffers a Kubernetes Event to be emitted only after a successful status
// write. When no reconcile state is present (e.g. some tests), it emits
// immediately.
func (r *DecisionModelReconciler) event(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eventType, reason, noteFmt string,
	args ...interface{},
) {
	if st := reconcileStateFrom(ctx); st != nil {
		st.events = append(st.events, bufferedEvent{eventType: eventType, reason: reason, note: noteFmt, args: args})
		return
	}
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(dm, nil, eventType, reason, reason, noteFmt, args...)
}
