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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// resetMetrics clears every custom series so each test starts from a clean
// slate (the metrics are process-global, registered once in init).
func resetMetrics() {
	phaseGauge.Reset()
	modelReadyReplicasGauge.Reset()
	desiredReplicasGauge.Reset()
	rolloutsTotal.Reset()
	phaseDurationSeconds.Reset()
	evaluationAccuracyGauge.Reset()
	evaluationECEGauge.Reset()
	evaluationMacroF1Gauge.Reset()
	probeResultsTotal.Reset()
	revisionInfoGauge.Reset()
	// registryResolveDuration is a plain Histogram (no labels) and cannot be
	// reset; tests that assert on it read its sample count as a delta.
}

func newDM(name string) *decisionmodelv1alpha1.DecisionModel {
	return &decisionmodelv1alpha1.DecisionModel{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
	}
}

func TestRecordStatusMetricsPhaseIsOneHot(t *testing.T) {
	tests := []struct {
		name  string
		phase decisionmodelv1alpha1.DecisionModelPhase
	}{
		{"ready", decisionmodelv1alpha1.PhaseReady},
		{"caching", decisionmodelv1alpha1.PhaseCaching},
		{"degraded", decisionmodelv1alpha1.PhaseDegraded},
		{"failed", decisionmodelv1alpha1.PhaseFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			dm := newDM("dm")
			dm.Status.Phase = tc.phase
			recordStatusMetrics(dm)

			for _, p := range allPhases {
				want := 0.0
				if p == tc.phase {
					want = 1.0
				}
				got := testutil.ToFloat64(phaseGauge.WithLabelValues("ns", "dm", string(p)))
				if got != want {
					t.Errorf("phase %q gauge = %v, want %v", p, got, want)
				}
			}
		})
	}
}

func TestRecordStatusMetricsReplicasAndEvaluation(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhaseReady
	dm.Status.Replicas = decisionmodelv1alpha1.ReplicaStatus{Desired: 3, ModelReady: 2}
	dm.Status.Evaluation = &decisionmodelv1alpha1.EvaluationStatus{
		Accuracy: "0.9200",
		ECE:      "0.0500",
		MacroF1:  "0.8800",
	}
	recordStatusMetrics(dm)

	if got := testutil.ToFloat64(desiredReplicasGauge.WithLabelValues("ns", "dm")); got != 3 {
		t.Errorf("desired replicas = %v, want 3", got)
	}
	if got := testutil.ToFloat64(modelReadyReplicasGauge.WithLabelValues("ns", "dm")); got != 2 {
		t.Errorf("model-ready replicas = %v, want 2", got)
	}
	if got := testutil.ToFloat64(evaluationAccuracyGauge.WithLabelValues("ns", "dm")); got != 0.92 {
		t.Errorf("evaluation accuracy = %v, want 0.92", got)
	}
	if got := testutil.ToFloat64(evaluationECEGauge.WithLabelValues("ns", "dm")); got != 0.05 {
		t.Errorf("evaluation ece = %v, want 0.05", got)
	}
	if got := testutil.ToFloat64(evaluationMacroF1Gauge.WithLabelValues("ns", "dm")); got != 0.88 {
		t.Errorf("evaluation macro-f1 = %v, want 0.88", got)
	}
}

// TestRecordStatusMetricsMacroF1AbsentDeletesSeries covers the fail-closed rule:
// a completed evaluation with no classifiable question leaves MacroF1 empty, and
// the gauge must have no series (not a stale 0, which would read as perfectly
// wrong on a dashboard). A later run that does classify sets it again.
func TestRecordStatusMetricsMacroF1AbsentDeletesSeries(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhaseReady

	// First: a run with a macro-F1 value.
	dm.Status.Evaluation = &decisionmodelv1alpha1.EvaluationStatus{Accuracy: "0.9000", MacroF1: "0.7500"}
	recordStatusMetrics(dm)
	if got := testutil.ToFloat64(evaluationMacroF1Gauge.WithLabelValues("ns", "dm")); got != 0.75 {
		t.Fatalf("macro-f1 after classifiable run = %v, want 0.75", got)
	}

	// Then: an evaluation that produced no classifiable question (MacroF1 empty).
	dm.Status.Evaluation = &decisionmodelv1alpha1.EvaluationStatus{Accuracy: "0.9100", MacroF1: ""}
	recordStatusMetrics(dm)
	if n := testutil.CollectAndCount(evaluationMacroF1Gauge); n != 0 {
		t.Errorf("macro-f1 series after unclassifiable run = %d, want 0 (deleted, not stale 0)", n)
	}
	// accuracy still tracks the latest run.
	if got := testutil.ToFloat64(evaluationAccuracyGauge.WithLabelValues("ns", "dm")); got != 0.91 {
		t.Errorf("accuracy = %v, want 0.91", got)
	}
}

func TestRecordStatusMetricsNoEvaluationLeavesGaugesUnset(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhaseStarting
	recordStatusMetrics(dm)

	// No evaluation recorded -> the accuracy/ece gauges must have no series.
	if n := testutil.CollectAndCount(evaluationAccuracyGauge); n != 0 {
		t.Errorf("evaluation accuracy series = %d, want 0", n)
	}
	if n := testutil.CollectAndCount(evaluationECEGauge); n != 0 {
		t.Errorf("evaluation ece series = %d, want 0", n)
	}
	if n := testutil.CollectAndCount(evaluationMacroF1Gauge); n != 0 {
		t.Errorf("evaluation macro-f1 series = %d, want 0", n)
	}
}

func TestFlushMetricsBufferedCounters(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhaseReady

	st := &reconcileState{base: dm.DeepCopy()}
	ctx := context.WithValue(context.Background(), reconcileStateKey{}, st)

	bufferRollout(ctx, rolloutPromoted)
	bufferProbeResult(ctx, probeReady, 2)
	bufferProbeResult(ctx, probeDigestMismatch, 1)
	bufferPhaseDuration(ctx, decisionmodelv1alpha1.PhaseStarting, 42*time.Second)

	// Buffered, not yet applied.
	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutPromoted)); got != 0 {
		t.Fatalf("rollout counter applied before flush: %v", got)
	}

	flushMetrics(dm, st)

	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutPromoted)); got != 1 {
		t.Errorf("promoted rollouts = %v, want 1", got)
	}
	if got := testutil.ToFloat64(probeResultsTotal.WithLabelValues("ns", "dm", probeReady)); got != 2 {
		t.Errorf("ready probes = %v, want 2", got)
	}
	if got := testutil.ToFloat64(probeResultsTotal.WithLabelValues("ns", "dm", probeDigestMismatch)); got != 1 {
		t.Errorf("digest_mismatch probes = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(phaseDurationSeconds); n != 1 {
		t.Errorf("phase duration histogram series = %d, want 1", n)
	}
	// flushMetrics also records the current status: phase gauge one-hot.
	if got := testutil.ToFloat64(phaseGauge.WithLabelValues("ns", "dm", string(decisionmodelv1alpha1.PhaseReady))); got != 1 {
		t.Errorf("phase Ready gauge = %v, want 1", got)
	}
	// The buffer is cleared after flush.
	if len(st.metrics) != 0 {
		t.Errorf("metrics buffer not cleared: %d entries", len(st.metrics))
	}
}

func TestBufferHelpersNoStateAreNoops(t *testing.T) {
	resetMetrics()
	// A context without reconcileState must not panic and must record nothing.
	ctx := context.Background()
	bufferRollout(ctx, rolloutFailed)
	bufferProbeResult(ctx, probeError, 3)
	bufferPhaseDuration(ctx, decisionmodelv1alpha1.PhaseCaching, time.Minute)

	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutFailed)); got != 0 {
		t.Errorf("rollout counter changed without state: %v", got)
	}
	if got := testutil.ToFloat64(probeResultsTotal.WithLabelValues("ns", "dm", probeError)); got != 0 {
		t.Errorf("probe counter changed without state: %v", got)
	}
}

// TestFlushMetricsRecreateRolloutResults covers the Recreate-strategy rollout
// outcomes: stopping the stable to free capacity and restoring it after a failed
// candidate / rollback are counted on decisionmodel_rollouts_total with their own
// result labels, flushed only after the status write (same path as promoted).
func TestFlushMetricsRecreateRolloutResults(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhasePending

	st := &reconcileState{base: dm.DeepCopy()}
	ctx := context.WithValue(context.Background(), reconcileStateKey{}, st)

	bufferRollout(ctx, rolloutStableStopped)
	bufferRollout(ctx, rolloutStableRestored)

	// Buffered, not yet applied.
	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutStableStopped)); got != 0 {
		t.Fatalf("stable_stopped applied before flush: %v", got)
	}

	flushMetrics(dm, st)

	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutStableStopped)); got != 1 {
		t.Errorf("stable_stopped rollouts = %v, want 1", got)
	}
	if got := testutil.ToFloat64(rolloutsTotal.WithLabelValues("ns", "dm", rolloutStableRestored)); got != 1 {
		t.Errorf("stable_restored rollouts = %v, want 1", got)
	}
	if len(st.metrics) != 0 {
		t.Errorf("metrics buffer not cleared: %d entries", len(st.metrics))
	}
}

func TestBufferProbeResultZeroIsNoop(t *testing.T) {
	resetMetrics()
	st := &reconcileState{}
	ctx := context.WithValue(context.Background(), reconcileStateKey{}, st)
	bufferProbeResult(ctx, probeReady, 0)
	if len(st.metrics) != 0 {
		t.Errorf("zero-count probe result was buffered: %d entries", len(st.metrics))
	}
}

func TestDeleteMetricsRemovesAllSeriesForDM(t *testing.T) {
	resetMetrics()
	// Two DecisionModels; deleting one must not touch the other.
	keep := newDM("keep")
	keep.Status.Phase = decisionmodelv1alpha1.PhaseReady
	keep.Status.Replicas = decisionmodelv1alpha1.ReplicaStatus{Desired: 1, ModelReady: 1}
	recordStatusMetrics(keep)

	gone := newDM("gone")
	gone.Status.Phase = decisionmodelv1alpha1.PhaseReady
	gone.Status.Replicas = decisionmodelv1alpha1.ReplicaStatus{Desired: 1, ModelReady: 1}
	gone.Status.Evaluation = &decisionmodelv1alpha1.EvaluationStatus{Accuracy: "0.5000", ECE: "0.1000", MacroF1: "0.4000"}
	recordStatusMetrics(gone)

	st := &reconcileState{base: gone.DeepCopy()}
	ctx := context.WithValue(context.Background(), reconcileStateKey{}, st)
	bufferRollout(ctx, rolloutPromoted)
	bufferProbeResult(ctx, probeReady, 1)
	flushMetrics(gone, st)

	deleteMetrics("ns", "gone")

	// Every series for "gone" is removed.
	if n := testutil.CollectAndCount(phaseGauge); n != len(allPhases) {
		t.Errorf("phase gauge series after delete = %d, want %d (only keep)", n, len(allPhases))
	}
	if got := testutil.ToFloat64(desiredReplicasGauge.WithLabelValues("ns", "keep")); got != 1 {
		t.Errorf("keep desired replicas lost: %v", got)
	}
	// gone's replica/eval/counter series are gone.
	for _, c := range []int{
		testutil.CollectAndCount(evaluationAccuracyGauge),
		testutil.CollectAndCount(evaluationMacroF1Gauge),
		testutil.CollectAndCount(rolloutsTotal),
		testutil.CollectAndCount(probeResultsTotal),
	} {
		if c != 0 {
			t.Errorf("a gone series survived delete: count %d", c)
		}
	}
	// keep's phase gauge is still one-hot on Ready.
	if got := testutil.ToFloat64(phaseGauge.WithLabelValues("ns", "keep", string(decisionmodelv1alpha1.PhaseReady))); got != 1 {
		t.Errorf("keep phase Ready gauge = %v, want 1", got)
	}
}

func TestRevisionInfoBoundedToTwoSeriesAndStaleCleaned(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.Phase = decisionmodelv1alpha1.PhaseReady
	dm.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
		Model: "laya:en", Digest: "c305a927000000000000000000000000000000000000000000000000000000aa",
	}
	dm.Status.CandidateRevision = &decisionmodelv1alpha1.RevisionStatus{
		Model: "kev:en", Digest: "1a2b3c4d000000000000000000000000000000000000000000000000000000bb",
	}
	recordStatusMetrics(dm)

	if n := testutil.CollectAndCount(revisionInfoGauge); n != 2 {
		t.Fatalf("revision_info series = %d, want 2 (stable+candidate)", n)
	}
	if got := testutil.ToFloat64(revisionInfoGauge.WithLabelValues(
		"ns", "dm", roleStable, "laya:en", dm.Status.StableRevision.Digest)); got != 1 {
		t.Errorf("stable revision_info = %v, want 1", got)
	}
	if got := testutil.ToFloat64(revisionInfoGauge.WithLabelValues(
		"ns", "dm", roleCandidate, "kev:en", dm.Status.CandidateRevision.Digest)); got != 1 {
		t.Errorf("candidate revision_info = %v, want 1", got)
	}

	// Promote: candidate becomes stable with a new model/digest, no candidate.
	// The old stable series (and the old candidate series) must not linger.
	dm.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
		Model: "kev:en", Digest: "1a2b3c4d000000000000000000000000000000000000000000000000000000bb",
	}
	dm.Status.CandidateRevision = nil
	recordStatusMetrics(dm)

	if n := testutil.CollectAndCount(revisionInfoGauge); n != 1 {
		t.Fatalf("revision_info series after promote = %d, want 1 (stable only)", n)
	}
	if got := testutil.ToFloat64(revisionInfoGauge.WithLabelValues(
		"ns", "dm", roleStable, "kev:en", dm.Status.StableRevision.Digest)); got != 1 {
		t.Errorf("new stable revision_info = %v, want 1", got)
	}
}

func TestRevisionInfoDeletedOnDMDelete(t *testing.T) {
	resetMetrics()
	dm := newDM("dm")
	dm.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{Model: "laya:en", Digest: "abc"}
	recordStatusMetrics(dm)
	if n := testutil.CollectAndCount(revisionInfoGauge); n != 1 {
		t.Fatalf("revision_info series = %d, want 1", n)
	}
	deleteMetrics("ns", "dm")
	if n := testutil.CollectAndCount(revisionInfoGauge); n != 0 {
		t.Errorf("revision_info series after delete = %d, want 0", n)
	}
}

func TestObserveRegistryResolveRecordsSample(t *testing.T) {
	before := histogramSampleCount(t, registryResolveDuration)
	observeRegistryResolve(150 * time.Millisecond)
	after := histogramSampleCount(t, registryResolveDuration)
	if after != before+1 {
		t.Errorf("registry resolve sample count = %d, want %d", after, before+1)
	}
}

// histogramSampleCount reads the observation count of a plain (unlabeled)
// histogram via its dto representation.
func histogramSampleCount(t *testing.T, h prometheus.Collector) uint64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	h.Collect(ch)
	close(ch)
	m := <-ch
	var dtoMetric dto.Metric
	if err := m.Write(&dtoMetric); err != nil {
		t.Fatalf("write histogram metric: %v", err)
	}
	return dtoMetric.GetHistogram().GetSampleCount()
}
