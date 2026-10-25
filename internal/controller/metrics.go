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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// Custom operator metrics. They are registered on controller-runtime's global
// registry (the same one the manager's /metrics endpoint serves) so they need
// no separate wiring in cmd/main.go.
//
// Cardinality is bounded: per-DecisionModel series carry only namespace+name
// labels (no revision hash), plus a small fixed set of enum labels (phase,
// result). Series for a DecisionModel are deleted when the object is gone
// (see deleteMetrics), so a churn of short-lived DecisionModels does not leak
// series.
//
// All per-DecisionModel gauges/counters are updated only after a successful
// status write (same rule as Events); event-style counters are buffered on the
// reconcileState and flushed in finish. This keeps metrics consistent with the
// status the API server actually accepted.

// Metric names.
const (
	metricPhase                   = "decisionmodel_phase"
	metricModelReadyReplicas      = "decisionmodel_model_ready_replicas"
	metricDesiredReplicas         = "decisionmodel_desired_replicas"
	metricRolloutsTotal           = "decisionmodel_rollouts_total"
	metricPhaseDurationSeconds    = "decisionmodel_phase_duration_seconds"
	metricEvaluationAccuracy      = "decisionmodel_evaluation_accuracy"
	metricEvaluationECE           = "decisionmodel_evaluation_ece"
	metricEvaluationMacroF1       = "decisionmodel_evaluation_macro_f1"
	metricProbeResultsTotal       = "decisionmodel_probe_results_total"
	metricServingPodsRecreated    = "decisionmodel_serving_pods_recreated_total"
	metricRegistryResolveDuration = "decisionmodel_registry_resolve_duration_seconds"
	metricRevisionInfo            = "decisionmodel_revision_info"
	metricCondition               = "decisionmodel_condition"
)

// Metric label keys.
const (
	labelNamespace = "namespace"
	labelName      = "name"
	labelPhase     = "phase"
	labelResult    = "result"
	labelRole      = "role"
	labelModel     = "model"
	labelDigest    = "digest"
	labelType      = "type"
	labelStatus    = "status"
	labelReason    = "reason"
)

// Revision-info role label values.
const (
	roleStable    = "stable"
	roleCandidate = "candidate"
)

// Rollout result label values.
const (
	rolloutPromoted                 = "promoted"
	rolloutRolledBack               = "rolled_back"
	rolloutFailed                   = "failed"
	rolloutRolledBackAfterPromotion = "rolled_back_after_promotion"
	// Recreate strategy capacity management (bounded: two fixed values added to the
	// existing decisionmodel_rollouts_total result label — no new metric, no
	// unbounded cardinality).
	rolloutStableStopped  = "stable_stopped"
	rolloutStableRestored = "stable_restored"
)

// Probe result label values.
const (
	probeReady          = "ready"
	probeDigestMismatch = "digest_mismatch"
	probeDeviceMismatch = "device_mismatch"
	probeNotPinned      = "not_pinned"
	probeError          = "error"
)

// allPhases is the fixed set of phases exported by decisionmodel_phase; the
// gauge carries 1 for the current phase and 0 for every other phase, so a
// dashboard/alert never sees a stale phase reported as active.
var allPhases = []decisionmodelv1alpha1.DecisionModelPhase{
	decisionmodelv1alpha1.PhasePending,
	decisionmodelv1alpha1.PhaseResolving,
	decisionmodelv1alpha1.PhaseCaching,
	decisionmodelv1alpha1.PhaseStarting,
	decisionmodelv1alpha1.PhaseEvaluating,
	decisionmodelv1alpha1.PhaseDegraded,
	decisionmodelv1alpha1.PhasePromoting,
	decisionmodelv1alpha1.PhaseAwaitingPromotion,
	decisionmodelv1alpha1.PhaseReady,
	decisionmodelv1alpha1.PhaseFailed,
	decisionmodelv1alpha1.PhaseRolledBack,
}

var (
	phaseGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricPhase,
		Help: "Current lifecycle phase of a DecisionModel (1 for the active phase, 0 otherwise).",
	}, []string{labelNamespace, labelName, labelPhase})

	modelReadyReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricModelReadyReplicas,
		Help: "Number of serving replicas that passed the model-ready gate.",
	}, []string{labelNamespace, labelName})

	desiredReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricDesiredReplicas,
		Help: "Desired number of serving replicas.",
	}, []string{labelNamespace, labelName})

	rolloutsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricRolloutsTotal,
		Help: "Total rollout outcomes by result (promoted, rolled_back, rolled_back_after_promotion, failed).",
	}, []string{labelNamespace, labelName, labelResult})

	phaseDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: metricPhaseDurationSeconds,
		Help: "Time a DecisionModel spent in a phase before leaving it, in seconds.",
		// Buckets suited to minutes: 5s .. 1h.
		Buckets: []float64{5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600},
	}, []string{labelNamespace, labelName, labelPhase})

	evaluationAccuracyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricEvaluationAccuracy,
		Help: "Accuracy of the last completed evaluation (decimal in [0,1]).",
	}, []string{labelNamespace, labelName})

	evaluationECEGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricEvaluationECE,
		Help: "Expected Calibration Error of the last completed evaluation (decimal in [0,1]).",
	}, []string{labelNamespace, labelName})

	evaluationMacroF1Gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricEvaluationMacroF1,
		Help: "Macro-averaged F1 of the last completed evaluation over choice/bool " +
			"questions (decimal in [0,1]). Not exported when the dataset had no " +
			"classifiable question.",
	}, []string{labelNamespace, labelName})

	probeResultsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricProbeResultsTotal,
		Help: "Total model-readiness probe results by outcome " +
			"(ready, digest_mismatch, device_mismatch, error).",
	}, []string{labelNamespace, labelName, labelResult})

	// servingPodsRecreatedTotal counts how often a ReadWriteOnce store's serving
	// Pods were recreated because spec.replicas crossed the 1 / N>1 boundary and
	// the co-location affinity changed the Pod template. Incremented immediately
	// at the emit point (the matching ServingPodsRecreated Event), not through the
	// status-write buffer: the transition is read from live cluster state and can
	// be observed at most once, so a failed status write must not drop the signal.
	servingPodsRecreatedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricServingPodsRecreated,
		Help: "Total serving-Pod recreations caused by spec.replicas crossing 1 on a " +
			"ReadWriteOnce store (the co-location affinity changed the Pod template).",
	}, []string{labelNamespace, labelName})

	registryResolveDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    metricRegistryResolveDuration,
		Help:    "Duration of a model registry tag->digest resolution, in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	revisionInfoGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricRevisionInfo,
		Help: "The model and digest of each live revision role of a DecisionModel " +
			"(always 1). At most two series per DecisionModel: role=stable and role=candidate.",
	}, []string{labelNamespace, labelName, labelRole, labelModel, labelDigest})

	// conditionGauge carries the current status+reason of a DecisionModel's
	// Ready and Degraded conditions (always 1). Exactly one series per
	// (DecisionModel, type): the old series is deleted when the status or reason
	// changes, so a dashboard never sees a stale reason reported as current.
	// Cardinality is bounded — type is one of two fixed values, status is
	// True/False, and reason strings are controller constants.
	conditionGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricCondition,
		Help: "Current status and reason of a DecisionModel condition (always 1). " +
			"One series per (DecisionModel, type) for type=Ready and type=Degraded; " +
			"the status and reason labels carry the live condition.",
	}, []string{labelNamespace, labelName, labelType, labelStatus, labelReason})
)

func init() {
	metrics.Registry.MustRegister(
		phaseGauge,
		modelReadyReplicasGauge,
		desiredReplicasGauge,
		rolloutsTotal,
		phaseDurationSeconds,
		evaluationAccuracyGauge,
		evaluationECEGauge,
		evaluationMacroF1Gauge,
		probeResultsTotal,
		registryResolveDuration,
		servingPodsRecreatedTotal,
		revisionInfoGauge,
		conditionGauge,
	)
}

// recordStatusMetrics reflects the DecisionModel's just-persisted status into
// the gauges. It is called from finish only after the status write succeeded,
// so a gauge never advertises a phase/replica count the API server rejected.
func recordStatusMetrics(dm *decisionmodelv1alpha1.DecisionModel) {
	ns, name := dm.Namespace, dm.Name

	for _, p := range allPhases {
		v := 0.0
		if dm.Status.Phase == p {
			v = 1.0
		}
		phaseGauge.WithLabelValues(ns, name, string(p)).Set(v)
	}

	modelReadyReplicasGauge.WithLabelValues(ns, name).Set(float64(dm.Status.Replicas.ModelReady))
	desiredReplicasGauge.WithLabelValues(ns, name).Set(float64(dm.Status.Replicas.Desired))

	if e := dm.Status.Evaluation; e != nil {
		if v, err := strconv.ParseFloat(e.Accuracy, 64); err == nil {
			evaluationAccuracyGauge.WithLabelValues(ns, name).Set(v)
		}
		if v, err := strconv.ParseFloat(e.ECE, 64); err == nil {
			evaluationECEGauge.WithLabelValues(ns, name).Set(v)
		}
		// Macro-F1 is only meaningful when the dataset had a classifiable question;
		// status leaves MacroF1 empty otherwise. Do NOT export a stale 0: set the
		// gauge when present, delete the series when absent.
		if v, err := strconv.ParseFloat(e.MacroF1, 64); err == nil {
			evaluationMacroF1Gauge.WithLabelValues(ns, name).Set(v)
		} else {
			evaluationMacroF1Gauge.DeleteLabelValues(ns, name)
		}
	}

	recordRevisionInfo(ns, name, roleStable, dm.Status.StableRevision)
	recordRevisionInfo(ns, name, roleCandidate, dm.Status.CandidateRevision)

	recordConditionGauge(ns, name, dm, decisionmodelv1alpha1.ConditionReady)
	recordConditionGauge(ns, name, dm, decisionmodelv1alpha1.ConditionDegraded)
}

// recordConditionGauge keeps exactly one decisionmodel_condition series per
// (DecisionModel, type): it first deletes any existing series for the type (the
// status and reason are in the label set, so a status/reason change would
// otherwise leave a stale series), then sets the current one. When the condition
// is absent only the delete runs, so the metric stays bounded to the condition
// types that actually exist. Reason strings are controller constants, so
// cardinality is bounded.
func recordConditionGauge(ns, name string, dm *decisionmodelv1alpha1.DecisionModel, condType string) {
	conditionGauge.DeletePartialMatch(prometheus.Labels{
		labelNamespace: ns, labelName: name, labelType: condType,
	})
	c := meta.FindStatusCondition(dm.Status.Conditions, condType)
	if c == nil {
		return
	}
	conditionGauge.WithLabelValues(ns, name, condType, string(c.Status), c.Reason).Set(1)
}

// recordRevisionInfo keeps exactly one decisionmodel_revision_info series per
// (DecisionModel, role). It first deletes any existing series for the role (the
// model/digest are in the label set, so a model change would otherwise leave a
// stale series), then sets the current one. When the role is empty (no such
// revision) only the delete runs, so the metric stays bounded to the roles that
// actually exist — at most two series per DecisionModel.
func recordRevisionInfo(ns, name, role string, rev *decisionmodelv1alpha1.RevisionStatus) {
	revisionInfoGauge.DeletePartialMatch(prometheus.Labels{
		labelNamespace: ns, labelName: name, labelRole: role,
	})
	if rev == nil || rev.Model == "" {
		return
	}
	revisionInfoGauge.WithLabelValues(ns, name, role, rev.Model, rev.Digest).Set(1)
}

// deleteMetrics removes every series for a DecisionModel. Called from the
// NotFound branch of Reconcile so a deleted object's series stop being exported.
func deleteMetrics(namespace, name string) {
	l := prometheus.Labels{labelNamespace: namespace, labelName: name}
	phaseGauge.DeletePartialMatch(l)
	modelReadyReplicasGauge.DeletePartialMatch(l)
	desiredReplicasGauge.DeletePartialMatch(l)
	rolloutsTotal.DeletePartialMatch(l)
	phaseDurationSeconds.DeletePartialMatch(l)
	evaluationAccuracyGauge.DeletePartialMatch(l)
	evaluationECEGauge.DeletePartialMatch(l)
	evaluationMacroF1Gauge.DeletePartialMatch(l)
	probeResultsTotal.DeletePartialMatch(l)
	servingPodsRecreatedTotal.DeletePartialMatch(l)
	revisionInfoGauge.DeletePartialMatch(l)
	conditionGauge.DeletePartialMatch(l)
}

// observeRegistryResolve records a registry resolution duration. No DM labels,
// so it is safe to call directly (not deferred behind a status write).
func observeRegistryResolve(d time.Duration) {
	registryResolveDuration.Observe(d.Seconds())
}
