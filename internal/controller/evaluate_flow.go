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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// evaluateOrPromote gates promotion on an eval run when spec.rollout.evaluation
// is set. Without evaluation it promotes immediately.
func (r *DecisionModelReconciler) evaluateOrPromote(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	precision string,
	cacheDegraded bool,
	apiKey string,
	policyChanged bool,
) (ctrl.Result, error) {
	evalSpec := evaluationSpec(dm)
	if evalSpec == nil {
		return r.promoteOrAwait(ctx, dm, eng, candidate, precision, cacheDegraded, policyChanged)
	}

	// The engine must implement the optional Decider capability.
	dec, ok := eng.(engine.Decider)
	if !ok {
		return r.rollbackOrFailPermanent(ctx, dm, candidate, reasonEvaluationUnsupported,
			"engine does not support evaluation (no Decider)")
	}

	// Load and parse the dataset.
	raw, err := r.loadDataset(ctx, dm, &evalSpec.DatasetRef)
	if err != nil {
		if errors.Is(err, errSecretNotAllowed) {
			return r.rollbackOrFailPermanent(ctx, dm, candidate, reasonSecretNotAllowed, err.Error())
		}
		// A missing/unreadable dataset object is transient (may be created later);
		// only a genuinely invalid dataset (parse error below) is permanent.
		return r.rollbackOrFail(ctx, dm, candidate, reasonDatasetInvalid, err.Error())
	}
	maxCases := int(evalSpec.MaxCases)
	if maxCases == 0 {
		maxCases = defaultMaxCases
	}
	cases, err := parseDataset(raw, maxCases)
	if err != nil {
		return r.rollbackOrFailPermanent(ctx, dm, candidate, reasonDatasetInvalid, err.Error())
	}
	dsHash := datasetHash(raw)

	// Ensure the candidate evaluation is running / read its result.
	candKey := evalKey{dm.Namespace, dm.Name, candidate.Hash, dsHash, maxCases}
	// resuming is true when a prior reconcile already advanced *this* candidate
	// into Evaluating and persisted it — i.e. the manager restarted (or leadership
	// moved) mid-evaluation and the in-memory eval store is empty again. In that
	// case the eval restarts from scratch, but EvaluationStarted must not be
	// emitted a second time for the same revision (derive it from persisted
	// cluster state, not the in-memory store).
	resuming := dm.Status.Phase == decisionmodelv1alpha1.PhaseEvaluating &&
		dm.Status.CandidateRevision != nil && dm.Status.CandidateRevision.Hash == candidate.Hash
	candRes, running, evErr := r.ensureEval(ctx, dm, dec, candidate, apiKey, cases, candKey, resuming)
	if evErr != nil {
		// A Pod-list/ownership failure must abort (workqueue backoff), not be
		// mistaken for "still running".
		return r.finish(ctx, dm, ctrl.Result{}, evErr)
	}

	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseEvaluating)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionFalse,
		Reason:  reasonEvaluating,
		Message: "running golden-dataset evaluation",
	})

	if running {
		if r.phaseExceeded(dm, evaluatingTimeout(dm)) {
			return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationTimeout, "evaluation exceeded deadline")
		}
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
	}

	if candRes.timedOut {
		return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationTimeout, "evaluation timed out")
	}
	if candRes.err != nil {
		return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationFailed, candRes.err.Error())
	}

	// Optionally establish the stable baseline for this dataset. When the stable
	// baseline is required by a relative gate (maxAccuracyDrop / maxECEIncrease)
	// but cannot be computed, do not promote: hold in Evaluating with
	// Evaluated=False/BaselineUnavailable until the evaluation timeout, which then
	// takes the normal rollback path (fail-closed).
	maxDrop, hasDrop := parseDecimal(evalSpec.MaxAccuracyDrop)
	maxECE, hasMaxECE := parseDecimal(evalSpec.MaxECE)
	maxECEInc, hasMaxECEInc := parseDecimal(evalSpec.MaxECEIncrease)
	needsBaseline := hasDrop || hasMaxECEInc

	// Only compute / await the stable baseline when a relative gate needs it.
	// Without a relative gate the absolute gates decide; the baseline
	// is not run at all and never delays promotion or causes EvaluationTimeout.
	var baseline *evalResult
	if needsBaseline {
		b, baselineDone, baselineUnavailable, bErr := r.baselineResult(ctx, dm, dec, apiKey, cases, dsHash, maxCases)
		if bErr != nil {
			return r.finish(ctx, dm, ctrl.Result{}, bErr)
		}
		if !baselineDone {
			// The baseline run shares the Evaluating phase budget with the candidate:
			// the deadline is measured from phaseTransitionTime, not restarted for the
			// baseline, so the whole phase cannot take ~2x the timeout.
			if r.phaseExceeded(dm, evaluatingTimeout(dm)) {
				return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationTimeout,
					"baseline evaluation exceeded the Evaluating deadline")
			}
			return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		}
		if baselineUnavailable {
			if r.phaseExceeded(dm, evaluatingTimeout(dm)) {
				return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationTimeout,
					"stable baseline unavailable for a relative eval gate before the evaluation deadline")
			}
			setStatusCondition(dm, metav1.Condition{
				Type:    decisionmodelv1alpha1.ConditionEvaluated,
				Status:  metav1.ConditionFalse,
				Reason:  reasonBaselineUnavailable,
				Message: "waiting for a reachable stable Pod to compute the baseline for a relative eval gate",
			})
			return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		}
		baseline = b
	}

	minAcc, _ := parseDecimal(evalSpec.MinAccuracy)

	// Record the result in status.
	dm.Status.Evaluation = &decisionmodelv1alpha1.EvaluationStatus{
		Revision:    candidate.Hash,
		Accuracy:    formatDecimal(candRes.accuracy),
		ECE:         formatDecimal(candRes.ece),
		Brier:       formatDecimal(candRes.brier),
		Cases:       int32(candRes.total),
		FailedCases: int32(candRes.failedCases),
		PolicyHash:  evalPolicyHash(evalSpec),
		CompletedAt: ptrTime(metav1.NewTime(r.now())),
	}
	if baseline != nil {
		dm.Status.Evaluation.BaselineAccuracy = formatDecimal(baseline.accuracy)
		dm.Status.Evaluation.BaselineECE = formatDecimal(baseline.ece)
	}

	// Gate evaluation: accuracy floor, accuracy drop, and calibration (ECE).
	if failMsg, failed := evalGateFailure(candRes, baseline, evalGates{
		minAcc: minAcc, maxDrop: maxDrop, hasDrop: hasDrop,
		maxECE: maxECE, hasMaxECE: hasMaxECE, maxECEInc: maxECEInc, hasMaxECEInc: hasMaxECEInc,
	}); failed {
		r.event(ctx, dm, corev1.EventTypeWarning, eventEvaluationFailed, "evaluation failed: %s", failMsg)
		return r.rollbackOrFail(ctx, dm, candidate, reasonEvaluationFailed, failMsg)
	}

	// Emit EvaluationPassed once, on the Evaluated condition transition to True.
	if !meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated) {
		r.event(ctx, dm, corev1.EventTypeNormal, eventEvaluationPassed,
			"evaluation passed: accuracy %.4f", candRes.accuracy)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionTrue,
		Reason:  reasonEvaluated,
		Message: fmt.Sprintf("accuracy %.4f", candRes.accuracy),
	})
	return r.promoteOrAwait(ctx, dm, eng, candidate, precision, cacheDegraded, policyChanged)
}

// evalGates bundles the parsed gate thresholds from the evaluation spec.
type evalGates struct {
	minAcc       float64
	maxDrop      float64
	hasDrop      bool
	maxECE       float64
	hasMaxECE    bool
	maxECEInc    float64
	hasMaxECEInc bool
}

// evalGateFailure applies the accuracy floor, accuracy-drop, and calibration
// (ECE / ECE-increase) gates. It returns a non-empty message and failed=true on
// the first gate the candidate fails; relative gates apply only when a baseline
// is available.
func evalGateFailure(candRes evalResult, baseline *evalResult, g evalGates) (string, bool) {
	switch {
	case candRes.accuracy < g.minAcc:
		return fmt.Sprintf("accuracy %.4f < minAccuracy %.4f", candRes.accuracy, g.minAcc), true
	case baseline != nil && g.hasDrop && (baseline.accuracy-candRes.accuracy) > g.maxDrop:
		return fmt.Sprintf("accuracy dropped %.4f (baseline %.4f) > maxAccuracyDrop %.4f",
			baseline.accuracy-candRes.accuracy, baseline.accuracy, g.maxDrop), true
	case g.hasMaxECE && candRes.ece > g.maxECE:
		return fmt.Sprintf("ECE %.4f > maxECE %.4f", candRes.ece, g.maxECE), true
	case baseline != nil && g.hasMaxECEInc && (candRes.ece-baseline.ece) > g.maxECEInc:
		return fmt.Sprintf("ECE increased %.4f (baseline %.4f) > maxECEIncrease %.4f",
			candRes.ece-baseline.ece, baseline.ece, g.maxECEInc), true
	}
	return "", false
}

// ensureEval starts the candidate evaluation if not started, and returns its
// result, whether it is still running, and a hard error (a Pod-list/ownership
// failure) that must abort the reconcile rather than look like "still running".
func (r *DecisionModelReconciler) ensureEval(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	dec engine.Decider,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
	cases []evalCase,
	key evalKey,
	resuming bool,
) (evalResult, bool, error) {
	store := r.evalStoreOrInit()

	if e, ok := store.get(key); ok {
		if e.result.done {
			// A run invalidated by a transport blip is not a verdict: forget it so
			// the next reconcile retries rather than rolling back on a flake.
			if e.result.transport > 0 {
				store.forget(key)
				return evalResult{}, true, nil
			}
			return e.result, false, nil
		}
		return evalResult{}, true, nil
	}

	baseURL, ok, err := r.candidatePodBaseURL(ctx, dm, candidate)
	if err != nil {
		return evalResult{}, true, err
	}
	if !ok {
		// No ready Pod to hit yet; report running and let the next reconcile retry.
		return evalResult{}, true, nil
	}

	// Emit EvaluationStarted only when this is a genuinely new evaluation, not a
	// post-restart resume of an already-announced one (the in-memory store is
	// empty after a restart, so presence-in-store cannot be the signal).
	if !resuming {
		r.event(ctx, dm, corev1.EventTypeNormal, eventEvaluationStarted,
			"evaluating revision %s against %d cases", candidate.Hash, len(cases))
	}

	evalTimeout := evaluatingTimeout(dm)
	runCtx, cancel := context.WithCancel(r.bgContext())
	store.start(key, cancel)
	go func() {
		res := runEvaluation(runCtx, dec, baseURL, apiKey, candidate.Model, cases, r.now, evalTimeout)
		store.finish(key, res)
	}()
	return evalResult{}, true, nil
}

// baselineResult evaluates the stable revision for this dataset if a baseline
// is not yet recorded. Returns (result, done, unavailable). result is nil when
// there is no stable revision (no baseline needed) or the baseline could not be
// computed. unavailable is true only in the latter case — a baseline was
// expected but could not be produced (no reachable stable Pod, transport error,
// or timeout) — so the caller can fail closed on a relative gate.
func (r *DecisionModelReconciler) baselineResult(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	dec engine.Decider,
	apiKey string,
	cases []evalCase,
	dsHash string,
	maxCases int,
) (res *evalResult, done bool, unavailable bool, err error) {
	stable := dm.Status.StableRevision
	if stable == nil {
		return nil, true, false, nil
	}
	store := r.evalStoreOrInit()
	key := evalKey{dm.Namespace, dm.Name, stable.Hash, dsHash, maxCases}
	if e, ok := store.get(key); ok {
		if e.result.done {
			if e.result.transport > 0 {
				// A transport blip is not a baseline: forget it so it retries, and
				// report unavailable this round (never cache a blip as a baseline).
				store.forget(key)
				return nil, true, true, nil
			}
			if e.result.err != nil || e.result.timedOut {
				// Baseline could not be computed.
				return nil, true, true, nil
			}
			r := e.result
			return &r, true, false, nil
		}
		return nil, false, false, nil
	}
	baseURL, ok, berr := r.revisionPodBaseURL(ctx, dm, stable.Hash)
	if berr != nil {
		// A Pod-list/ownership failure must abort, not be recorded as an
		// unavailable baseline (which would roll the candidate back).
		return nil, false, false, berr
	}
	if !ok {
		// No stable Pod reachable yet: baseline is not available this round.
		return nil, true, true, nil
	}
	evalTimeout := evaluatingTimeout(dm)
	runCtx, cancel := context.WithCancel(r.bgContext())
	store.start(key, cancel)
	go func() {
		res := runEvaluation(runCtx, dec, baseURL, apiKey, stable.Model, cases, r.now, evalTimeout)
		store.finish(key, res)
	}()
	return nil, false, false, nil
}

// candidatePodBaseURL returns the base URL of one model-ready candidate Pod. The
// error distinguishes a Pod-list failure (abort) from "no ready Pod yet" (wait).
func (r *DecisionModelReconciler) candidatePodBaseURL(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	candidate *decisionmodelv1alpha1.RevisionStatus,
) (string, bool, error) {
	return r.revisionPodBaseURL(ctx, dm, candidate.Hash)
}

// revisionPodBaseURL returns http://<podIP>:<port> for a gated Pod of a revision.
// A listing/ownership error is returned (not swallowed as "no Pod") so the eval
// flow aborts and the workqueue backs off instead of silently waiting.
func (r *DecisionModelReconciler) revisionPodBaseURL(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (string, bool, error) {
	pods, err := r.revisionPods(ctx, dm, rev)
	if err != nil {
		return "", false, err
	}
	port := int32(0)
	if eng, ok := r.Engines[engineOrDefault(dm.Spec.Engine)]; ok {
		port = eng.ServicePort()
	}
	for i := range pods {
		p := &pods[i]
		if p.Status.PodIP != "" && servingReady(p) {
			return podBaseURL(p.Status.PodIP, port), true, nil
		}
	}
	return "", false, nil
}

// loadDataset reads the JSONL dataset from a ConfigMap or Secret.
func (r *DecisionModelReconciler) loadDataset(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	ref *decisionmodelv1alpha1.DatasetRef,
) ([]byte, error) {
	log := logf.FromContext(ctx)
	rdr, err := r.reader()
	if err != nil {
		return nil, err
	}
	switch {
	case ref.ConfigMapRef != nil:
		var cm corev1.ConfigMap
		if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.ConfigMapRef.Name}, &cm); err != nil {
			return nil, fmt.Errorf("read ConfigMap %s: %w", ref.ConfigMapRef.Name, err)
		}
		if v, ok := cm.Data[ref.ConfigMapRef.Key]; ok {
			return []byte(v), nil
		}
		if v, ok := cm.BinaryData[ref.ConfigMapRef.Key]; ok {
			return v, nil
		}
		return nil, fmt.Errorf("key %q not found in ConfigMap %s", ref.ConfigMapRef.Key, ref.ConfigMapRef.Name)
	case ref.SecretRef != nil:
		var sec corev1.Secret
		if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.SecretRef.Name}, &sec); err != nil {
			return nil, fmt.Errorf("read Secret %s: %w", ref.SecretRef.Name, err)
		}
		if err := requireAPIKeyLabel(sec.Labels); err != nil {
			return nil, err
		}
		if v, ok := sec.Data[ref.SecretRef.Key]; ok {
			return v, nil
		}
		return nil, fmt.Errorf("key %q not found in Secret %s", ref.SecretRef.Key, ref.SecretRef.Name)
	default:
		log.V(1).Info("dataset ref has neither configMapRef nor secretRef")
		return nil, fmt.Errorf("datasetRef has neither configMapRef nor secretRef")
	}
}

// evaluationSpec returns the evaluation spec, or nil when unset.
func evaluationSpec(dm *decisionmodelv1alpha1.DecisionModel) *decisionmodelv1alpha1.EvaluationSpec {
	if dm.Spec.Rollout == nil {
		return nil
	}
	return dm.Spec.Rollout.Evaluation
}

// evalPolicyHash returns a stable hash (16 hex) of the effective evaluation
// policy: the gate thresholds, the datasetRef identity, and maxCases.
// A change to any of these means a parked candidate's recorded result is stale
// and the candidate must be re-evaluated rather than promoted on the old result.
// Returns "" when there is no evaluation spec.
func evalPolicyHash(evalSpec *decisionmodelv1alpha1.EvaluationSpec) string {
	if evalSpec == nil {
		return ""
	}
	var ref string
	switch {
	case evalSpec.DatasetRef.ConfigMapRef != nil:
		ref = "cm/" + evalSpec.DatasetRef.ConfigMapRef.Name + "/" + evalSpec.DatasetRef.ConfigMapRef.Key
	case evalSpec.DatasetRef.SecretRef != nil:
		ref = "secret/" + evalSpec.DatasetRef.SecretRef.Name + "/" + evalSpec.DatasetRef.SecretRef.Key
	}
	payload := strings.Join([]string{
		evalSpec.MinAccuracy, evalSpec.MaxAccuracyDrop, evalSpec.MaxECE, evalSpec.MaxECEIncrease,
		ref, strconv.Itoa(int(evalSpec.MaxCases)),
	}, "\x1f")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])[:16]
}

// parseDecimal parses a decimal string in [0,1]; ok=false when empty/invalid.
func parseDecimal(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// formatDecimal renders an accuracy as a fixed 4-dp decimal string.
func formatDecimal(v float64) string {
	return strconv.FormatFloat(v, 'f', 4, 64)
}

func ptrTime(t metav1.Time) *metav1.Time { return &t }
