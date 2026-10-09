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
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
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
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionEvaluated,
			Status:  metav1.ConditionTrue,
			Reason:  reasonEvaluationSkipped,
			Message: "no evaluation configured; promotion follows model readiness",
		})
		return r.promoteOrAwait(ctx, dm, eng, candidate, precision, cacheDegraded, policyChanged)
	}

	// The engine must implement the optional Decider capability.
	dec, ok := eng.(engine.Decider)
	if !ok {
		return r.rollbackOrFailPermanent(ctx, dm, candidate, reasonEvaluationUnsupported,
			"engine does not support evaluation (no Decider)")
	}

	// Enter Evaluating before loading the dataset, so the Evaluating timeout
	// measures any hold on a missing/unreadable dataset (phaseTransitionTime is
	// set on the transition and not reset on requeue). Capture whether we were
	// ALREADY in Evaluating for this candidate first: that distinguishes a
	// post-restart resume (do not re-announce EvaluationStarted) from a fresh
	// entry, and setPhase below would otherwise erase the distinction.
	resuming := dm.Status.Phase == decisionmodelv1alpha1.PhaseEvaluating &&
		dm.Status.CandidateRevision != nil && dm.Status.CandidateRevision.Hash == candidate.Hash
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseEvaluating)

	// Load and parse the dataset.
	raw, deprecatedLabel, err := r.loadDataset(ctx, dm, &evalSpec.DatasetRef)
	if err != nil {
		switch {
		case errors.Is(err, errDatasetNotFound):
			return r.holdForDataset(ctx, dm, candidate, reasonDatasetNotFound, err)
		case errors.Is(err, errDatasetKeyNotFound):
			return r.holdForDataset(ctx, dm, candidate, reasonDatasetKeyNotFound, err)
		case errors.Is(err, errSecretNotAllowed):
			// A later label fix is picked up by the requeue; fail only on timeout.
			return r.holdForDataset(ctx, dm, candidate, reasonSecretNotAllowed, err)
		default:
			// Any other read error (timeout, 5xx, throttling, RBAC) is transient:
			// back off via the workqueue without recording a failed revision. The
			// Evaluating timeout still bounds the overall wait on the next pass.
			if r.phaseExceeded(dm, evaluatingTimeout(dm)) {
				return r.rollbackOrFail(ctx, dm, candidate, reasonDatasetNotFound,
					"dataset unreadable before the evaluation deadline: "+err.Error())
			}
			return r.finish(ctx, dm, ctrl.Result{}, err)
		}
	}
	if deprecatedLabel {
		r.event(ctx, dm, corev1.EventTypeWarning, eventDeprecatedSecretLabel,
			"dataset Secret %q is accepted via the deprecated %s label; add %s=true instead",
			evalSpec.DatasetRef.SecretRef.Name,
			decisionmodelv1alpha1.LabelAPIKey, decisionmodelv1alpha1.LabelEvalDataset)
	}
	maxCases := int(evalSpec.MaxCases)
	if maxCases == 0 {
		maxCases = defaultMaxCases
	}
	cases, err := parseDataset(raw, maxCases)
	if err != nil {
		return r.rollbackOrFailPermanent(ctx, dm, candidate, reasonDatasetInvalid,
			fmt.Sprintf("%s (fix the dataset and set the %s annotation to retry)",
				err.Error(), decisionmodelv1alpha1.AnnotationRetry))
	}
	dsHash := datasetHash(raw)
	dsDigest := datasetDigestFull(raw)
	scoreTol := scoreTolerance(evalSpec)

	// Ensure the candidate evaluation is running / read its result.
	candKey := evalKey{dm.Namespace, dm.Name, candidate.Hash, dsHash, maxCases, scoreTol}
	// Cancel any in-flight evaluation for THIS revision started under a different
	// eval identity (an in-place dataset edit, a maxCases or tolerance change
	// mid-Evaluating): forgetExcept only keys on revision, so the stale goroutine
	// would otherwise keep sending old-dataset requests until the revision
	// changed. The current candKey entry (if any) is kept.
	r.evalStoreOrInit().forgetMismatched(candKey)
	// resuming (computed above, before entering Evaluating) is true when a prior
	// reconcile already advanced *this* candidate into Evaluating and persisted
	// it — i.e. the manager restarted (or leadership moved) mid-evaluation and the
	// in-memory eval store is empty again. In that case the eval restarts from
	// scratch, but EvaluationStarted must not be emitted a second time for the
	// same revision (derive it from persisted cluster state, not the in-memory
	// store).
	candRes, running, evErr := r.ensureEval(ctx, dm, dec, candidate, apiKey, cases, candKey, resuming, scoreTol)
	if evErr != nil {
		// A Pod-list/ownership failure must abort (workqueue backoff), not be
		// mistaken for "still running".
		return r.finish(ctx, dm, ctrl.Result{}, evErr)
	}

	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseEvaluating)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionFalse,
		Reason:  reasonEvaluationRunning,
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
	// A case the runtime rejected means the golden dataset itself is broken.
	// Retrying cannot help, but a transient dataset edit can: hold the candidate
	// in Evaluating (Evaluated=False/DatasetInvalid naming the line) and roll back
	// only at the evaluation timeout — exactly like a missing dataset. A dataset
	// content change produces a new evalKey and re-runs automatically. The run is
	// not retried in between (the rejected result is cached under this dataset
	// hash). Emit the Event once (on the condition reason/message transition).
	if candRes.rejected {
		msg := fmt.Sprintf("golden dataset case on line %d rejected by the runtime: %s",
			candRes.rejectedLine, candRes.rejectedDetail)
		return r.holdForDataset(ctx, dm, candidate, reasonDatasetInvalid, errors.New(msg))
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
	minMacroF1, hasMinF1 := parseDecimal(evalSpec.MinMacroF1)
	maxF1Drop, hasF1Drop := parseDecimal(evalSpec.MaxMacroF1Drop)
	needsBaseline := hasDrop || hasMaxECEInc || hasF1Drop

	// Only compute / await the stable baseline when a relative gate needs it.
	// Without a relative gate the absolute gates decide; the baseline
	// is not run at all and never delays promotion or causes EvaluationTimeout.
	var baseline *evalResult
	if needsBaseline {
		b, baselineDone, baselineUnavailable, bErr := r.baselineResult(ctx, dm, dec, apiKey, cases, dsHash, maxCases, scoreTol)
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
	policyHash := evalPolicyHash(evalSpec)
	dm.Status.Evaluation = buildEvaluationStatus(evalSpec, candidate, candRes, baseline, policyHash, dsDigest, r.now())

	// Gate evaluation: accuracy floor, accuracy drop, calibration (ECE) and macro-F1.
	if failReason, failMsg, failed := evalGateFailure(candRes, baseline, evalGates{
		minAcc: minAcc, maxDrop: maxDrop, hasDrop: hasDrop,
		maxECE: maxECE, hasMaxECE: hasMaxECE, maxECEInc: maxECEInc, hasMaxECEInc: hasMaxECEInc,
		minMacroF1: minMacroF1, hasMinF1: hasMinF1, maxF1Drop: maxF1Drop, hasF1Drop: hasF1Drop,
	}); failed {
		dm.Status.Evaluation.Result = decisionmodelv1alpha1.EvaluationFailed
		dm.Status.Evaluation.Reason = failMsg
		r.event(ctx, dm, corev1.EventTypeWarning, eventEvaluationFailed,
			"candidate %s failed evaluation: %s; %s keeps serving",
			modelRef(candidate), failMsg, modelRef(dm.Status.StableRevision))
		return r.rollbackOrFail(ctx, dm, candidate, failReason, failMsg)
	}
	dm.Status.Evaluation.Result = decisionmodelv1alpha1.EvaluationPassed

	// Emit EvaluationPassed once, on the Evaluated condition transition to True.
	if !meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated) {
		r.event(ctx, dm, corev1.EventTypeNormal, eventEvaluationPassed,
			"candidate %s passed evaluation: accuracy %.4f", modelRef(candidate), candRes.accuracy)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionTrue,
		Reason:  reasonEvaluationPassed,
		Message: fmt.Sprintf("accuracy %.4f", candRes.accuracy),
	})
	return r.promoteOrAwait(ctx, dm, eng, candidate, precision, cacheDegraded, policyChanged)
}

// holdForDataset keeps a candidate in Evaluating while its golden dataset is
// missing/unreadable in a way a user can fix without a spec change (object or
// key not yet created, Secret not yet labelled). No traffic moves and the
// candidate Deployment is kept. It sets Evaluated=False with the given reason,
// emits one Warning Event per distinct cause (not on every requeue), and
// requeues at regateInterval — unless the Evaluating timeout has expired, in
// which case the candidate is rolled back with the hold reason (fail-closed).
func (r *DecisionModelReconciler) holdForDataset(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	reason string,
	cause error,
) (ctrl.Result, error) {
	if r.phaseExceeded(dm, evaluatingTimeout(dm)) {
		return r.rollbackOrFail(ctx, dm, candidate, reason,
			cause.Error()+" before the evaluation deadline")
	}
	// Emit the Warning once per cause: only when the Evaluated condition is not
	// already False for this same reason.
	cur := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated)
	if cur == nil || cur.Status != metav1.ConditionFalse || cur.Reason != reason {
		r.event(ctx, dm, corev1.EventTypeWarning, eventEvaluationOnHold,
			"evaluation on hold (%s): %s", reason, cause.Error())
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: cause.Error(),
	})
	return r.finish(ctx, dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
}

type evalGates struct {
	minAcc       float64
	maxDrop      float64
	hasDrop      bool
	maxECE       float64
	hasMaxECE    bool
	maxECEInc    float64
	hasMaxECEInc bool
	minMacroF1   float64
	hasMinF1     bool
	maxF1Drop    float64
	hasF1Drop    bool
}

// evalGateFailure applies the accuracy floor, accuracy-drop, and calibration
// (ECE / ECE-increase) gates. It returns a reason, a non-empty message and
// failed=true on the first gate the candidate fails; relative gates apply only
// when a baseline is available. A calibration gate with no calibrated cases
// (candidate, or baseline for the relative gate) fails as CalibrationUnavailable
// rather than passing on an ECE of 0 over an empty set.
func evalGateFailure(candRes evalResult, baseline *evalResult, g evalGates) (reason, msg string, failed bool) {
	switch {
	case candRes.accuracy < g.minAcc:
		return reasonEvaluationFailed,
			fmt.Sprintf("accuracy %.4f < minAccuracy %.4f", candRes.accuracy, g.minAcc), true
	case baseline != nil && g.hasDrop && (baseline.accuracy-candRes.accuracy) > g.maxDrop:
		return reasonEvaluationFailed,
			fmt.Sprintf("accuracy dropped %.4f (baseline %.4f) > maxAccuracyDrop %.4f",
				baseline.accuracy-candRes.accuracy, baseline.accuracy, g.maxDrop), true
	case g.hasMaxECE && candRes.calibratedCases == 0:
		return reasonCalibrationUnavailable,
			"maxECE is set but no scored case produced a probability distribution (0 calibrated cases); " +
				"ECE cannot be evaluated", true
	case baseline != nil && g.hasMaxECEInc && (candRes.calibratedCases == 0 || baseline.calibratedCases == 0):
		return reasonCalibrationUnavailable,
			"maxECEIncrease is set but the candidate or baseline produced 0 calibrated cases; " +
				"the ECE increase cannot be evaluated", true
	case g.hasMaxECE && candRes.ece > g.maxECE:
		return reasonEvaluationFailed,
			fmt.Sprintf("ECE %.4f > maxECE %.4f", candRes.ece, g.maxECE), true
	case baseline != nil && g.hasMaxECEInc && (candRes.ece-baseline.ece) > g.maxECEInc:
		return reasonEvaluationFailed,
			fmt.Sprintf("ECE increased %.4f (baseline %.4f) > maxECEIncrease %.4f",
				candRes.ece-baseline.ece, baseline.ece, g.maxECEInc), true
	case g.hasMinF1 && candRes.classifiableCases == 0:
		return reasonClassificationUnavailable,
			"minMacroF1 is set but the dataset has no choice or bool question (0 classifiable cases); " +
				"macro-F1 cannot be evaluated", true
	case baseline != nil && g.hasF1Drop && (candRes.classifiableCases == 0 || baseline.classifiableCases == 0):
		return reasonClassificationUnavailable,
			"maxMacroF1Drop is set but the candidate or baseline has no classifiable question (0 classifiable cases); " +
				"the macro-F1 drop cannot be evaluated", true
	case g.hasMinF1 && candRes.macroF1 < g.minMacroF1:
		return reasonEvaluationFailed,
			fmt.Sprintf("macroF1 %.4f < minMacroF1 %.4f", candRes.macroF1, g.minMacroF1), true
	case baseline != nil && g.hasF1Drop && (baseline.macroF1-candRes.macroF1) > g.maxF1Drop:
		return reasonEvaluationFailed,
			fmt.Sprintf("macroF1 dropped %.4f (baseline %.4f) > maxMacroF1Drop %.4f",
				baseline.macroF1-candRes.macroF1, baseline.macroF1, g.maxF1Drop), true
	}
	return "", "", false
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
	scoreTol float64,
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
		res := runEvaluation(runCtx, dec, baseURL, apiKey, candidate.Model, cases, r.now, evalTimeout, scoreTol)
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
	scoreTol float64,
) (res *evalResult, done bool, unavailable bool, err error) {
	stable := dm.Status.StableRevision
	if stable == nil {
		return nil, true, false, nil
	}
	store := r.evalStoreOrInit()
	key := evalKey{dm.Namespace, dm.Name, stable.Hash, dsHash, maxCases, scoreTol}
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
		res := runEvaluation(runCtx, dec, baseURL, apiKey, stable.Model, cases, r.now, evalTimeout, scoreTol)
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

// loadDataset reads the JSONL dataset from a ConfigMap or Secret. deprecatedLabel
// is true when a dataset Secret was accepted only via the legacy
// decisionmodel.io/api-key label (the caller emits a deprecation Warning).
func (r *DecisionModelReconciler) loadDataset(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	ref *decisionmodelv1alpha1.DatasetRef,
) (data []byte, deprecatedLabel bool, err error) {
	log := logf.FromContext(ctx)
	rdr, rerr := r.reader()
	if rerr != nil {
		return nil, false, rerr
	}
	switch {
	case ref.ConfigMapRef != nil:
		var cm corev1.ConfigMap
		if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.ConfigMapRef.Name}, &cm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("%w: ConfigMap %s", errDatasetNotFound, ref.ConfigMapRef.Name)
			}
			return nil, false, fmt.Errorf("read ConfigMap %s: %w", ref.ConfigMapRef.Name, err)
		}
		if v, ok := cm.Data[ref.ConfigMapRef.Key]; ok {
			return []byte(v), false, nil
		}
		if v, ok := cm.BinaryData[ref.ConfigMapRef.Key]; ok {
			return v, false, nil
		}
		return nil, false, fmt.Errorf("%w: key %q in ConfigMap %s", errDatasetKeyNotFound, ref.ConfigMapRef.Key, ref.ConfigMapRef.Name)
	case ref.SecretRef != nil:
		var sec corev1.Secret
		if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.SecretRef.Name}, &sec); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, fmt.Errorf("%w: Secret %s", errDatasetNotFound, ref.SecretRef.Name)
			}
			return nil, false, fmt.Errorf("read Secret %s: %w", ref.SecretRef.Name, err)
		}
		deprecated, lerr := requireEvalDatasetLabel(sec.Labels)
		if lerr != nil {
			return nil, false, lerr
		}
		if v, ok := sec.Data[ref.SecretRef.Key]; ok {
			return v, deprecated, nil
		}
		return nil, deprecated, fmt.Errorf("%w: key %q in Secret %s", errDatasetKeyNotFound, ref.SecretRef.Key, ref.SecretRef.Name)
	default:
		log.V(1).Info("dataset ref has neither configMapRef nor secretRef")
		return nil, false, fmt.Errorf("datasetRef has neither configMapRef nor secretRef")
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
// policy: the gate thresholds, the datasetRef identity, maxCases, and the
// effective score tolerance. A change to any of these means a parked candidate's
// recorded result is stale and the candidate must be re-evaluated rather than
// promoted on the old result. Returns "" when there is no evaluation spec.
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
	// The tolerance decides which score answers count as correct, so it is part
	// of the verdict's identity. Hash the EFFECTIVE value in canonical form
	// (default applied, number formatting normalised) so "0.5", "0.50" and unset
	// all hash the same, and only a real change (e.g. "1") shifts the hash.
	tol := strconv.FormatFloat(scoreTolerance(evalSpec), 'f', -1, 64)
	// scorerVersion covers HOW we score (not just the inputs): a change to the
	// scoring/calibration/parsing/aggregation code bumps it, so a parked result
	// computed by an older build is re-evaluated after an upgrade.
	fields := []string{
		evalSpec.MinAccuracy, evalSpec.MaxAccuracyDrop, evalSpec.MaxECE, evalSpec.MaxECEIncrease,
		ref, strconv.Itoa(int(evalSpec.MaxCases)), tol, strconv.Itoa(scorerVersion),
	}
	// The macro-F1 thresholds join the policy identity ONLY when set, so a
	// DecisionModel that configures neither keeps exactly the same policyHash (and
	// therefore approvalId) as before this field existed: an operator upgrade does
	// not re-evaluate a parked candidate that never used a macro-F1 gate. Each
	// present threshold is tagged so an empty value and an absent value never
	// collide with an unrelated field.
	if evalSpec.MinMacroF1 != "" {
		fields = append(fields, "minMacroF1="+evalSpec.MinMacroF1)
	}
	if evalSpec.MaxMacroF1Drop != "" {
		fields = append(fields, "maxMacroF1Drop="+evalSpec.MaxMacroF1Drop)
	}
	payload := strings.Join(fields, "\x1f")
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

// questionEvaluations maps the bounded per-question eval summary to the status
// type, formatting the decimals. Returns nil for an empty summary.
func questionEvaluations(qs []eval.QuestionSummary) []decisionmodelv1alpha1.QuestionEvaluation {
	if len(qs) == 0 {
		return nil
	}
	out := make([]decisionmodelv1alpha1.QuestionEvaluation, 0, len(qs))
	for _, q := range qs {
		out = append(out, decisionmodelv1alpha1.QuestionEvaluation{
			ID:       truncateQuestionID(q.ID),
			Cases:    int32(q.Cases),
			Accuracy: formatDecimal(q.Accuracy),
			MacroF1:  formatDecimal(q.MacroF1),
		})
	}
	return out
}

// maxQuestionIDLen bounds a question id in status (CRD MaxLength on the field).
const maxQuestionIDLen = 63

// truncateQuestionID keeps a question id within maxQuestionIDLen. A longer id is
// cut and given a short sha256-derived suffix so two long ids that share a prefix
// do not collapse to the same status entry.
func truncateQuestionID(id string) string {
	if len(id) <= maxQuestionIDLen {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	const suffix = 9 // "-" + 8 hex
	cut := maxQuestionIDLen - suffix
	for cut > 0 && !utf8.RuneStart(id[cut]) { // never split a multi-byte rune
		cut--
	}
	return id[:cut] + "-" + hex.EncodeToString(sum[:])[:8]
}

// buildEvaluationStatus assembles the EvaluationStatus for a finished candidate
// run: the measured metrics, the echoed thresholds, the bounded per-question
// summary, and (when a baseline was computed) the baseline metrics. macroF1 /
// baselineMacroF1 are left empty when there was no classifiable question, so
// status never shows a spurious 0.0000 for a metric that could not be computed.
func buildEvaluationStatus(
	evalSpec *decisionmodelv1alpha1.EvaluationSpec,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	candRes evalResult,
	baseline *evalResult,
	policyHash, dsDigest string,
	now time.Time,
) *decisionmodelv1alpha1.EvaluationStatus {
	es := &decisionmodelv1alpha1.EvaluationStatus{
		Revision:          candidate.Hash,
		Accuracy:          formatDecimal(candRes.accuracy),
		ECE:               formatDecimal(candRes.ece),
		Brier:             formatDecimal(candRes.brier),
		Cases:             int32(candRes.total),
		FailedCases:       int32(candRes.failedCases),
		CalibratedCases:   int32(candRes.calibratedCases),
		ClassifiableCases: int32(candRes.classifiableCases),
		PolicyHash:        policyHash,
		ScorerVersion:     int32(scorerVersion),
		DatasetDigest:     dsDigest,
		ApprovalID:        approvalID(candidate.Hash, policyHash, dsDigest),
		CompletedAt:       ptrTime(metav1.NewTime(now)),
		MinAccuracy:       evalSpec.MinAccuracy,
		MaxAccuracyDrop:   evalSpec.MaxAccuracyDrop,
		MaxECE:            evalSpec.MaxECE,
		MaxECEIncrease:    evalSpec.MaxECEIncrease,
		MinMacroF1:        evalSpec.MinMacroF1,
		MaxMacroF1Drop:    evalSpec.MaxMacroF1Drop,
		Questions:         questionEvaluations(candRes.questions),
		Truncated:         candRes.questionsTruncated,
	}
	if candRes.classifiableCases > 0 {
		es.MacroF1 = formatDecimal(candRes.macroF1)
	}
	if baseline != nil {
		es.BaselineAccuracy = formatDecimal(baseline.accuracy)
		es.BaselineECE = formatDecimal(baseline.ece)
		if baseline.classifiableCases > 0 {
			es.BaselineMacroF1 = formatDecimal(baseline.macroF1)
		}
	}
	return es
}

func ptrTime(t metav1.Time) *metav1.Time { return &t }

// evalIdentityStale reports whether a parked candidate's recorded evaluation
// result no longer matches the current policy or dataset content, so it must be
// re-evaluated rather than promoted. datasetChanged is true specifically when the
// dataset bytes changed (a DatasetChanged Event/condition). A dataset that is now
// unreadable is also stale (re-evaluation will hold on it). No ConfigMap/Secret
// watch is used: this re-reads on each reconcile while parked, so the detection
// delay is at most one regate interval.
func (r *DecisionModelReconciler) evalIdentityStale(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	evalSpec *decisionmodelv1alpha1.EvaluationSpec,
) (stale, datasetChanged bool) {
	ev := dm.Status.Evaluation
	if ev == nil {
		return true, false
	}
	if ev.PolicyHash != evalPolicyHash(evalSpec) {
		return true, false
	}
	raw, _, err := r.loadDataset(ctx, dm, &evalSpec.DatasetRef)
	if err != nil {
		// Unreadable now (deleted/relabelled): the recorded result is stale.
		return true, false
	}
	if datasetDigestFull(raw) != ev.DatasetDigest {
		return true, true
	}
	return false, false
}

// datasetDigestFull returns the full sha256 (64 bare-hex chars) of the dataset
// bytes, recorded in status.evaluation.datasetDigest as part of a result's
// identity. (datasetHash returns a 16-char form used only as an in-memory
// evalKey component.)
func datasetDigestFull(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// scoreTolerance parses spec.rollout.evaluation.scoreTolerance into a level
// band, defaulting to eval.DefaultScoreTolerance when unset/invalid.
func scoreTolerance(evalSpec *decisionmodelv1alpha1.EvaluationSpec) float64 {
	if evalSpec == nil || evalSpec.ScoreTolerance == "" {
		return eval.DefaultScoreTolerance
	}
	v, err := strconv.ParseFloat(evalSpec.ScoreTolerance, 64)
	if err != nil || v < 0 {
		return eval.DefaultScoreTolerance
	}
	return v
}

// approvalID is the identity a manual approval must name: the first 12 hex of
// sha256(revisionHash + policyHash + datasetDigest). Without evaluation
// (policyHash and datasetDigest both empty) it is derived from the revision hash
// alone, so a manual-only hold still has a stable approvalID.
func approvalID(revHash, policyHash, datasetDigest string) string {
	sum := sha256.Sum256([]byte(revHash + "\x1f" + policyHash + "\x1f" + datasetDigest))
	return hex.EncodeToString(sum[:])[:12]
}
