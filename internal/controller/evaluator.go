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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
	"github.com/maks3201/decision-model-operator/internal/eval/calibration"
)

// Evaluation limits.
const (
	// maxDatasetBytes caps the golden dataset size (1 MiB).
	maxDatasetBytes = 1 << 20
	// defaultMaxCases is used when spec.rollout.evaluation.maxCases is unset.
	defaultMaxCases = 500
	// perCaseTimeout bounds a single Decide call.
	perCaseTimeout = 30 * time.Second
	// evalDeadline is the default bound of a whole evaluation run (and of the
	// Evaluating phase); spec.rollout.timeouts.evaluating overrides it.
	evalDeadline = 10 * time.Minute
)

// evalCase is one golden dataset line.
type evalCase struct {
	State     json.RawMessage            `json:"state"`
	Questions json.RawMessage            `json:"questions"`
	Expected  map[string]json.RawMessage `json:"expected"`
	// Tolerance optionally overrides the score-question correctness band per
	// question id (levels). Absent entries fall back to the spec scoreTolerance.
	Tolerance map[string]float64 `json:"tolerance,omitempty"`
	// Line is the 1-based source line of this case in the dataset, set during
	// parsing (not from JSON). Reported when the runtime rejects the case so the
	// user can find the offending line.
	Line int `json:"-"`
}

// datasetHash returns a stable content hash for a dataset (bare hex, 16 chars).
func datasetHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

// parseDataset parses JSONL into cases, capping at maxCases (first N).
// A malformed line makes the whole dataset invalid.
func parseDataset(raw []byte, maxCases int) ([]evalCase, error) {
	if len(raw) > maxDatasetBytes {
		return nil, fmt.Errorf("dataset exceeds %d bytes", maxDatasetBytes)
	}
	var cases []evalCase
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), maxDatasetBytes)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var c evalCase
		if err := json.Unmarshal([]byte(text), &c); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if len(c.Expected) == 0 {
			return nil, fmt.Errorf("line %d: no expected answers", line)
		}
		c.Line = line
		cases = append(cases, c)
		if maxCases > 0 && len(cases) >= maxCases {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("dataset has no cases")
	}
	return cases, nil
}

// scorerVersion identifies the scoring/calibration implementation. It is part of
// the evaluation identity (evalPolicyHash -> approvalId), so an operator upgrade
// that changes HOW a result is computed re-evaluates a parked candidate instead
// of promoting it on a stale result. Bump it on any change to: scorePrediction /
// MatchScore semantics, the calibration (ECE/Brier) computation, dataset case
// parsing, or result aggregation.
const scorerVersion = 1

// scoreOutcome is the result of scoring one answer against its golden value.
type scoreOutcome struct {
	scored         bool                   // false: unknown type / malformed expectation -> wrong
	correct        bool                   // accuracy contribution
	pred           calibration.Prediction // calibration contribution (only when hasCalibration)
	hasCalibration bool                   // whether pred feeds ECE/Brier
	// classifiable is true for choice/bool (noul) answers that are scored: they
	// contribute to macro-F1. expectedClass/predictedClass are the class labels
	// (a choice label, or "true"/"false" for noul). Score answers are never
	// classifiable (there is no class to average).
	classifiable   bool
	expectedClass  string
	predictedClass string
}

// scorePrediction evaluates one answer against the expected golden value. tol is
// the score-question correctness band (levels) for this question. choice/noul
// contribute to both accuracy and calibration; a score question always counts
// toward accuracy (MatchScore on the expected value) and contributes to
// calibration only when the runtime returned a usable probability distribution.
// Unknown types or malformed expectations are scored-and-wrong with no
// calibration contribution.
func scorePrediction(a engine.Answer, expected json.RawMessage, tol float64) scoreOutcome {
	switch a.Type {
	case "choice":
		var want string
		if err := json.Unmarshal(expected, &want); err != nil {
			return scoreOutcome{scored: false}
		}
		cls := scoreOutcome{classifiable: true, expectedClass: want, predictedClass: a.Choice}
		if p, ok := calibration.FromChoice(a.Probabilities, a.Choice, want); ok {
			cls.scored, cls.correct, cls.pred, cls.hasCalibration = true, p.Correct, p, true
			return cls
		}
		cls.scored, cls.correct = true, a.Choice == want
		cls.pred, cls.hasCalibration = calibration.Prediction{Confidence: 0, Correct: a.Choice == want}, true
		return cls
	case "noul":
		var want bool
		if err := json.Unmarshal(expected, &want); err != nil {
			return scoreOutcome{scored: false}
		}
		cls := scoreOutcome{classifiable: true, expectedClass: boolClass(want)}
		if a.Noul == nil {
			// No predicted side: scored, wrong, and counted as the opposite class
			// so it is a miss for macro-F1 (never silently dropped).
			cls.scored, cls.correct = true, false
			cls.predictedClass = boolClass(!want)
			cls.pred, cls.hasCalibration = calibration.Prediction{Confidence: 0, Correct: false}, true
			return cls
		}
		p := calibration.FromNoul(*a.Noul, want)
		cls.scored, cls.correct = true, p.Correct
		cls.predictedClass = boolClass(*a.Noul >= 0.5)
		cls.pred, cls.hasCalibration = p, true
		return cls
	case "score":
		want, err := eval.ParseScoreExpected(expected)
		if err != nil {
			return scoreOutcome{scored: false}
		}
		if a.Score == nil {
			// No predicted level: scored and wrong, no calibration contribution.
			return scoreOutcome{scored: true, correct: false}
		}
		out := scoreOutcome{scored: true, correct: eval.MatchScore(*a.Score, want, tol)}
		// Calibration only when the runtime returned a usable distribution.
		if conf, calOK, ok := eval.ScoreCalibration(a.Probabilities, want); ok {
			out.pred = calibration.Prediction{Confidence: conf, Correct: calOK}
			out.hasCalibration = true
		}
		return out
	default:
		return scoreOutcome{scored: false}
	}
}

// boolClass maps a bool to its class label for macro-F1 over noul questions.
func boolClass(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// evalResult is the outcome of an evaluation run.
type evalResult struct {
	accuracy        float64
	ece             float64
	brier           float64
	macroF1         float64
	total           int
	failedCases     int
	calibratedCases int // scored questions that fed ECE/Brier (len(preds))
	// classifiableCases is the number of scored choice/bool records that fed
	// macro-F1. 0 when the dataset has no choice/bool question: a macro-F1 gate
	// then fails rather than passing on a macro-F1 of 0.
	classifiableCases int
	// questions is the bounded per-question summary (choice/bool only), worst
	// macro-F1 first, capped by macroF1Summary.
	questions          []eval.QuestionSummary
	questionsTruncated bool
	transport          int // number of per-case transport errors (not scored as answers)
	done               bool
	err                error // non-nil on dataset/timeout/transport/rejected failure
	timedOut           bool
	// rejected is set when the runtime rejected a case as invalid
	// (engine.ErrRequestRejected): the golden dataset itself is broken. The run
	// stops at once (no retry); rejectedLine/rejectedDetail name the offending
	// case and the runtime's error for the Evaluated=False/DatasetInvalid message.
	rejected       bool
	rejectedLine   int
	rejectedDetail string
}

// runEvaluation scores every case sequentially against a single Pod baseURL.
// It respects per-case and overall deadlines via ctx.
func runEvaluation(
	ctx context.Context,
	dec engine.Decider,
	baseURL, apiKey, model string,
	cases []evalCase,
	now func() time.Time,
	timeout time.Duration,
	scoreTolerance float64,
) evalResult {
	deadline := now().Add(timeout)
	var total, correct, failed, transport int
	var preds []calibration.Prediction
	var records []eval.Record
	for _, c := range cases {
		if now().After(deadline) {
			return evalResult{done: true, timedOut: true, err: fmt.Errorf("evaluation deadline exceeded")}
		}
		caseCtx, cancel := context.WithTimeout(ctx, perCaseTimeout)
		resp, err := dec.Decide(caseCtx, baseURL, apiKey, engine.DecideRequest{
			Model:     model,
			State:     c.State,
			Questions: c.Questions,
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return evalResult{done: true, timedOut: true, err: ctx.Err()}
			}
			// A rejected case is the dataset's fault, not the transport's: the
			// runtime says this case is invalid (bad schema, unknown question
			// type, …). Retrying cannot help, and scoring it would silently change
			// accuracy. Stop at once and report which line and why; the caller
			// holds the candidate in Evaluating until the dataset is fixed (a
			// dataset change re-runs it).
			if errors.Is(err, engine.ErrRequestRejected) {
				return evalResult{
					done:           true,
					rejected:       true,
					rejectedLine:   c.Line,
					rejectedDetail: err.Error(),
					err:            fmt.Errorf("case on line %d rejected by the runtime: %v", c.Line, err),
				}
			}
			// A transport error is NOT an answer: scoring it as a wrong, zero-
			// confidence prediction would silently drive accuracy/calibration down
			// and let a network blip pass a lenient gate or poison a baseline.
			// Count it separately; a run with any transport error is not a valid
			// score (handled by the caller: candidate rolls back, baseline is
			// unavailable — never cached).
			transport++
			continue
		}
		for qid, want := range c.Expected {
			total++
			ans, ok := resp.Answers[qid]
			if !ok {
				failed++
				preds = append(preds, calibration.Prediction{Confidence: 0, Correct: false})
				continue
			}
			tol := scoreTolerance
			if c.Tolerance != nil {
				if t, ok := c.Tolerance[qid]; ok {
					tol = t
				}
			}
			out := scorePrediction(ans, want, tol)
			if !out.scored {
				failed++
				preds = append(preds, calibration.Prediction{Confidence: 0, Correct: false})
				continue
			}
			if out.hasCalibration {
				preds = append(preds, out.pred)
			}
			if out.classifiable {
				records = append(records, eval.Record{
					QuestionID: qid,
					Expected:   out.expectedClass,
					Predicted:  out.predictedClass,
				})
			}
			if out.correct {
				correct++
			} else {
				failed++
			}
		}
	}
	acc := 0.0
	if total > 0 {
		acc = float64(correct) / float64(total)
	}
	macroF1, questions, truncated := macroF1Summary(records)
	res := evalResult{
		accuracy:           acc,
		ece:                calibration.ECE(preds, 0),
		brier:              calibration.Brier(preds),
		macroF1:            macroF1,
		total:              total,
		failedCases:        failed,
		calibratedCases:    len(preds),
		classifiableCases:  len(records),
		questions:          questions,
		questionsTruncated: truncated,
		transport:          transport,
		done:               true,
	}
	// Any transport error invalidates the whole run: a partial/blip score must
	// never promote a candidate or stand in as a baseline.
	if transport > 0 {
		res.err = fmt.Errorf("%d case(s) failed with a transport error; evaluation result is not valid", transport)
	}
	return res
}

// macroF1SummaryLimit caps the per-question list carried in evalResult/status.
const macroF1SummaryLimit = 20

// macroF1Summary computes the overall macro-F1 over the classifiable records and
// a bounded per-question list ordered worst macro-F1 first (ties by question id).
// The list is capped at macroF1SummaryLimit; truncated is true when more
// questions existed than are returned. Empty input returns 0, nil, false.
func macroF1Summary(records []eval.Record) (overall float64, questions []eval.QuestionSummary, truncated bool) {
	if len(records) == 0 {
		return 0, nil, false
	}
	overallF1, perQuestion := eval.Score(records)
	all := make([]eval.QuestionSummary, 0, len(perQuestion))
	for _, qm := range perQuestion {
		all = append(all, eval.QuestionSummary{
			ID:       qm.QuestionID,
			Accuracy: qm.Accuracy,
			MacroF1:  qm.MacroF1,
			Cases:    qm.Cases,
		})
	}
	// Worst macro-F1 first; ties broken by question id for determinism.
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].MacroF1 != all[j].MacroF1 {
			return all[i].MacroF1 < all[j].MacroF1
		}
		return all[i].ID < all[j].ID
	})
	if len(all) > macroF1SummaryLimit {
		all = all[:macroF1SummaryLimit]
		truncated = true
	}
	return overallF1, all, truncated
}

// evalKey identifies a running/finished evaluation.
type evalKey struct {
	namespace string
	name      string
	revision  string
	dataset   string  // dataset content hash
	maxCases  int     // cases cap (a change re-runs)
	tolerance float64 // effective score tolerance (a change re-scores -> re-runs)
}

// evalStore holds evaluation results keyed by evalKey, guarded by a mutex.
// Each entry tracks a cancel func so a rev change / DM deletion can stop it.
type evalStore struct {
	mu      sync.Mutex
	results map[evalKey]*evalEntry
}

type evalEntry struct {
	result evalResult
	cancel context.CancelFunc
}

func newEvalStore() *evalStore {
	return &evalStore{results: make(map[evalKey]*evalEntry)}
}

// get returns a snapshot (copy) of the entry for a key, if present. Returning a
// copy means callers never read e.result while the background finish writes it
// under the lock.
func (s *evalStore) get(k evalKey) (evalEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.results[k]
	if !ok {
		return evalEntry{}, false
	}
	return *e, true
}

// forget drops a single entry (cancelling it if still running) so the next
// reconcile re-runs it. Used to retry a run invalidated by a transport blip.
func (s *evalStore) forget(k evalKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.results[k]; ok {
		if e.cancel != nil {
			e.cancel()
		}
		delete(s.results, k)
	}
}

// start registers a running evaluation with its cancel func.
func (s *evalStore) start(k evalKey, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[k] = &evalEntry{cancel: cancel}
}

// finish records a result for a key. If the key was already forgotten (e.g. the
// evaluation was cancelled on a rev change), the result is dropped.
func (s *evalStore) finish(k evalKey, r evalResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.results[k]; ok {
		e.result = r
		e.cancel = nil
	}
}

// forgetMismatched cancels and drops any entry for the same
// (namespace,name,revision) as cur whose other identity fields
// (dataset/maxCases/tolerance) differ from cur — i.e. a run started for a
// now-superseded eval identity of the SAME revision (e.g. an in-place dataset
// edit or a tolerance change while Evaluating). forgetExcept only keys on
// revision, so without this the old-dataset goroutine for the current revision
// keeps sending requests until the revision changes.
func (s *evalStore) forgetMismatched(cur evalKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.results {
		if k.namespace != cur.namespace || k.name != cur.name || k.revision != cur.revision {
			continue
		}
		if k == cur {
			continue // the current identity: keep it
		}
		if e.cancel != nil {
			e.cancel()
		}
		delete(s.results, k)
	}
}

// forgetExcept cancels and drops any entries for (namespace,name) whose revision
// is not in keepRevs. Used to cancel stale evaluations on a rev change or
// deletion. Passing no (or empty) revisions drops everything for the object.
func (s *evalStore) forgetExcept(namespace, name string, keepRevs ...string) {
	keep := make(map[string]struct{}, len(keepRevs))
	for _, r := range keepRevs {
		if r != "" {
			keep[r] = struct{}{}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.results {
		if k.namespace != namespace || k.name != name {
			continue
		}
		if _, ok := keep[k.revision]; ok {
			continue
		}
		if e.cancel != nil {
			e.cancel()
		}
		delete(s.results, k)
	}
}
