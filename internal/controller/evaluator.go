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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
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

// scorePrediction evaluates one answer against the expected golden value and
// returns a calibration.Prediction plus whether it could be scored. Unknown
// types, missing answers, or malformed expectations count as scored-and-wrong
// with zero confidence so they penalise both accuracy and calibration.
func scorePrediction(a engine.Answer, expected json.RawMessage) (calibration.Prediction, bool) {
	switch a.Type {
	case "choice":
		var want string
		if err := json.Unmarshal(expected, &want); err != nil {
			return calibration.Prediction{}, false
		}
		if p, ok := calibration.FromChoice(a.Probabilities, a.Choice, want); ok {
			return p, true
		}
		// No probability for the chosen label: score correctness with 0 confidence.
		return calibration.Prediction{Confidence: 0, Correct: a.Choice == want}, true
	case "noul":
		var want bool
		if err := json.Unmarshal(expected, &want); err != nil {
			return calibration.Prediction{}, false
		}
		if a.Noul == nil {
			return calibration.Prediction{Confidence: 0, Correct: false}, true
		}
		return calibration.FromNoul(*a.Noul, want), true
	default:
		return calibration.Prediction{}, false
	}
}

// evalResult is the outcome of an evaluation run.
type evalResult struct {
	accuracy    float64
	ece         float64
	brier       float64
	total       int
	failedCases int
	transport   int // number of per-case transport errors (not scored as answers)
	done        bool
	err         error // non-nil on dataset/timeout/transport failure
	timedOut    bool
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
) evalResult {
	deadline := now().Add(timeout)
	var total, correct, failed, transport int
	var preds []calibration.Prediction
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
			pred, scored := scorePrediction(ans, want)
			if !scored {
				failed++
				preds = append(preds, calibration.Prediction{Confidence: 0, Correct: false})
				continue
			}
			preds = append(preds, pred)
			if pred.Correct {
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
	res := evalResult{
		accuracy:    acc,
		ece:         calibration.ECE(preds, 0),
		brier:       calibration.Brier(preds),
		total:       total,
		failedCases: failed,
		transport:   transport,
		done:        true,
	}
	// Any transport error invalidates the whole run: a partial/blip score must
	// never promote a candidate or stand in as a baseline.
	if transport > 0 {
		res.err = fmt.Errorf("%d case(s) failed with a transport error; evaluation result is not valid", transport)
	}
	return res
}

// evalKey identifies a running/finished evaluation.
type evalKey struct {
	namespace string
	name      string
	revision  string
	dataset   string // dataset content hash
	maxCases  int    // cases cap (a change re-runs)
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
