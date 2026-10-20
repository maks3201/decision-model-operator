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
	"encoding/json"
	"testing"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
)

// mapDecider returns a fixed set of answers for every case, keyed by question id.
// A question id absent from answers is omitted from the response (a missing
// answer), exercising the calibration path for unanswered questions.
type mapDecider struct {
	*fakeEngine
	answers map[string]engine.Answer
}

func (d *mapDecider) Decide(_ context.Context, _, _ string, _ engine.DecideRequest) (engine.DecideResponse, error) {
	out := make(map[string]engine.Answer, len(d.answers))
	for q, a := range d.answers {
		out[q] = a
	}
	return engine.DecideResponse{Answers: out}, nil
}

var _ engine.Decider = (*mapDecider)(nil)

// choiceQ builds one choice-question case expecting "yes".
func choiceQ(qid string) evalCase {
	return evalCase{
		Questions: json.RawMessage(`{"` + qid + `":{"type":"choice"}}`),
		Expected:  map[string]json.RawMessage{qid: json.RawMessage(`"yes"`)},
	}
}

// TestMissingAnswersDoNotDiluteCalibration proves the fix for the ECE-dilution
// gap: a candidate that ANSWERS some questions wrongly-with-real-confidence and
// OMITS the rest must not get a lower ECE (nor a higher calibratedCases) than one
// that answers only the questions it actually answered, with the same confidence.
// Before the fix a missing/unscorable answer was appended as a
// perfectly-calibrated (confidence 0, wrong) prediction, so each |0-0|=0 pulled
// the mixed ECE toward 0 and inflated calibratedCases — exactly the lever that
// let a candidate pass maxECEIncrease by skipping questions.
func TestMissingAnswersDoNotDiluteCalibration(t *testing.T) {
	now := func() time.Time { return time.Unix(0, 0) }
	run := func(cases []evalCase, d engine.Decider) evalResult {
		return runEvaluation(context.Background(), d, "http://x", "", "laya:en", cases, now, time.Minute, eval.DefaultScoreTolerance)
	}
	// A decider that answers q1 wrongly but with a real, high confidence (badly
	// calibrated); it never answers q2 (so q2 is a missing answer when asked).
	badlyCalibrated := &mapDecider{fakeEngine: newFakeEngine(), answers: map[string]engine.Answer{
		"q1": {Type: "choice", Choice: "no", Probabilities: map[string]float64{"no": 0.9, "yes": 0.1}},
	}}

	// Baseline run: only the answered question (q1), three cases.
	answeredOnly := []evalCase{choiceQ("q1"), choiceQ("q1"), choiceQ("q1")}
	base := run(answeredOnly, badlyCalibrated)

	// Same three q1 cases PLUS three cases that ask q2, which the decider omits.
	withMissing := append(append([]evalCase{}, answeredOnly...),
		choiceQ("q2"), choiceQ("q2"), choiceQ("q2"))
	mixed := run(withMissing, badlyCalibrated)

	// The answered q1 predictions are confidently wrong -> high ECE on both runs.
	if base.calibratedCases != 3 {
		t.Fatalf("baseline calibratedCases = %d, want 3", base.calibratedCases)
	}
	// The three missing q2 answers must add NOTHING to calibration.
	if mixed.calibratedCases != base.calibratedCases {
		t.Errorf("mixed calibratedCases = %d, want %d: missing answers inflated calibratedCases",
			mixed.calibratedCases, base.calibratedCases)
	}
	// ...and must not drag the ECE down toward 0 (the dilution the gate feared).
	if mixed.ece != base.ece {
		t.Errorf("mixed ECE = %v, baseline ECE = %v: missing answers diluted ECE", mixed.ece, base.ece)
	}
	// The missing answers still count as accuracy misses (3 right of 0, then 3
	// more misses): accuracy drops, but calibration is untouched.
	if mixed.total != 6 || mixed.failedCases != 6 {
		t.Errorf("mixed total/failed = %d/%d, want 6/6 (missing answers stay accuracy misses)",
			mixed.total, mixed.failedCases)
	}
}

// TestMissingNoulDoesNotFeedCalibration covers the scorePrediction noul path: a
// nil predicted noul value is a miss for accuracy/macro-F1 but, like a missing
// answer, must not feed calibration (hasCalibration stays false).
func TestMissingNoulDoesNotFeedCalibration(t *testing.T) {
	out := scorePrediction(engine.Answer{Type: "noul"}, json.RawMessage(`true`), eval.DefaultScoreTolerance)
	if !out.scored || out.correct {
		t.Fatalf("missing noul: scored=%v correct=%v, want scored=true correct=false", out.scored, out.correct)
	}
	if out.hasCalibration {
		t.Errorf("missing noul must not feed calibration (hasCalibration=true)")
	}
	if !out.classifiable || out.predictedClass != noClass {
		t.Errorf("missing noul: classifiable=%v predicted=%q, want classifiable=true predicted=%q",
			out.classifiable, out.predictedClass, noClass)
	}
}
