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

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
)

func f64(v float64) *float64 { return &v }

// TestScorePredictionScore covers the "score" question type: accuracy via
// MatchScore on the expected level, calibration only when a usable distribution
// is returned.
func TestScorePredictionScore(t *testing.T) {
	tests := []struct {
		name       string
		answer     engine.Answer
		expected   string
		tol        float64
		wantScored bool
		wantCorr   bool
		wantCal    bool
	}{
		{
			name:     "within tolerance, with distribution -> correct + calibrated",
			answer:   engine.Answer{Type: "score", Score: f64(2.3), Probabilities: map[string]float64{"0": 0.1, "1": 0.2, "2": 0.7}},
			expected: "2", tol: eval.DefaultScoreTolerance, wantScored: true, wantCorr: true, wantCal: true,
		},
		{
			name:     "outside tolerance -> wrong",
			answer:   engine.Answer{Type: "score", Score: f64(0.4), Probabilities: map[string]float64{"0": 0.6, "1": 0.2, "2": 0.2}},
			expected: "2", tol: eval.DefaultScoreTolerance, wantScored: true, wantCorr: false, wantCal: true,
		},
		{
			name:     "no probabilities -> accuracy only, no calibration",
			answer:   engine.Answer{Type: "score", Score: f64(2.0)},
			expected: "2", tol: eval.DefaultScoreTolerance, wantScored: true, wantCorr: true, wantCal: false,
		},
		{
			name:     "no predicted level -> wrong, no calibration",
			answer:   engine.Answer{Type: "score"},
			expected: "2", tol: eval.DefaultScoreTolerance, wantScored: true, wantCorr: false, wantCal: false,
		},
		{
			name:     "non-integer expected -> not scored",
			answer:   engine.Answer{Type: "score", Score: f64(2.0)},
			expected: "2.5", tol: eval.DefaultScoreTolerance, wantScored: false,
		},
		{
			name:     "custom tolerance widens the band",
			answer:   engine.Answer{Type: "score", Score: f64(1.0)},
			expected: "2", tol: 1.0, wantScored: true, wantCorr: true, wantCal: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := scorePrediction(tc.answer, json.RawMessage(tc.expected), tc.tol)
			if out.scored != tc.wantScored {
				t.Fatalf("scored = %v, want %v", out.scored, tc.wantScored)
			}
			if !tc.wantScored {
				return
			}
			if out.correct != tc.wantCorr {
				t.Errorf("correct = %v, want %v", out.correct, tc.wantCorr)
			}
			if out.hasCalibration != tc.wantCal {
				t.Errorf("hasCalibration = %v, want %v", out.hasCalibration, tc.wantCal)
			}
		})
	}
}

// scoreDecider answers every question with a fixed score answer.
type scoreDecider struct {
	*fakeEngine
	score float64
	probs map[string]float64
}

func (d *scoreDecider) Decide(_ context.Context, _, _ string, req engine.DecideRequest) (engine.DecideResponse, error) {
	ans := engine.Answer{Type: "score", Score: &d.score, Probabilities: d.probs}
	return engine.DecideResponse{Answers: map[string]engine.Answer{"q1": ans}}, nil
}

var _ engine.Decider = (*scoreDecider)(nil)

// TestRunEvaluationScoreQuestions proves a score question is scored (not treated
// as always-wrong) end to end through runEvaluation, with the per-case tolerance
// override taking precedence over the spec default.
func TestRunEvaluationScoreQuestions(t *testing.T) {
	dec := &scoreDecider{fakeEngine: newFakeEngine(), score: 2.2,
		probs: map[string]float64{"0": 0.1, "1": 0.2, "2": 0.7}}
	cases := []evalCase{
		{Questions: json.RawMessage(`{"q1":{"type":"score"}}`), Expected: map[string]json.RawMessage{"q1": json.RawMessage("2")}},
		{Questions: json.RawMessage(`{"q1":{"type":"score"}}`), Expected: map[string]json.RawMessage{"q1": json.RawMessage("0")}},
	}
	now := func() time.Time { return time.Unix(0, 0) }
	res := runEvaluation(context.Background(), dec, "http://x", "", "laya:en", cases, now, time.Minute, eval.DefaultScoreTolerance)
	if res.total != 2 {
		t.Fatalf("total = %d, want 2", res.total)
	}
	// 2.2 rounds to level 2 (correct for case 1), far from 0 (wrong for case 2).
	if res.accuracy != 0.5 {
		t.Errorf("accuracy = %v, want 0.5", res.accuracy)
	}

	// A per-case tolerance of 3 makes level 2.2 correct for expected 0 too.
	cases[1].Tolerance = map[string]float64{"q1": 3}
	res = runEvaluation(context.Background(), dec, "http://x", "", "laya:en", cases, now, time.Minute, eval.DefaultScoreTolerance)
	if res.accuracy != 1.0 {
		t.Errorf("accuracy with per-case tolerance = %v, want 1.0", res.accuracy)
	}
}

func TestApprovalIDStableAndDistinct(t *testing.T) {
	a := approvalID("rev1", "pol1", "ds1")
	if len(a) != 12 {
		t.Fatalf("approvalID len = %d, want 12", len(a))
	}
	if approvalID("rev1", "pol1", "ds1") != a {
		t.Error("approvalID is not stable for the same inputs")
	}
	for _, other := range [][3]string{{"rev2", "pol1", "ds1"}, {"rev1", "pol2", "ds1"}, {"rev1", "pol1", "ds2"}} {
		if approvalID(other[0], other[1], other[2]) == a {
			t.Errorf("approvalID collision for %v", other)
		}
	}
	// No-eval manual hold: derived from the revision alone, still stable.
	noEval := approvalID("rev1", "", "")
	if noEval != approvalID("rev1", "", "") {
		t.Error("no-eval approvalID is not stable")
	}
	if noEval == a {
		t.Error("no-eval approvalID must differ from the full-identity one")
	}
}

func TestScoreToleranceParsing(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"", eval.DefaultScoreTolerance},
		{"0", 0},
		{"1.5", 1.5},
		{"bad", eval.DefaultScoreTolerance},
	}
	for _, tc := range tests {
		got := scoreTolerance(&decisionmodelv1alpha1.EvaluationSpec{ScoreTolerance: tc.in})
		if got != tc.want {
			t.Errorf("scoreTolerance(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if got := scoreTolerance(nil); got != eval.DefaultScoreTolerance {
		t.Errorf("scoreTolerance(nil) = %v, want default", got)
	}
}
