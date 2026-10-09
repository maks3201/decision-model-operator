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

// Item 46-48: approvalId derives from (revision, policyHash, datasetDigest). Each
// INDIVIDUAL policy field must shift it (through evalPolicyHash); unrelated spec
// metadata must not. A changed dataset digest shifts it; the revision alone does.
func TestApprovalIDSensitivity(t *testing.T) {
	base := &decisionmodelv1alpha1.EvaluationSpec{
		DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
		MinAccuracy: "0.90",
	}
	const rev, ds = "rev-abc", "dsdigest-1"
	baseID := approvalID(rev, evalPolicyHash(base), ds)

	changers := map[string]func(*decisionmodelv1alpha1.EvaluationSpec){
		"minAccuracy":     func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MinAccuracy = "0.95" },
		"maxAccuracyDrop": func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MaxAccuracyDrop = "0.03" },
		"maxECE":          func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MaxECE = "0.10" },
		"maxECEIncrease":  func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MaxECEIncrease = "0.02" },
		"minMacroF1":      func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MinMacroF1 = "0.80" },
		"maxMacroF1Drop":  func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MaxMacroF1Drop = "0.05" },
		"maxCases":        func(s *decisionmodelv1alpha1.EvaluationSpec) { s.MaxCases = 1000 },
		"scoreTolerance":  func(s *decisionmodelv1alpha1.EvaluationSpec) { s.ScoreTolerance = "1.5" },
		"datasetRef":      func(s *decisionmodelv1alpha1.EvaluationSpec) { s.DatasetRef.ConfigMapRef.Key = "other.jsonl" },
	}
	for name, mut := range changers {
		t.Run(name+" changes approvalId", func(t *testing.T) {
			s := base.DeepCopy()
			mut(s)
			if got := approvalID(rev, evalPolicyHash(s), ds); got == baseID {
				t.Errorf("approvalId unchanged when %s changed", name)
			}
		})
	}

	// A changed dataset DIGEST (content) shifts it even with the same policy.
	if approvalID(rev, evalPolicyHash(base), "dsdigest-2") == baseID {
		t.Error("approvalId unchanged when the dataset digest changed")
	}
	// The revision alone shifts it (two revisions, same policy+dataset).
	if approvalID("rev-xyz", evalPolicyHash(base), ds) == baseID {
		t.Error("approvalId unchanged across revisions")
	}
	// Invariance: re-rendering the identical policy yields the identical id, and
	// canonicalisation means an equivalent tolerance spelling does not shift it.
	if approvalID(rev, evalPolicyHash(base.DeepCopy()), ds) != baseID {
		t.Error("approvalId not stable for an equal policy")
	}
	tolA := base.DeepCopy()
	tolA.ScoreTolerance = "0.5"
	tolB := base.DeepCopy()
	tolB.ScoreTolerance = "0.50" // same effective value, different spelling
	if approvalID(rev, evalPolicyHash(tolA), ds) != approvalID(rev, evalPolicyHash(tolB), ds) {
		t.Error("approvalId must be invariant to tolerance spelling (canonicalised)")
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
