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
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
)

// Macro-F1 weights classes equally, so a majority-only predictor fails it while
// accuracy stays high. These table-driven cases pin the gate's decision for the
// absolute floor, the relative drop (with and without a baseline), and the
// no-classifiable-question fail-closed rule.
func TestEvalGateFailureMacroF1(t *testing.T) {
	cand := func(acc, f1 float64, classifiable int) evalResult {
		return evalResult{accuracy: acc, macroF1: f1, classifiableCases: classifiable}
	}
	base := func(acc, f1 float64, classifiable int) *evalResult {
		r := cand(acc, f1, classifiable)
		return &r
	}

	tests := []struct {
		name       string
		candRes    evalResult
		baseline   *evalResult
		gates      evalGates
		wantFailed bool
		wantReason string
		wantSubstr string
	}{
		{
			name:    "accuracy passes but macroF1 below floor fails",
			candRes: cand(0.90, 0.60, 20),
			gates:   evalGates{minAcc: 0.80, minMacroF1: 0.80, hasMinF1: true},
			// accuracy 0.90 >= 0.80, but macroF1 0.60 < 0.80.
			wantFailed: true,
			wantReason: reasonEvaluationFailed,
			wantSubstr: "macroF1 0.6000 < minMacroF1 0.8000",
		},
		{
			name:       "macroF1 at the floor passes",
			candRes:    cand(0.90, 0.80, 20),
			gates:      evalGates{minAcc: 0.80, minMacroF1: 0.80, hasMinF1: true},
			wantFailed: false,
		},
		{
			name:       "macroF1 drop over the limit with a baseline fails",
			candRes:    cand(0.90, 0.70, 20),
			baseline:   base(0.90, 0.80, 20),
			gates:      evalGates{maxF1Drop: 0.05, hasF1Drop: true},
			wantFailed: true,
			wantReason: reasonEvaluationFailed,
			wantSubstr: "macroF1 dropped 0.1000 (baseline 0.8000) > maxMacroF1Drop 0.0500",
		},
		{
			name:       "macroF1 drop limit without a baseline is not enforced",
			candRes:    cand(0.90, 0.10, 20),
			baseline:   nil,
			gates:      evalGates{maxF1Drop: 0.05, hasF1Drop: true},
			wantFailed: false,
		},
		{
			name:       "minMacroF1 set but no classifiable question fails closed",
			candRes:    cand(0.95, 0.0, 0),
			gates:      evalGates{minMacroF1: 0.80, hasMinF1: true},
			wantFailed: true,
			wantReason: reasonClassificationUnavailable,
			wantSubstr: "0 classifiable cases",
		},
		{
			name:       "maxMacroF1Drop set but no classifiable question fails closed",
			candRes:    cand(0.95, 0.0, 0),
			baseline:   base(0.95, 0.0, 0),
			gates:      evalGates{maxF1Drop: 0.05, hasF1Drop: true},
			wantFailed: true,
			wantReason: reasonClassificationUnavailable,
			wantSubstr: "0 classifiable cases",
		},
		{
			name:       "no macroF1 gate configured: a low macroF1 does not fail",
			candRes:    cand(0.95, 0.05, 20),
			gates:      evalGates{minAcc: 0.90},
			wantFailed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason, msg, failed := evalGateFailure(tc.candRes, tc.baseline, tc.gates)
			if failed != tc.wantFailed {
				t.Fatalf("failed=%v, want %v (reason=%q msg=%q)", failed, tc.wantFailed, reason, msg)
			}
			if !failed {
				return
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
			if tc.wantSubstr != "" && !strings.Contains(msg, tc.wantSubstr) {
				t.Errorf("msg=%q, want substring %q", msg, tc.wantSubstr)
			}
		})
	}
}

// The absolute floor is checked before the relative drop, and 0-classifiable
// is checked before both, so the message always names the most specific cause.
func TestEvalGateFailureMacroF1Ordering(t *testing.T) {
	// Both the floor and the drop would fail; the floor message wins (checked
	// first, consistent with the accuracy gates).
	candRes := evalResult{accuracy: 0.90, macroF1: 0.50, classifiableCases: 10}
	baseline := &evalResult{accuracy: 0.90, macroF1: 0.90, classifiableCases: 10}
	reason, msg, failed := evalGateFailure(candRes, baseline, evalGates{
		minMacroF1: 0.80, hasMinF1: true, maxF1Drop: 0.05, hasF1Drop: true,
	})
	if !failed || reason != reasonEvaluationFailed {
		t.Fatalf("failed=%v reason=%q, want failed with EvaluationFailed", failed, reason)
	}
	if !strings.Contains(msg, "< minMacroF1") {
		t.Errorf("expected the floor message to win, got %q", msg)
	}
}

// macroF1Summary orders the per-question list worst macro-F1 first, breaks ties
// by id, caps at macroF1SummaryLimit and reports truncation.
func TestMacroF1SummaryOrderAndTruncation(t *testing.T) {
	// Build 25 questions. Question qNN gets records such that its macro-F1 grows
	// with NN: qooo (all wrong) has F1 0, higher ids get more correct.
	var records []eval.Record
	for n := 0; n < 25; n++ {
		id := "q" + pad2(n)
		// For each question, 2 classes "a"/"b". Correct count scales with n so
		// macro-F1 is strictly increasing in n (q00 worst, q24 best).
		correct := n // 0..24 correct, plus a fixed wrong to keep both classes present
		for i := 0; i < correct; i++ {
			records = append(records, eval.Record{QuestionID: id, Expected: "a", Predicted: "a"})
		}
		// Always add one "b" expected, predicted "a" when n is small (a miss) so
		// the minority class drags macro-F1 down; predicted "b" (hit) for large n.
		pred := "a"
		if n >= 20 {
			pred = "b"
		}
		records = append(records, eval.Record{QuestionID: id, Expected: "b", Predicted: pred})
		// And one "a" expected predicted "a" so the "a" class has support even at n=0.
		records = append(records, eval.Record{QuestionID: id, Expected: "a", Predicted: "a"})
	}

	overall, questions, truncated := macroF1Summary(records)
	if overall < 0 || overall > 1 {
		t.Fatalf("overall macroF1 %v out of range", overall)
	}
	if !truncated {
		t.Fatalf("expected truncated=true for 25 questions, cap %d", macroF1SummaryLimit)
	}
	if len(questions) != macroF1SummaryLimit {
		t.Fatalf("len(questions)=%d, want %d", len(questions), macroF1SummaryLimit)
	}
	// Worst first: non-decreasing macro-F1 across the list.
	for i := 1; i < len(questions); i++ {
		if questions[i].MacroF1 < questions[i-1].MacroF1 {
			t.Errorf("not ordered worst-first at %d: %v < %v", i, questions[i].MacroF1, questions[i-1].MacroF1)
		}
	}
	// The very worst (q00) must be present; the best (q24) must be cut.
	if !hasQuestion(questions, "q00") {
		t.Errorf("worst question q00 missing from the truncated list")
	}
	if hasQuestion(questions, "q24") {
		t.Errorf("best question q24 should have been truncated out")
	}
}

// Item 74: controller-side status bounds. A huge run (5000 cases, 200 questions,
// 300-char ids) must still produce a small, bounded status.evaluation: the
// per-question list caps at macroF1SummaryLimit, every id is <= maxQuestionIDLen,
// and the serialized status stays well under a few KiB (so the API server never
// rejects the object and etcd is not bloated).
func TestEvaluationStatusStaysBoundedForHugeRun(t *testing.T) {
	longID := func(n int) string {
		return "question-" + strings.Repeat("x", 300) + "-" + pad2(n)
	}
	var records []eval.Record
	for n := 0; n < 200; n++ {
		id := longID(n)
		correct := n % 7
		for i := 0; i < correct; i++ {
			records = append(records, eval.Record{QuestionID: id, Expected: "a", Predicted: "a"})
		}
		records = append(records, eval.Record{QuestionID: id, Expected: "b", Predicted: "a"})
		records = append(records, eval.Record{QuestionID: id, Expected: "a", Predicted: "a"})
	}
	_, questions, truncated := macroF1Summary(records)
	if !truncated {
		t.Fatalf("expected truncated=true for 200 questions")
	}
	if len(questions) != macroF1SummaryLimit {
		t.Fatalf("summary not capped: %d questions", len(questions))
	}

	candRes := evalResult{
		accuracy:           0.9123,
		ece:                0.0456,
		brier:              0.0789,
		macroF1:            0.8765,
		total:              5000,
		failedCases:        3,
		calibratedCases:    5000,
		classifiableCases:  5000,
		questions:          questions,
		questionsTruncated: truncated,
	}
	evalSpec := &decisionmodelv1alpha1.EvaluationSpec{
		DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
		MinAccuracy: "0.90",
	}
	candidate := &decisionmodelv1alpha1.RevisionStatus{Hash: "abcdef0123456789"}
	es := buildEvaluationStatus(evalSpec, candidate, candRes, nil, "p0l1cyh4sh000000", "d4t4d1g3st000000", time.Unix(1_700_000_000, 0))

	// Per-question list and id lengths are bounded.
	if len(es.Questions) > macroF1SummaryLimit {
		t.Errorf("status carries %d questions, want <= %d", len(es.Questions), macroF1SummaryLimit)
	}
	for _, q := range es.Questions {
		if len(q.ID) > maxQuestionIDLen {
			t.Errorf("question id %q is %d chars, want <= %d", q.ID, len(q.ID), maxQuestionIDLen)
		}
	}
	// The whole status.evaluation marshals small regardless of dataset size.
	b, err := json.Marshal(es)
	if err != nil {
		t.Fatalf("marshal status.evaluation: %v", err)
	}
	const ceiling = 8 << 10 // 8 KiB — comfortably under the API server object limit
	if len(b) > ceiling {
		t.Errorf("status.evaluation is %d bytes, want < %d", len(b), ceiling)
	}
}

// A no-classifiable dataset yields an empty summary and 0 overall.
func TestMacroF1SummaryEmpty(t *testing.T) {
	overall, questions, truncated := macroF1Summary(nil)
	if overall != 0 || questions != nil || truncated {
		t.Fatalf("empty input: got overall=%v questions=%v truncated=%v", overall, questions, truncated)
	}
}

// expectedClass classifies a golden expected value by type: string -> choice
// label, bool -> noul class, everything else not classifiable.
func TestExpectedClass(t *testing.T) {
	tests := []struct {
		raw        string
		wantLabel  string
		wantClassC bool
	}{
		{`"billing"`, "billing", true},
		{`""`, "", true}, // an empty expected label is still a (degenerate) class
		{`true`, "true", true},
		{`false`, "false", true},
		{`2`, "", false},    // score question
		{`2.5`, "", false},  // number
		{`null`, "", false}, // null
		{`{"a":1}`, "", false},
	}
	for _, tc := range tests {
		label, ok := expectedClass([]byte(tc.raw))
		if ok != tc.wantClassC || label != tc.wantLabel {
			t.Errorf("expectedClass(%s) = (%q,%v), want (%q,%v)", tc.raw, label, ok, tc.wantLabel, tc.wantClassC)
		}
	}
}

// A missing answer for the minority class is recorded as a miss (predicted
// noClass), so macro-F1 drops below 1 — this is the core of the review blocker:
// a candidate that only ever answers the majority class must not score 1.0.
func TestMacroF1CountsMissingAnswersAsMiss(t *testing.T) {
	// q_major: answered correctly every time. q_minor: never answered (missing).
	var records []eval.Record
	for i := 0; i < 9; i++ {
		records = append(records, eval.Record{QuestionID: "q_major", Expected: "billing", Predicted: "billing"})
	}
	// The minority question's expected class is recorded with the sentinel as the
	// prediction (the model skipped it).
	records = append(records, eval.Record{QuestionID: "q_minor", Expected: "refund", Predicted: noClass})

	overall, questions, _ := macroF1Summary(records)
	if overall >= 1.0 {
		t.Fatalf("overall macro-F1 = %v, must be < 1 when a class is never answered", overall)
	}
	// q_minor's macro-F1 is 0 (both "refund" and "<none>" have F1 0); it sorts first.
	if len(questions) == 0 || questions[0].ID != "q_minor" || questions[0].MacroF1 != 0 {
		t.Fatalf("expected q_minor worst with macroF1 0, got %+v", questions)
	}
}

// truncateQuestionID keeps ids within the status bound and gives two long ids a
// distinct suffix so they do not collide.
func TestTruncateQuestionID(t *testing.T) {
	short := "q1"
	if truncateQuestionID(short) != short {
		t.Errorf("short id must be unchanged")
	}
	long := strings.Repeat("a", 200)
	tr := truncateQuestionID(long)
	if len(tr) > maxQuestionIDLen {
		t.Errorf("truncated id length %d > %d", len(tr), maxQuestionIDLen)
	}
	// Two long ids sharing the first 63 chars must not collapse.
	a := strings.Repeat("x", 100) + "A"
	b := strings.Repeat("x", 100) + "B"
	if truncateQuestionID(a) == truncateQuestionID(b) {
		t.Errorf("two distinct long ids collided: %q", truncateQuestionID(a))
	}
}

// expected/predicted class labels (what macro-F1 is computed from), and never
// marks a score answer classifiable.
func TestScorePredictionClassifiable(t *testing.T) {
	noul := func(p float64) *float64 { return &p }
	score := func(v float64) *float64 { return &v }
	tests := []struct {
		name        string
		ans         engine.Answer
		expected    string // JSON
		wantClass   bool
		wantExp     string
		wantPred    string
		wantCorrect bool
	}{
		{
			name:      "choice hit",
			ans:       engine.Answer{Type: "choice", Choice: "billing"},
			expected:  `"billing"`,
			wantClass: true, wantExp: "billing", wantPred: "billing", wantCorrect: true,
		},
		{
			name:      "choice miss keeps both class labels",
			ans:       engine.Answer{Type: "choice", Choice: "sales"},
			expected:  `"billing"`,
			wantClass: true, wantExp: "billing", wantPred: "sales", wantCorrect: false,
		},
		{
			name:      "noul yes hit",
			ans:       engine.Answer{Type: "noul", Noul: noul(0.9)},
			expected:  `true`,
			wantClass: true, wantExp: "true", wantPred: "true", wantCorrect: true,
		},
		{
			name:      "noul no predicts false",
			ans:       engine.Answer{Type: "noul", Noul: noul(0.2)},
			expected:  `true`,
			wantClass: true, wantExp: "true", wantPred: "false", wantCorrect: false,
		},
		{
			name:      "noul nil counts as the sentinel class, a miss",
			ans:       engine.Answer{Type: "noul"},
			expected:  `true`,
			wantClass: true, wantExp: "true", wantPred: noClass, wantCorrect: false,
		},
		{
			name:      "score is never classifiable",
			ans:       engine.Answer{Type: "score", Score: score(2)},
			expected:  `2`,
			wantClass: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := scorePrediction(tc.ans, []byte(tc.expected), eval.DefaultScoreTolerance)
			if out.classifiable != tc.wantClass {
				t.Fatalf("classifiable=%v, want %v", out.classifiable, tc.wantClass)
			}
			if !tc.wantClass {
				return
			}
			if out.expectedClass != tc.wantExp || out.predictedClass != tc.wantPred {
				t.Errorf("labels=(%q,%q), want (%q,%q)", out.expectedClass, out.predictedClass, tc.wantExp, tc.wantPred)
			}
			if out.correct != tc.wantCorrect {
				t.Errorf("correct=%v, want %v", out.correct, tc.wantCorrect)
			}
		})
	}
}

// The macro-F1 thresholds join the policy hash ONLY when set, so a DecisionModel
// that configures neither keeps the exact policyHash it had before this field
// existed (the value below is pinned from the 0.4.0 release). An operator upgrade
// must not re-evaluate a parked candidate that never used a macro-F1 gate.
func TestEvalPolicyHashMacroF1Compatibility(t *testing.T) {
	cmRef := &decisionmodelv1alpha1.DatasetRef{
		ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"},
	}
	// A 0.4.0-shaped spec: accuracy gate only, no macro-F1 fields.
	legacy := &decisionmodelv1alpha1.EvaluationSpec{
		DatasetRef:  *cmRef,
		MinAccuracy: "0.90",
		MaxCases:    500,
	}
	const pinned0_4_0 = "1e1178f2fc1b80cd" // the policyHash this spec produced on the 0.4.0 release

	// Adding macro-F1 fields must CHANGE the hash (they are part of identity when
	// set), but leaving them unset must NOT change it vs the legacy spec.
	withF1 := legacy.DeepCopy()
	withF1.MinMacroF1 = "0.80"

	legacyHash := evalPolicyHash(legacy)
	withF1Hash := evalPolicyHash(withF1)

	if legacyHash != pinned0_4_0 {
		t.Errorf("legacy policyHash drifted: got %q, pinned %q", legacyHash, pinned0_4_0)
	}
	if legacyHash == withF1Hash {
		t.Errorf("setting minMacroF1 must change the policyHash, both are %q", legacyHash)
	}
	// Re-copy without the F1 field hashes identically (determinism + no field
	// leakage into the empty-case payload).
	again := legacy.DeepCopy()
	if evalPolicyHash(again) != legacyHash {
		t.Errorf("policyHash not deterministic for an unchanged legacy spec")
	}
	// maxMacroF1Drop alone also shifts the hash and differs from minMacroF1 alone.
	withDrop := legacy.DeepCopy()
	withDrop.MaxMacroF1Drop = "0.05"
	if h := evalPolicyHash(withDrop); h == legacyHash || h == withF1Hash {
		t.Errorf("maxMacroF1Drop must produce a distinct hash: %q (legacy %q, minF1 %q)", h, legacyHash, withF1Hash)
	}

	t.Logf("legacy(no-F1) policyHash = %s", legacyHash)
}

func pad2(n int) string {
	s := strconv.Itoa(n)
	if len(s) < 2 {
		return "0" + s
	}
	return s
}
func hasQuestion(qs []eval.QuestionSummary, id string) bool {
	for _, q := range qs {
		if q.ID == id {
			return true
		}
	}
	return false
}
