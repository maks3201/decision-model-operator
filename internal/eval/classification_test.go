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

package eval

import (
	"math"
	"testing"
)

const eps = 1e-9

func approx(a, b float64) bool { return math.Abs(a-b) < eps }

// rec is a terse constructor for a Record.
func rec(q, expected, predicted string) Record {
	return Record{QuestionID: q, Expected: expected, Predicted: predicted}
}

// classByLabel indexes a QuestionMetrics.Classes slice by label.
func classByLabel(qm QuestionMetrics) map[string]ClassMetrics {
	m := make(map[string]ClassMetrics, len(qm.Classes))
	for _, c := range qm.Classes {
		m[c.Label] = c
	}
	return m
}

func TestScoreQuestionHandComputed(t *testing.T) {
	// One question, 3 classes. Records (expected -> predicted):
	//   billing   -> billing     (TP billing)
	//   billing   -> billing     (TP billing)
	//   billing   -> technical   (FN billing, FP technical)
	//   technical -> technical   (TP technical)
	//   sales     -> billing     (FN sales,  FP billing)
	//
	// billing:   tp2 fp1 fn1 -> P=2/3 R=2/3 F1=2/3  support 3
	// technical: tp1 fp1 fn0 -> P=1/2 R=1   F1=2/3  support 1
	// sales:     tp0 fp0 fn1 -> P=0   R=0   F1=0    support 1
	// accuracy = 3/5 = 0.6 ; macro-F1 = (2/3 + 2/3 + 0)/3 = 4/9
	records := []Record{
		rec("dept", "billing", "billing"),
		rec("dept", "billing", "billing"),
		rec("dept", "billing", "technical"),
		rec("dept", "technical", "technical"),
		rec("dept", "sales", "billing"),
	}
	qm := ScoreQuestion("dept", records)

	if qm.Cases != 5 {
		t.Errorf("cases = %d, want 5", qm.Cases)
	}
	if !approx(qm.Accuracy, 0.6) {
		t.Errorf("accuracy = %v, want 0.6", qm.Accuracy)
	}
	if !approx(qm.MacroF1, 4.0/9.0) {
		t.Errorf("macroF1 = %v, want %v", qm.MacroF1, 4.0/9.0)
	}

	cls := classByLabel(qm)
	if len(cls) != 3 {
		t.Fatalf("classes = %d, want 3", len(cls))
	}
	wantClass := map[string]ClassMetrics{
		"billing":   {Precision: 2.0 / 3.0, Recall: 2.0 / 3.0, F1: 2.0 / 3.0, Support: 3},
		"technical": {Precision: 0.5, Recall: 1.0, F1: 2.0 / 3.0, Support: 1},
		"sales":     {Precision: 0, Recall: 0, F1: 0, Support: 1},
	}
	for label, w := range wantClass {
		g := cls[label]
		if !approx(g.Precision, w.Precision) || !approx(g.Recall, w.Recall) ||
			!approx(g.F1, w.F1) || g.Support != w.Support {
			t.Errorf("class %q = %+v, want P=%v R=%v F1=%v support=%d",
				label, g, w.Precision, w.Recall, w.F1, w.Support)
		}
	}

	// Classes must be sorted by label for determinism.
	for i := 1; i < len(qm.Classes); i++ {
		if qm.Classes[i-1].Label > qm.Classes[i].Label {
			t.Errorf("classes not sorted: %v", qm.Classes)
		}
	}
}

func TestScoreQuestionTableDriven(t *testing.T) {
	tests := []struct {
		name        string
		records     []Record
		wantAcc     float64
		wantMacroF1 float64
		// wantClass is label -> {P, R, F1, support}; only checked when non-nil.
		wantClass map[string]ClassMetrics
	}{
		{
			name:        "empty is zero",
			records:     nil,
			wantAcc:     0,
			wantMacroF1: 0,
		},
		{
			name: "perfect single class",
			records: []Record{
				rec("q", "yes", "yes"),
				rec("q", "yes", "yes"),
			},
			wantAcc:     1,
			wantMacroF1: 1,
			wantClass: map[string]ClassMetrics{
				"yes": {Precision: 1, Recall: 1, F1: 1, Support: 2},
			},
		},
		{
			name: "imbalance: majority-only predictor misses the rare class",
			// 8 billing (all correct), 2 sales (both predicted billing).
			// billing: tp8 fp2 fn0 -> P=0.8 R=1   F1=0.8889
			// sales:   tp0 fp0 fn2 -> P=0   R=0   F1=0
			// accuracy = 8/10 = 0.8 but macro-F1 = (0.8889+0)/2 = 0.4444
			records: func() []Record {
				var rs []Record
				for i := 0; i < 8; i++ {
					rs = append(rs, rec("q", "billing", "billing"))
				}
				rs = append(rs, rec("q", "sales", "billing"), rec("q", "sales", "billing"))
				return rs
			}(),
			wantAcc:     0.8,
			wantMacroF1: (2.0 * 0.8 * 1.0 / (0.8 + 1.0)) / 2.0, // (0.888..)/2
			wantClass: map[string]ClassMetrics{
				"billing": {Precision: 0.8, Recall: 1, F1: 2.0 * 0.8 * 1.0 / 1.8, Support: 8},
				"sales":   {Precision: 0, Recall: 0, F1: 0, Support: 2},
			},
		},
		{
			name: "class never predicted (zero precision denominator stays 0)",
			// expected has class b, but b is never predicted; a predicted instead.
			records: []Record{
				rec("q", "a", "a"),
				rec("q", "b", "a"),
			},
			wantAcc: 0.5,
			// a: tp1 fp1 fn0 -> P=0.5 R=1 F1=2/3 ; b: tp0 fp0 fn1 -> 0
			wantMacroF1: (2.0 / 3.0) / 2.0,
			wantClass: map[string]ClassMetrics{
				"a": {Precision: 0.5, Recall: 1, F1: 2.0 / 3.0, Support: 1},
				"b": {Precision: 0, Recall: 0, F1: 0, Support: 1},
			},
		},
		{
			name: "noul two-class, all correct",
			records: []Record{
				rec("q", "true", "true"),
				rec("q", "false", "false"),
			},
			wantAcc:     1,
			wantMacroF1: 1,
			wantClass: map[string]ClassMetrics{
				"true":  {Precision: 1, Recall: 1, F1: 1, Support: 1},
				"false": {Precision: 1, Recall: 1, F1: 1, Support: 1},
			},
		},
		{
			name: "all wrong single expected class",
			records: []Record{
				rec("q", "a", "b"),
				rec("q", "a", "c"),
			},
			wantAcc:     0,
			wantMacroF1: 0,
			wantClass: map[string]ClassMetrics{
				"a": {Precision: 0, Recall: 0, F1: 0, Support: 2},
				"b": {Precision: 0, Recall: 0, F1: 0, Support: 0},
				"c": {Precision: 0, Recall: 0, F1: 0, Support: 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qm := ScoreQuestion("q", tt.records)
			if !approx(qm.Accuracy, tt.wantAcc) {
				t.Errorf("accuracy = %v, want %v", qm.Accuracy, tt.wantAcc)
			}
			if !approx(qm.MacroF1, tt.wantMacroF1) {
				t.Errorf("macroF1 = %v, want %v", qm.MacroF1, tt.wantMacroF1)
			}
			if tt.wantClass == nil {
				return
			}
			cls := classByLabel(qm)
			if len(cls) != len(tt.wantClass) {
				t.Fatalf("classes = %d (%v), want %d", len(cls), qm.Classes, len(tt.wantClass))
			}
			for label, w := range tt.wantClass {
				g, ok := cls[label]
				if !ok {
					t.Errorf("missing class %q", label)
					continue
				}
				if !approx(g.Precision, w.Precision) || !approx(g.Recall, w.Recall) ||
					!approx(g.F1, w.F1) || g.Support != w.Support {
					t.Errorf("class %q = %+v, want P=%v R=%v F1=%v support=%d",
						label, g, w.Precision, w.Recall, w.F1, w.Support)
				}
			}
		})
	}
}

func TestScoreOverallMeanOverQuestions(t *testing.T) {
	// Two questions; overall macro-F1 is the mean of the two per-question
	// macro-F1 values, NOT weighted by case count.
	// q1: perfect single class -> macroF1 1.0 (1 case)
	// q2: imbalance a/b, a perfect, b never predicted (3 cases) -> macroF1 (2/3)/2
	records := []Record{
		rec("q1", "x", "x"),
		rec("q2", "a", "a"),
		rec("q2", "a", "a"),
		rec("q2", "b", "a"),
	}
	// q2: a tp2 fp1 fn0 -> P=2/3 R=1 F1=0.8 ; b tp0 fp0 fn1 -> 0 ; macro=(0.8)/2=0.4
	overall, per := Score(records)
	if len(per) != 2 {
		t.Fatalf("questions = %d, want 2", len(per))
	}
	// Sorted by id: q1 then q2.
	if per[0].QuestionID != "q1" || per[1].QuestionID != "q2" {
		t.Errorf("question order = %q,%q want q1,q2", per[0].QuestionID, per[1].QuestionID)
	}
	q2macro := (2.0 * (2.0 / 3.0) * 1.0 / ((2.0 / 3.0) + 1.0)) / 2.0 // (0.8)/2 = 0.4
	wantOverall := (1.0 + q2macro) / 2.0
	if !approx(overall, wantOverall) {
		t.Errorf("overall macroF1 = %v, want %v", overall, wantOverall)
	}
	// Equal-weight check: q2 has 3x the cases of q1 but counts the same.
	if !approx(per[0].MacroF1, 1.0) || !approx(per[1].MacroF1, q2macro) {
		t.Errorf("per-question macroF1 = %v,%v want 1.0,%v", per[0].MacroF1, per[1].MacroF1, q2macro)
	}
}

func TestScoreEmpty(t *testing.T) {
	overall, per := Score(nil)
	if overall != 0 || per != nil {
		t.Errorf("Score(nil) = %v,%v want 0,nil", overall, per)
	}
}
