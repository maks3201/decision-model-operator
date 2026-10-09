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

// Item 50 gaps not covered by classification_test.go: a literal "<none>" class
// label is treated as any other opaque label (not special-cased), and a
// finiteness invariant — Accuracy and MacroF1 must never be NaN or ±Inf for any
// input, including pathological ones. A NaN/Inf would reach status.evaluation
// and corrupt the macro-F1 gate and dashboards.

// finite reports whether every metric value a scored result can expose is a
// finite number in [0,1] (accuracy and all F1/precision/recall are rates).
func assertFiniteInRange(t *testing.T, name string, v float64) {
	t.Helper()
	if math.IsNaN(v) || math.IsInf(v, 0) {
		t.Errorf("%s = %v, must be finite (never NaN/Inf)", name, v)
	}
	if v < 0 || v > 1 {
		t.Errorf("%s = %v, must be in [0,1]", name, v)
	}
}

// TestNoneClassLabelIsOrdinary pins that the literal label "<none>" is scored
// like any other class — the metrics never interpret label text, so a model that
// answers "<none>" (e.g. an abstain choice) gets ordinary precision/recall/F1.
func TestNoneClassLabelIsOrdinary(t *testing.T) {
	records := []Record{
		rec("q", "<none>", "<none>"),  // correct abstain
		rec("q", "<none>", "billing"), // abstain expected, model guessed
		rec("q", "billing", "<none>"), // billing expected, model abstained
	}
	qm := ScoreQuestion("q", records)
	if qm.Cases != 3 {
		t.Fatalf("cases = %d, want 3", qm.Cases)
	}
	cls := make(map[string]ClassMetrics, len(qm.Classes))
	for _, c := range qm.Classes {
		cls[c.Label] = c
	}
	none, ok := cls["<none>"]
	if !ok {
		t.Fatalf("<none> must be a scored class, got classes %+v", qm.Classes)
	}
	// <none>: tp1 fp1 fn1 -> P=0.5 R=0.5 F1=0.5, support 2 (expected twice).
	if !approx(none.Precision, 0.5) || !approx(none.Recall, 0.5) || !approx(none.F1, 0.5) || none.Support != 2 {
		t.Errorf("<none> metrics = %+v, want P=0.5 R=0.5 F1=0.5 support=2", none)
	}
	// accuracy = 1/3; macro-F1 over {<none>, billing}.
	assertFiniteInRange(t, "accuracy", qm.Accuracy)
	assertFiniteInRange(t, "macroF1", qm.MacroF1)
}

// TestScoreMetricsAlwaysFinite feeds a range of pathological inputs and asserts
// every exposed metric stays finite and in [0,1]. These are the inputs most
// likely to divide by zero if the zero-division convention regressed.
func TestScoreMetricsAlwaysFinite(t *testing.T) {
	cases := []struct {
		name    string
		records []Record
	}{
		{"nil", nil},
		{"empty slice", []Record{}},
		{"single record", []Record{rec("q", "a", "a")}},
		{"single wrong record", []Record{rec("q", "a", "b")}},
		{"all empty-string labels", []Record{rec("q", "", ""), rec("q", "", "")}},
		{"expected empty predicted set", []Record{rec("q", "", "x"), rec("q", "", "y")}},
		{"many classes one each", func() []Record {
			rs := make([]Record, 0, 5)
			for _, l := range []string{"a", "b", "c", "d", "e"} {
				rs = append(rs, rec("q", l, "a")) // all predicted "a"
			}
			return rs
		}()},
		{"duplicate labels across questions", []Record{
			rec("q1", "x", "x"), rec("q2", "x", "y"), rec("q2", "y", "x"),
		}},
		{"identical label whitespace variants", []Record{
			rec("q", " a", "a "), rec("q", "a ", " a"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			overall, per := Score(tc.records)
			assertFiniteInRange(t, "overall macroF1", overall)
			for _, qm := range per {
				assertFiniteInRange(t, "question accuracy", qm.Accuracy)
				assertFiniteInRange(t, "question macroF1", qm.MacroF1)
				for _, c := range qm.Classes {
					assertFiniteInRange(t, "class precision", c.Precision)
					assertFiniteInRange(t, "class recall", c.Recall)
					assertFiniteInRange(t, "class F1", c.F1)
				}
			}
			// ScoreQuestion directly, too (Score routes through it but a
			// single-question path is the common controller call).
			qm := ScoreQuestion("q", tc.records)
			assertFiniteInRange(t, "direct accuracy", qm.Accuracy)
			assertFiniteInRange(t, "direct macroF1", qm.MacroF1)
		})
	}
}
