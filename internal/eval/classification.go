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

// Package eval provides pure, dependency-free classification metrics for the
// eval-gated rollout. Accuracy alone hides class imbalance (a router that never
// predicts a rare class can still score high), so these functions compute
// per-class precision/recall/F1 and macro-F1, which weight every class equally.
//
// All functions are safe on empty input. Labels are opaque strings: the caller
// maps each answer to a class label (a choice label, or "true"/"false" for a
// boolean "noul" question) and the metrics never interpret them.
//
// Zero-division convention (documented and tested): for one class,
//
//	precision = TP / (TP + FP)   -- 0 when the class is never predicted (TP+FP = 0)
//	recall    = TP / (TP + FN)   -- 0 when the class is never expected   (TP+FN = 0)
//	F1        = 2*P*R / (P + R)  -- 0 when P + R = 0
//
// This matches the common scikit-learn default (zero_division=0). Macro-F1 for a
// question is the unweighted mean of F1 over the set of classes that appear in
// that question (the union of expected and predicted labels); a question with no
// records has macro-F1 0. The overall macro-F1 is the unweighted mean of the
// per-question macro-F1 values (every question counts equally, regardless of its
// case count).
package eval

import "sort"

// Record is one scored (question, expected, predicted) triple. Expected is the
// golden class label; Predicted is the label the model chose. For a boolean
// (noul) question the caller passes "true"/"false" (two classes).
type Record struct {
	QuestionID string
	Expected   string
	Predicted  string
}

// ClassMetrics holds per-class precision, recall, F1 and support (the number of
// records whose Expected label is this class).
type ClassMetrics struct {
	Label     string
	Precision float64
	Recall    float64
	F1        float64
	Support   int
}

// QuestionMetrics holds the metrics for one question id.
type QuestionMetrics struct {
	QuestionID string
	Accuracy   float64
	MacroF1    float64
	Cases      int
	// Classes is the per-class breakdown, sorted by label for determinism.
	Classes []ClassMetrics
}

// maxSummaryQuestions caps the per-question list carried in status.
const maxSummaryQuestions = 20

// Summary is a bounded result for status: the overall macro-F1 (mean over
// questions), the total number of scored records, and a per-question list capped
// at maxSummaryQuestions. When more questions exist, the list is truncated
// (lowest question ids kept, for determinism) and Truncated is true.
type Summary struct {
	MacroF1   float64
	Cases     int
	Questions []QuestionSummary
	Truncated bool
}

// QuestionSummary is the bounded per-question entry in a Summary.
type QuestionSummary struct {
	ID       string
	Accuracy float64
	MacroF1  float64
	Cases    int
}

// safeDiv returns num/den, or 0 when den == 0 (the zero-division convention).
func safeDiv(num, den float64) float64 {
	if den == 0 {
		return 0
	}
	return num / den
}

// f1 returns the harmonic mean of precision and recall, or 0 when both are 0.
func f1(precision, recall float64) float64 {
	return safeDiv(2*precision*recall, precision+recall)
}

// ScoreQuestion computes the metrics for a single question's records (all
// records are treated as belonging to one question; QuestionID is not read).
// Classes is the union of expected and predicted labels, sorted. Empty input
// returns a zero-value QuestionMetrics (Accuracy 0, MacroF1 0, Cases 0).
func ScoreQuestion(questionID string, records []Record) QuestionMetrics {
	qm := QuestionMetrics{QuestionID: questionID, Cases: len(records)}
	if len(records) == 0 {
		return qm
	}

	// Per-class true positives, false positives, false negatives, and support.
	type counts struct{ tp, fp, fn, support int }
	byClass := make(map[string]*counts)
	get := func(label string) *counts {
		c, ok := byClass[label]
		if !ok {
			c = &counts{}
			byClass[label] = c
		}
		return c
	}

	correct := 0
	for _, r := range records {
		if r.Expected == r.Predicted {
			correct++
			c := get(r.Expected)
			c.tp++
			c.support++
			continue
		}
		// A miss: a false negative for the expected class and a false positive
		// for the predicted class.
		ec := get(r.Expected)
		ec.fn++
		ec.support++
		get(r.Predicted).fp++
	}

	qm.Accuracy = safeDiv(float64(correct), float64(len(records)))

	labels := make([]string, 0, len(byClass))
	for label := range byClass {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	var sumF1 float64
	qm.Classes = make([]ClassMetrics, 0, len(labels))
	for _, label := range labels {
		c := byClass[label]
		precision := safeDiv(float64(c.tp), float64(c.tp+c.fp))
		recall := safeDiv(float64(c.tp), float64(c.tp+c.fn))
		score := f1(precision, recall)
		qm.Classes = append(qm.Classes, ClassMetrics{
			Label:     label,
			Precision: precision,
			Recall:    recall,
			F1:        score,
			Support:   c.support,
		})
		sumF1 += score
	}
	// Macro-F1: unweighted mean of per-class F1 over the classes present.
	qm.MacroF1 = safeDiv(sumF1, float64(len(labels)))
	return qm
}

// Score computes per-question metrics for all records plus the overall macro-F1
// (the unweighted mean of the per-question macro-F1 values). The returned
// per-question slice is sorted by question id. Empty input returns a zero-value
// result. Use Summarize to get a bounded form for status.
func Score(records []Record) (overallMacroF1 float64, perQuestion []QuestionMetrics) {
	if len(records) == 0 {
		return 0, nil
	}

	byQuestion := make(map[string][]Record)
	order := make([]string, 0)
	for _, r := range records {
		if _, seen := byQuestion[r.QuestionID]; !seen {
			order = append(order, r.QuestionID)
		}
		byQuestion[r.QuestionID] = append(byQuestion[r.QuestionID], r)
	}
	sort.Strings(order)

	perQuestion = make([]QuestionMetrics, 0, len(order))
	var sumMacro float64
	for _, qid := range order {
		qm := ScoreQuestion(qid, byQuestion[qid])
		perQuestion = append(perQuestion, qm)
		sumMacro += qm.MacroF1
	}
	overallMacroF1 = safeDiv(sumMacro, float64(len(order)))
	return overallMacroF1, perQuestion
}

// Summarize scores records and returns a bounded Summary for status: the overall
// macro-F1, the total record count, and a per-question list capped at 20 entries
// (the lowest question ids, for a deterministic truncation) with Truncated set
// when more exist.
func Summarize(records []Record) Summary {
	overall, perQuestion := Score(records)
	s := Summary{MacroF1: overall, Cases: len(records)}
	if len(perQuestion) > maxSummaryQuestions {
		s.Truncated = true
		perQuestion = perQuestion[:maxSummaryQuestions]
	}
	s.Questions = make([]QuestionSummary, 0, len(perQuestion))
	for _, qm := range perQuestion {
		s.Questions = append(s.Questions, QuestionSummary{
			ID:       qm.QuestionID,
			Accuracy: qm.Accuracy,
			MacroF1:  qm.MacroF1,
			Cases:    qm.Cases,
		})
	}
	return s
}
