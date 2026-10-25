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
	"math"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/eval"
)

// TestScorePredictionNonFiniteIsNotCalibrated covers a choice or noul answer whose
// probability is NaN or ±Inf. The runtime cannot send these (encoding/json rejects
// them), so this is defensive. Such an answer is scored for accuracy as before
// (choice) or as a miss (noul), counted for macro-F1, and never feeds calibration.
func TestScorePredictionNonFiniteIsNotCalibrated(t *testing.T) {
	nan := math.NaN()
	inf := math.Inf(1)
	ninf := math.Inf(-1)

	tests := []struct {
		name        string
		answer      engine.Answer
		expected    string
		wantCorrect bool
		wantClass   string // predicted class for macro-F1
	}{
		{
			name:        "choice NaN, right label: accuracy as before, no calibration",
			answer:      engine.Answer{Type: "choice", Choice: "billing", Probabilities: map[string]float64{"billing": nan}},
			expected:    `"billing"`,
			wantCorrect: true,
			wantClass:   "billing",
		},
		{
			name:        "choice +Inf, wrong label",
			answer:      engine.Answer{Type: "choice", Choice: "technical", Probabilities: map[string]float64{"technical": inf}},
			expected:    `"billing"`,
			wantCorrect: false,
			wantClass:   "technical",
		},
		{
			name:        "choice -Inf, right label",
			answer:      engine.Answer{Type: "choice", Choice: "billing", Probabilities: map[string]float64{"billing": ninf}},
			expected:    `"billing"`,
			wantCorrect: true,
			wantClass:   "billing",
		},
		{
			name:        "noul NaN is not a confident no: a miss on the sentinel class",
			answer:      engine.Answer{Type: "noul", Noul: &nan},
			expected:    `false`,
			wantCorrect: false,
			wantClass:   noClass,
		},
		{
			name:        "noul +Inf is a miss on the sentinel class",
			answer:      engine.Answer{Type: "noul", Noul: &inf},
			expected:    `true`,
			wantCorrect: false,
			wantClass:   noClass,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := scorePrediction(tt.answer, json.RawMessage(tt.expected), eval.DefaultScoreTolerance)
			if !out.scored {
				t.Fatalf("scored=false, want true (the case is answered, just unusable for calibration)")
			}
			if out.correct != tt.wantCorrect {
				t.Errorf("correct = %v, want %v", out.correct, tt.wantCorrect)
			}
			if out.hasCalibration {
				t.Errorf("hasCalibration = true, want false: a non-finite value must not feed ECE/Brier")
			}
			if !out.classifiable || out.predictedClass != tt.wantClass {
				t.Errorf("classifiable=%v predicted=%q, want classifiable=true predicted=%q",
					out.classifiable, out.predictedClass, tt.wantClass)
			}
		})
	}
}
