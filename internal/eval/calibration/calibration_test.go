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

package calibration

import (
	"math"
	"math/rand"
	"testing"
)

const eps = 1e-9

func approx(a, b float64) bool { return math.Abs(a-b) < eps }

func TestECE(t *testing.T) {
	tests := []struct {
		name  string
		preds []Prediction
		bins  int
		want  float64
	}{
		{"empty is zero", nil, 10, 0},
		{
			// Perfectly calibrated: conf 1.0 always correct, conf 0.0 always wrong.
			name: "perfectly calibrated -> 0",
			preds: []Prediction{
				{Confidence: 1, Correct: true},
				{Confidence: 1, Correct: true},
				{Confidence: 0, Correct: false},
			},
			bins: 10,
			want: 0,
		},
		{
			// Always fully confident but always wrong -> ECE = 1.
			name: "always confident and wrong -> 1",
			preds: []Prediction{
				{Confidence: 1, Correct: false},
				{Confidence: 1, Correct: false},
			},
			bins: 10,
			want: 1,
		},
		{
			// Textbook 3-bin example. bins=3 -> edges [0,1/3),[1/3,2/3),[2/3,1].
			// bin0: conf 0.1 (wrong) -> acc 0, conf 0.1, |0-0.1|=0.1, count 1
			// bin1: conf 0.5 (correct) + 0.5 (wrong) -> acc 0.5, conf 0.5, diff 0, count 2
			// bin2: conf 0.9 (correct) -> acc 1, conf 0.9, |1-0.9|=0.1, count 1
			// ECE = (1/4)*0.1 + (2/4)*0 + (1/4)*0.1 = 0.05
			name: "three-bin textbook -> 0.05",
			preds: []Prediction{
				{Confidence: 0.1, Correct: false},
				{Confidence: 0.5, Correct: true},
				{Confidence: 0.5, Correct: false},
				{Confidence: 0.9, Correct: true},
			},
			bins: 3,
			want: 0.05,
		},
		{
			// bins <= 0 defaults to 10; single sample conf 0.75 correct ->
			// bin [0.7,0.8): acc 1, conf 0.75, diff 0.25, weight 1 -> ECE 0.25.
			name: "default bins on <=0",
			preds: []Prediction{
				{Confidence: 0.75, Correct: true},
			},
			bins: 0,
			want: 0.25,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ECE(tt.preds, tt.bins)
			if !approx(got, tt.want) {
				t.Errorf("ECE = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestECEClampsOutOfRange(t *testing.T) {
	// NaN -> 0, >1 -> 1, <0 -> 0; all still counted.
	preds := []Prediction{
		{Confidence: math.NaN(), Correct: false}, // -> 0, bin0, correct? no -> acc 0 conf 0 diff 0
		{Confidence: 2.0, Correct: true},         // -> 1, last bin, acc 1 conf 1 diff 0
		{Confidence: -0.5, Correct: false},       // -> 0, bin0
	}
	// bin0 has two samples (conf 0, both wrong): acc 0, conf 0, diff 0.
	// last bin one sample conf 1 correct: diff 0. ECE = 0.
	if got := ECE(preds, 10); !approx(got, 0) {
		t.Errorf("ECE with clamped values = %v, want 0", got)
	}
}

func TestBrier(t *testing.T) {
	tests := []struct {
		name  string
		preds []Prediction
		want  float64
	}{
		{"empty is zero", nil, 0},
		{
			// (1-1)^2 + (0-0)^2 = 0 over 2 -> 0.
			name:  "perfect -> 0",
			preds: []Prediction{{Confidence: 1, Correct: true}, {Confidence: 0, Correct: false}},
			want:  0,
		},
		{
			// (1-0)^2 = 1 -> mean 1.
			name:  "confident wrong -> 1",
			preds: []Prediction{{Confidence: 1, Correct: false}},
			want:  1,
		},
		{
			// (0.5-1)^2 = 0.25 ; (0.5-0)^2 = 0.25 -> mean 0.25.
			name:  "half confidence -> 0.25",
			preds: []Prediction{{Confidence: 0.5, Correct: true}, {Confidence: 0.5, Correct: false}},
			want:  0.25,
		},
		{
			// NaN clamped to 0, correct -> (0-1)^2 = 1.
			name:  "nan clamped",
			preds: []Prediction{{Confidence: math.NaN(), Correct: true}},
			want:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Brier(tt.preds)
			if !approx(got, tt.want) {
				t.Errorf("Brier = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromChoice(t *testing.T) {
	probs := map[string]float64{"billing": 0.8, "technical": 0.2}

	p, ok := FromChoice(probs, "billing", "billing")
	if !ok || !p.Correct || !approx(p.Confidence, 0.8) {
		t.Errorf("correct choice: got %+v ok=%v", p, ok)
	}

	p, ok = FromChoice(probs, "technical", "billing")
	if !ok || p.Correct || !approx(p.Confidence, 0.2) {
		t.Errorf("wrong choice: got %+v ok=%v", p, ok)
	}

	if _, ok := FromChoice(probs, "missing", "billing"); ok {
		t.Errorf("choice absent from probabilities must return ok=false")
	}
	if _, ok := FromChoice(nil, "billing", "billing"); ok {
		t.Errorf("nil map must return ok=false")
	}

	// Out-of-range probability is clamped.
	p, ok = FromChoice(map[string]float64{"x": 1.5}, "x", "x")
	if !ok || !approx(p.Confidence, 1) {
		t.Errorf("clamp: got %+v ok=%v", p, ok)
	}
}

func TestFromNoul(t *testing.T) {
	tests := []struct {
		name     string
		pYes     float64
		expected bool
		wantConf float64
		wantOK   bool // Correct
	}{
		{"high yes, expected yes", 0.9, true, 0.9, true},
		{"high yes, expected no", 0.9, false, 0.9, false},
		{"low yes -> chooses no, expected no", 0.1, false, 0.9, true},
		{"low yes -> chooses no, expected yes", 0.1, true, 0.9, false},
		{"boundary 0.5 chooses yes", 0.5, true, 0.5, true},
		{"clamp >1", 1.4, true, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := FromNoul(tt.pYes, tt.expected)
			if !approx(p.Confidence, tt.wantConf) || p.Correct != tt.wantOK {
				t.Errorf("FromNoul(%v,%v) = %+v, want conf %v correct %v",
					tt.pYes, tt.expected, p, tt.wantConf, tt.wantOK)
			}
		})
	}
}

// TestProperties checks ECE and Brier stay in [0,1] over random inputs,
// including out-of-range confidences.
func TestProperties(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		n := r.Intn(50)
		preds := make([]Prediction, n)
		for j := range preds {
			// Deliberately include out-of-range and occasional NaN.
			c := r.Float64()*1.4 - 0.2
			if r.Intn(20) == 0 {
				c = math.NaN()
			}
			preds[j] = Prediction{Confidence: c, Correct: r.Intn(2) == 0}
		}
		bins := r.Intn(15) - 2 // sometimes <= 0
		if e := ECE(preds, bins); e < 0 || e > 1 {
			t.Fatalf("ECE out of [0,1]: %v (n=%d bins=%d)", e, n, bins)
		}
		if b := Brier(preds); b < 0 || b > 1 {
			t.Fatalf("Brier out of [0,1]: %v (n=%d)", b, n)
		}
	}
}
