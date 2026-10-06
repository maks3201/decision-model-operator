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
	"encoding/json"
	"math"
	"testing"
)

func TestMatchScore(t *testing.T) {
	tests := []struct {
		name      string
		predicted float64
		expected  int
		tolerance float64
		want      bool
	}{
		{"exact hit", 2.0, 2, DefaultScoreTolerance, true},
		{"within default band", 2.3, 2, DefaultScoreTolerance, true},
		{"just outside default band", 2.6, 2, DefaultScoreTolerance, false},
		{"lower boundary inclusive", 1.5, 2, DefaultScoreTolerance, true},
		{"upper boundary inclusive", 2.5, 2, DefaultScoreTolerance, true},
		{"halfway counts for lower neighbour", 1.5, 1, DefaultScoreTolerance, true},
		{"halfway counts for upper neighbour", 1.5, 2, DefaultScoreTolerance, true},
		{"tolerance 0 exact only", 3.0, 3, 0, true},
		{"tolerance 0 rejects near", 3.01, 3, 0, false},
		{"tolerance 1 wider band", 1.0, 2, 1, true},
		{"tolerance 1 at edge", 0.0, 2, 1, false},
		{"negative tolerance never matches", 2.0, 2, -0.1, false},
		{"NaN never matches", math.NaN(), 2, DefaultScoreTolerance, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchScore(tt.predicted, tt.expected, tt.tolerance); got != tt.want {
				t.Errorf("MatchScore(%v, %d, %v) = %v, want %v",
					tt.predicted, tt.expected, tt.tolerance, got, tt.want)
			}
		})
	}
}

func TestScoreCalibration(t *testing.T) {
	tests := []struct {
		name     string
		probs    map[string]float64
		expected int
		wantConf float64
		wantOK   bool
		wantCorr bool
	}{
		{
			name:     "argmax correct",
			probs:    map[string]float64{"0": 0.1, "1": 0.2, "2": 0.7},
			expected: 2,
			wantConf: 0.7,
			wantOK:   true,
			wantCorr: true,
		},
		{
			name:     "argmax wrong",
			probs:    map[string]float64{"0": 0.6, "1": 0.3, "2": 0.1},
			expected: 2,
			wantConf: 0.6,
			wantOK:   true,
			wantCorr: false,
		},
		{
			name:     "two levels",
			probs:    map[string]float64{"0": 0.25, "1": 0.75},
			expected: 1,
			wantConf: 0.75,
			wantOK:   true,
			wantCorr: true,
		},
		{
			name:     "tie breaks to lowest index",
			probs:    map[string]float64{"0": 0.5, "1": 0.5},
			expected: 0,
			wantConf: 0.5,
			wantOK:   true,
			wantCorr: true, // argmax -> 0 on a tie
		},
		{
			name:     "tie: higher index is not chosen",
			probs:    map[string]float64{"0": 0.5, "1": 0.5},
			expected: 1,
			wantConf: 0.5,
			wantOK:   true,
			wantCorr: false,
		},
		{
			name:     "rounded probabilities within sum tolerance",
			probs:    map[string]float64{"0": 0.3333, "1": 0.3333, "2": 0.3334},
			expected: 2,
			wantConf: 0.3334,
			wantOK:   true,
			wantCorr: true,
		},
		{"too few levels", map[string]float64{"0": 1.0}, 0, 0, false, false},
		{
			name:     "too many levels",
			probs:    buildUniform(11),
			expected: 0,
			wantOK:   false,
		},
		{
			name:     "non-contiguous keys",
			probs:    map[string]float64{"0": 0.5, "2": 0.5},
			expected: 0,
			wantOK:   false,
		},
		{
			name:     "non-integer key",
			probs:    map[string]float64{"0": 0.5, "x": 0.5},
			expected: 0,
			wantOK:   false,
		},
		{
			name:     "value out of range",
			probs:    map[string]float64{"0": 1.5, "1": -0.5},
			expected: 0,
			wantOK:   false,
		},
		{
			name:     "NaN value",
			probs:    map[string]float64{"0": math.NaN(), "1": 0.5},
			expected: 0,
			wantOK:   false,
		},
		{
			name:     "sum far from one",
			probs:    map[string]float64{"0": 0.1, "1": 0.1},
			expected: 0,
			wantOK:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, corr, ok := ScoreCalibration(tt.probs, tt.expected)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if !approx(conf, tt.wantConf) {
				t.Errorf("confidence = %v, want %v", conf, tt.wantConf)
			}
			if corr != tt.wantCorr {
				t.Errorf("correct = %v, want %v", corr, tt.wantCorr)
			}
		})
	}
}

// buildUniform returns a probability map over n contiguous levels, each 1/n.
func buildUniform(n int) map[string]float64 {
	m := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		m[itoa(i)] = 1.0 / float64(n)
	}
	return m
}

func itoa(i int) string {
	return string(rune('0' + i)) // only used for small i in tests
}

func TestParseScoreExpected(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{"integer", "2", 2, false},
		{"zero", "0", 0, false},
		{"integral float", "2.0", 2, false},
		{"large integral float", "9.0", 9, false},
		{"fraction rejected", "2.5", 0, true},
		{"negative rejected", "-1", 0, true},
		{"negative integral float rejected", "-2.0", 0, true},
		{"string rejected", `"2"`, 0, true},
		{"bool rejected", "true", 0, true},
		{"null rejected", "null", 0, true},
		{"empty rejected", "", 0, true},
		{"object rejected", "{}", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseScoreExpected(json.RawMessage(tt.raw))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseScoreExpected(%s) = %d, want error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScoreExpected(%s) error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("ParseScoreExpected(%s) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}
