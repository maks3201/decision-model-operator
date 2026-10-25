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
	"fmt"
	"math"
	"strconv"
)

// Scoring for "score" questions. Ollaya's answer (v0.10.0 docs/api.md) is an
// expected level `score = Σ i·pᵢ` (a float, usually between integer levels),
// a `probabilities` map `{"0": p0, "1": p1, …}` over 2–10 ordered levels, and a
// `confidence`. The golden value is an integer level.

const (
	// DefaultScoreTolerance is the correctness band for a score answer: a
	// prediction counts as correct when it is within this many levels of the
	// expected integer level. 0.5 means "rounds to the expected level".
	DefaultScoreTolerance = 0.5

	// minScoreLevels / maxScoreLevels bound a valid level set (docs/api.md of
	// Ollaya v0.18.0: "score ... 2–10 levels", level 0 first).
	minScoreLevels = 2
	maxScoreLevels = 10

	// maxScoreExpected is the highest golden level that can exist: levels run
	// 0..maxScoreLevels-1. A higher expected value can never equal the argmax of a
	// valid distribution, so it is rejected up front instead of silently failing.
	maxScoreExpected = maxScoreLevels - 1

	// scoreProbSumTolerance is how far the probabilities may sum from 1.0 and
	// still be accepted. Ollaya rounds each probability to 4 decimals, so a valid
	// distribution can be off by a few 1e-4 (up to ~5e-4 over 10 levels); 1e-2 is
	// a generous band that still rejects a genuinely malformed distribution.
	scoreProbSumTolerance = 1e-2
)

// MatchScore reports whether a predicted expected-value level is correct for the
// golden integer level, within tolerance: |predicted − expected| <= tolerance.
//
// The comparison is inclusive (<=): with the default tolerance 0.5, a prediction
// that lands exactly halfway between two levels (e.g. 1.5 for expected 1 or 2) is
// counted as correct for EITHER neighbour. That is deliberate and acceptable —
// each golden case fixes one expected level, so a boundary prediction is scored
// once against that level; the inclusive band just means "nearest level, ties
// count as a hit" rather than silently dropping a halfway prediction. A negative
// tolerance never matches. A NaN predicted value never matches.
func MatchScore(predicted float64, expected int, tolerance float64) bool {
	if math.IsNaN(predicted) || tolerance < 0 {
		return false
	}
	return math.Abs(predicted-float64(expected)) <= tolerance
}

// ScoreCalibration turns a score answer's probability map into a calibration
// input: confidence = the largest level probability, correct = argmax level ==
// expected. ok is false when the map is not a usable distribution — keys are not
// exactly "0".."n-1" for some n in [2,10], a value is outside [0,1] or NaN, or
// the values do not sum to ~1 — so the caller can still count accuracy (via
// MatchScore on the expected value) but skip this case for ECE/Brier.
//
// argmax ties break to the LOWEST level index, deterministically.
func ScoreCalibration(probabilities map[string]float64, expected int) (confidence float64, correct bool, ok bool) {
	n := len(probabilities)
	if n < minScoreLevels || n > maxScoreLevels {
		return 0, false, false
	}

	// Keys must be exactly the contiguous integer levels 0..n-1 in canonical
	// decimal form. strconv.Atoi also accepts "+0" and "00", which would map two
	// keys onto the same level and make the result depend on map order.
	probs := make([]float64, n)
	for key, p := range probabilities {
		idx, err := strconv.Atoi(key)
		if err != nil || idx < 0 || idx >= n || strconv.Itoa(idx) != key {
			return 0, false, false
		}
		if math.IsNaN(p) || p < 0 || p > 1 {
			return 0, false, false
		}
		probs[idx] = p
	}

	sum := 0.0
	for _, p := range probs {
		sum += p
	}
	if math.Abs(sum-1.0) > scoreProbSumTolerance {
		return 0, false, false
	}

	// argmax, ties to the lowest index.
	best := 0
	for i := 1; i < n; i++ {
		if probs[i] > probs[best] {
			best = i
		}
	}
	return probs[best], best == expected, true
}

// ParseScoreExpected parses a golden "score" expected value: an integer level in
// [0, maxScoreExpected]. It accepts a JSON integer (2) and an integral JSON float
// (2.0), and rejects strings, negatives, non-integral fractions (2.5), and levels
// above the bound.
func ParseScoreExpected(raw json.RawMessage) (int, error) {
	// json.Unmarshal of "null" into a float is a no-op (leaves 0, no error), so
	// reject it explicitly rather than silently reading it as level 0.
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("eval: score expected is missing")
	}
	var f float64
	// encoding/json rejects NaN, Inf and number literals beyond float64's range
	// (above MaxFloat64, e.g. 1e400) here. Numbers that fit in float64 but are
	// larger than any level (1e300, 2^63) are rejected by the bound check below,
	// before the int conversion, which is implementation-defined for them.
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0, fmt.Errorf("eval: score expected must be a number: %w", err)
	}
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("eval: score expected %v is not an integer level", f)
	}
	if f < 0 {
		return 0, fmt.Errorf("eval: score expected %v is negative", f)
	}
	if f > maxScoreExpected {
		return 0, fmt.Errorf("eval: score expected %v is above the highest level %d", f, maxScoreExpected)
	}
	return int(f), nil
}
