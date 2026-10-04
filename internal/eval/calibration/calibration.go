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

// Package calibration provides pure, dependency-free calibration metrics for
// the eval-gated rollout. A model can be accurate yet mis-calibrated (e.g.
// over-confident); these metrics catch that. All functions are safe on empty
// input and clamp out-of-range/NaN confidences into [0,1].
package calibration

import "math"

// Prediction is one scored question: the probability the model assigned to the
// label it chose (choice -> probabilities[choice]; noul -> max(p, 1-p)) and
// whether that choice was correct.
type Prediction struct {
	// Confidence is in [0,1]. Values outside [0,1] or NaN are clamped by the
	// metric functions (NaN -> 0) and still counted.
	Confidence float64
	// Correct is true when the model's chosen label matched the expected one.
	Correct bool
}

// clampConfidence maps NaN to 0 and clamps into [0,1]. Out-of-range and NaN
// inputs are clamped and still counted (they are real predictions).
func clampConfidence(c float64) float64 {
	if math.IsNaN(c) {
		return 0
	}
	if c < 0 {
		return 0
	}
	if c > 1 {
		return 1
	}
	return c
}

// defaultBins is used when bins <= 0.
const defaultBins = 10

// ECE is the expected calibration error with equal-width bins over [0,1].
// bins <= 0 uses 10. Each bin's contribution is its share of samples times
// |accuracy - meanConfidence|; bins with zero samples are skipped. Empty input
// returns 0.
func ECE(preds []Prediction, bins int) float64 {
	if len(preds) == 0 {
		return 0
	}
	if bins <= 0 {
		bins = defaultBins
	}

	type bin struct {
		count   int
		sumConf float64
		correct int
	}
	buckets := make([]bin, bins)

	for _, p := range preds {
		c := clampConfidence(p.Confidence)
		// Bucket index in [0, bins-1]. c == 1 lands in the last bin.
		idx := int(c * float64(bins))
		if idx >= bins {
			idx = bins - 1
		}
		buckets[idx].count++
		buckets[idx].sumConf += c
		if p.Correct {
			buckets[idx].correct++
		}
	}

	n := float64(len(preds))
	var ece float64
	for _, b := range buckets {
		if b.count == 0 {
			continue // skip empty bins
		}
		acc := float64(b.correct) / float64(b.count)
		conf := b.sumConf / float64(b.count)
		ece += (float64(b.count) / n) * math.Abs(acc-conf)
	}
	return ece
}

// Brier is the mean squared error of Confidence (clamped) vs Correct (1/0).
// Empty input returns 0. Result is in [0,1].
func Brier(preds []Prediction) float64 {
	if len(preds) == 0 {
		return 0
	}
	var sum float64
	for _, p := range preds {
		c := clampConfidence(p.Confidence)
		outcome := 0.0
		if p.Correct {
			outcome = 1.0
		}
		d := c - outcome
		sum += d * d
	}
	return sum / float64(len(preds))
}

// FromChoice builds a Prediction from a choice answer: Confidence is the
// probability assigned to the chosen label, Correct is choice == expected.
// Returns ok=false when the chosen label has no probability entry (nothing to
// score), including a nil/empty map.
func FromChoice(probabilities map[string]float64, choice, expected string) (Prediction, bool) {
	p, present := probabilities[choice]
	if !present {
		return Prediction{}, false
	}
	return Prediction{
		Confidence: clampConfidence(p),
		Correct:    choice == expected,
	}, true
}

// FromNoul builds a Prediction from a noul answer. pYes is the model's
// probability the statement holds; the model's choice is "yes" when pYes >= 0.5.
// Confidence is the probability of the chosen side (max(pYes, 1-pYes)); Correct
// is whether the chosen side matches expected.
func FromNoul(pYes float64, expected bool) Prediction {
	p := clampConfidence(pYes)
	yes := p >= 0.5
	conf := p
	if !yes {
		conf = 1 - p
	}
	return Prediction{
		Confidence: conf,
		Correct:    yes == expected,
	}
}
