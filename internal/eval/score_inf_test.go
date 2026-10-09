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

// Item 51 gap: NaN is already covered for MatchScore and ScoreCalibration; these
// add the ±Inf cases so a non-finite predicted value or probability can never be
// counted as correct or produce a usable calibration input.

func TestMatchScoreInfNeverMatches(t *testing.T) {
	for _, p := range []float64{math.Inf(1), math.Inf(-1)} {
		if MatchScore(p, 2, DefaultScoreTolerance) {
			t.Errorf("MatchScore(%v, 2, %v) = true, want false (Inf must never match)", p, DefaultScoreTolerance)
		}
		// Even with a huge tolerance, Inf must not match a finite level.
		if MatchScore(p, 2, 1e308) {
			t.Errorf("MatchScore(%v, 2, 1e308) = true, want false", p)
		}
	}
}

func TestScoreCalibrationRejectsInfProbability(t *testing.T) {
	for _, name := range []string{"+Inf", "-Inf"} {
		p := math.Inf(1)
		if name == "-Inf" {
			p = math.Inf(-1)
		}
		t.Run(name, func(t *testing.T) {
			_, _, ok := ScoreCalibration(map[string]float64{"0": p, "1": 0.5}, 0)
			if ok {
				t.Errorf("ScoreCalibration with an %s probability must return ok=false", name)
			}
		})
	}
}
