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
	"strings"
	"testing"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// TestTruncateMessage covers the condition/Event message cap: a short message is
// unchanged, an exact-limit message is unchanged, an over-limit message is cut to
// at most the limit with the truncated suffix, and a multi-byte rune straddling
// the cut is not split.
func TestTruncateMessage(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		max      int
		wantSame bool // output must equal input unchanged
	}{
		{
			name:     "short, under the limit",
			in:       "hello",
			max:      32768,
			wantSame: true,
		},
		{
			name:     "exact limit, unchanged",
			in:       strings.Repeat("a", 32768),
			max:      32768,
			wantSame: true,
		},
		{
			name: "over the limit, cut with suffix",
			in:   strings.Repeat("a", 40000),
			max:  32768,
		},
		{
			name: "multi-byte rune straddling the cut is not split",
			// Fill right up to one byte before the budget with ASCII, then pack
			// 3-byte runes (世, U+4E16) across the boundary so the cut lands inside
			// a rune.
			in:  strings.Repeat("a", 32760) + strings.Repeat("世", 50),
			max: 32768,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := truncateMessage(tc.in, tc.max)
			if tc.wantSame {
				if out != tc.in {
					t.Fatalf("message changed; want unchanged (len in=%d out=%d)", len(tc.in), len(out))
				}
				return
			}
			if len(out) > tc.max {
				t.Fatalf("truncated length %d > cap %d", len(out), tc.max)
			}
			if !utf8.ValidString(out) {
				t.Fatalf("truncated message is not valid UTF-8 (a rune was split)")
			}
			if !strings.HasSuffix(out, truncatedSuffix) {
				t.Fatalf("truncated message missing the %q suffix", truncatedSuffix)
			}
		})
	}
}

// TestSetStatusConditionCapsMessage proves setStatusCondition caps an oversized
// runtime error string to the CRD limit before it is written, so a status update
// carrying it is not rejected by the API server (the wedge this guards against).
func TestSetStatusConditionCapsMessage(t *testing.T) {
	dm := &decisionmodelv1alpha1.DecisionModel{}
	dm.Generation = 7
	huge := strings.Repeat("x", 1<<20) // 1 MiB, the runtime body cap
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionEvaluated,
		Status:  metav1.ConditionFalse,
		Reason:  "DatasetInvalid",
		Message: huge,
	})
	if len(dm.Status.Conditions) != 1 {
		t.Fatalf("want 1 condition, got %d", len(dm.Status.Conditions))
	}
	got := dm.Status.Conditions[0]
	if len(got.Message) > maxConditionMessageBytes {
		t.Fatalf("message length %d > cap %d", len(got.Message), maxConditionMessageBytes)
	}
	if got.ObservedGeneration != 7 {
		t.Errorf("observedGeneration = %d, want 7", got.ObservedGeneration)
	}
}
