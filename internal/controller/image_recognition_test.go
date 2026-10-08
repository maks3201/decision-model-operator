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

import "testing"

// TestSameServingImage pins the identity-recognition contract that keeps an
// operator upgrade from rolling a quiet DecisionModel: a stable recorded by an
// older release as the tag-only image is the SAME revision this build renders as
// repo:tag@sha256:<index> for the same tag. A plain string compare (what the code
// did before) treats the two as different and starts a candidate — the "same
// tag, added digest" rows below are exactly the ones that regressed, so this test
// fails against that older behaviour.
func TestSameServingImage(t *testing.T) {
	const (
		tag    = "ghcr.io/ollaya-dev/ollaya:0.10.0"
		pinned = "ghcr.io/ollaya-dev/ollaya:0.10.0@sha256:13fd0aad32f60cc2e5e7bb8007eed51194052dd75a066bf87f5dfbece4edbaa8"
		pin2   = "ghcr.io/ollaya-dev/ollaya:0.10.0@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		newTag = "ghcr.io/ollaya-dev/ollaya:0.12.0"
	)
	tests := []struct {
		name               string
		recorded, computed string
		want               bool
	}{
		{"identical tag-only", tag, tag, true},
		{"identical pinned", pinned, pinned, true},
		{"recorded tag-only, computed pinned (the upgrade case)", tag, pinned, true},
		{"recorded pinned, computed tag-only", pinned, tag, true},
		{"different tag never collapses", tag, newTag, false},
		{"different tag both pinned", pinned, newTag + "@sha256:abc", false},
		{"same tag but two different explicit digests do not collapse", pinned, pin2, false},
		{"empty never matches a real image", "", tag, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameServingImage(tt.recorded, tt.computed); got != tt.want {
				t.Errorf("sameServingImage(%q, %q) = %v, want %v", tt.recorded, tt.computed, got, tt.want)
			}
			// Guard: the pre-fix behaviour was a plain string compare. For the two
			// "added digest" rows a plain compare returns false, which proves this
			// contract is exactly what changed.
			if tt.recorded != tt.computed && (tt.name == "recorded tag-only, computed pinned (the upgrade case)" ||
				tt.name == "recorded pinned, computed tag-only") {
				if (tt.recorded == tt.computed) == tt.want {
					t.Errorf("expected the added-digest case to differ from a plain string compare")
				}
			}
		})
	}
}

// TestSplitImagePin pins the helper sameServingImage relies on.
func TestSplitImagePin(t *testing.T) {
	tests := []struct {
		in, wantTag, wantPin string
	}{
		{"repo:tag", "repo:tag", ""},
		{"repo:tag@sha256:abc", "repo:tag", "sha256:abc"},
		{"repo:tag@sha256:a@b", "repo:tag", "sha256:a@b"}, // split on the first @ only
		{"", "", ""},
	}
	for _, tt := range tests {
		gotTag, gotPin := splitImagePin(tt.in)
		if gotTag != tt.wantTag || gotPin != tt.wantPin {
			t.Errorf("splitImagePin(%q) = (%q, %q), want (%q, %q)", tt.in, gotTag, gotPin, tt.wantTag, tt.wantPin)
		}
	}
}
