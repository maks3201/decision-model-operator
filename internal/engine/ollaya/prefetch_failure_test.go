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

package ollaya

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// TestPrefetchFailFastPolicy pins the Job's fail-fast wiring: a podFailurePolicy
// that FailJob's on the two permanent exit codes, RestartPolicy Never (required
// by the policy), and the termination-message plumbing the script writes to.
func TestPrefetchFailFastPolicy(t *testing.T) {
	js := New().PrefetchJobSpec(baseParams())

	if js.PodFailurePolicy == nil || len(js.PodFailurePolicy.Rules) != 1 {
		t.Fatalf("want exactly one podFailurePolicy rule, got %+v", js.PodFailurePolicy)
	}
	rule := js.PodFailurePolicy.Rules[0]
	if rule.Action != batchv1.PodFailurePolicyActionFailJob {
		t.Errorf("rule action = %q, want FailJob", rule.Action)
	}
	if rule.OnExitCodes == nil || rule.OnExitCodes.Operator != batchv1.PodFailurePolicyOnExitCodesOpIn {
		t.Fatalf("want OnExitCodes In, got %+v", rule.OnExitCodes)
	}
	got := map[int32]bool{}
	for _, v := range rule.OnExitCodes.Values {
		got[v] = true
	}
	if !got[prefetchExitModelNotFound] || !got[prefetchExitDigestMismatch] || !got[prefetchExitTagMoved] || !got[prefetchExitSeedUnavailable] {
		t.Errorf("fail-fast exit codes = %v, want %d, %d, %d and %d",
			rule.OnExitCodes.Values, prefetchExitModelNotFound, prefetchExitDigestMismatch, prefetchExitTagMoved, prefetchExitSeedUnavailable)
	}
	if got[prefetchExitTransient] {
		t.Errorf("transient exit %d must NOT be in the fail-fast set", prefetchExitTransient)
	}

	if js.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", js.Template.Spec.RestartPolicy)
	}
	c := js.Template.Spec.Containers[0]
	if c.TerminationMessagePath != terminationMessagePath {
		t.Errorf("terminationMessagePath = %q, want %q", c.TerminationMessagePath, terminationMessagePath)
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Errorf("terminationMessagePolicy = %q, want FallbackToLogsOnError", c.TerminationMessagePolicy)
	}
}

// TestPrefetchScriptClassifies pins that the rendered script classifies the two
// permanent failure modes with the matching reason + exit code and keeps the
// digest check.
func TestPrefetchScriptClassifies(t *testing.T) {
	script := New().PrefetchJobSpec(baseParams()).Template.Spec.Containers[0].Args[0]
	wants := []string{
		`not found in registry`,                           // 404 message match
		"fail " + PrefetchReasonModelNotFound + " " + "3", // -> exit 3
		"fail " + PrefetchReasonTransient + " " + "1",     // network -> exit 1
		`got $got want $EXPECT_DIGEST`,                    // digest check kept
		`"schemaVersion"`,                                 // well-formedness probe
		"/dev/termination-log",                            // reason recorded
		// The two permanent branches now pass a detail with both short digests;
		// the "reason: <X>" first line (what the classifier matches) is unchanged.
		"fail " + PrefetchReasonUpstreamTagMoved + " " + "5" + ` "recorded $want_short, registry now serves $got_short"`,
		"fail " + PrefetchReasonDigestMismatch + " " + "4" + ` "recorded $want_short, got $got_short"`,
		`detail: %s\n`, // fail() writes the optional detail line
	}
	for _, w := range wants {
		if !strings.Contains(script, w) {
			t.Errorf("prefetch script missing %q", w)
		}
	}
}

func TestClassifyPrefetchFailure(t *testing.T) {
	tests := []struct {
		name          string
		msg           string
		exit          int32
		wantReason    string
		wantPermanent bool
	}{
		{"digest mismatch from message", "reason: DigestMismatch\n", 4, PrefetchReasonDigestMismatch, true},
		{"upstream tag moved from message", "reason: UpstreamTagMoved\n", 5, PrefetchReasonUpstreamTagMoved, true},
		{"model not found from message", "reason: ModelNotFound\n", 3, PrefetchReasonModelNotFound, true},
		{"transient from message", "reason: Transient\n", 1, PrefetchReasonTransient, false},
		{"seed unavailable from message", "reason: SeedUnavailable\n", 6, PrefetchReasonSeedUnavailable, true},
		{"digest mismatch from exit code only", "", 4, PrefetchReasonDigestMismatch, true},
		{"upstream tag moved from exit code only", "", 5, PrefetchReasonUpstreamTagMoved, true},
		{"model not found from exit code only", "", 3, PrefetchReasonModelNotFound, true},
		{"seed unavailable from exit code only", "", 6, PrefetchReasonSeedUnavailable, true},
		{"transient from exit code only", "", 1, PrefetchReasonTransient, false},
		{"unknown exit code is transient", "", 2, PrefetchReasonTransient, false},
		{"empty is transient", "", 0, PrefetchReasonTransient, false},
		{"message wins over mismatched exit code", "reason: ModelNotFound\n", 1, PrefetchReasonModelNotFound, true},
		{"garbage message falls back to exit code", "boom\n", 4, PrefetchReasonDigestMismatch, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, permanent := classifyPrefetchFailure(tt.msg, tt.exit)
			if reason != tt.wantReason || permanent != tt.wantPermanent {
				t.Errorf("classifyPrefetchFailure(%q, %d) = (%q, %v), want (%q, %v)",
					tt.msg, tt.exit, reason, permanent, tt.wantReason, tt.wantPermanent)
			}
		})
	}
}

// TestPrefetchFailFastImageIndependentOfVersion is a guard that the policy is
// attached regardless of the resolved image/version.
func TestPrefetchFailFastImageIndependentOfVersion(t *testing.T) {
	p := baseParams()
	p.RuntimeVersion = "0.9.0"
	if New().PrefetchJobSpec(p).PodFailurePolicy == nil {
		t.Error("podFailurePolicy must be set for a pinned-version prefetch too")
	}
	_ = engine.DeviceCPU
}
