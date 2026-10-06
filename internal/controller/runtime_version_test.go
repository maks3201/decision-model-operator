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
	"testing"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

func dmWith(runtimeVersion, image string) *decisionmodelv1alpha1.DecisionModel {
	return &decisionmodelv1alpha1.DecisionModel{
		Spec: decisionmodelv1alpha1.DecisionModelSpec{RuntimeVersion: runtimeVersion, Image: image},
	}
}

// TestEffectiveRuntimeVersion pins the policy table, especially that under
// Pinned an unset spec.runtimeVersion reuses the stable's recorded version so an
// operator-default bump does not change a revision.
func TestEffectiveRuntimeVersion(t *testing.T) {
	eng := newFakeEngine()
	stable := &decisionmodelv1alpha1.RevisionStatus{RuntimeVersion: "0.8.0"}
	legacyStable := &decisionmodelv1alpha1.RevisionStatus{Image: "ghcr.io/ollaya-dev/ollaya:0.9.0"}
	userImageStable := &decisionmodelv1alpha1.RevisionStatus{Image: "example.com/custom:1"}

	tests := []struct {
		name   string
		policy string
		dm     *decisionmodelv1alpha1.DecisionModel
		stable *decisionmodelv1alpha1.RevisionStatus
		want   string
	}{
		{"explicit version wins", RuntimeVersionPinned, dmWith("0.10.0", ""), stable, "0.10.0"},
		{"image override -> unknown", RuntimeVersionPinned, dmWith("", "x/y:1"), stable, ""},
		{"pinned reuses stable version", RuntimeVersionPinned, dmWith("", ""), stable, "0.8.0"},
		{"pinned derives from legacy stable image", RuntimeVersionPinned, dmWith("", ""), legacyStable, "0.9.0"},
		{"pinned cannot derive from user image", RuntimeVersionPinned, dmWith("", ""), userImageStable, ""},
		{"pinned no stable -> default (empty)", RuntimeVersionPinned, dmWith("", ""), nil, ""},
		{"follow-operator ignores stable", RuntimeVersionFollowOperator, dmWith("", ""), stable, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &DecisionModelReconciler{RuntimeVersionPolicy: tc.policy}
			if got := r.effectiveRuntimeVersion(eng, tc.dm, tc.stable); got != tc.want {
				t.Errorf("effectiveRuntimeVersion = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRuntimeUpdateAvailable(t *testing.T) {
	eng := newFakeEngine()
	def := eng.DefaultRuntimeVersion() // "0.10.0"
	tests := []struct {
		pinned string
		want   bool
	}{
		{"0.7.3", true},
		{"0.9.0", true},
		{def, false},
		{"99.0.0", false},
		{"", false}, // unknown
		{"not-a-version", false},
	}
	for _, tc := range tests {
		if got := runtimeUpdateAvailable(eng, tc.pinned); got != tc.want {
			t.Errorf("runtimeUpdateAvailable(%q) = %v, want %v", tc.pinned, got, tc.want)
		}
	}
}

// recordedRuntimeVersion derives the version from a recorded revision via the
// engine capability (image tag) or the explicit field.
func TestRecordedRuntimeVersion(t *testing.T) {
	eng := newFakeEngine()
	tests := []struct {
		name string
		rev  *decisionmodelv1alpha1.RevisionStatus
		want string
	}{
		{"explicit field", &decisionmodelv1alpha1.RevisionStatus{RuntimeVersion: "0.8.0"}, "0.8.0"},
		{"default image tag", &decisionmodelv1alpha1.RevisionStatus{Image: "ghcr.io/ollaya-dev/ollaya:0.10.0"}, "0.10.0"},
		{"cuda image tag", &decisionmodelv1alpha1.RevisionStatus{Image: "ghcr.io/ollaya-dev/ollaya:0.10.0-cuda"}, "0.10.0"},
		{"non-default tag", &decisionmodelv1alpha1.RevisionStatus{Image: "ghcr.io/ollaya-dev/ollaya:latest"}, ""},
		{"user image", &decisionmodelv1alpha1.RevisionStatus{Image: "example.com/custom:1"}, ""},
		{"nil", nil, ""},
	}
	for _, tc := range tests {
		if got := recordedRuntimeVersion(eng, tc.rev); got != tc.want {
			t.Errorf("%s: recordedRuntimeVersion = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// An engine without the RuntimeVersioner capability: a set spec.runtimeVersion
// is unsupported; everything else degrades to "no pinning".
func TestRuntimeVersionWithoutCapability(t *testing.T) {
	eng := bareEngine{}
	if runtimeVersioner(eng) != nil {
		t.Fatal("bareEngine must not expose RuntimeVersioner")
	}
	if err := validateRuntimeVersion(eng, "0.10.0"); err == nil {
		t.Error("a set runtimeVersion must be unsupported without the capability")
	}
	if err := validateRuntimeVersion(eng, ""); err != nil {
		t.Errorf("empty runtimeVersion must be allowed: %v", err)
	}
	r := &DecisionModelReconciler{RuntimeVersionPolicy: RuntimeVersionPinned}
	if got := r.effectiveRuntimeVersion(eng, dmWith("", ""),
		&decisionmodelv1alpha1.RevisionStatus{RuntimeVersion: "0.8.0"}); got != "" {
		t.Errorf("no-capability effective version = %q, want empty", got)
	}
	if runtimeUpdateAvailable(eng, "0.8.0") {
		t.Error("no-capability engine must never advertise a runtime update")
	}
}

func TestValidRuntimeVersionPolicy(t *testing.T) {
	for _, ok := range []string{RuntimeVersionPinned, RuntimeVersionFollowOperator} {
		if !ValidRuntimeVersionPolicy(ok) {
			t.Errorf("ValidRuntimeVersionPolicy(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "pinned", "follow", "x"} {
		if ValidRuntimeVersionPolicy(bad) {
			t.Errorf("ValidRuntimeVersionPolicy(%q) = true, want false", bad)
		}
	}
}

// compile-time: fakeEngine implements RuntimeVersioner; bareEngine does not.
var _ engine.RuntimeVersioner = (*fakeEngine)(nil)
