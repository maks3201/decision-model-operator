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
)

func TestRolloutStrategyDefault(t *testing.T) {
	tests := []struct {
		name string
		dm   *decisionmodelv1alpha1.DecisionModel
		want decisionmodelv1alpha1.RolloutStrategy
	}{
		{"no rollout", &decisionmodelv1alpha1.DecisionModel{}, decisionmodelv1alpha1.RolloutBlueGreen},
		{"empty rollout", &decisionmodelv1alpha1.DecisionModel{
			Spec: decisionmodelv1alpha1.DecisionModelSpec{Rollout: &decisionmodelv1alpha1.RolloutSpec{}},
		}, decisionmodelv1alpha1.RolloutBlueGreen},
		{"explicit BlueGreen", &decisionmodelv1alpha1.DecisionModel{
			Spec: decisionmodelv1alpha1.DecisionModelSpec{Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutBlueGreen}},
		}, decisionmodelv1alpha1.RolloutBlueGreen},
		{"Recreate", &decisionmodelv1alpha1.DecisionModel{
			Spec: decisionmodelv1alpha1.DecisionModelSpec{Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate}},
		}, decisionmodelv1alpha1.RolloutRecreate},
	}
	for _, tc := range tests {
		if got := rolloutStrategy(tc.dm); got != tc.want {
			t.Errorf("%s: rolloutStrategy = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// desiredReplicasForRevision renders the stable at 0 only while it is stopped for
// a Recreate rollout; every other revision (and the candidate) keeps its replicas.
func TestDesiredReplicasForRevision(t *testing.T) {
	rep := func(n int32) *int32 { return &n }
	dm := &decisionmodelv1alpha1.DecisionModel{
		Spec: decisionmodelv1alpha1.DecisionModelSpec{Replicas: rep(3)},
		Status: decisionmodelv1alpha1.DecisionModelStatus{
			StableRevision:    &decisionmodelv1alpha1.RevisionStatus{Hash: "stable1"},
			CandidateRevision: &decisionmodelv1alpha1.RevisionStatus{Hash: "cand1"},
		},
	}

	// No stop marker: both get spec.replicas.
	if got := desiredReplicasForRevision(dm, "stable1"); got != 3 {
		t.Errorf("stable without stop = %d, want 3", got)
	}
	if got := desiredReplicasForRevision(dm, "cand1"); got != 3 {
		t.Errorf("candidate = %d, want 3", got)
	}

	// Stop marker set for cand1: the stable renders at 0, the candidate still 3.
	dm.Status.StableStoppedForRevision = "cand1"
	if got := desiredReplicasForRevision(dm, "stable1"); got != 0 {
		t.Errorf("stopped stable = %d, want 0", got)
	}
	if got := desiredReplicasForRevision(dm, "cand1"); got != 3 {
		t.Errorf("candidate while stable stopped = %d, want 3", got)
	}
}

func TestRecreateBaselineUnavailable(t *testing.T) {
	dm := &decisionmodelv1alpha1.DecisionModel{}
	if recreateBaselineUnavailable(dm) {
		t.Errorf("no stop marker: baseline should be available")
	}
	dm.Status.StableStoppedForRevision = "cand1"
	if !recreateBaselineUnavailable(dm) {
		t.Errorf("stop marker set: baseline must be unavailable")
	}
}
