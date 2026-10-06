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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/jsonpath"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPrinterColumnJSONPaths verifies that every +kubebuilder:printcolumn
// JSONPath on DecisionModel resolves against a fully populated object and
// yields the expected value. The JSONPaths are copied verbatim from the markers
// in decisionmodel_types.go; this guards against a typo or a status-field rename
// silently breaking `kubectl get dm` / `-o wide`.
func TestPrinterColumnJSONPaths(t *testing.T) {
	dm := &decisionmodelv1alpha1.DecisionModel{
		ObjectMeta: metav1.ObjectMeta{Name: "dm", Namespace: "ns"},
		Status: decisionmodelv1alpha1.DecisionModelStatus{
			Phase: decisionmodelv1alpha1.PhaseRolledBack,
			StableRevision: &decisionmodelv1alpha1.RevisionStatus{
				Model: "laya:en", Digest: "c305a927cafef00d", Device: "cpu",
			},
			CandidateRevision: &decisionmodelv1alpha1.RevisionStatus{
				Model: "kev:en",
			},
			Evaluation:        &decisionmodelv1alpha1.EvaluationStatus{Accuracy: "0.9400"},
			LastPromotionTime: ptrTime(metav1.Now()),
			Replicas:          decisionmodelv1alpha1.ReplicaStatus{ModelReady: 2},
			Conditions: []metav1.Condition{
				{Type: decisionmodelv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: reasonCandidateRejected},
			},
		},
	}

	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dm)
	if err != nil {
		t.Fatalf("to unstructured: %v", err)
	}

	tests := []struct {
		column string
		path   string
		want   string
	}{
		{"Active", "{.status.stableRevision.model}", "laya:en"},
		{"Candidate", "{.status.candidateRevision.model}", "kev:en"},
		{"Accuracy", "{.status.evaluation.accuracy}", "0.9400"},
		{"Phase", "{.status.phase}", string(decisionmodelv1alpha1.PhaseRolledBack)},
		{"Reason", `{.status.conditions[?(@.type=="Ready")].reason}`, reasonCandidateRejected},
		{"Digest", "{.status.stableRevision.digest}", "c305a927cafef00d"},
		{"Device", "{.status.stableRevision.device}", "cpu"},
		{"Ready", "{.status.replicas.modelReady}", "2"},
	}

	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			jp := jsonpath.New(tc.column)
			if err := jp.Parse(tc.path); err != nil {
				t.Fatalf("parse %q: %v", tc.path, err)
			}
			var sb strings.Builder
			if err := jp.Execute(&sb, u); err != nil {
				t.Fatalf("execute %q: %v", tc.path, err)
			}
			if got := sb.String(); got != tc.want {
				t.Errorf("column %s: JSONPath %q = %q, want %q", tc.column, tc.path, got, tc.want)
			}
		})
	}

	// The Promoted (-o wide) column points at lastPromotionTime; just confirm it
	// resolves to a non-empty value (an RFC3339 timestamp).
	jp := jsonpath.New("Promoted")
	if err := jp.Parse("{.status.lastPromotionTime}"); err != nil {
		t.Fatalf("parse lastPromotionTime: %v", err)
	}
	var sb strings.Builder
	if err := jp.Execute(&sb, u); err != nil {
		t.Fatalf("execute lastPromotionTime: %v", err)
	}
	if sb.Len() == 0 {
		t.Error("lastPromotionTime JSONPath resolved to empty")
	}
}
