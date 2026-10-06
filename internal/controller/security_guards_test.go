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
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// checkRegistryAllowed uses the engine's own parser (RegistryHoster)
// and fails closed. Mixed-case hosts must not bypass the allow-list.
func TestE2CheckRegistryAllowed(t *testing.T) {
	eng := newFakeEngine() // fake engine implements engine.RegistryHoster

	cases := []struct {
		name       string
		model      string
		allowed    []string
		insecureOK bool
		wantReason string
	}{
		{"bare tag -> default host allowed", "laya:en", nil, false, ""},
		{"default host explicit allowed", "ollaya.dev/library/laya:en", nil, false, ""},
		{"UPPER localhost denied", "LOCALHOST/ns/m:t", nil, false, reasonRegistryNotAllowed},
		{"mixed-case host+port denied", "LocalHost:5000/ns/m:t", nil, false, reasonRegistryNotAllowed},
		{"internal host denied", "Metadata.Internal:80/ns/m:t", nil, false, reasonRegistryNotAllowed},
		{"insecure default host denied", "HTTP://ollaya.dev/library/laya:en", nil, false, reasonRegistryNotAllowed},
		{"insecure allowed with flag", "HTTP://ollaya.dev/library/laya:en", nil, true, ""},
		{"localhost allowed when listed", "localhost/ns/m:t", []string{"localhost"}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &DecisionModelReconciler{AllowedRegistries: tc.allowed, AllowInsecureRegistries: tc.insecureOK}
			reason, _ := r.checkRegistryAllowed(eng, tc.model)
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// bareEngine implements engine.Engine but NOT engine.RegistryHoster (nor
// CanonicalName), so the controller cannot learn which host it targets and must
// deny.
type bareEngine struct{}

func (bareEngine) Name() string { return "bare" }
func (bareEngine) Resolve(context.Context, string) (engine.ModelRef, error) {
	return engine.ModelRef{}, nil
}
func (bareEngine) ServingPodSpec(engine.Params) corev1.PodSpec   { return corev1.PodSpec{} }
func (bareEngine) PrefetchJobSpec(engine.Params) batchv1.JobSpec { return batchv1.JobSpec{} }
func (bareEngine) ServicePort() int32                            { return 11435 }
func (bareEngine) Inspect(context.Context, string, string) ([]engine.Loaded, error) {
	return nil, nil
}
func (bareEngine) Warmup(context.Context, string, string, string) error { return nil }

// an engine WITHOUT RegistryHoster is denied (fail closed), never allowed.
func TestE2FailClosedWithoutCapability(t *testing.T) {
	r := &DecisionModelReconciler{}
	reason, _ := r.checkRegistryAllowed(bareEngine{}, "laya:en")
	if reason != reasonRegistryNotAllowed {
		t.Fatalf("engine without RegistryHoster must be denied, got reason %q", reason)
	}
}

// validateModelName rejects invalid names via the engine's CanonicalName.
func TestE4ValidateModelName(t *testing.T) {
	eng := newFakeEngine()
	if err := validateModelName(eng, "laya:en"); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}
	if err := validateModelName(eng, "--insecure:x"); err == nil {
		t.Fatal("expected an invalid name to be rejected")
	}
}

// requireAPIKeyLabel enforces the opt-in Secret label.
func TestSecretAPIKeyLabel(t *testing.T) {
	if err := requireAPIKeyLabel(map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}); err != nil {
		t.Fatalf("labelled secret rejected: %v", err)
	}
	if err := requireAPIKeyLabel(nil); err == nil {
		t.Fatal("unlabelled secret should be rejected")
	}
}

// requireEvalDatasetLabel: the eval-dataset label is canonical; the api-key
// label is accepted as deprecated; neither is rejected.
func TestRequireEvalDatasetLabel(t *testing.T) {
	tests := []struct {
		name    string
		labels  map[string]string
		wantDep bool
		wantErr bool
	}{
		{"eval-dataset label", map[string]string{decisionmodelv1alpha1.LabelEvalDataset: "true"}, false, false},
		{"deprecated api-key label", map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}, true, false},
		{"both -> canonical wins", map[string]string{
			decisionmodelv1alpha1.LabelEvalDataset: "true", decisionmodelv1alpha1.LabelAPIKey: "true"}, false, false},
		{"neither", nil, false, true},
		{"download-token is not a dataset label", map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"}, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dep, err := requireEvalDatasetLabel(tc.labels)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if dep != tc.wantDep {
				t.Errorf("deprecated = %v, want %v", dep, tc.wantDep)
			}
		})
	}
}

func TestSecretDownloadTokenLabel(t *testing.T) {
	if err := requireDownloadTokenLabel(map[string]string{decisionmodelv1alpha1.LabelDownloadToken: "true"}); err != nil {
		t.Fatalf("labelled secret rejected: %v", err)
	}
	if err := requireDownloadTokenLabel(nil); err == nil {
		t.Fatal("unlabelled secret should be rejected")
	}
}
