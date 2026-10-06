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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// ptrInt32 is a small helper for building *int32 spec fields in tests.
func ptrInt32(v int32) *int32 { return &v }

func baseSpec() decisionmodelv1alpha1.DecisionModelSpec {
	return decisionmodelv1alpha1.DecisionModelSpec{
		Engine:   "ollaya",
		Model:    "laya:en",
		Device:   "cpu",
		Replicas: ptrInt32(1),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("3500Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
	}
}

// TestRevisionHash verifies two properties:
//   - revision inputs (engine, model, digest, device, image, resources and, since
//     scheduling) change the hash;
//   - everything else (replicas, cache, auth) does NOT change the hash.
func TestRevisionHash(t *testing.T) {
	const (
		digest = "ab00000000000000000000000000000000000000000000000000000000000000"
		image  = "ghcr.io/ollaya-dev/ollaya:0.10.0"
	)

	baseline := RevisionHash(baseSpec(), digest, image)

	// The hash must be a stable 10-char hex string.
	if len(baseline) != 10 {
		t.Fatalf("expected 10-char hash, got %q (len %d)", baseline, len(baseline))
	}
	for _, r := range baseline {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			t.Fatalf("hash %q contains non-hex char %q", baseline, r)
		}
	}

	// Determinism: same inputs -> same hash.
	if again := RevisionHash(baseSpec(), digest, image); again != baseline {
		t.Fatalf("hash not deterministic: %q != %q", again, baseline)
	}

	t.Run("model-affecting fields change the hash", func(t *testing.T) {
		cases := []struct {
			name          string
			spec          func() decisionmodelv1alpha1.DecisionModelSpec
			digest, image string
		}{
			{
				name:   "engine",
				spec:   func() decisionmodelv1alpha1.DecisionModelSpec { s := baseSpec(); s.Engine = "laya"; return s },
				digest: digest, image: image,
			},
			{
				name:   "model",
				spec:   func() decisionmodelv1alpha1.DecisionModelSpec { s := baseSpec(); s.Model = "kev:en"; return s },
				digest: digest, image: image,
			},
			{
				name:   "device",
				spec:   func() decisionmodelv1alpha1.DecisionModelSpec { s := baseSpec(); s.Device = "cuda"; return s },
				digest: digest, image: image,
			},
			{
				name: "resources",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					s.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("2000Mi")
					return s
				},
				digest: digest, image: image,
			},
			{
				name: "scheduling.nodeSelector",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					s.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{NodeSelector: map[string]string{"disktype": "ssd"}}
					return s
				},
				digest: digest, image: image,
			},
			{
				name: "scheduling.tolerations",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					s.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{
						Tolerations: []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists}}}
					return s
				},
				digest: digest, image: image,
			},
			{
				name: "scheduling.affinity",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					s.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}}}}}}}
					return s
				},
				digest: digest, image: image,
			},
			{
				name: "scheduling.runtimeClassName",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					rc := "nvidia"
					s.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{RuntimeClassName: &rc}
					return s
				},
				digest: digest, image: image,
			},
			{
				name:   "resolvedDigest",
				spec:   baseSpec,
				digest: "cd00000000000000000000000000000000000000000000000000000000000000",
				image:  image,
			},
			{
				name:   "image",
				spec:   baseSpec,
				digest: digest,
				image:  "ghcr.io/ollaya-dev/ollaya:cuda",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := RevisionHash(tc.spec(), tc.digest, tc.image)
				if got == baseline {
					t.Errorf("expected hash to change when %s changes, still %q", tc.name, got)
				}
			})
		}
	})

	t.Run("non-model fields do not change the hash", func(t *testing.T) {
		cases := []struct {
			name string
			spec func() decisionmodelv1alpha1.DecisionModelSpec
		}{
			{
				name: "replicas",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec { s := baseSpec(); s.Replicas = ptrInt32(5); return s },
			},
			{
				name: "cache",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					sc := "fast"
					q := resource.MustParse("50Gi")
					s.Cache = &decisionmodelv1alpha1.CacheSpec{Size: q, StorageClassName: &sc}
					return s
				},
			},
			{
				name: "auth",
				spec: func() decisionmodelv1alpha1.DecisionModelSpec {
					s := baseSpec()
					s.Auth = &decisionmodelv1alpha1.AuthSpec{
						APIKeySecretRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "key"},
							Key:                  "token",
						},
					}
					return s
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := RevisionHash(tc.spec(), digest, image)
				if got != baseline {
					t.Errorf("expected hash unchanged when %s changes, got %q want %q", tc.name, got, baseline)
				}
			})
		}
	})
}

// "No scheduling" must hash exactly as before placement became a revision input,
// otherwise an operator upgrade would give every scheduling-less DecisionModel a
// new revision (upgrade safety). The expectation is built from an
// independent copy of the previous inputs struct, not from the production one.
func TestRevisionHashNoSchedulingIsBackwardCompatible(t *testing.T) {
	type previousInputs struct {
		Engine    string                      `json:"engine"`
		Model     string                      `json:"model"`
		Digest    string                      `json:"digest"`
		Device    string                      `json:"device"`
		Image     string                      `json:"image"`
		Resources corev1.ResourceRequirements `json:"resources"`
	}
	const (
		digest = "ab00000000000000000000000000000000000000000000000000000000000000"
		image  = "ghcr.io/ollaya-dev/ollaya:0.10.0"
	)
	previous := func(s decisionmodelv1alpha1.DecisionModelSpec) string {
		b, err := json.Marshal(previousInputs{s.Engine, s.Model, digest, s.Device, image, s.Resources})
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])[:10]
	}
	withEmptyScheduling := baseSpec()
	withEmptyScheduling.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{
		NodeSelector: map[string]string{}, Tolerations: []corev1.Toleration{}}
	for name, spec := range map[string]decisionmodelv1alpha1.DecisionModelSpec{
		"nil scheduling":   baseSpec(),
		"empty scheduling": withEmptyScheduling,
	} {
		if got, want := RevisionHash(spec, digest, image), previous(spec); got != want {
			t.Errorf("%s: hash = %s, previous formula gives %s", name, got, want)
		}
	}
}

func TestPlacementRecordAndAdoption(t *testing.T) {
	rc := "nvidia"
	sched := &decisionmodelv1alpha1.SchedulingSpec{RuntimeClassName: &rc}
	if got := placementRecord(nil); got != placementNone {
		t.Errorf("placementRecord(nil) = %q, want %q", got, placementNone)
	}
	if got := placementRecord(&decisionmodelv1alpha1.SchedulingSpec{}); got != placementNone {
		t.Errorf("empty scheduling = %q, want %q", got, placementNone)
	}
	if h := placementRecord(sched); h == placementNone || h == "" {
		t.Errorf("non-empty scheduling must record a hash, got %q", h)
	}

	cur := &decisionmodelv1alpha1.RevisionStatus{
		Hash: "newhash", Engine: "ollaya", Model: "laya:en", Digest: "d", Device: "cpu",
		Image: "img", Placement: placementRecord(sched),
	}
	tests := []struct {
		name       string
		stable     *decisionmodelv1alpha1.RevisionStatus
		legacyHash string
		wantAdopt  bool
	}{
		{"nil stable", nil, "old", false},
		{"legacy stable (no record) matching the legacy hash", &decisionmodelv1alpha1.RevisionStatus{Hash: "old"}, "old", true},
		{"legacy stable that no longer matches the spec", &decisionmodelv1alpha1.RevisionStatus{Hash: "other"}, "old", false},
		{"modern stable with no scheduling is NOT adopted (would hide a scheduling change)",
			&decisionmodelv1alpha1.RevisionStatus{Hash: "old", Placement: placementNone}, "old", false},
		{"modern stable with scheduling is left alone",
			&decisionmodelv1alpha1.RevisionStatus{Hash: "old", Placement: "abc"}, "old", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before string
			if tt.stable != nil {
				before = tt.stable.Placement
			}
			adoptLegacyStable(tt.stable, cur, tt.legacyHash)
			adopted := tt.stable != nil && tt.stable.Placement != before
			if adopted != tt.wantAdopt {
				t.Fatalf("adopted = %v, want %v (stable now %+v)", adopted, tt.wantAdopt, tt.stable)
			}
			if tt.wantAdopt {
				if tt.stable.Placement != cur.Placement || tt.stable.Image != cur.Image {
					t.Errorf("adoption must record placement and image: %+v", tt.stable)
				}
				if tt.stable.Hash != "old" {
					t.Errorf("adoption must keep the old hash name, got %q", tt.stable.Hash)
				}
			}
		})
	}
}
