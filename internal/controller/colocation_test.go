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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// applyColocation adds a required host pod-affinity only when the store is
// non-shareable AND replicas > 1, merges with a user affinity instead of
// replacing it, and is idempotent across reconciles.
func TestApplyColocation(t *testing.T) {
	labels := map[string]string{"decisionmodel.io/name": "dm", "decisionmodel.io/revision": "abc123"}
	rwo := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	rwx := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	rox := []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}

	t.Run("RWO + replicas 2 adds the term", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		applyColocation(spec, labels, 2, rwo)
		if !hasColocationTerm(spec.Affinity, labels) {
			t.Fatalf("expected a co-location term for RWO + replicas 2")
		}
		terms := spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 || terms[0].TopologyKey != hostnameTopologyKey {
			t.Fatalf("unexpected terms: %+v", terms)
		}
	})

	t.Run("RWX adds nothing", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		applyColocation(spec, labels, 2, rwx)
		if spec.Affinity != nil {
			t.Fatalf("RWX must not add affinity, got %+v", spec.Affinity)
		}
	})

	t.Run("ROX adds nothing", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		applyColocation(spec, labels, 2, rox)
		if spec.Affinity != nil {
			t.Fatalf("ROX must not add affinity, got %+v", spec.Affinity)
		}
	})

	t.Run("replicas 1 adds nothing", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		applyColocation(spec, labels, 1, rwo)
		if spec.Affinity != nil {
			t.Fatalf("replicas 1 must not add affinity, got %+v", spec.Affinity)
		}
	})

	t.Run("merges with a user affinity instead of replacing it", func(t *testing.T) {
		userNode := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"},
					}},
				}},
			},
		}}
		userPodAffinity := corev1.PodAffinityTerm{
			TopologyKey:   "topology.kubernetes.io/zone",
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
		}
		userNode.PodAffinity = &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{userPodAffinity},
		}
		spec := &corev1.PodSpec{Affinity: userNode}
		applyColocation(spec, labels, 2, rwo)

		// User node affinity preserved.
		if spec.Affinity.NodeAffinity == nil {
			t.Errorf("user node affinity was dropped")
		}
		// Both the user pod-affinity term and our co-location term are present.
		terms := spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 2 {
			t.Fatalf("expected 2 pod-affinity terms (user + co-location), got %d: %+v", len(terms), terms)
		}
		if !hasColocationTerm(spec.Affinity, labels) {
			t.Errorf("co-location term missing after merge")
		}
	})

	t.Run("idempotent: a second apply does not duplicate the term", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		applyColocation(spec, labels, 2, rwo)
		applyColocation(spec, labels, 2, rwo)
		terms := spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 {
			t.Fatalf("expected 1 term after a repeated apply, got %d", len(terms))
		}
	})
}
