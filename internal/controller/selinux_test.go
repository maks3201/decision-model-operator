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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

func dmNamed(ns, name string) *decisionmodelv1alpha1.DecisionModel {
	return &decisionmodelv1alpha1.DecisionModel{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func TestSELinuxLevelFormatAndStability(t *testing.T) {
	for i := range 2000 {
		dm := dmNamed(fmt.Sprintf("ns-%d", i%7), fmt.Sprintf("dm-%d", i))
		lvl := selinuxLevel(dm)
		var a, b int
		if n, err := fmt.Sscanf(lvl, "s0:c%d,c%d", &a, &b); n != 2 || err != nil {
			t.Fatalf("%s: bad level %q", dm.Name, lvl)
		}
		if a < 0 || b > 1023 || a >= b {
			t.Fatalf("%s: level %q must be s0:cX,cY with 0<=X<Y<=1023", dm.Name, lvl)
		}
		if again := selinuxLevel(dmNamed(dm.Namespace, dm.Name)); again != lvl {
			t.Fatalf("%s: level not stable: %q vs %q", dm.Name, lvl, again)
		}
	}
	if selinuxLevel(dmNamed("a", "x")) == selinuxLevel(dmNamed("b", "x")) {
		t.Fatalf("namespace must be part of the level")
	}
}

func TestApplySELinuxLevel(t *testing.T) {
	dm := dmNamed("ns", "m")
	tests := []struct {
		name string
		in   *corev1.PodSecurityContext
		want *corev1.SELinuxOptions
	}{
		{name: "nil security context", in: nil, want: &corev1.SELinuxOptions{Level: selinuxLevel(dm)}},
		{name: "empty security context", in: &corev1.PodSecurityContext{}, want: &corev1.SELinuxOptions{Level: selinuxLevel(dm)}},
		{
			name: "engine choice kept",
			in:   &corev1.PodSecurityContext{SELinuxOptions: &corev1.SELinuxOptions{Type: "spc_t"}},
			want: &corev1.SELinuxOptions{Type: "spc_t"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := &corev1.PodSpec{SecurityContext: tc.in}
			applySELinuxLevel(spec, dm)
			if got := spec.SecurityContext.SELinuxOptions; *got != *tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
