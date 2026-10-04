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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// qty is a small helper to build a ResourceList in tests.
func qty(cpu, mem string) corev1.ResourceList {
	rl := corev1.ResourceList{}
	if cpu != "" {
		rl[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if mem != "" {
		rl[corev1.ResourceMemory] = resource.MustParse(mem)
	}
	return rl
}

// want* read a resource value as a string, or "" when absent.
func reqStr(res corev1.ResourceRequirements, name corev1.ResourceName) string {
	if q, ok := res.Requests[name]; ok {
		return q.String()
	}
	return ""
}

func limStr(res corev1.ResourceRequirements, name corev1.ResourceName) string {
	if q, ok := res.Limits[name]; ok {
		return q.String()
	}
	return ""
}

func TestApplyModelDefaults(t *testing.T) {
	tests := []struct {
		name    string
		model   string
		device  string
		user    corev1.ResourceRequirements
		wantCPU string // expected cpu request, "" = unset
		wantMem string // expected memory request, "" = unset
	}{
		{
			name:    "known laya:en CPU fills both",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			wantCPU: "1",
			wantMem: "3584Mi",
		},
		{
			name:    "known laya:multilingual CPU",
			model:   "laya:multilingual",
			device:  engine.DeviceCPU,
			wantCPU: "1",
			wantMem: "2Gi",
		},
		{
			name:    "known nli:latest CPU fills measured anon",
			model:   "nli:latest",
			device:  engine.DeviceCPU,
			wantCPU: "1",
			wantMem: "4Gi",
		},
		{
			name:    "known gliclass:latest CPU",
			model:   "gliclass:latest",
			device:  engine.DeviceCPU,
			wantCPU: "1",
			wantMem: "2560Mi",
		},
		{
			name:    "known laya:en CUDA: cpu only, memory unset (unmeasured)",
			model:   "laya:en",
			device:  engine.DeviceCUDA,
			wantCPU: "1",
			wantMem: "", // no measured CUDA host RAM: left unset, not guessed
		},
		{
			name:    "unmeasured tag laya:latest unchanged",
			model:   "laya:latest",
			device:  engine.DeviceCPU,
			wantCPU: "",
			wantMem: "",
		},
		{
			name:    "host-qualified name unchanged",
			model:   "mirror.corp/library/nli:latest",
			device:  engine.DeviceCPU,
			wantCPU: "",
			wantMem: "",
		},
		{
			name:    "non-library namespace unchanged",
			model:   "acme/laya:en",
			device:  engine.DeviceCPU,
			wantCPU: "",
			wantMem: "",
		},
		{
			name:    "explicit default host still defaults",
			model:   "ollaya.dev/library/laya:en",
			device:  engine.DeviceCPU,
			wantCPU: "1",
			wantMem: "3584Mi",
		},
		{
			name:    "unknown model unchanged",
			model:   "mystery:v1",
			device:  engine.DeviceCPU,
			wantCPU: "",
			wantMem: "",
		},
		{
			name:    "user memory request set wins, cpu still defaulted",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			user:    corev1.ResourceRequirements{Requests: qty("", "8Gi")},
			wantCPU: "1",   // filled (user left cpu unset)
			wantMem: "8Gi", // user value kept, not overwritten by the default
		},
		{
			name:    "user cpu request set wins, memory still defaulted",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			user:    corev1.ResourceRequirements{Requests: qty("500m", "")},
			wantCPU: "500m",
			wantMem: "3584Mi",
		},
		{
			name:    "user sets both requests: untouched",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			user:    corev1.ResourceRequirements{Requests: qty("250m", "1Gi")},
			wantCPU: "250m",
			wantMem: "1Gi",
		},
		{
			// blocker: a memory LIMIT must suppress the memory request
			// default (a 3584Mi request > 1Gi limit would be rejected by the API
			// server), and must not strip the limit-implied request.
			name:    "user memory limit only: no memory request default",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			user:    corev1.ResourceRequirements{Limits: qty("", "1Gi")},
			wantCPU: "1", // cpu is unconstrained, still defaulted
			wantMem: "",  // memory constrained by the limit: NOT defaulted
		},
		{
			// A cpu limit suppresses the cpu request default; memory (unconstrained)
			// is still defaulted (no mem limit to exceed).
			name:    "user cpu limit only: cpu request not defaulted, memory defaulted",
			model:   "laya:en",
			device:  engine.DeviceCPU,
			user:    corev1.ResourceRequirements{Limits: qty("2", "")},
			wantCPU: "", // cpu constrained by its limit: NOT defaulted
			wantMem: "3584Mi",
		},
		{
			name:    "unparsable name unchanged",
			model:   "la*ya:bad",
			device:  engine.DeviceCPU,
			wantCPU: "",
			wantMem: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyModelDefaults(tt.model, tt.device, tt.user)
			if c := reqStr(got, corev1.ResourceCPU); c != tt.wantCPU {
				t.Errorf("cpu request = %q, want %q", c, tt.wantCPU)
			}
			if m := reqStr(got, corev1.ResourceMemory); m != tt.wantMem {
				t.Errorf("memory request = %q, want %q", m, tt.wantMem)
			}
			// Defaulting must NEVER add a memory or cpu limit.
			if l := limStr(got, corev1.ResourceMemory); l != limStr(tt.user, corev1.ResourceMemory) {
				t.Errorf("memory limit changed to %q (defaulting must not set limits)", l)
			}
			if l := limStr(got, corev1.ResourceCPU); l != limStr(tt.user, corev1.ResourceCPU) {
				t.Errorf("cpu limit changed to %q (defaulting must not set a cpu limit)", l)
			}
		})
	}
}

// TestModelDefaultsRequestNotAboveLimit is an invariant over every table row:
// for a user who set only a limit equal to the table's own request value, the
// defaulter must not produce request > limit (it must leave the request unset,
// since the key is user-constrained). This guards against a future row whose
// request exceeds a plausible user limit.
func TestModelDefaultsRequestNotAboveLimit(t *testing.T) {
	for _, device := range []string{engine.DeviceCPU, engine.DeviceCUDA} {
		table := cpuDefaults
		if device == engine.DeviceCUDA {
			table = cudaDefaults
		}
		for key, d := range table {
			// Build a limit-only spec using the row's own request values as limits.
			user := corev1.ResourceRequirements{Limits: corev1.ResourceList{}}
			if d.memRequest != "" {
				user.Limits[corev1.ResourceMemory] = resource.MustParse(d.memRequest)
			}
			if d.cpuRequest != "" {
				user.Limits[corev1.ResourceCPU] = resource.MustParse(d.cpuRequest)
			}
			got := applyModelDefaults(key, device, user)
			for name := range got.Requests {
				req := got.Requests[name]
				lim, ok := got.Limits[name]
				if ok && req.Cmp(lim) > 0 {
					t.Errorf("%s/%s: defaulted request %s=%s exceeds limit %s", device, key, name, req.String(), lim.String())
				}
			}
		}
	}
}

// TestApplyModelDefaultsDeterministic: the same inputs always produce the same
// output (the revision hash and stable rendering depend on it).
func TestApplyModelDefaultsDeterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		got := applyModelDefaults("laya:en", engine.DeviceCPU, corev1.ResourceRequirements{})
		if reqStr(got, corev1.ResourceCPU) != "1" || reqStr(got, corev1.ResourceMemory) != "3584Mi" {
			t.Fatalf("non-deterministic defaults on iteration %d: %+v", i, got.Requests)
		}
	}
}

// TestApplyModelDefaultsDoesNotMutateInput: the caller's ResourceRequirements
// must not be modified in place (we DeepCopy).
func TestApplyModelDefaultsDoesNotMutateInput(t *testing.T) {
	user := corev1.ResourceRequirements{Requests: qty("", "")}
	user.Requests = nil // genuinely empty
	_ = applyModelDefaults("laya:en", engine.DeviceCPU, user)
	if user.Requests != nil {
		t.Errorf("input Requests was mutated: %+v", user.Requests)
	}
}

// TestServingResourcesCUDAKnownModelKeepsGPUAndDefaultsCPU: a known model on
// CUDA must still get nvidia.com/gpu:1 (from servingResources) AND the modest
// CPU request from the table, with memory left unset.
func TestServingResourcesCUDAKnownModelKeepsGPUAndDefaultsCPU(t *testing.T) {
	p := baseParams()
	p.Device = engine.DeviceCUDA
	e := New()
	c := e.ServingPodSpec(p).Containers[0]
	res := c.Resources

	if g := limStr(res, gpuResourceName); g != "1" {
		t.Errorf("cuda serving must add %s=1, got %q", gpuResourceName, g)
	}
	if cpu := reqStr(res, corev1.ResourceCPU); cpu != "1" {
		t.Errorf("cuda serving cpu request = %q, want 1 (table default)", cpu)
	}
	if mem := reqStr(res, corev1.ResourceMemory); mem != "" {
		t.Errorf("cuda serving memory request = %q, want unset (no measured CUDA value)", mem)
	}
}

// TestServingResourcesUnknownModelCUDAStillGetsGPU: an unknown model on CUDA
// gets no request defaults but must still receive the GPU limit.
func TestServingResourcesUnknownModelCUDAStillGetsGPU(t *testing.T) {
	p := baseParams()
	p.Model = engine.ModelRef{Name: "mystery:v1", Digest: "deadbeef"}
	p.Device = engine.DeviceCUDA
	e := New()
	res := e.ServingPodSpec(p).Containers[0].Resources

	if g := limStr(res, gpuResourceName); g != "1" {
		t.Errorf("unknown cuda model must still add %s=1, got %q", gpuResourceName, g)
	}
	if cpu := reqStr(res, corev1.ResourceCPU); cpu != "" {
		t.Errorf("unknown model must not get a cpu request default, got %q", cpu)
	}
	if mem := reqStr(res, corev1.ResourceMemory); mem != "" {
		t.Errorf("unknown model must not get a memory request default, got %q", mem)
	}
}

// TestServingResourcesUserGPUKeptWithDefaults: user GPU value is kept and the
// table CPU default is still applied for a known CUDA model.
func TestServingResourcesUserGPUKeptWithDefaults(t *testing.T) {
	p := baseParams()
	p.Device = engine.DeviceCUDA
	p.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{gpuResourceName: resource.MustParse("2")},
	}
	e := New()
	res := e.ServingPodSpec(p).Containers[0].Resources
	if g := limStr(res, gpuResourceName); g != "2" {
		t.Errorf("must keep user GPU value 2, got %q", g)
	}
	if cpu := reqStr(res, corev1.ResourceCPU); cpu != "1" {
		t.Errorf("cpu request = %q, want 1 (table default)", cpu)
	}
}
