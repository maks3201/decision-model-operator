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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// modelDefaults holds the default serving-container resource hints for one
// measured model (one "name:tag") on one device class. An empty string means
// "no measured value, leave the field unset" — we never guess. These feed
// applyModelDefaults, which only fills keys the user left unset.
//
// Values are the measured CPU / F32 numbers from docs/sizing.md and the
// re-measurement (2026-10-02): memory request = cgroup `anon` rounded up to the
// next 512Mi, with a modest CPU request for low-latency single-request serving.
// We deliberately set NO memory limit here (the user sizes the limit; see
// docs/sizing.md's rule of thumb) and NEVER a CPU limit. CUDA host-RAM numbers
// are UNVERIFIED (no GPU on the measurement host), so for CUDA we leave memory
// unset and only hint a modest CPU request.
type modelDefaults struct {
	cpuRequest string // "" = leave unset
	memRequest string // "" = leave unset
}

// cpuDefaults is keyed by the exact measured canonical "model:tag" (default
// Ollaya registry, "library" namespace only). We do NOT key by family: an
// unmeasured tag can be a different, heavier artifact (spike 001: a bare "laya"
// → "laya:latest" is heavier than "laya:en"), so only the specific tags we
// actually measured get a default; everything else is left unchanged.
//
// Memory request = measured cgroup `anon` (not `docker stats`, which can include
// page cache) after loading with keep_alive:-1, rounded UP to the next 512Mi.
// Re-measured 2026-10-02 on ghcr.io/ollaya-dev/ollaya:0.7.3, CPU / F32, OrbStack
// arm64, reading /sys/fs/cgroup/memory.stat `anon` inside the container;
// spike 006 re-checked laya:en on 0.10.0 (3139 MiB, within the same request);
// see docs/sizing.md for the commands):
//
//	model              anon (MiB)   request (round up 512Mi)
//	laya:en            3167         3584Mi
//	laya:multilingual  1894         2048Mi
//	nli:latest         3711         4096Mi
//	gliclass:latest    2474         2560Mi
//
// No memory limit, no CPU limit (the user sizes the limit).
var cpuDefaults = map[string]modelDefaults{
	"laya:en":           {cpuRequest: "1", memRequest: "3584Mi"}, // anon ~3.09 GiB
	"laya:multilingual": {cpuRequest: "1", memRequest: "2048Mi"}, // anon ~1.85 GiB
	"nli:latest":        {cpuRequest: "1", memRequest: "4096Mi"}, // anon ~3.62 GiB
	"gliclass:latest":   {cpuRequest: "1", memRequest: "2560Mi"}, // anon ~2.42 GiB
}

// cudaDefaults is keyed by the same measured "model:tag". Host RAM on CUDA is
// lower than CPU/F32 but UNVERIFIED (no measurement), so we only hint a modest
// CPU request and leave memory unset rather than guess. The GPU limit itself
// (nvidia.com/gpu:1) is added by servingResources, not here.
var cudaDefaults = map[string]modelDefaults{
	"laya:en":           {cpuRequest: "1"},
	"laya:multilingual": {cpuRequest: "1"},
	"nli:latest":        {cpuRequest: "1"},
	"gliclass:latest":   {cpuRequest: "1"},
}

// defaultsFor returns the modelDefaults for a model name on a device, and
// whether an entry exists. It applies ONLY to a name on the default Ollaya
// registry and default "library" namespace (an explicit host or a non-library
// namespace means a different artifact we have not measured). The lookup key is
// the exact canonical "model:tag". Unparsable, host-qualified, non-library, or
// unmeasured names return ok=false (no change). The result depends only on the
// inputs — the maps are compile-time constants — so the serving template stays
// deterministic.
func defaultsFor(modelName, device string) (modelDefaults, bool) {
	p, err := parseName(modelName)
	if err != nil {
		return modelDefaults{}, false
	}
	// Only the default registry (no host, or the default host) and the default
	// "library" namespace are measured. Anything else is a different artifact.
	if !hostIsDefault(p.host) || p.namespace != defaultNamespace {
		return modelDefaults{}, false
	}
	key := p.model + ":" + p.tag
	switch device {
	case engine.DeviceCUDA:
		d, ok := cudaDefaults[key]
		return d, ok
	default: // CPU and any non-CUDA value use the CPU/F32 table
		d, ok := cpuDefaults[key]
		return d, ok
	}
}

// applyModelDefaults returns a copy of user with memory/cpu requests filled from
// the per-model table for any key the user did NOT already constrain. A key is
// considered user-set — and therefore left untouched — when it appears in EITHER
// Requests OR Limits: defaulting a request next to a user-set limit could make
// request > limit (the API server would reject the Pod) and would also turn a
// Guaranteed limit-only Pod into Burstable. User-set values always win per key.
// It never sets limits (no memory limit, no CPU limit) and never touches an
// unmeasured / unknown model (returns the input copy unchanged). GPU limits for
// CUDA are handled by servingResources.
func applyModelDefaults(modelName, device string, user corev1.ResourceRequirements) corev1.ResourceRequirements {
	res := *user.DeepCopy()
	d, ok := defaultsFor(modelName, device)
	if !ok {
		return res // unmeasured/unknown model: unchanged (today's behaviour)
	}
	// constrained reports whether the user already set name in Requests or Limits.
	constrained := func(name corev1.ResourceName) bool {
		if _, set := res.Requests[name]; set {
			return true
		}
		_, set := res.Limits[name]
		return set
	}
	ensureReq := func(name corev1.ResourceName, val string) {
		if val == "" || constrained(name) {
			return
		}
		if res.Requests == nil {
			res.Requests = corev1.ResourceList{}
		}
		res.Requests[name] = resource.MustParse(val)
	}
	ensureReq(corev1.ResourceMemory, d.memRequest)
	ensureReq(corev1.ResourceCPU, d.cpuRequest)

	// If we added nothing and the user had no requests, keep the input's exact
	// Requests value (nil vs empty map) so the result equals the input.
	if len(res.Requests) == 0 {
		res.Requests = user.Requests
	}
	return res
}
