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

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Params.Registry records the registry a revision was resolved/prefetched
// against so a running revision renders from it, not from the engine's current
// --ollaya-registry flag. These tests pin the precedence for every render site
// (ServingPodSpec, PrefetchJobSpec OLLAYA_REGISTRY, PrefetchJobSpec MANIFEST_PATH):
//
//	p.Registry set     -> use p.Registry (even if the engine flag differs)
//	p.Registry empty   -> use the engine's configured registry (today's behaviour)
//	effective = default -> no OLLAYA_REGISTRY env (byte-identical to the default case)
//
// paramsWithRegistry is baseParams with a recorded registry.
func paramsWithRegistry(reg string) engine.Params {
	p := baseParams() // host-less model "laya:en"
	p.Registry = reg
	return p
}

func servingRegistryEnv(e *Engine, p engine.Params) (string, bool) {
	v, ok := envMap(e.ServingPodSpec(p).Containers[0].Env)["OLLAYA_REGISTRY"]
	return v.Value, ok
}

func prefetchRegistryEnv(e *Engine, p engine.Params) (string, bool) {
	v, ok := envMap(e.PrefetchJobSpec(p).Template.Spec.Containers[0].Env)["OLLAYA_REGISTRY"]
	return v.Value, ok
}

func prefetchManifestPath(e *Engine, p engine.Params) string {
	return envMap(e.PrefetchJobSpec(p).Template.Spec.Containers[0].Env)["MANIFEST_PATH"].Value
}

// TestParamsRegistryPrecedence covers the three required cases for all render
// sites, including that a recorded p.Registry overrides a DIFFERENT engine flag
// (the whole point: a flag change must not change a running revision's render).
func TestParamsRegistryPrecedence(t *testing.T) {
	const mirror = "https://reg.internal:8443"
	const otherMirror = "http://mirror:8080"

	tests := []struct {
		name string
		// engineReg is the engine's configured --ollaya-registry ("" = default).
		engineReg string
		// paramReg is p.Registry ("" = not recorded on the revision).
		paramReg string
		// wantEnv is the expected OLLAYA_REGISTRY value ("" = env must be absent).
		wantEnv string
		// wantHost is the on-disk manifest host dir for the host-less model.
		wantHost string
	}{
		{
			name:      "param set overrides a default engine flag",
			engineReg: "", paramReg: mirror,
			wantEnv: mirror, wantHost: "reg.internal_8443",
		},
		{
			name:      "param set overrides a DIFFERENT engine flag",
			engineReg: otherMirror, paramReg: mirror,
			wantEnv: mirror, wantHost: "reg.internal_8443",
		},
		{
			name:      "param empty falls back to the engine flag",
			engineReg: mirror, paramReg: "",
			wantEnv: mirror, wantHost: "reg.internal_8443",
		},
		{
			name:      "param empty + default engine flag: no env",
			engineReg: "", paramReg: "",
			wantEnv: "", wantHost: "ollaya.dev",
		},
		{
			name:      "param is the default registry: no env even if the flag is a mirror",
			engineReg: otherMirror, paramReg: "https://ollaya.dev",
			wantEnv: "", wantHost: "ollaya.dev",
		},
		{
			name:      "param trailing slash is trimmed",
			engineReg: "", paramReg: mirror + "/",
			wantEnv: mirror, wantHost: "reg.internal_8443",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.engineReg == "" {
				e = New()
			} else {
				e = New(WithRegistryURL(tt.engineReg))
			}
			p := paramsWithRegistry(tt.paramReg)

			// Serving Pod env.
			if got, ok := servingRegistryEnv(e, p); tt.wantEnv == "" {
				if ok {
					t.Errorf("serving OLLAYA_REGISTRY must be absent, got %q", got)
				}
			} else if !ok || got != tt.wantEnv {
				t.Errorf("serving OLLAYA_REGISTRY = %q (present=%v), want %q", got, ok, tt.wantEnv)
			}

			// Prefetch Job env.
			if got, ok := prefetchRegistryEnv(e, p); tt.wantEnv == "" {
				if ok {
					t.Errorf("prefetch OLLAYA_REGISTRY must be absent, got %q", got)
				}
			} else if !ok || got != tt.wantEnv {
				t.Errorf("prefetch OLLAYA_REGISTRY = %q (present=%v), want %q", got, ok, tt.wantEnv)
			}

			// On-disk manifest host must match the registry env host so the pull
			// writes and the digest check read the same manifests/<host>/ path.
			wantPath := "manifests/" + tt.wantHost + "/library/laya/en"
			if got := prefetchManifestPath(e, p); got != wantPath {
				t.Errorf("MANIFEST_PATH = %q, want %q", got, wantPath)
			}
		})
	}
}

// TestParamsRegistryDoesNotAffectExplicitHostName pins that an explicit host in
// the model name still wins over both the flag and p.Registry for the on-disk
// path (the CLI uses the name's host); p.Registry only governs the host-less case
// and the OLLAYA_REGISTRY env.
func TestParamsRegistryDoesNotAffectExplicitHostName(t *testing.T) {
	e := New(WithRegistryURL("http://mirror:8080"))
	p := paramsWithRegistry("https://reg.internal:8443")
	p.Model = engine.ModelRef{Name: nameRegistryLaya, Digest: "deadbeef"} // registry.example.com/ns/laya:en
	if got := prefetchManifestPath(e, p); got != "manifests/registry.example.com/ns/laya/en" {
		t.Errorf("explicit-host MANIFEST_PATH = %q, want manifests/registry.example.com/ns/laya/en", got)
	}
	// The OLLAYA_REGISTRY env still reflects the effective registry (p.Registry),
	// which the CLI ignores for an explicit-host name but is harmless to set.
	if got, ok := prefetchRegistryEnv(e, p); !ok || got != "https://reg.internal:8443" {
		t.Errorf("OLLAYA_REGISTRY = %q (present=%v), want https://reg.internal:8443", got, ok)
	}
}
