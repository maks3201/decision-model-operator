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

// RecordedRegistry returns the registry a revision is ACTUALLY served/prefetched
// from, which the controller records on the revision: the name's own host for an
// explicit-host name (scheme defaulted to https, an explicit scheme kept), else
// the engine's configured registry. For an explicit-host name this is the real
// source (the CLI ignores OLLAYA_REGISTRY), so the recorded value — and the
// store-recovery allow-list guard that reads it — reflect the true host.
func TestRecordedRegistry(t *testing.T) {
	const flag = "http://mirror:8080"
	tests := []struct {
		name     string
		registry string // engine --ollaya-registry (empty = engine default)
		model    string
		want     string
	}{
		// host-less names fall back to the configured registry (the flag, or the
		// engine default when unset).
		{"host-less, default engine", "", "laya:en", defaultRegistryURL},
		{"host-less, flag set", flag, "laya:en", flag},
		{"host-less with namespace, flag set", flag, "ns/laya:en", flag},
		// explicit-host names record the NAME's host, independent of the flag.
		{"explicit host, no scheme -> https", flag, "custom.registry.io/ns/laya:en", "https://custom.registry.io"},
		{"explicit host, default engine", "", "custom.registry.io/ns/laya:en", "https://custom.registry.io"},
		{"explicit host with port -> https", flag, "registry.example.com:5000/ns/laya:en", "https://registry.example.com:5000"},
		{"explicit http scheme kept", flag, "http://localhost:5000/ns/laya:en", "http://localhost:5000"},
		{"explicit https scheme kept", flag, "https://reg.internal:8443/ns/laya:en", "https://reg.internal:8443"},
		{"explicit host, default namespace", flag, "ollaya.dev/laya:en", "https://ollaya.dev"},
		// a name that cannot parse falls back to the configured registry (the
		// controller resolves/guards the bad name elsewhere); never returns "".
		{"unparseable name -> configured registry", flag, "not a valid name", flag},
		{"empty name -> configured registry", flag, "", flag},
		{"empty name, default engine -> engine default", "", "", defaultRegistryURL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.registry == "" {
				e = New()
			} else {
				e = New(WithRegistryURL(tt.registry))
			}
			if got := e.RecordedRegistry(tt.model); got != tt.want {
				t.Errorf("RecordedRegistry(%q) = %q, want %q", tt.model, got, tt.want)
			}
			// It must never return "" (a revision would record "unrecorded").
			if e.RecordedRegistry(tt.model) == "" {
				t.Errorf("RecordedRegistry(%q) returned empty", tt.model)
			}
		})
	}
}

// RecordedRegistry agrees with RegistryHost on which host an explicit-host name
// targets: the recorded registry's authority (scheme stripped) equals the host
// RegistryHost reports, so the store-recovery guard checks the same host the
// name resolves to.
func TestRecordedRegistryAgreesWithRegistryHost(t *testing.T) {
	e := New(WithRegistryURL("http://mirror:8080"))
	for _, model := range []string{
		"custom.registry.io/ns/laya:en",
		"registry.example.com:5000/ns/laya:en",
		"http://localhost:5000/ns/laya:en",
	} {
		rec := e.RecordedRegistry(model)
		recHost, _ := hostFromRegistryURL(rec)
		wantHost, _, err := e.RegistryHost(model)
		if err != nil {
			t.Fatalf("RegistryHost(%q) error: %v", model, err)
		}
		if recHost != wantHost {
			t.Errorf("model %q: RecordedRegistry host %q != RegistryHost %q", model, recHost, wantHost)
		}
	}
}
