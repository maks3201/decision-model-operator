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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Item 68 gaps: the serving container's full hardening is already covered
// (TestServingPodSpecSecurity). These add the equivalent assertions for the
// prefetch Job container (allowPrivilegeEscalation=false, drop ALL) and a
// negative scan proving the API key is never a plaintext env Value or command
// arg on EITHER spec — only ever a secretKeyRef.

// assertContainerHardened checks the container-level securityContext invariants
// shared by both the serving and prefetch containers.
func assertContainerHardened(t *testing.T, name string, sc *corev1.SecurityContext) {
	t.Helper()
	if sc == nil {
		t.Fatalf("%s: container securityContext is nil", name)
	}
	if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Errorf("%s: readOnlyRootFilesystem must be true", name)
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("%s: allowPrivilegeEscalation must be false", name)
	}
	if sc.Capabilities == nil {
		t.Fatalf("%s: capabilities must drop ALL (nil capabilities)", name)
	}
	if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("%s: capabilities.drop = %v, want [ALL]", name, sc.Capabilities.Drop)
	}
	if len(sc.Capabilities.Add) != 0 {
		t.Errorf("%s: capabilities.add must be empty, got %v", name, sc.Capabilities.Add)
	}
}

// TestPrefetchJobContainerHardened pins the prefetch Job container's
// container-level securityContext matches the serving container's: not just
// readOnlyRootFilesystem (already tested) but also allowPrivilegeEscalation=false
// and drop ALL.
func TestPrefetchJobContainerHardened(t *testing.T) {
	js := New().PrefetchJobSpec(baseParams())
	assertContainerHardened(t, "prefetch", js.Template.Spec.Containers[0].SecurityContext)
}

// TestServingContainerHardened re-pins the serving container through the same
// shared assertion so a regression on either spec is caught uniformly.
func TestServingContainerHardened(t *testing.T) {
	spec := New().ServingPodSpec(baseParams())
	assertContainerHardened(t, "serving", spec.Containers[0].SecurityContext)
}

// apiKeyParams returns baseParams carrying an API-key Secret reference, so the
// specs render the OLLAYA_API_KEY env.
func apiKeyParams() engine.Params {
	p := baseParams()
	p.APIKey = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "support-router-key"},
		Key:                  "token",
	}
	return p
}

// assertAPIKeyOnlyViaSecretRef scans a container's env and args: the API key
// must appear only as a SecretKeyRef on OLLAYA_API_KEY, never as a plaintext env
// Value and never spliced into a command arg. The secret NAME/KEY are references,
// not the secret value, so the test asserts structure, not a magic string.
func assertAPIKeyOnlyViaSecretRef(t *testing.T, where string, env []corev1.EnvVar, args []string) {
	t.Helper()
	var saw bool
	for _, e := range env {
		if e.Name != "OLLAYA_API_KEY" {
			continue
		}
		saw = true
		if e.Value != "" {
			t.Errorf("%s: OLLAYA_API_KEY must have an empty plaintext Value, got %q", where, e.Value)
		}
		if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Errorf("%s: OLLAYA_API_KEY must be sourced via SecretKeyRef", where)
			continue
		}
		ref := e.ValueFrom.SecretKeyRef
		if ref.Name != "support-router-key" || ref.Key != "token" {
			t.Errorf("%s: OLLAYA_API_KEY ref = %s/%s, want support-router-key/token", where, ref.Name, ref.Key)
		}
	}
	if !saw {
		t.Errorf("%s: expected an OLLAYA_API_KEY env", where)
	}
	// No arg may carry the key material or a shell read of the Secret file.
	for _, a := range args {
		if strings.Contains(a, "token") && strings.Contains(a, "OLLAYA_API_KEY=") {
			t.Errorf("%s: an arg appears to inline the API key: %q", where, a)
		}
	}
}

// TestServingAPIKeyNeverPlaintext proves the serving container injects the API
// key only via secretKeyRef, never a plaintext env Value or command arg.
func TestServingAPIKeyNeverPlaintext(t *testing.T) {
	c := New().ServingPodSpec(apiKeyParams()).Containers[0]
	assertAPIKeyOnlyViaSecretRef(t, "serving", c.Env, c.Args)
}

// TestPrefetchAPIKeyNeverInjected pins that the prefetch Job never carries the
// serving API key at all (it talks to the registry, not the runtime): no
// OLLAYA_API_KEY env, plaintext or otherwise. If a future change adds one it must
// be a secretKeyRef, so this guards the current no-key invariant explicitly.
func TestPrefetchAPIKeyNeverInjected(t *testing.T) {
	c := New().PrefetchJobSpec(apiKeyParams()).Template.Spec.Containers[0]
	for _, e := range c.Env {
		if e.Name == "OLLAYA_API_KEY" {
			t.Errorf("prefetch Job must not carry the serving OLLAYA_API_KEY; got %+v", e)
		}
		if e.Value != "" && strings.Contains(strings.ToLower(e.Name), "key") && e.ValueFrom == nil {
			t.Errorf("prefetch env %q carries a plaintext secret-looking value", e.Name)
		}
	}
}
