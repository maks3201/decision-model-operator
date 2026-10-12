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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// runPrefetchScriptCustom renders the real prefetch script for p and runs it in
// /bin/sh with caller-supplied stub programs on PATH, shadowing the real tools.
// Each entry in stubs maps a tool name (e.g. "ollaya", "sha256sum") to its shell
// body (without the "#!/bin/sh" line). The stub dir is first on PATH so it wins.
// No network, nothing touches the real FS outside t.TempDir().
func runPrefetchScriptCustom(t *testing.T, p engine.Params, stubs map[string]string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	script := c.Args[0]

	store := t.TempDir()
	manifestPath := "manifests/ollaya.dev/library/laya/en"

	bin := t.TempDir()
	for name, body := range stubs {
		prog := "#!/bin/sh\n" + body
		if err := os.WriteFile(filepath.Join(bin, name), []byte(prog), 0o755); err != nil {
			t.Fatalf("write stub %s: %v", name, err)
		}
	}

	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	env["OLLAYA_MODELS"] = store
	env["MANIFEST_PATH"] = manifestPath

	cmd := exec.Command("/bin/sh", "-c", script)
	// The stub dir is first so a stubbed tool shadows the real one; the rest of
	// PATH stays so unstubbed tools (printf, cut, grep, mkdir, sha256sum when not
	// stubbed) still resolve.
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// pullWritingStub emulates `ollaya pull` writing body to the manifest path.
func pullWritingStub(body string) string {
	return "dest=\"$OLLAYA_MODELS/$MANIFEST_PATH\"\n" +
		"mkdir -p \"$(dirname \"$dest\")\"\n" +
		"printf '%s' '" + body + "' > \"$dest\"\n" +
		"exit 0\n"
}

// TestPrefetchManifestReadFailures pins that a missing or unreadable manifest
// AFTER a successful pull is a non-permanent Unknown failure (exit 2), never a
// permanent DigestMismatch, and that the normal mismatch path is unchanged.
// Table-driven.
func TestPrefetchManifestReadFailures(t *testing.T) {
	base := baseParams() // no Manifest -> no seed, so the only sha256sum call is post-pull

	tests := []struct {
		name          string
		stubs         map[string]string
		wantExit      int
		wantReason    string
		wantPermanent bool
		wantInOutput  string
	}{
		{
			name: "manifest missing after a successful pull -> Unknown, not DigestMismatch",
			stubs: map[string]string{
				// pull "succeeds" but writes NO file.
				"ollaya": "echo 'pulled (stub wrote nothing)'\nexit 0\n",
			},
			wantExit:      prefetchExitUnknown,
			wantReason:    PrefetchReasonUnknown,
			wantPermanent: false,
			wantInOutput:  "manifest missing after pull",
		},
		{
			name: "sha256sum fails on the manifest -> Unknown, not DigestMismatch",
			stubs: map[string]string{
				"ollaya": pullWritingStub(`{"schemaVersion":2,"layers":[]}`),
				// a sha256sum that always errors (simulated IO failure).
				"sha256sum": "echo 'sha256sum: read error' >&2\nexit 1\n",
			},
			wantExit:      prefetchExitUnknown,
			wantReason:    PrefetchReasonUnknown,
			wantPermanent: false,
			wantInOutput:  "manifest unreadable after pull",
		},
		{
			name: "unchanged: a corrupt/different manifest is still a permanent DigestMismatch",
			stubs: map[string]string{
				"ollaya": pullWritingStub(`not a manifest at all`),
			},
			wantExit:      prefetchExitDigestMismatch,
			wantReason:    PrefetchReasonDigestMismatch,
			wantPermanent: true,
			wantInOutput:  "digest mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runPrefetchScriptCustom(t, base, tc.stubs)
			if err == nil {
				t.Fatalf("script must fail; output:\n%s", out)
			}
			assertExitCode(t, err, tc.wantExit)
			if reason, permanent := classifyPrefetchFailure(out, int32(tc.wantExit)); reason != tc.wantReason || permanent != tc.wantPermanent {
				t.Errorf("classify = (%q, %v), want (%q, %v); output:\n%s",
					reason, permanent, tc.wantReason, tc.wantPermanent, out)
			}
			if tc.wantInOutput != "" && !strings.Contains(out, tc.wantInOutput) {
				t.Errorf("output missing %q; output:\n%s", tc.wantInOutput, out)
			}
		})
	}
}

// TestPrefetchManifestSuccessUnchanged pins that the success path still works
// when the pulled bytes hash to the recorded digest (regression for the rewrite).
func TestPrefetchManifestSuccessUnchanged(t *testing.T) {
	seeded := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(seeded)
	out, err := runPrefetchScriptCustom(t, p, map[string]string{
		"ollaya": pullWritingStub(string(seeded)),
	})
	if err != nil {
		t.Fatalf("prefetch should succeed when the tag is unchanged; output:\n%s", out)
	}
	if !strings.Contains(out, "digest ok") {
		t.Errorf("expected the post-pull digest check to pass; output:\n%s", out)
	}
}
