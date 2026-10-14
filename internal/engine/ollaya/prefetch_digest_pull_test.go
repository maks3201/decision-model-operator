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

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// expectedPinnedModel is the digest-pinned name the prefetch Job must pull on a
// runtime >= RuntimeDigestPullVersion: "<model>@sha256:<digest>".
func expectedPinnedModel(p engine.Params) string {
	return p.Model.Name + "@sha256:" + p.Model.Digest
}

// modelFromSpec returns the MODEL env value PrefetchJobSpec renders for p.
func modelFromSpec(p engine.Params) string {
	for _, e := range New().PrefetchJobSpec(p).Template.Spec.Containers[0].Env {
		if e.Name == "MODEL" {
			return e.Value
		}
	}
	return ""
}

// TestPrefetchDigestPullModelPinnedByVersion pins the version gate: a runtime at
// or above RuntimeDigestPullVersion pulls the pinned "<model>@sha256:<digest>"
// name; an older pinned runtime (and an empty digest) keeps the bare tag.
func TestPrefetchDigestPullModelPinnedByVersion(t *testing.T) {
	tests := []struct {
		name           string
		runtimeVersion string
		digest         string
		wantPinned     bool
	}{
		{"default runtime (>= 0.13.0) pins", "", baseParams().Model.Digest, true},
		{"0.13.0 pins", "0.13.0", baseParams().Model.Digest, true},
		{"newer runtime pins", "1.4.0", baseParams().Model.Digest, true},
		{"pre-0.13.0 keeps the tag", preDigestPullVersion, baseParams().Model.Digest, false},
		{"0.7.3 keeps the tag", "0.7.3", baseParams().Model.Digest, false},
		{"no recorded digest keeps the tag even on 0.13.0", "0.13.0", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseParams()
			p.RuntimeVersion = tt.runtimeVersion
			p.Model.Digest = tt.digest
			got := modelFromSpec(p)
			if tt.wantPinned {
				want := p.Model.Name + "@sha256:" + tt.digest
				if got != want {
					t.Errorf("MODEL = %q, want pinned %q", got, want)
				}
			} else {
				if got != p.Model.Name {
					t.Errorf("MODEL = %q, want bare %q", got, p.Model.Name)
				}
				if strings.Contains(got, "@sha256:") {
					t.Errorf("MODEL %q must not be digest-pinned on %q", got, tt.runtimeVersion)
				}
			}
		})
	}
}

// TestPrefetchDigestPullSkipsSeed pins that the digest path carries no seed
// (MANIFEST_SEED_FILE / MANIFEST_SEED_B64): the pinned pull verifies before
// writing, so seeding is unnecessary. Even with usable manifest bytes present,
// the default runtime must not seed; the pre-0.13.0 runtime still seeds.
func TestPrefetchDigestPullSkipsSeed(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)

	// Digest path (default runtime): no seed env, even with bytes present.
	pDigest := seedParams(manifest)   // seedParams carries matching Manifest bytes
	pDigest.RuntimeVersion = "0.13.0" // override seedParams' pre-digest pin
	for _, e := range New().PrefetchJobSpec(pDigest).Template.Spec.Containers[0].Env {
		if e.Name == "MANIFEST_SEED_FILE" || e.Name == "MANIFEST_SEED_B64" {
			t.Errorf("digest path must not seed, found env %s", e.Name)
		}
	}

	// Tag path (pre-0.13.0): the seed env IS present (regression guard).
	pTag := seedParams(manifest) // already pinned to preDigestPullVersion
	seen := false
	for _, e := range New().PrefetchJobSpec(pTag).Template.Spec.Containers[0].Env {
		if e.Name == "MANIFEST_SEED_B64" {
			seen = true
		}
	}
	if !seen {
		t.Errorf("tag path (pre-0.13.0) must still seed the manifest")
	}
}

// digestPullParams returns params on the digest-pull default runtime whose
// recorded digest matches manifest, so a correct pull verifies.
func digestPullParams(digest string) engine.Params {
	p := baseParams()
	p.RuntimeVersion = "0.13.0"
	p.Model.Digest = digest
	return p
}

// pinnedPullStub emulates `ollaya pull -- <pinned-name>`. It fails the test
// (exit 3, a code no classifier case maps so it is unmistakable) if MODEL is not
// the digest-pinned name, proving the Job pulls by digest on 0.13.0. On a match
// it writes body to the manifest path and exits 0.
func pinnedPullStub(wantModel, body string) string {
	return "if [ \"$MODEL\" != '" + wantModel + "' ]; then\n" +
		"  echo \"stub: MODEL=$MODEL, want pinned " + wantModel + "\" >&2\n" +
		"  exit 97\n" +
		"fi\n" +
		"dest=\"$OLLAYA_MODELS/$MANIFEST_PATH\"\n" +
		"mkdir -p \"$(dirname \"$dest\")\"\n" +
		"printf '%s' '" + body + "' > \"$dest\"\n" +
		"exit 0\n"
}

// TestPrefetchDigestPullHostRun exercises the real prefetch script on the
// digest-pull runtime for success, an unknown digest, a registry digest
// mismatch, and confirms UpstreamTagMoved is never produced on the digest path.
// Table-driven; no network.
func TestPrefetchDigestPullHostRun(t *testing.T) {
	good := []byte(`{"schemaVersion":2,"layers":[]}`)
	goodDigest := digestOf(good)
	p := digestPullParams(goodDigest)
	pinned := expectedPinnedModel(p)

	tests := []struct {
		name          string
		stubs         map[string]string
		wantErr       bool
		wantExit      int
		wantReason    string
		wantPermanent bool
		wantInOutput  string
	}{
		{
			name: "success: pinned pull writes bytes that hash to the recorded digest",
			stubs: map[string]string{
				"ollaya": pinnedPullStub(pinned, string(good)),
			},
			wantErr:      false,
			wantInOutput: "digest ok",
		},
		{
			name: "unknown digest -> ModelNotFound (permanent)",
			stubs: map[string]string{
				// Ollaya 0.13.0 reports an unknown digest as not-found (spike 011).
				"ollaya": "echo 'Error: model \"laya:en@sha256:0000\" not found in registry ollaya.dev: it has no manifest with that digest' >&2\nexit 1\n",
			},
			wantErr:       true,
			wantExit:      prefetchExitModelNotFound,
			wantReason:    PrefetchReasonModelNotFound,
			wantPermanent: true,
		},
		{
			name: "registry served wrong bytes for the digest -> DigestMismatch (permanent)",
			stubs: map[string]string{
				"ollaya": "echo 'Error: the registry served manifest sha256:f6888fde for \"laya:en@sha256:aaaa\", not the one its digest names; nothing was stored' >&2\nexit 1\n",
			},
			wantErr:       true,
			wantExit:      prefetchExitDigestMismatch,
			wantReason:    PrefetchReasonDigestMismatch,
			wantPermanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runPrefetchScriptCustom(t, p, tc.stubs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("script must fail; output:\n%s", out)
				}
				assertExitCode(t, err, tc.wantExit)
				if reason, permanent := classifyPrefetchFailure(out, int32(tc.wantExit)); reason != tc.wantReason || permanent != tc.wantPermanent {
					t.Errorf("classify = (%q, %v), want (%q, %v); output:\n%s",
						reason, permanent, tc.wantReason, tc.wantPermanent, out)
				}
				// A moved tag is impossible on the digest path.
				if strings.Contains(out, PrefetchReasonUpstreamTagMoved) {
					t.Errorf("digest path must never produce UpstreamTagMoved; output:\n%s", out)
				}
			} else {
				if err != nil {
					t.Fatalf("digest-pull success path failed: %v\noutput:\n%s", err, out)
				}
			}
			if tc.wantInOutput != "" && !strings.Contains(out, tc.wantInOutput) {
				t.Errorf("output missing %q; output:\n%s", tc.wantInOutput, out)
			}
		})
	}
}

// digestThenTagStub emulates a registry/mirror that does NOT serve the by-digest
// manifest route: the pinned pull (arg contains "@sha256:") 404s, while the tag
// pull (no "@sha256:") writes tagBody and exits 0. The stub distinguishes the two
// calls by inspecting its arguments, so the test exercises the real fallback.
func digestThenTagStub(tagBody string) string {
	return "case \"$*\" in\n" +
		"  *@sha256:*)\n" +
		"    echo 'Error: model not found in registry ollaya.dev: it has no manifest with that digest' >&2\n" +
		"    exit 1 ;;\n" +
		"  *)\n" +
		"    dest=\"$OLLAYA_MODELS/$MANIFEST_PATH\"\n" +
		"    mkdir -p \"$(dirname \"$dest\")\"\n" +
		"    printf '%s' '" + tagBody + "' > \"$dest\"\n" +
		"    exit 0 ;;\n" +
		"esac\n"
}

// TestPrefetchDigestFallbackToTag exercises the digest-path tag fallback: a
// registry without the by-digest route 404s the pinned pull, and the script
// retries the tag pull once. The post-pull sha256 check still gates the result,
// so an unchanged tag succeeds and a moved tag ends as UpstreamTagMoved (0.12
// behaviour). A transient digest error or a DIGEST_MISMATCH never falls back.
func TestPrefetchDigestFallbackToTag(t *testing.T) {
	good := []byte(`{"schemaVersion":2,"layers":[]}`)
	goodDigest := digestOf(good)
	p := digestPullParams(goodDigest)

	// A different, well-formed manifest the "moved tag" fallback would pull.
	moved := []byte(`{"schemaVersion":2,"config":{"digest":"moved"}}`)
	if digestOf(moved) == goodDigest {
		t.Fatal("test setup: moved manifest must differ from the recorded digest")
	}

	tests := []struct {
		name          string
		stubs         map[string]string
		wantErr       bool
		wantExit      int
		wantReason    string
		wantPermanent bool
		wantInOutput  string
	}{
		{
			name:         "digest 404, then the tag pull serves the recorded bytes -> success",
			stubs:        map[string]string{"ollaya": digestThenTagStub(string(good))},
			wantErr:      false,
			wantInOutput: "Falling back to the tag pull",
		},
		{
			name:          "digest 404, then the tag moved upstream -> UpstreamTagMoved (permanent)",
			stubs:         map[string]string{"ollaya": digestThenTagStub(string(moved))},
			wantErr:       true,
			wantExit:      prefetchExitTagMoved,
			wantReason:    PrefetchReasonUpstreamTagMoved,
			wantPermanent: true,
			wantInOutput:  "Falling back to the tag pull",
		},
		{
			name: "digest transient -> Transient, no fallback",
			stubs: map[string]string{
				"ollaya": "echo 'Error: HTTP status server error (503 Service Unavailable) for url (http://mirror/v2/library/laya/manifests/sha256:abc)' >&2\nexit 1\n",
			},
			wantErr:       true,
			wantExit:      prefetchExitTransient,
			wantReason:    PrefetchReasonTransient,
			wantPermanent: false,
		},
		{
			name: "DIGEST_MISMATCH -> permanent, no fallback",
			stubs: map[string]string{
				"ollaya": "echo 'Error: the registry served manifest sha256:f6 for \"laya:en@sha256:aa\", not the one its digest names; nothing was stored' >&2\nexit 1\n",
			},
			wantErr:       true,
			wantExit:      prefetchExitDigestMismatch,
			wantReason:    PrefetchReasonDigestMismatch,
			wantPermanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runPrefetchScriptCustom(t, p, tc.stubs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("script must fail; output:\n%s", out)
				}
				assertExitCode(t, err, tc.wantExit)
				if reason, permanent := classifyPrefetchFailure(out, int32(tc.wantExit)); reason != tc.wantReason || permanent != tc.wantPermanent {
					t.Errorf("classify = (%q, %v), want (%q, %v); output:\n%s",
						reason, permanent, tc.wantReason, tc.wantPermanent, out)
				}
			} else {
				if err != nil {
					t.Fatalf("fallback success path failed: %v\noutput:\n%s", err, out)
				}
				if !strings.Contains(out, "digest ok") {
					t.Errorf("expected the post-pull digest check to pass; output:\n%s", out)
				}
			}
			// The no-fallback cases must not log a fallback attempt.
			if !tc.wantErr || tc.wantReason == PrefetchReasonUpstreamTagMoved {
				// fallback cases: the log line is expected (checked via wantInOutput)
			} else if strings.Contains(out, "Falling back to the tag pull") {
				t.Errorf("%s must not fall back to the tag; output:\n%s", tc.name, out)
			}
			if tc.wantInOutput != "" && !strings.Contains(out, tc.wantInOutput) {
				t.Errorf("output missing %q; output:\n%s", tc.wantInOutput, out)
			}
		})
	}
}

// TestPrefetchDigestPullSetsFallbackModel pins that FALLBACK_MODEL is the bare
// tag on the digest path and absent on the tag path.
func TestPrefetchDigestPullSetsFallbackModel(t *testing.T) {
	pDigest := digestPullParams(baseParams().Model.Digest)
	var fb string
	var seen bool
	for _, e := range New().PrefetchJobSpec(pDigest).Template.Spec.Containers[0].Env {
		if e.Name == "FALLBACK_MODEL" {
			fb, seen = e.Value, true
		}
	}
	if !seen || fb != pDigest.Model.Name {
		t.Errorf("FALLBACK_MODEL = (%q, present=%v), want the bare tag %q", fb, seen, pDigest.Model.Name)
	}

	pTag := baseParams()
	pTag.RuntimeVersion = preDigestPullVersion
	for _, e := range New().PrefetchJobSpec(pTag).Template.Spec.Containers[0].Env {
		if e.Name == "FALLBACK_MODEL" {
			t.Errorf("tag path must not set FALLBACK_MODEL, got %q", e.Value)
		}
	}
}

// TestPrefetchDigestPullNeedsKnownVersion pins that a user-set image without an
// explicit runtimeVersion keeps the tag pull (its version is unknown and an older
// binary rejects "name@sha256:"), while an explicit runtimeVersion still decides.
func TestPrefetchDigestPullNeedsKnownVersion(t *testing.T) {
	modelEnv := func(p engine.Params) string {
		for _, e := range New().PrefetchJobSpec(p).Template.Spec.Containers[0].Env {
			if e.Name == "MODEL" {
				return e.Value
			}
		}
		return ""
	}
	tests := []struct {
		name       string
		image      string
		version    string
		wantPinned bool
	}{
		{name: "default image and version", wantPinned: true},
		{name: "custom image, no version", image: "registry.example/ollaya:custom", wantPinned: false},
		{name: "custom image, version 0.13.0", image: "registry.example/ollaya:custom", version: "0.13.0", wantPinned: true},
		{name: "custom image, version 0.12.1", image: "registry.example/ollaya:custom", version: preDigestPullVersion, wantPinned: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := baseParams()
			p.Image = tc.image
			p.RuntimeVersion = tc.version
			got := modelEnv(p)
			pinned := got == p.Model.Name+"@sha256:"+p.Model.Digest
			if pinned != tc.wantPinned {
				t.Errorf("MODEL = %q, pinned=%v, want pinned=%v", got, pinned, tc.wantPinned)
			}
		})
	}
}
