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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// seedParams returns baseParams whose Model carries a manifest whose sha256
// matches the Digest, so seeding is enabled.
func seedParams(manifest []byte) engine.Params {
	p := baseParams()
	sum := sha256.Sum256(manifest)
	p.Model = engine.ModelRef{
		Name:     modelLayaEn,
		Digest:   hex.EncodeToString(sum[:]),
		Manifest: manifest,
	}
	return p
}

func seedEnv(env []corev1.EnvVar) (string, bool) {
	for _, e := range env {
		if e.Name == "MANIFEST_SEED_B64" {
			return e.Value, true
		}
	}
	return "", false
}

func TestPrefetchSeedEnvPresentWhenManifestSet(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	js := New().PrefetchJobSpec(seedParams(manifest))
	env := js.Template.Spec.Containers[0].Env

	v, ok := seedEnv(env)
	if !ok {
		t.Fatalf("MANIFEST_SEED_B64 must be set when a valid manifest is provided")
	}
	decoded, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		t.Fatalf("seed env is not valid base64: %v", err)
	}
	if string(decoded) != string(manifest) {
		t.Errorf("seed env decodes to %q, want %q", decoded, manifest)
	}
}

func TestPrefetchNoSeedEnvWithoutManifest(t *testing.T) {
	// baseParams carries a Digest but no Manifest bytes -> no seed, today's behaviour.
	js := New().PrefetchJobSpec(baseParams())
	if _, ok := seedEnv(js.Template.Spec.Containers[0].Env); ok {
		t.Error("MANIFEST_SEED_B64 must not be set when no manifest is provided")
	}
}

func TestPrefetchNoSeedWhenManifestDigestMismatch(t *testing.T) {
	// A manifest whose sha256 does not equal Digest is a controller bug; the
	// engine must not seed it (defence in depth; the script re-checks too).
	p := baseParams() // Digest = laya:en's real digest
	p.Model.Manifest = []byte(`{"schemaVersion":2,"different":"content"}`)
	js := New().PrefetchJobSpec(p)
	if _, ok := seedEnv(js.Template.Spec.Containers[0].Env); ok {
		t.Error("must not seed a manifest whose sha256 != Digest")
	}
}

func TestPrefetchNoSeedWhenManifestTooLarge(t *testing.T) {
	big := make([]byte, maxSeedManifestBytes+1)
	js := New().PrefetchJobSpec(seedParams(big))
	if _, ok := seedEnv(js.Template.Spec.Containers[0].Env); ok {
		t.Error("must not seed a manifest larger than the cap (falls back to pull-by-tag)")
	}
}

func TestSeedManifestUsable(t *testing.T) {
	good := []byte(`{"schemaVersion":2}`)
	sum := sha256.Sum256(good)
	goodDigest := hex.EncodeToString(sum[:])

	tests := []struct {
		name string
		ref  engine.ModelRef
		want bool
	}{
		{"valid pair", engine.ModelRef{Digest: goodDigest, Manifest: good}, true},
		{"no manifest", engine.ModelRef{Digest: goodDigest}, false},
		{"no digest", engine.ModelRef{Manifest: good}, false},
		{"digest mismatch", engine.ModelRef{Digest: goodDigest, Manifest: []byte("other")}, false},
		{"too large", engine.ModelRef{Digest: goodDigest, Manifest: make([]byte, maxSeedManifestBytes+1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := seedManifestUsable(tt.ref); got != tt.want {
				t.Errorf("seedManifestUsable = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPrefetchScriptSeedBlock pins that the rendered script, when a seed is
// present, verifies the seed digest BEFORE writing and writes it to the tag
// path, then still runs pull + the post-pull digest check.
func TestPrefetchScriptSeedBlock(t *testing.T) {
	script := New().PrefetchJobSpec(seedParams([]byte(`{"schemaVersion":2}`))).Template.Spec.Containers[0].Args[0]
	wants := []string{
		`MANIFEST_SEED_B64`,               // the seed input
		`base64 -d`,                       // decode
		`seed_digest=`,                    // compute the seed's digest
		`!= "$EXPECT_DIGEST"`,             // verify before writing
		`refusing to write`,               // the guard message
		`"$OLLAYA_MODELS/$MANIFEST_PATH"`, // write to the tag path
		`ollaya pull -- "$MODEL"`,         // then pull (trusts on-disk manifest)
		"fail " + PrefetchReasonDigestMismatch + " " + "4", // bad seed -> permanent
	}
	for _, w := range wants {
		if !strings.Contains(script, w) {
			t.Errorf("prefetch script missing %q", w)
		}
	}
	// The seed write must appear before the pull in the script order.
	if strings.Index(script, `mv "$seed_tmp"`) > strings.Index(script, `ollaya pull -- "$MODEL"`) {
		t.Error("seed must be written before ollaya pull")
	}
}
