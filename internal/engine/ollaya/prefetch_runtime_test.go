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
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// runPrefetchScript renders the real prefetch script for p and runs it in
// /bin/sh with a stub `ollaya` on PATH that writes pulledBytes to
// $OLLAYA_MODELS/$MANIFEST_PATH (standing in for a real `ollaya pull`). The
// store and manifest path are temp/relative so nothing touches the real FS.
// It returns the combined output and the process error (nil on exit 0). No
// network.
func runPrefetchScript(t *testing.T, p engine.Params, pulledBytes []byte) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	script := c.Args[0]

	store := t.TempDir()
	manifestPath := "manifests/ollaya.dev/library/laya/en"

	// A stub `ollaya` that emulates `ollaya pull -- <model>` by writing the
	// caller's bytes to the manifest path, then exits 0. It reads OLLAYA_MODELS
	// and MANIFEST_PATH from the environment the script exports to it.
	bin := t.TempDir()
	stub := "#!/bin/sh\n" +
		"# stub ollaya: emulate pull by writing the test's bytes to the manifest path\n" +
		"dest=\"$OLLAYA_MODELS/$MANIFEST_PATH\"\n" +
		"mkdir -p \"$(dirname \"$dest\")\"\n" +
		"printf '%s' \"$OLLAYA_STUB_BODY\" > \"$dest\"\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "ollaya"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	env["OLLAYA_MODELS"] = store
	env["MANIFEST_PATH"] = manifestPath
	env["OLLAYA_STUB_BODY"] = string(pulledBytes)

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// digestOf returns the bare-hex sha256 of b.
func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Item 40 (no seed): the pull writes a well-formed manifest whose digest differs
// from EXPECT_DIGEST (the tag moved upstream). The script must detect this and
// exit with UpstreamTagMoved (5), NOT DigestMismatch (4) — a well-formed but
// different manifest is a moved tag, which re-pulling the tag cannot fix.
func TestPrefetchNoSeedTagMovedExitsUpstreamTagMoved(t *testing.T) {
	p := baseParams() // no Manifest -> no seed
	// EXPECT_DIGEST is baseParams' recorded laya:en digest; the pull returns a
	// DIFFERENT but well-formed (schemaVersion 2) manifest.
	moved := []byte(`{"schemaVersion":2,"config":{"digest":"newer"}}`)
	if digestOf(moved) == p.Model.Digest {
		t.Fatal("test setup: moved manifest must differ from the recorded digest")
	}

	out, err := runPrefetchScript(t, p, moved)
	if err == nil {
		t.Fatalf("script must fail when the pulled digest differs; output:\n%s", out)
	}
	assertExitCode(t, err, prefetchExitTagMoved)
	if reason, permanent := classifyPrefetchFailure(out, prefetchExitTagMoved); reason != PrefetchReasonUpstreamTagMoved || !permanent {
		t.Errorf("classify = (%q, %v), want (UpstreamTagMoved, true); output:\n%s", reason, permanent, out)
	}
}

// Item 40 (no seed): the pull writes a NOT-well-formed manifest (no
// schemaVersion 2) whose digest differs. That is corruption, not a moved tag:
// the script must exit DigestMismatch (4), distinct from UpstreamTagMoved (5).
func TestPrefetchNoSeedCorruptManifestExitsDigestMismatch(t *testing.T) {
	p := baseParams()
	corrupt := []byte(`not a manifest at all`)
	if digestOf(corrupt) == p.Model.Digest {
		t.Fatal("test setup: corrupt manifest must differ from the recorded digest")
	}

	out, err := runPrefetchScript(t, p, corrupt)
	if err == nil {
		t.Fatalf("script must fail on a corrupt manifest; output:\n%s", out)
	}
	assertExitCode(t, err, prefetchExitDigestMismatch)
	if reason, permanent := classifyPrefetchFailure(out, prefetchExitDigestMismatch); reason != PrefetchReasonDigestMismatch || !permanent {
		t.Errorf("classify = (%q, %v), want (DigestMismatch, true); output:\n%s", reason, permanent, out)
	}
}

// Item 40: "ollaya pull <tag>" OVERWRITES the on-disk (seeded) manifest
// with the tag's current bytes (ollaya-dev/ollaya#64). So a manifest seed does
// NOT rescue a moved tag: after the pull the store holds the tag's current
// manifest, and if the tag moved the post-pull digest check must fail the Job
// with UpstreamTagMoved — it must never silently succeed on the seeded digest.
// The fake `ollaya` here overwrites the seed with DIFFERENT, well-formed bytes,
// exactly what the real CLI does on a moved tag.
func TestPrefetchSeedDoesNotRescueMovedTag(t *testing.T) {
	seeded := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(seeded) // Model.Digest == sha256(seeded), seeding enabled

	// The pull overwrites the seed with a different but well-formed manifest (the
	// tag moved upstream). The post-pull check must catch it.
	moved := []byte(`{"schemaVersion":2,"config":{"digest":"moved"}}`)
	if digestOf(moved) == p.Model.Digest {
		t.Fatal("test setup: moved manifest must differ from the recorded digest")
	}

	out, err := runPrefetchScript(t, p, moved)
	if err == nil {
		t.Fatalf("a seed must NOT make a moved tag succeed; output:\n%s", out)
	}
	assertExitCode(t, err, prefetchExitTagMoved)
	if reason, permanent := classifyPrefetchFailure(out, prefetchExitTagMoved); reason != PrefetchReasonUpstreamTagMoved || !permanent {
		t.Errorf("classify = (%q, %v), want (UpstreamTagMoved, true); output:\n%s", reason, permanent, out)
	}
	// The seed was still written first (defence in depth), then overwritten.
	if !contains(out, "seeded manifest for") {
		t.Errorf("expected the seed to be written before pull; output:\n%s", out)
	}
}

// The normal success path: the seed is present AND the tag still serves the
// recorded bytes (the pull overwrites the seed with identical bytes). The Job
// succeeds. This pins that an unchanged tag still works with seeding enabled —
// it does NOT claim the seed rescues anything.
func TestPrefetchSeedSucceedsWhenTagUnchanged(t *testing.T) {
	seeded := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(seeded)

	out, err := runPrefetchScript(t, p, seeded) // pull writes the same bytes
	if err != nil {
		t.Fatalf("prefetch should succeed when the tag is unchanged; output:\n%s", out)
	}
	if !contains(out, "digest ok") {
		t.Errorf("expected the post-pull digest check to pass; output:\n%s", out)
	}
}

// assertExitCode checks that err is an *exec.ExitError with the given code.
func assertExitCode(t *testing.T, err error, want int) {
	t.Helper()
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("error is not an exec.ExitError: %v", err)
	}
	if got := ee.ExitCode(); got != want {
		t.Errorf("exit code = %d, want %d", got, want)
	}
}
