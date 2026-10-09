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
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// enginePkgScripts returns every shell script string the engine renders, so a
// guard test can assert none of them writes outside a mounted writable path.
func enginePkgScripts() map[string]string {
	return map[string]string{
		"prefetchScript":       prefetchScript,
		"unparsableNameScript": unparsableNameScript,
		"invalidSubPathScript": invalidSubPathScript,
	}
}

// TestScriptsNeverWriteOutsideMounts is the static guard: no rendered script may
// write to the read-only root filesystem. The container mounts only /models (RW,
// the store), /store-root (RW, the PVC root for pruning), /home/ollaya/.ollaya
// (RW emptyDir) and /dev/termination-log (kubelet-writable). A bare `mktemp` or a
// literal /tmp path would land on the read-only root, so both are forbidden.
// Comments are stripped first so documentation that mentions /tmp does not trip it.
func TestScriptsNeverWriteOutsideMounts(t *testing.T) {
	for name, script := range enginePkgScripts() {
		for _, raw := range strings.Split(script, "\n") {
			l := strings.TrimSpace(raw)
			if l == "" || strings.HasPrefix(l, "#") {
				continue // skip blank and comment lines
			}
			// Strip a trailing inline comment (best effort; our scripts put inline
			// comments after "  #").
			if i := strings.Index(l, "  #"); i >= 0 {
				l = strings.TrimSpace(l[:i])
			}
			if strings.Contains(l, "/tmp") {
				t.Errorf("%s: writes/paths under /tmp (read-only root): %q", name, l)
			}
			if !strings.Contains(l, "mktemp") {
				continue
			}
			// Every mktemp must take an explicit quoted-variable template, not a
			// bare `mktemp` and not -t/-p/-d forms, all of which honour TMPDIR and
			// default to the read-only /tmp.
			if strings.Contains(l, "mktemp -") {
				t.Errorf("%s: mktemp with a flag honours TMPDIR -> read-only /tmp: %q", name, l)
			}
			if !mktempTemplateRE.MatchString(l) {
				t.Errorf("%s: mktemp without an explicit quoted-variable template: %q", name, l)
			}
		}
	}

	// The seed temp dir must be a hidden sibling of manifests/ under the writable
	// store mount (.seed-tmp), NOT inside a tag directory — a leftover temp in a
	// tag dir makes `ollaya list` fail (verified on 0.12.0). The mktemp template
	// resolves under /models (not /tmp).
	if !strings.Contains(prefetchScript, `seed_tmpdir="$OLLAYA_MODELS/.seed-tmp"`) {
		t.Error("prefetchScript: the seed temp dir must be $OLLAYA_MODELS/.seed-tmp")
	}
	if !strings.Contains(prefetchScript, `mktemp "$seed_tmpdir/seed.XXXXXX"`) {
		t.Error("prefetchScript: the seed mktemp must use the $seed_tmpdir template under the store")
	}
	// The temp must NOT be created inside the manifests tag directory.
	if strings.Contains(prefetchScript, `mktemp "$seed_dir/`) {
		t.Error("prefetchScript: the seed temp must not be created inside the manifests tag dir ($seed_dir)")
	}
}

// mktempTemplateRE matches a mktemp whose template is a quoted shell variable
// (e.g. mktemp "$seed_dir/.seed.XXXXXX") — i.e. an explicit path, never a bare
// mktemp that would default to TMPDIR.
var mktempTemplateRE = regexp.MustCompile(`mktemp\s+"\$`)

// TestSeedWritesInsideStoreUnderReadOnlyTmp runs the real rendered prefetch
// script with TMPDIR pointing at a read-only directory and a writable store
// mount, proving the seed temp file is created inside the store (not TMPDIR). A
// regression to a bare `mktemp` would try the read-only TMPDIR and fail here.
// A stub `ollaya` on PATH makes the pull a no-op; no network.
func TestSeedWritesInsideStoreUnderReadOnlyTmp(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(manifest)
	js := New().PrefetchJobSpec(p)
	c := js.Template.Spec.Containers[0]
	script := c.Args[0]

	// A writable store standing in for the /models RW mount.
	store := t.TempDir()

	// A read-only TMPDIR: a bare mktemp would default here and fail.
	roTmp := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(roTmp, 0o500); err != nil {
		t.Fatalf("mkdir ro tmp: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roTmp, 0o700) }) // so t.TempDir cleanup can remove it

	// A stub `ollaya` that writes the expected manifest to MANIFEST_PATH (so the
	// post-pull digest check passes) and exits 0 — stands in for a real pull.
	bin := t.TempDir()
	stub := "#!/bin/sh\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "ollaya"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	// Build the env the Job would set, overriding OLLAYA_MODELS to the writable
	// store and MANIFEST_PATH to a relative path under it.
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	env["OLLAYA_MODELS"] = store
	env["MANIFEST_PATH"] = "manifests/ollaya.dev/library/laya/en"

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TMPDIR="+roTmp,
	)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("prefetch script failed under read-only TMPDIR: %v\n%s", err, out)
	}

	// The seed must have been written to its destination inside the store.
	dest := filepath.Join(store, env["MANIFEST_PATH"])
	if _, statErr := os.Stat(dest); statErr != nil {
		t.Errorf("seed manifest not written to the store destination %q: %v\noutput:\n%s", dest, statErr, out)
	}
	// Nothing left in the manifests tag directory except the manifest itself — a
	// stray file there would make `ollaya list` fail (verified on 0.12.0).
	entries, _ := os.ReadDir(filepath.Dir(dest))
	for _, e := range entries {
		if e.Name() != filepath.Base(dest) {
			t.Errorf("unexpected file %q left in the manifests tag dir (would break ollaya list)", e.Name())
		}
	}
	// The hidden .seed-tmp dir is removed after a successful seed.
	if _, statErr := os.Stat(filepath.Join(store, ".seed-tmp")); !os.IsNotExist(statErr) {
		t.Errorf(".seed-tmp must be cleaned after a successful seed (stat err = %v)", statErr)
	}
}

// TestSeedStaleTempCleanup pins that a stale temp from a crashed run (left in
// $OLLAYA_MODELS/.seed-tmp/) is removed at the start of the next seed, so it
// never accumulates. Runs the real script with a pre-existing stale temp dir.
func TestSeedStaleTempCleanup(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(manifest)
	script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]

	store := t.TempDir()
	manifestPath := "manifests/ollaya.dev/library/laya/en"
	// A stale temp dir with a leftover file, as a crashed run would leave.
	staleDir := filepath.Join(store, ".seed-tmp")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("mkdir stale dir: %v", err)
	}
	stale := filepath.Join(staleDir, "seed.deadbe")
	if err := os.WriteFile(stale, []byte("junk"), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ollaya"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OLLAYA_MODELS="+store,
		"MANIFEST_PATH="+manifestPath,
	)
	for _, e := range New().PrefetchJobSpec(p).Template.Spec.Containers[0].Env {
		if e.Name == "MANIFEST_SEED_B64" || e.Name == "EXPECT_DIGEST" || e.Name == "MODEL" {
			cmd.Env = append(cmd.Env, e.Name+"="+e.Value)
		}
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("script failed: %v\n%s", err, out)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale temp was not cleaned up (stat err = %v)", err)
	}
	// manifests tag dir must hold only the manifest (no stray temp).
	tagDir := filepath.Join(store, filepath.Dir(manifestPath))
	entries, _ := os.ReadDir(tagDir)
	for _, e := range entries {
		if e.Name() != "en" {
			t.Errorf("unexpected file %q in the manifests tag dir", e.Name())
		}
	}
}

// Guard that the Job's prefetch container really mounts the store RW (no
// ReadOnly), so writing the seed there is valid; a regression to a RO store
// mount would make the seed fail and should fail this instead.
func TestPrefetchStoreMountIsWritable(t *testing.T) {
	js := New().PrefetchJobSpec(seedParams([]byte(`{"schemaVersion":2}`)))
	var saw bool
	for _, m := range js.Template.Spec.Containers[0].VolumeMounts {
		if m.Name == modelsVolume && m.MountPath == modelsMount {
			saw = true
			if m.ReadOnly {
				t.Errorf("prefetch store mount %q must be RW (the seed and pull write there)", modelsMount)
			}
		}
	}
	if !saw {
		t.Fatalf("no models mount at %q on the prefetch container", modelsMount)
	}
}

// TestFileSeedWritesInsideStore runs the real rendered script for the ConfigMap
// (file) seed path: MANIFEST_SEED_FILE points at a file on disk (standing in for
// the read-only ConfigMap mount), TMPDIR is read-only, and the writable store is
// a temp dir. The seed must be verified and written to the manifest path, with no
// MANIFEST_SEED_B64 involved and nothing left in the tag dir or .seed-tmp.
func TestFileSeedWritesInsideStore(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(manifest)
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
		Key:                  "manifest",
	}
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	script := c.Args[0]

	store := t.TempDir()

	// Read-only TMPDIR: a bare mktemp would default here and fail.
	roTmp := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(roTmp, 0o500); err != nil {
		t.Fatalf("mkdir ro tmp: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roTmp, 0o700) })

	// The mounted manifest file (what the ConfigMap volume would project).
	seedMount := t.TempDir()
	seedFile := filepath.Join(seedMount, "manifest")
	if err := os.WriteFile(seedFile, manifest, 0o444); err != nil {
		t.Fatalf("write seed file: %v", err)
	}

	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ollaya"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	env["OLLAYA_MODELS"] = store
	env["MANIFEST_PATH"] = "manifests/ollaya.dev/library/laya/en"
	env["MANIFEST_SEED_FILE"] = seedFile // override the in-container mount path

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TMPDIR="+roTmp,
	)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("file-seed script failed under read-only TMPDIR: %v\n%s", err, out)
	}

	dest := filepath.Join(store, env["MANIFEST_PATH"])
	got, statErr := os.ReadFile(dest)
	if statErr != nil {
		t.Fatalf("seed not written to %q: %v\noutput:\n%s", dest, statErr, out)
	}
	if string(got) != string(manifest) {
		t.Errorf("seeded manifest = %q, want %q", got, manifest)
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	for _, e := range entries {
		if e.Name() != filepath.Base(dest) {
			t.Errorf("unexpected file %q left in the manifests tag dir", e.Name())
		}
	}
	if _, statErr := os.Stat(filepath.Join(store, ".seed-tmp")); !os.IsNotExist(statErr) {
		t.Errorf(".seed-tmp must be cleaned after a successful file seed (stat err = %v)", statErr)
	}
}

// TestFileSeedRefusesUnreadableFile pins that a missing/unreadable seed file is a
// permanent SeedUnavailable failure — its own reason, distinct from a registry
// serving different bytes (DigestMismatch). The ConfigMap is required; a missing
// one must not be silently skipped. ollaya is NOT stubbed, so a regression that
// fell through to pull would error differently; here the script must fail before
// any pull.
func TestFileSeedRefusesUnreadableFile(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(manifest)
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
		Key:                  "manifest",
	}
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	script := c.Args[0]

	store := t.TempDir()
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	env["OLLAYA_MODELS"] = store
	env["MANIFEST_PATH"] = "manifests/ollaya.dev/library/laya/en"
	env["MANIFEST_SEED_FILE"] = filepath.Join(store, "does-not-exist")

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("script must fail when the seed file is unreadable; output:\n%s", out)
	}
	if !strings.Contains(string(out), "reason: "+PrefetchReasonSeedUnavailable) {
		t.Errorf("expected a SeedUnavailable reason for an unreadable seed file; output:\n%s", out)
	}
}
