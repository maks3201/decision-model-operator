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
)

func seedFileEnv(env []corev1.EnvVar) (string, bool) {
	for _, e := range env {
		if e.Name == "MANIFEST_SEED_FILE" {
			return e.Value, true
		}
	}
	return "", false
}

func volumeByName(vols []corev1.Volume, name string) (corev1.Volume, bool) {
	for _, v := range vols {
		if v.Name == name {
			return v, true
		}
	}
	return corev1.Volume{}, false
}

func mountByName(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, m := range mounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

// When ManifestConfigMap is set, the Job mounts the key read-only and points the
// script at the file; it must NOT also carry the bytes in an env var.
func TestPrefetchSeedFromConfigMap(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	p := seedParams(manifest)
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "support-router-manifest-abcdef0123"},
		Key:                  "manifest.json",
	}
	js := New().PrefetchJobSpec(p)
	c := js.Template.Spec.Containers[0]

	// env: MANIFEST_SEED_FILE set to the mount path, MANIFEST_SEED_B64 absent.
	path, ok := seedFileEnv(c.Env)
	if !ok {
		t.Fatalf("MANIFEST_SEED_FILE must be set when ManifestConfigMap is given")
	}
	if want := manifestSeedMount + "/" + manifestSeedFile; path != want {
		t.Errorf("MANIFEST_SEED_FILE = %q, want %q", path, want)
	}
	if _, ok := seedEnv(c.Env); ok {
		t.Error("MANIFEST_SEED_B64 must not be set when seeding from a ConfigMap")
	}

	// mount: read-only at the mount path.
	m, ok := mountByName(c.VolumeMounts, manifestSeedVolume)
	if !ok {
		t.Fatalf("missing volume mount %q", manifestSeedVolume)
	}
	if m.MountPath != manifestSeedMount {
		t.Errorf("mount path = %q, want %q", m.MountPath, manifestSeedMount)
	}
	if !m.ReadOnly {
		t.Error("manifest seed mount must be read-only")
	}

	// volume: a configMap volume projecting exactly the one key to a fixed path,
	// defaultMode 0444, Optional=false (required).
	v, ok := volumeByName(js.Template.Spec.Volumes, manifestSeedVolume)
	if !ok {
		t.Fatalf("missing volume %q", manifestSeedVolume)
	}
	cm := v.ConfigMap
	if cm == nil {
		t.Fatalf("volume %q must be a configMap source", manifestSeedVolume)
	}
	if cm.Name != p.ManifestConfigMap.Name {
		t.Errorf("configMap name = %q, want %q", cm.Name, p.ManifestConfigMap.Name)
	}
	if len(cm.Items) != 1 || cm.Items[0].Key != p.ManifestConfigMap.Key || cm.Items[0].Path != manifestSeedFile {
		t.Errorf("configMap items = %+v, want one {Key:%q Path:%q}", cm.Items, p.ManifestConfigMap.Key, manifestSeedFile)
	}
	if cm.DefaultMode == nil || *cm.DefaultMode != 0o444 {
		t.Errorf("defaultMode = %v, want 0444", cm.DefaultMode)
	}
	if cm.Optional == nil || *cm.Optional {
		t.Error("configMap must be required (Optional=false) so a missing manifest is not silently skipped")
	}
}

// A manifest too large for the env var but with no ConfigMap gets no seed and
// falls back to pull-by-tag (no MANIFEST_SEED_* env, no seed volume).
func TestPrefetchNoSeedWhenEnvCapExceededAndNoConfigMap(t *testing.T) {
	big := make([]byte, maxEnvSeedManifestBytes+1) // over the env cap, under the overall cap
	js := New().PrefetchJobSpec(seedParams(big))
	c := js.Template.Spec.Containers[0]
	if _, ok := seedEnv(c.Env); ok {
		t.Error("MANIFEST_SEED_B64 must not be set above the env cap")
	}
	if _, ok := seedFileEnv(c.Env); ok {
		t.Error("MANIFEST_SEED_FILE must not be set without a ConfigMap")
	}
	if _, ok := volumeByName(js.Template.Spec.Volumes, manifestSeedVolume); ok {
		t.Error("no seed volume without a ConfigMap")
	}
}

// But that same large manifest IS seeded via a ConfigMap (no env size limit).
func TestPrefetchConfigMapSeedsAboveEnvCap(t *testing.T) {
	big := make([]byte, maxEnvSeedManifestBytes+1)
	p := seedParams(big)
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
		Key:                  "manifest",
	}
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	if _, ok := seedFileEnv(c.Env); !ok {
		t.Error("MANIFEST_SEED_FILE must be set for a ConfigMap seed regardless of size")
	}
}

// The env-cap boundary: exactly at the cap seeds via env; one byte over does not
// (with no ConfigMap).
func TestPrefetchEnvSeedCapBoundary(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantEnv bool
	}{
		{"at cap", maxEnvSeedManifestBytes, true},
		{"one over cap", maxEnvSeedManifestBytes + 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := New().PrefetchJobSpec(seedParams(make([]byte, tt.size)))
			if _, ok := seedEnv(js.Template.Spec.Containers[0].Env); ok != tt.wantEnv {
				t.Errorf("MANIFEST_SEED_B64 present = %v, want %v (size %d)", ok, tt.wantEnv, tt.size)
			}
		})
	}
}

// A refuse script (unparsable name / bad sub-path) must never mount a seed
// ConfigMap or set a seed env, even if the controller passed one.
func TestPrefetchNoSeedConfigMapWithRefuseScript(t *testing.T) {
	p := seedParams([]byte(`{"schemaVersion":2}`))
	p.Model.Name = "not a valid name" // forces unparsableNameScript
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
		Key:                  "manifest",
	}
	js := New().PrefetchJobSpec(p)
	if _, ok := seedFileEnv(js.Template.Spec.Containers[0].Env); ok {
		t.Error("a refuse script must not carry MANIFEST_SEED_FILE")
	}
	if _, ok := volumeByName(js.Template.Spec.Volumes, manifestSeedVolume); ok {
		t.Error("a refuse script must not mount the seed ConfigMap")
	}
}

// The rendered script, when seeding from a file, reads the file, verifies the
// digest before writing, and refuses an unreadable file.
func TestPrefetchScriptFileSeedBranch(t *testing.T) {
	p := seedParams([]byte(`{"schemaVersion":2}`))
	p.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
		Key:                  "manifest",
	}
	script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]
	wants := []string{
		`MANIFEST_SEED_FILE`,        // the file source is handled
		`-r "$MANIFEST_SEED_FILE"`,  // readability guard
		`cat "$MANIFEST_SEED_FILE"`, // read the mounted file
		`seed_digest=`,              // verify
		`!= "$EXPECT_DIGEST"`,       // before writing
		`mv "$seed_tmp"`,            // atomic write to the tag path
		`ollaya pull -- "$MODEL"`,   // then pull
		// An unreadable/missing seed file is its own permanent reason, not a
		// digest mismatch (exit 6 / SeedUnavailable).
		"fail " + PrefetchReasonSeedUnavailable + " " + "6",
	}
	for _, w := range wants {
		if !strings.Contains(script, w) {
			t.Errorf("prefetch script missing %q", w)
		}
	}
	// The file read must come before the pull.
	if strings.Index(script, `cat "$MANIFEST_SEED_FILE"`) > strings.Index(script, `ollaya pull -- "$MODEL"`) {
		t.Error("the file seed must be written before ollaya pull")
	}
}
