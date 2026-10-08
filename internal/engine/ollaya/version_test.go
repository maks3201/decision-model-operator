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
	"errors"
	"regexp"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

func TestValidateRuntimeVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{"empty is default (valid)", "", false},
		{"default version", "0.12.0", false},
		{"minimum version", "0.7.3", false},
		{"newer version", "1.2.3", false},
		{"patch above minimum", "0.7.4", false},
		{"v-prefixed rejected", "v0.10.0", true},
		{"two-component rejected", "0.10", true},
		{"tag word rejected", "latest", true},
		{"shell injection rejected", "0.10.0;rm", true},
		{"suffix rejected", "0.10.0-cuda", true},
		{"below minimum rejected", "0.7.2", true},
		{"way below minimum rejected", "0.1.0", true},
		{"non-numeric rejected", "a.b.c", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRuntimeVersion(tt.version)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ValidateRuntimeVersion(%q) = nil, want error", tt.version)
				}
				var ive *InvalidRuntimeVersionError
				if !errors.As(err, &ive) {
					t.Errorf("error type = %T, want *InvalidRuntimeVersionError", err)
				}
				return
			}
			if err != nil {
				t.Errorf("ValidateRuntimeVersion(%q) = %v, want nil", tt.version, err)
			}
		})
	}
}

func TestImageForParamsVersion(t *testing.T) {
	const repo = "ghcr.io/ollaya-dev/ollaya"
	// The default version is digest-pinned; build the expected pinned refs from
	// the same table the engine uses so the test tracks a digest refresh.
	wantDefaultCPU := repo + ":" + DefaultRuntimeVersion + "@" + runtimeImageDigests[DefaultRuntimeVersion].cpu
	wantDefaultCUDA := repo + ":" + DefaultRuntimeVersion + "-cuda@" + runtimeImageDigests[DefaultRuntimeVersion].cuda
	tests := []struct {
		name   string
		params engine.Params
		want   string
	}{
		{
			name:   "default cpu is digest-pinned",
			params: engine.Params{Device: engine.DeviceCPU},
			want:   wantDefaultCPU,
		},
		{
			name:   "default cuda is digest-pinned",
			params: engine.Params{Device: engine.DeviceCUDA},
			want:   wantDefaultCUDA,
		},
		{
			name:   "explicit version without a known digest renders by tag (cpu)",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "0.9.0"},
			want:   repo + ":0.9.0",
		},
		{
			name:   "explicit version without a known digest renders by tag (cuda)",
			params: engine.Params{Device: engine.DeviceCUDA, RuntimeVersion: "0.9.0"},
			want:   repo + ":0.9.0-cuda",
		},
		{
			name:   "minimum version cpu is digest-pinned",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: MinRuntimeVersion},
			want:   repo + ":" + MinRuntimeVersion + "@" + runtimeImageDigests[MinRuntimeVersion].cpu,
		},
		{
			name:   "image override wins over version",
			params: engine.Params{Device: engine.DeviceCUDA, RuntimeVersion: "0.9.0", Image: customImage},
			want:   customImage,
		},
		{
			name:   "invalid version falls back to the (pinned) default (defence in depth)",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "latest"},
			want:   wantDefaultCPU,
		},
		{
			name:   "below-minimum version falls back to the (pinned) default",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "0.7.2"},
			want:   wantDefaultCPU,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := imageFor(tt.params); got != tt.want {
				t.Errorf("imageFor = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestServingPodImageFromVersion(t *testing.T) {
	p := baseParams()
	p.RuntimeVersion = "0.9.0"
	c := New().ServingPodSpec(p).Containers[0]
	if c.Image != "ghcr.io/ollaya-dev/ollaya:0.9.0" {
		t.Errorf("serving image = %q, want pinned 0.9.0", c.Image)
	}
}

func TestPrefetchImageUsesSameVersionCPU(t *testing.T) {
	// A cuda serving revision still prefetches with the CPU image of the SAME
	// runtime version (pulling needs no GPU).
	p := baseParams()
	p.Device = engine.DeviceCUDA
	p.RuntimeVersion = "0.9.0"
	c := New().PrefetchJobSpec(p).Template.Spec.Containers[0]
	if c.Image != "ghcr.io/ollaya-dev/ollaya:0.9.0" {
		t.Errorf("prefetch image = %q, want CPU image of 0.9.0", c.Image)
	}
}

func TestResolvedRuntimeVersion(t *testing.T) {
	tests := []struct {
		name   string
		params engine.Params
		want   string
	}{
		{"default when unset", engine.Params{}, DefaultRuntimeVersion},
		{"explicit version", engine.Params{RuntimeVersion: "0.9.0"}, "0.9.0"},
		{"unknown when image overridden", engine.Params{RuntimeVersion: "0.9.0", Image: customImage}, ""},
		{"unknown when only image set", engine.Params{Image: customImage}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolvedRuntimeVersion(tt.params); got != tt.want {
				t.Errorf("ResolvedRuntimeVersion = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultImagesDerivedFromDefaultVersion(t *testing.T) {
	// The default images carry the default version's tag AND its index digest.
	wantCPU := "ghcr.io/ollaya-dev/ollaya:" + DefaultRuntimeVersion + "@" + runtimeImageDigests[DefaultRuntimeVersion].cpu
	wantCUDA := "ghcr.io/ollaya-dev/ollaya:" + DefaultRuntimeVersion + "-cuda@" + runtimeImageDigests[DefaultRuntimeVersion].cuda
	if DefaultImageCPU != wantCPU {
		t.Errorf("DefaultImageCPU = %q, want %q", DefaultImageCPU, wantCPU)
	}
	if DefaultImageCUDA != wantCUDA {
		t.Errorf("DefaultImageCUDA = %q, want %q", DefaultImageCUDA, wantCUDA)
	}
}

// TestDefaultImagesArePinned fails if a future DefaultRuntimeVersion is set
// without a digest entry for it (CPU and CUDA). This guards the invariant that
// the engine's default runtime image is always rendered by an immutable digest:
// if someone bumps DefaultRuntimeVersion without adding its index digests to
// runtimeImageDigests, this test (and TestDefaultImagesDerivedFromDefaultVersion)
// catches it before the default silently falls back to a mutable tag.
func TestDefaultImagesArePinned(t *testing.T) {
	d, ok := runtimeImageDigests[DefaultRuntimeVersion]
	if !ok {
		t.Fatalf("DefaultRuntimeVersion %q has no entry in runtimeImageDigests; add its CPU and CUDA index digests", DefaultRuntimeVersion)
	}
	if d.cpu == "" {
		t.Errorf("DefaultRuntimeVersion %q has no CPU index digest", DefaultRuntimeVersion)
	}
	if d.cuda == "" {
		t.Errorf("DefaultRuntimeVersion %q has no CUDA index digest", DefaultRuntimeVersion)
	}
	// Digests must be the sha256:<64 hex> form the pull path expects.
	digestRE := regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	if !digestRE.MatchString(d.cpu) {
		t.Errorf("CPU digest %q is not sha256:<64 hex>", d.cpu)
	}
	if !digestRE.MatchString(d.cuda) {
		t.Errorf("CUDA digest %q is not sha256:<64 hex>", d.cuda)
	}
	e := New()
	if !e.RuntimeImagePinned("", engine.DeviceCPU) {
		t.Errorf("RuntimeImagePinned(default, cpu) = false, want true")
	}
	if !e.RuntimeImagePinned("", engine.DeviceCUDA) {
		t.Errorf("RuntimeImagePinned(default, cuda) = false, want true")
	}
}

// TestRuntimeImagePinned covers engine.RuntimeImagePinner: the default and any
// version in the digest table report true; a valid version without a digest, and
// an invalid version (which renders as the pinned default), report accordingly.
func TestRuntimeImagePinned(t *testing.T) {
	e := New()
	tests := []struct {
		name    string
		version string
		device  string
		want    bool
	}{
		{"default empty cpu", "", engine.DeviceCPU, true},
		{"default empty cuda", "", engine.DeviceCUDA, true},
		{"known version cpu", DefaultRuntimeVersion, engine.DeviceCPU, true},
		{"known version cuda", DefaultRuntimeVersion, engine.DeviceCUDA, true},
		{"valid version without a digest cpu", "0.9.0", engine.DeviceCPU, false},
		{"valid version without a digest cuda", "0.9.0", engine.DeviceCUDA, false},
		{"minimum version is pinned", MinRuntimeVersion, engine.DeviceCPU, true},
		// Invalid -> renders as the (pinned) default -> pinned.
		{"invalid version renders as pinned default", "latest", engine.DeviceCPU, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.RuntimeImagePinned(tt.version, tt.device); got != tt.want {
				t.Errorf("RuntimeImagePinned(%q, %q) = %v, want %v", tt.version, tt.device, got, tt.want)
			}
		})
	}
}

// verifiedVersions are the runtime releases the project documents as verified
// (spikes 001/006/008) and must therefore render by digest, so a Pinned
// candidate recorded under any of them is accepted once the controller enforces
// pinning. They all lie within [MinRuntimeVersion, DefaultRuntimeVersion].
var verifiedVersions = []string{"0.7.3", "0.10.0", "0.11.0", "0.12.0"}

// TestVerifiedVersionsArePinned asserts that every verified version has both a
// CPU and a CUDA index digest and that RuntimeImagePinned reports true for both
// devices. This is the guard the task asks for: a verified version that is left
// out of runtimeImageDigests (or loses a device digest) fails here, before a
// Pinned DecisionModel created under it would be refused on upgrade.
func TestVerifiedVersionsArePinned(t *testing.T) {
	e := New()
	digestRE := regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	for _, v := range verifiedVersions {
		t.Run(v, func(t *testing.T) {
			// Within the accepted range.
			if compareVersions(v, MinRuntimeVersion) < 0 || compareVersions(v, DefaultRuntimeVersion) > 0 {
				t.Fatalf("verified version %q is outside [%s, %s]", v, MinRuntimeVersion, DefaultRuntimeVersion)
			}
			d, ok := runtimeImageDigests[v]
			if !ok {
				t.Fatalf("verified version %q has no runtimeImageDigests entry", v)
			}
			if !digestRE.MatchString(d.cpu) {
				t.Errorf("verified version %q CPU digest %q is not sha256:<64 hex>", v, d.cpu)
			}
			if !digestRE.MatchString(d.cuda) {
				t.Errorf("verified version %q CUDA digest %q is not sha256:<64 hex>", v, d.cuda)
			}
			if !e.RuntimeImagePinned(v, engine.DeviceCPU) {
				t.Errorf("RuntimeImagePinned(%q, cpu) = false, want true", v)
			}
			if !e.RuntimeImagePinned(v, engine.DeviceCUDA) {
				t.Errorf("RuntimeImagePinned(%q, cuda) = false, want true", v)
			}
		})
	}
}

func TestRuntimeVersionerDefaultAndValidate(t *testing.T) {
	e := New()
	if e.DefaultRuntimeVersion() != DefaultRuntimeVersion {
		t.Errorf("DefaultRuntimeVersion() = %q, want %q", e.DefaultRuntimeVersion(), DefaultRuntimeVersion)
	}
	// Delegates to the package validator.
	if err := e.ValidateRuntimeVersion("0.10.0"); err != nil {
		t.Errorf("ValidateRuntimeVersion(0.10.0) = %v, want nil", err)
	}
	if err := e.ValidateRuntimeVersion("latest"); err == nil {
		t.Errorf("ValidateRuntimeVersion(latest) = nil, want error")
	}
	if err := e.ValidateRuntimeVersion(""); err != nil {
		t.Errorf("ValidateRuntimeVersion(\"\") = %v, want nil (empty is valid)", err)
	}
}

func TestCompareRuntimeVersions(t *testing.T) {
	e := New()
	tests := []struct {
		a, b string
		want int
	}{
		{"0.7.3", "0.10.0", -1},
		{"0.10.0", "0.7.3", 1},
		{"0.10.0", "0.10.0", 0},
		{"1.2.3", "1.2.4", -1},
		{"1.3.0", "1.2.9", 1},
		{"2.0.0", "1.9.9", 1},
		// An invalid or below-minimum version is "unknown" -> compares equal (0).
		{"latest", "0.10.0", 0},
		{"0.10.0", "latest", 0},
		{"0.7.2", "0.10.0", 0}, // below minimum -> unknown
		{"", "0.10.0", 0},      // empty is valid-but-not-a-number here -> unknown
	}
	for _, tt := range tests {
		if got := e.CompareRuntimeVersions(tt.a, tt.b); got != tt.want {
			t.Errorf("CompareRuntimeVersions(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestRuntimeVersionFromImage(t *testing.T) {
	e := New()
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{"cpu default tag", "ghcr.io/ollaya-dev/ollaya:0.10.0", "0.10.0"},
		{"cuda default tag", "ghcr.io/ollaya-dev/ollaya:0.10.0-cuda", "0.10.0"},
		{"older cpu tag", "ghcr.io/ollaya-dev/ollaya:0.7.3", "0.7.3"},
		{"digest-pinned cpu", "ghcr.io/ollaya-dev/ollaya:0.12.0@sha256:f79e865fda7af45aa66617b85fe16b08688d27f83a3137a21c75d3b137d8cf39", "0.12.0"},
		{"digest-pinned cuda", "ghcr.io/ollaya-dev/ollaya:0.12.0-cuda@sha256:ee3c316db37b1dfc828bd8bf5db1d269c98e49ad4918cf2e18748292e1f178e7", "0.12.0"},
		{"empty image", "", ""},
		{"other repository", "docker.io/library/ollaya:0.10.0", ""},
		{"mirror of the image", "mirror.corp/ollaya-dev/ollaya:0.10.0", ""},
		{"digest-only reference", "ghcr.io/ollaya-dev/ollaya@sha256:abc", ""},
		{"non-version tag", "ghcr.io/ollaya-dev/ollaya:latest", ""},
		{"below-minimum tag", "ghcr.io/ollaya-dev/ollaya:0.7.2", ""},
		{"cuda12 suffix not stripped", "ghcr.io/ollaya-dev/ollaya:0.10.0-cuda12", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.RuntimeVersionFromImage(tt.image); got != tt.want {
				t.Errorf("RuntimeVersionFromImage(%q) = %q, want %q", tt.image, got, tt.want)
			}
		})
	}
}

// TestRuntimeVersionFromImageRoundTrip pins that an image the engine itself
// builds round-trips back to the version it was built from.
func TestRuntimeVersionFromImageRoundTrip(t *testing.T) {
	e := New()
	for _, ver := range []string{"0.7.3", "0.10.0", "1.2.3", DefaultRuntimeVersion} {
		for _, dev := range []string{engine.DeviceCPU, engine.DeviceCUDA} {
			img := imageForVersion(ver, dev)
			if got := e.RuntimeVersionFromImage(img); got != ver {
				t.Errorf("RuntimeVersionFromImage(%q) = %q, want %q", img, got, ver)
			}
		}
	}
}
