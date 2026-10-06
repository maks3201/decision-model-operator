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
		{"default version", "0.10.0", false},
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
	tests := []struct {
		name   string
		params engine.Params
		want   string
	}{
		{
			name:   "default cpu",
			params: engine.Params{Device: engine.DeviceCPU},
			want:   repo + ":" + DefaultRuntimeVersion,
		},
		{
			name:   "default cuda",
			params: engine.Params{Device: engine.DeviceCUDA},
			want:   repo + ":" + DefaultRuntimeVersion + "-cuda",
		},
		{
			name:   "explicit version cpu",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "0.9.0"},
			want:   repo + ":0.9.0",
		},
		{
			name:   "explicit version cuda",
			params: engine.Params{Device: engine.DeviceCUDA, RuntimeVersion: "0.9.0"},
			want:   repo + ":0.9.0-cuda",
		},
		{
			name:   "minimum version cpu",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: MinRuntimeVersion},
			want:   repo + ":" + MinRuntimeVersion,
		},
		{
			name:   "image override wins over version",
			params: engine.Params{Device: engine.DeviceCUDA, RuntimeVersion: "0.9.0", Image: customImage},
			want:   customImage,
		},
		{
			name:   "invalid version falls back to default (defence in depth)",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "latest"},
			want:   repo + ":" + DefaultRuntimeVersion,
		},
		{
			name:   "below-minimum version falls back to default",
			params: engine.Params{Device: engine.DeviceCPU, RuntimeVersion: "0.7.2"},
			want:   repo + ":" + DefaultRuntimeVersion,
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
	wantCPU := "ghcr.io/ollaya-dev/ollaya:" + DefaultRuntimeVersion
	wantCUDA := wantCPU + "-cuda"
	if DefaultImageCPU != wantCPU {
		t.Errorf("DefaultImageCPU = %q, want %q", DefaultImageCPU, wantCPU)
	}
	if DefaultImageCUDA != wantCUDA {
		t.Errorf("DefaultImageCUDA = %q, want %q", DefaultImageCUDA, wantCUDA)
	}
}
