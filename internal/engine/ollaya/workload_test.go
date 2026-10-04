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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

const (
	customImage      = "example.com/custom/ollaya:dev"
	nameRegistryLaya = "registry.example.com/ns/laya:en"
	claimName        = "laya-cache"
)

func envMap(vars []corev1.EnvVar) map[string]corev1.EnvVar {
	m := make(map[string]corev1.EnvVar, len(vars))
	for _, v := range vars {
		m[v.Name] = v
	}
	return m
}

func baseParams() engine.Params {
	return engine.Params{
		Model:          engine.ModelRef{Name: modelLayaEn, Digest: "c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d"},
		Device:         engine.DeviceCPU,
		CacheClaimName: claimName,
	}
}

func TestServingPodSpecCPUContainer(t *testing.T) {
	e := New()
	spec := e.ServingPodSpec(baseParams())

	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(spec.Containers))
	}
	c := spec.Containers[0]
	if c.Name != "ollaya" {
		t.Errorf("container name = %q, want ollaya", c.Name)
	}
	if c.Image != DefaultImageCPU {
		t.Errorf("image = %q, want %q", c.Image, DefaultImageCPU)
	}
	if c.Command != nil || c.Args != nil {
		t.Errorf("serving container must not override command/args, got cmd=%v args=%v", c.Command, c.Args)
	}
	if len(c.Ports) != 1 || c.Ports[0].Name != "http" || c.Ports[0].ContainerPort != 11435 {
		t.Errorf("port = %+v, want http/11435", c.Ports)
	}
	if _, ok := c.Resources.Limits[gpuResourceName]; ok {
		t.Errorf("CPU serving must not request a GPU")
	}
}

func TestServingPodSpecCPUEnv(t *testing.T) {
	e := New()
	c := e.ServingPodSpec(baseParams()).Containers[0]
	env := envMap(c.Env)

	want := map[string]string{
		"OLLAYA_HOST":       "0.0.0.0:11435",
		"OLLAYA_MODELS":     modelsMount,
		"OLLAYA_DEVICE":     "cpu",
		"OLLAYA_KEEP_ALIVE": "-1",
	}
	for k, v := range want {
		if env[k].Value != v {
			t.Errorf("%s = %q, want %q", k, env[k].Value, v)
		}
	}
	if _, ok := env["OLLAYA_API_KEY"]; ok {
		t.Errorf("OLLAYA_API_KEY must be absent when APIKey is nil")
	}
}

func TestServingPodSpecProbes(t *testing.T) {
	e := New()
	c := e.ServingPodSpec(baseParams()).Containers[0]

	if c.StartupProbe == nil || c.StartupProbe.HTTPGet == nil || c.StartupProbe.HTTPGet.Path != "/" {
		t.Errorf("startup probe should GET /, got %+v", c.StartupProbe)
	}
	if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet == nil || c.LivenessProbe.HTTPGet.Path != "/" {
		t.Errorf("liveness probe should GET /, got %+v", c.LivenessProbe)
	}
	if c.ReadinessProbe != nil {
		t.Errorf("serving container must not set a readiness probe (gate handles it)")
	}
}

func TestServingPodSpecSecurity(t *testing.T) {
	e := New()
	spec := e.ServingPodSpec(baseParams())
	c := spec.Containers[0]

	if c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
		t.Errorf("readOnlyRootFilesystem must be true")
	}
	if c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation must be false")
	}
	if c.SecurityContext.Capabilities == nil || len(c.SecurityContext.Capabilities.Drop) != 1 || c.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("capabilities must drop ALL, got %+v", c.SecurityContext.Capabilities)
	}
	assertPodSecurity(t, spec.SecurityContext)
	if spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 30 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 30", spec.TerminationGracePeriodSeconds)
	}
}

func TestServingPodSpecVolumes(t *testing.T) {
	e := New()
	spec := e.ServingPodSpec(baseParams())
	c := spec.Containers[0]

	assertModelsMount(t, c.VolumeMounts, true)
	assertStateEmptyDir(t, spec.Volumes)
	mv := findVolume(t, spec.Volumes, "models")
	if mv.PersistentVolumeClaim == nil || mv.PersistentVolumeClaim.ClaimName != claimName || !mv.PersistentVolumeClaim.ReadOnly {
		t.Errorf("models volume = %+v, want PVC laya-cache readOnly", mv)
	}
}

func TestServingPodSpecCUDAAddsGPU(t *testing.T) {
	p := baseParams()
	p.Device = engine.DeviceCUDA
	e := New()
	c := e.ServingPodSpec(p).Containers[0]

	if c.Image != DefaultImageCUDA {
		t.Errorf("image = %q, want %q", c.Image, DefaultImageCUDA)
	}
	if envMap(c.Env)["OLLAYA_DEVICE"].Value != "cuda" {
		t.Errorf("OLLAYA_DEVICE = %q, want cuda", envMap(c.Env)["OLLAYA_DEVICE"].Value)
	}
	q, ok := c.Resources.Limits[gpuResourceName]
	if !ok || q.String() != "1" {
		t.Errorf("cuda serving must add %s=1, got %v", gpuResourceName, c.Resources.Limits)
	}
}

func TestServingPodSpecCUDAKeepsUserGPU(t *testing.T) {
	p := baseParams()
	p.Device = engine.DeviceCUDA
	p.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{gpuResourceName: resource.MustParse("2")},
	}
	e := New()
	c := e.ServingPodSpec(p).Containers[0]
	q := c.Resources.Limits[gpuResourceName]
	if q.String() != "2" {
		t.Errorf("must keep user's GPU value 2, got %v", q.String())
	}
}

func TestServingPodSpecCustomImageAndAPIKey(t *testing.T) {
	p := baseParams()
	p.Image = customImage
	p.APIKey = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "laya-key"},
		Key:                  "token",
	}
	e := New()
	c := e.ServingPodSpec(p).Containers[0]

	if c.Image != customImage {
		t.Errorf("custom image not used: %q", c.Image)
	}
	ak, ok := envMap(c.Env)["OLLAYA_API_KEY"]
	if !ok || ak.ValueFrom == nil || ak.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("OLLAYA_API_KEY must be sourced from a secretKeyRef, got %+v", ak)
	}
	if ak.ValueFrom.SecretKeyRef.Name != "laya-key" || ak.ValueFrom.SecretKeyRef.Key != "token" {
		t.Errorf("secretKeyRef = %+v, want laya-key/token", ak.ValueFrom.SecretKeyRef)
	}
}

func TestPrefetchJobSpec(t *testing.T) {
	e := New()
	js := e.PrefetchJobSpec(baseParams())

	if js.BackoffLimit == nil || *js.BackoffLimit != 4 {
		t.Errorf("backoffLimit = %v, want 4", js.BackoffLimit)
	}
	if js.ActiveDeadlineSeconds == nil || *js.ActiveDeadlineSeconds != 1800 {
		t.Errorf("activeDeadlineSeconds = %v, want 1800", js.ActiveDeadlineSeconds)
	}
	if js.TTLSecondsAfterFinished != nil {
		t.Errorf("ttlSecondsAfterFinished must be unset (controller GCs), got %v", *js.TTLSecondsAfterFinished)
	}

	spec := js.Template.Spec
	if spec.RestartPolicy != corev1.RestartPolicyOnFailure {
		t.Errorf("restartPolicy = %q, want OnFailure", spec.RestartPolicy)
	}
	assertPodSecurity(t, spec.SecurityContext)

	c := spec.Containers[0]
	if c.Image != DefaultImageCPU {
		t.Errorf("prefetch must use CPU image by default, got %q", c.Image)
	}
	if len(c.Command) != 2 || c.Command[0] != "/bin/sh" || c.Command[1] != "-c" {
		t.Errorf("command = %v, want [/bin/sh -c]", c.Command)
	}
	if len(c.Args) != 1 || !contains(c.Args[0], "ollaya pull") || !contains(c.Args[0], "sha256sum") {
		t.Errorf("args must pull then sha256sum, got %v", c.Args)
	}

	env := envMap(c.Env)
	if env["MODEL"].Value != modelLayaEn {
		t.Errorf("MODEL = %q", env["MODEL"].Value)
	}
	if env["EXPECT_DIGEST"].Value != baseParams().Model.Digest {
		t.Errorf("EXPECT_DIGEST = %q", env["EXPECT_DIGEST"].Value)
	}
	if env["OLLAYA_MODELS"].Value != modelsMount {
		t.Errorf("OLLAYA_MODELS = %q", env["OLLAYA_MODELS"].Value)
	}
	if env["MANIFEST_PATH"].Value != "manifests/ollaya.dev/library/laya/en" {
		t.Errorf("MANIFEST_PATH = %q, want manifests/ollaya.dev/library/laya/en", env["MANIFEST_PATH"].Value)
	}

	// models mounted RW (not readOnly); ollaya-state emptyDir present.
	assertModelsMount(t, c.VolumeMounts, false)
	assertStateEmptyDir(t, spec.Volumes)
	mv := findVolume(t, spec.Volumes, "models")
	if mv.PersistentVolumeClaim == nil || mv.PersistentVolumeClaim.ClaimName != claimName {
		t.Errorf("models volume PVC = %+v, want laya-cache", mv.PersistentVolumeClaim)
	}
	if mv.PersistentVolumeClaim.ReadOnly {
		t.Errorf("prefetch models PVC must be RW, got readOnly=true")
	}
}

func TestPrefetchJobSpecCUDAStillCPUImage(t *testing.T) {
	p := baseParams()
	p.Device = engine.DeviceCUDA
	e := New()
	c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
	if c.Image != DefaultImageCPU {
		t.Errorf("prefetch must use CPU image even for cuda device, got %q", c.Image)
	}
}

func TestPrefetchJobResources(t *testing.T) {
	// The prefetch container must not be BestEffort (resources: {}), or it is
	// the first to be evicted / OOM-killed on a busy node. It carries a small
	// guaranteed cpu+memory request and a memory limit with headroom; cpu is
	// requested but not limited. These defaults are the same for every param
	// variant (device, custom image, sub-path/prune), so assert across them.
	tests := []struct {
		name  string
		apply func(p *engine.Params)
	}{
		{"cpu default", func(*engine.Params) {}},
		{"cuda device", func(p *engine.Params) { p.Device = engine.DeviceCUDA }},
		{"custom image", func(p *engine.Params) { p.Image = customImage }},
		{"prune enabled", func(p *engine.Params) {
			p.StoreSubPath = "cccccccccc"
			p.KeepStoreSubPaths = []string{"aaaaaaaaaa", "cccccccccc"}
		}},
	}
	e := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseParams()
			tt.apply(&p)
			c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
			res := c.Resources

			cpuReq := res.Requests[corev1.ResourceCPU]
			if cpuReq.String() != "100m" {
				t.Errorf("cpu request = %q, want 100m", cpuReq.String())
			}
			memReq := res.Requests[corev1.ResourceMemory]
			if memReq.String() != "256Mi" {
				t.Errorf("memory request = %q, want 256Mi", memReq.String())
			}
			memLim := res.Limits[corev1.ResourceMemory]
			if memLim.String() != "1Gi" {
				t.Errorf("memory limit = %q, want 1Gi", memLim.String())
			}
			// CPU is intentionally not limited (download throughput).
			if _, ok := res.Limits[corev1.ResourceCPU]; ok {
				t.Errorf("prefetch must not set a cpu limit, got %v", res.Limits[corev1.ResourceCPU])
			}
			// Prefetch never requests a GPU (pulling needs none).
			if _, ok := res.Requests[gpuResourceName]; ok {
				t.Errorf("prefetch must not request a GPU")
			}
			if _, ok := res.Limits[gpuResourceName]; ok {
				t.Errorf("prefetch must not limit a GPU")
			}
		})
	}
}

func TestPrefetchJobSpecCustomImage(t *testing.T) {
	p := baseParams()
	p.Image = customImage
	e := New()
	c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
	if c.Image != customImage {
		t.Errorf("custom image not used: %q", c.Image)
	}
}

// TestPrefetchJobSpecRegistryEnv checks that `ollaya pull` is pointed at the
// same registry the resolver uses: the prefetch Job carries OLLAYA_REGISTRY
// only when the engine registry is non-default, and byte-identically to today
// (no such env) for the default registry. The CLI reads OLLAYA_REGISTRY for a
// bare name and an explicit host in the name overrides it (verified against the
// image; evidence in the the spike).
func TestPrefetchJobSpecRegistryEnv(t *testing.T) {
	tests := []struct {
		name        string
		registryURL string // "" -> engine default (no WithRegistryURL)
		wantEnv     string // "" -> OLLAYA_REGISTRY must be absent
	}{
		{name: "default registry has no OLLAYA_REGISTRY", registryURL: "", wantEnv: ""},
		{name: "explicit default host has no OLLAYA_REGISTRY", registryURL: "https://ollaya.dev", wantEnv: ""},
		{name: "default host trailing slash still default", registryURL: "https://ollaya.dev/", wantEnv: ""},
		{name: "private https mirror sets OLLAYA_REGISTRY", registryURL: "https://reg.internal:8443", wantEnv: "https://reg.internal:8443"},
		{name: "http mirror sets OLLAYA_REGISTRY (not default)", registryURL: "http://ollaya.dev", wantEnv: "http://ollaya.dev"},
		{name: "insecure local mirror sets OLLAYA_REGISTRY", registryURL: "http://localhost:5000", wantEnv: "http://localhost:5000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.registryURL == "" {
				e = New()
			} else {
				e = New(WithRegistryURL(tt.registryURL))
			}
			c := e.PrefetchJobSpec(baseParams()).Template.Spec.Containers[0]
			got, ok := envMap(c.Env)["OLLAYA_REGISTRY"]
			if tt.wantEnv == "" {
				if ok {
					t.Errorf("OLLAYA_REGISTRY must be absent for %q, got %q", tt.registryURL, got.Value)
				}
				return
			}
			if !ok {
				t.Fatalf("OLLAYA_REGISTRY must be present for %q", tt.registryURL)
			}
			if got.Value != tt.wantEnv {
				t.Errorf("OLLAYA_REGISTRY = %q, want %q", got.Value, tt.wantEnv)
			}
		})
	}
}

// TestPrefetchJobSpecDefaultRegistryByteIdentical pins that the default-registry
// Job env is exactly what it was before OLLAYA_REGISTRY was introduced: the four
// original vars and nothing registry-related.
func TestPrefetchJobSpecDefaultRegistryByteIdentical(t *testing.T) {
	c := New().PrefetchJobSpec(baseParams()).Template.Spec.Containers[0]
	for _, ev := range c.Env {
		if ev.Name == "OLLAYA_REGISTRY" {
			t.Fatalf("default registry Job must not set OLLAYA_REGISTRY")
		}
	}
}

func TestRegistryIsDefault(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		{"", true},
		{"https://ollaya.dev", true},
		{"https://ollaya.dev/", true},
		{"HTTPS://OLLAYA.DEV", true},
		{"http://ollaya.dev", false}, // insecure scheme is not the default
		{"https://reg.internal:8443", false},
		{"http://localhost:5000", false},
	}
	for _, tt := range tests {
		if got := registryIsDefault(tt.url); got != tt.want {
			t.Errorf("registryIsDefault(%q) = %v, want %v", tt.url, got, tt.want)
		}
	}
}

func TestPrefetchManifestPathVariants(t *testing.T) {
	tests := []struct {
		modelName string
		want      string
	}{
		{modelLayaEn, "manifests/ollaya.dev/library/laya/en"},
		{"acme/x:1", "manifests/ollaya.dev/acme/x/1"},
		// An explicit host with a port keeps the port as "host_port" on disk
		// (the ollaya CLI replaces ':' with '_'; verified in the image).
		{"localhost:5000/ns/m:t", "manifests/localhost_5000/ns/m/t"},
		{nameRegistryLaya, "manifests/registry.example.com/ns/laya/en"},
	}
	e := New()
	for _, tt := range tests {
		t.Run(tt.modelName, func(t *testing.T) {
			p := baseParams()
			p.Model = engine.ModelRef{Name: tt.modelName, Digest: "deadbeef"}
			c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
			got := envMap(c.Env)["MANIFEST_PATH"].Value
			if got != tt.want {
				t.Errorf("MANIFEST_PATH for %q = %q, want %q", tt.modelName, got, tt.want)
			}
		})
	}
}

// TestPrefetchManifestPathHostLessUsesRegistry pins the fix: for a
// host-LESS model name the on-disk manifest host comes from the engine's
// configured registry (where `ollaya pull` writes it under OLLAYA_REGISTRY),
// with ':' -> '_'; the default registry still renders manifests/ollaya.dev/...
func TestPrefetchManifestPathHostLessUsesRegistry(t *testing.T) {
	tests := []struct {
		name        string
		registryURL string // "" -> engine default
		want        string
	}{
		{"default registry", "", "manifests/ollaya.dev/library/laya/en"},
		{"explicit default host", "https://ollaya.dev", "manifests/ollaya.dev/library/laya/en"},
		{"https mirror with port", "https://reg.internal:8443", "manifests/reg.internal_8443/library/laya/en"},
		{"http mirror with port", "http://mirror:8080", "manifests/mirror_8080/library/laya/en"},
		{"mirror svc no port", "http://mirror.ns.svc", "manifests/mirror.ns.svc/library/laya/en"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.registryURL == "" {
				e = New()
			} else {
				e = New(WithRegistryURL(tt.registryURL))
			}
			p := baseParams() // model is host-less "laya:en"
			c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
			got := envMap(c.Env)["MANIFEST_PATH"].Value
			if got != tt.want {
				t.Errorf("MANIFEST_PATH = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestServingPodRegistryEnv pins that serving Pods get OLLAYA_REGISTRY only when
// the engine registry is non-default (so `ollaya serve` finds a host-less model
// under manifests/<mirror-host>/...), byte-identically absent for the default.
func TestServingPodRegistryEnv(t *testing.T) {
	tests := []struct {
		name        string
		registryURL string
		wantEnv     string // "" -> must be absent
	}{
		{"default registry: no env", "", ""},
		{"explicit default host: no env", "https://ollaya.dev", ""},
		{"https mirror sets env", "https://reg.internal:8443", "https://reg.internal:8443"},
		{"http mirror sets env", "http://mirror:8080", "http://mirror:8080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.registryURL == "" {
				e = New()
			} else {
				e = New(WithRegistryURL(tt.registryURL))
			}
			c := e.ServingPodSpec(baseParams()).Containers[0]
			got, ok := envMap(c.Env)["OLLAYA_REGISTRY"]
			if tt.wantEnv == "" {
				if ok {
					t.Errorf("OLLAYA_REGISTRY must be absent for %q, got %q", tt.registryURL, got.Value)
				}
				return
			}
			if !ok || got.Value != tt.wantEnv {
				t.Errorf("OLLAYA_REGISTRY = %q (present=%v), want %q", got.Value, ok, tt.wantEnv)
			}
		})
	}
}

func TestManifestHostDir(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"ollaya.dev", "ollaya.dev"},
		{"https://ollaya.dev", "ollaya.dev"},
		{"http://mirror:8080", "mirror_8080"},
		{"https://reg.internal:8443/", "reg.internal_8443"},
		{"mirror.ns.svc", "mirror.ns.svc"},
		{"localhost:5000", "localhost_5000"},
	}
	for _, tt := range tests {
		if got := manifestHostDir(tt.in); got != tt.want {
			t.Errorf("manifestHostDir(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPrefetchScriptSkipsWhenNoDigest(t *testing.T) {
	p := baseParams()
	p.Model = engine.ModelRef{Name: modelLayaEn} // empty digest
	e := New()
	c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
	if envMap(c.Env)["EXPECT_DIGEST"].Value != "" {
		t.Errorf("EXPECT_DIGEST must be empty when digest unset")
	}
	// The script must handle an empty EXPECT_DIGEST by skipping verification.
	if !contains(c.Args[0], `-z "${EXPECT_DIGEST:-}"`) {
		t.Errorf("script must skip verification on empty digest, got:\n%s", c.Args[0])
	}
}

// --- shared assertions ---

func assertPodSecurity(t *testing.T, sc *corev1.PodSecurityContext) {
	t.Helper()
	if sc == nil {
		t.Fatalf("pod securityContext is nil")
	}
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Errorf("runAsNonRoot must be true")
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 1000 {
		t.Errorf("runAsUser = %v, want 1000", sc.RunAsUser)
	}
	if sc.RunAsGroup == nil || *sc.RunAsGroup != 1000 {
		t.Errorf("runAsGroup = %v, want 1000", sc.RunAsGroup)
	}
	if sc.FSGroup == nil || *sc.FSGroup != 1000 {
		t.Errorf("fsGroup = %v, want 1000", sc.FSGroup)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("seccompProfile must be RuntimeDefault, got %+v", sc.SeccompProfile)
	}
}

func assertModelsMount(t *testing.T, mounts []corev1.VolumeMount, wantReadOnly bool) {
	t.Helper()
	for _, m := range mounts {
		if m.Name == "models" {
			if m.MountPath != modelsMount {
				t.Errorf("models mountPath = %q, want /models", m.MountPath)
			}
			if m.ReadOnly != wantReadOnly {
				t.Errorf("models mount readOnly = %v, want %v", m.ReadOnly, wantReadOnly)
			}
			return
		}
	}
	t.Errorf("no models volume mount found in %+v", mounts)
}

func assertStateEmptyDir(t *testing.T, vols []corev1.Volume) {
	t.Helper()
	v := findVolume(t, vols, "ollaya-state")
	if v.EmptyDir == nil {
		t.Errorf("ollaya-state must be an emptyDir, got %+v", v)
	}
}

func findVolume(t *testing.T, vols []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, v := range vols {
		if v.Name == name {
			return v
		}
	}
	t.Fatalf("volume %q not found in %+v", name, vols)
	return corev1.Volume{}
}

func TestServingPodSpecSubPathAndAutomount(t *testing.T) {
	p := baseParams()
	p.StoreSubPath = "abc123"
	e := New()
	spec := e.ServingPodSpec(p)

	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("AutomountServiceAccountToken must be false")
	}
	c := spec.Containers[0]
	for _, m := range c.VolumeMounts {
		if m.Name == "models" {
			if m.SubPath != "abc123" {
				t.Errorf("models mount subPath = %q, want abc123", m.SubPath)
			}
			if !m.ReadOnly {
				t.Errorf("serving models mount must be readOnly")
			}
			return
		}
	}
	t.Errorf("models mount not found")
}

func TestPrefetchJobSecurityE5(t *testing.T) {
	e := New()
	js := e.PrefetchJobSpec(baseParams())
	spec := js.Template.Spec
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("Job AutomountServiceAccountToken must be false")
	}
	sc := spec.Containers[0].SecurityContext
	if sc == nil || sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Errorf("Job container must set readOnlyRootFilesystem=true")
	}
}

func TestPrefetchJobUsesDashDash(t *testing.T) {
	e := New()
	c := e.PrefetchJobSpec(baseParams()).Template.Spec.Containers[0]
	if !contains(c.Args[0], `ollaya pull -- "$MODEL"`) {
		t.Errorf("prefetch must use `ollaya pull -- \"$MODEL\"`, got:\n%s", c.Args[0])
	}
}

func TestPrefetchJobUnparsableNameRefuses(t *testing.T) {
	p := baseParams()
	p.Model = engine.ModelRef{Name: "la*ya:bad", Digest: "deadbeef"}
	e := New()
	c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
	if contains(c.Args[0], "ollaya pull") {
		t.Errorf("unparsable name must NOT run ollaya pull, got:\n%s", c.Args[0])
	}
	if !contains(c.Args[0], "exit 1") {
		t.Errorf("unparsable name script must exit 1, got:\n%s", c.Args[0])
	}
	if envMap(c.Env)["MANIFEST_PATH"].Value != "" {
		t.Errorf("MANIFEST_PATH must be empty for an unparsable name")
	}
}

func TestPrefetchJobSubPathAndPrune(t *testing.T) {
	p := baseParams()
	p.StoreSubPath = "cccccccccc"
	p.KeepStoreSubPaths = []string{"aaaaaaaaaa", "cccccccccc"}
	e := New()
	spec := e.PrefetchJobSpec(p).Template.Spec
	c := spec.Containers[0]

	// models mount RW at /models with subPath = the revision hash.
	var sawModels, sawRoot bool
	for _, m := range c.VolumeMounts {
		if m.Name == "models" && m.MountPath == "/models" {
			sawModels = true
			if m.SubPath != "cccccccccc" {
				t.Errorf("models mount subPath = %q, want cccccccccc", m.SubPath)
			}
			if m.ReadOnly {
				t.Errorf("Job models mount must be RW")
			}
		}
		if m.Name == "models" && m.MountPath == "/store-root" {
			sawRoot = true
			if m.SubPath != "" {
				t.Errorf("store-root mount must have no subPath, got %q", m.SubPath)
			}
		}
	}
	if !sawModels || !sawRoot {
		t.Errorf("expected both a subPath models mount and a store-root mount; got %+v", c.VolumeMounts)
	}

	env := envMap(c.Env)
	if env["STORE_ROOT"].Value != "/store-root" {
		t.Errorf("STORE_ROOT = %q", env["STORE_ROOT"].Value)
	}
	if env["KEEP_SUBPATHS"].Value != "aaaaaaaaaa cccccccccc" {
		t.Errorf("KEEP_SUBPATHS = %q, want 'aaaaaaaaaa cccccccccc'", env["KEEP_SUBPATHS"].Value)
	}
}

func TestPrefetchJobEmptyKeepListNoPrune(t *testing.T) {
	// fix 1: an empty KeepStoreSubPaths must DISABLE pruning entirely,
	// even though this revision has a valid sub-path.
	p := baseParams()
	p.StoreSubPath = "cccccccccc"
	e := New()
	spec := e.PrefetchJobSpec(p).Template.Spec
	c := spec.Containers[0]
	env := envMap(c.Env)
	if _, ok := env["KEEP_SUBPATHS"]; ok {
		t.Errorf("empty keep list must not set KEEP_SUBPATHS")
	}
	if _, ok := env["STORE_ROOT"]; ok {
		t.Errorf("empty keep list must not set STORE_ROOT")
	}
	for _, m := range c.VolumeMounts {
		if m.MountPath == "/store-root" {
			t.Errorf("empty keep list must not add a store-root mount")
		}
	}
}

func TestPrefetchJobInvalidKeepEntryDisablesPrune(t *testing.T) {
	// fix 2: any invalid keep entry disables pruning entirely (not dropped).
	p := baseParams()
	p.StoreSubPath = "cccccccccc"
	p.KeepStoreSubPaths = []string{"aaaaaaaaaa", "manifests"} // "manifests" is not a hash
	e := New()
	env := envMap(e.PrefetchJobSpec(p).Template.Spec.Containers[0].Env)
	if _, ok := env["KEEP_SUBPATHS"]; ok {
		t.Errorf("an invalid keep entry must disable pruning, but KEEP_SUBPATHS was set")
	}
}

func TestPrefetchJobNoSubPathNoPrune(t *testing.T) {
	// Legacy: no StoreSubPath -> no store-root mount, no prune env.
	p := baseParams()
	p.KeepStoreSubPaths = []string{"aaaaaaaaaa"}
	e := New()
	spec := e.PrefetchJobSpec(p).Template.Spec
	env := envMap(spec.Containers[0].Env)
	if _, ok := env["KEEP_SUBPATHS"]; ok {
		t.Errorf("no subPath must not enable pruning")
	}
	for _, m := range spec.Containers[0].VolumeMounts {
		if m.MountPath == "/store-root" {
			t.Errorf("no store-root mount without a subPath")
		}
	}
}

func TestPrefetchJobInvalidSubPathRefuses(t *testing.T) {
	// a non-empty sub-path that is not a revision hash -> refuse.
	p := baseParams()
	p.StoreSubPath = "not-a-hash"
	e := New()
	c := e.PrefetchJobSpec(p).Template.Spec.Containers[0]
	if contains(c.Args[0], "ollaya pull") {
		t.Errorf("invalid sub-path must NOT run ollaya pull, got:\n%s", c.Args[0])
	}
	if !contains(c.Args[0], "exit 1") {
		t.Errorf("invalid sub-path script must exit 1, got:\n%s", c.Args[0])
	}
}

func TestServingPodSpecInvalidSubPathPassedThrough(t *testing.T) {
	// serving must NOT fall back to no-subPath for an invalid
	// sub-path; it passes the value through as given (the controller must
	// validate). This test documents that contract.
	p := baseParams()
	p.StoreSubPath = "not-a-hash"
	e := New()
	c := e.ServingPodSpec(p).Containers[0]
	for _, m := range c.VolumeMounts {
		if m.Name == "models" {
			if m.SubPath != "not-a-hash" {
				t.Errorf("serving must pass the sub-path through as given, got %q", m.SubPath)
			}
			return
		}
	}
	t.Errorf("models mount not found")
}

func TestPruneKeepList(t *testing.T) {
	tests := []struct {
		name        string
		sub         string
		keep        []string
		wantEnabled bool
		wantKeep    []string
	}{
		{"empty keep disables", "cccccccccc", nil, false, nil},
		{"invalid sub disables", "not-a-hash", []string{"aaaaaaaaaa"}, false, nil},
		{"empty sub disables", "", []string{"aaaaaaaaaa"}, false, nil},
		{"invalid keep entry disables", "cccccccccc", []string{"aaaaaaaaaa", "manifests"}, false, nil},
		{"valid, current added", "cccccccccc", []string{"aaaaaaaaaa"}, true, []string{"aaaaaaaaaa", "cccccccccc"}},
		{"valid, current already present, deduped", "cccccccccc", []string{"aaaaaaaaaa", "cccccccccc", "aaaaaaaaaa"}, true, []string{"aaaaaaaaaa", "cccccccccc"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keep, enabled := pruneKeepList(engine.Params{StoreSubPath: tt.sub, KeepStoreSubPaths: tt.keep})
			if enabled != tt.wantEnabled {
				t.Fatalf("enabled = %v, want %v", enabled, tt.wantEnabled)
			}
			if !tt.wantEnabled {
				return
			}
			if len(keep) != len(tt.wantKeep) {
				t.Fatalf("keep = %v, want %v", keep, tt.wantKeep)
			}
			for i := range keep {
				if keep[i] != tt.wantKeep[i] {
					t.Errorf("keep[%d] = %q, want %q", i, keep[i], tt.wantKeep[i])
				}
			}
		})
	}
}
