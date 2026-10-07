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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// withTimeouts overrides the per-method default deadlines. Test-only helper so
// the "no caller deadline" path can be exercised without waiting real seconds.
func withTimeouts(resolve, inspect, warmup, decide time.Duration) Option {
	return func(e *Engine) {
		e.timeouts = methodTimeouts{resolve: resolve, inspect: inspect, warmup: warmup, decide: decide}
	}
}

const (
	modelLayaEn  = "laya:en"
	repoOrgModel = "org/model"
	nameLaya     = "laya"
	tagLatest    = "latest"
	modelWord    = "model"
	registryDev  = "https://ollaya.dev"
)

func TestNameAndServicePort(t *testing.T) {
	e := New()
	if got := e.Name(); got != "ollaya" {
		t.Errorf("Name() = %q, want %q", got, "ollaya")
	}
	if got := e.ServicePort(); got != 11435 {
		t.Errorf("ServicePort() = %d, want %d", got, 11435)
	}
}

func TestParseName(t *testing.T) {
	tests := []struct {
		name          string
		in            string
		wantErr       bool
		wantHost      string
		wantNamespace string
		wantModel     string
		wantTag       string
		wantCanonical string
	}{
		// Spec §3 examples.
		{
			name: "bare name defaults latest in library", in: nameLaya,
			wantNamespace: defaultNamespace, wantModel: nameLaya, wantTag: tagLatest,
			wantCanonical: "laya:latest",
		},
		{
			name: "case is normalized to lower", in: "Laya:EN",
			wantNamespace: defaultNamespace, wantModel: nameLaya, wantTag: "en",
			wantCanonical: modelLayaEn,
		},
		{
			name: "namespaced name defaults latest", in: "acme/triage",
			wantNamespace: "acme", wantModel: "triage", wantTag: tagLatest,
			wantCanonical: "acme/triage:latest",
		},
		{
			name: "host with port keeps namespace and tag", in: "localhost:8080/library/laya:en",
			wantHost: "localhost:8080", wantNamespace: defaultNamespace, wantModel: nameLaya, wantTag: "en",
			wantCanonical: "localhost:8080/library/laya:en",
		},
		// Edge cases.
		{
			name: "namespaced with tag", in: repoOrgModel + ":v1",
			wantNamespace: "org", wantModel: modelWord, wantTag: "v1",
			wantCanonical: "org/model:v1",
		},
		{
			name: "dotted host is a host not a namespace", in: "registry.example.com/ns/laya:en",
			wantHost: "registry.example.com", wantNamespace: "ns", wantModel: nameLaya, wantTag: "en",
			wantCanonical: "registry.example.com/ns/laya:en",
		},
		{
			name: "host with scheme", in: "http://localhost:5000/ns/model",
			wantHost: "http://localhost:5000", wantNamespace: "ns", wantModel: modelWord, wantTag: tagLatest,
			wantCanonical: "http://localhost:5000/ns/model:latest",
		},
		{
			name: "tag split only on last segment", in: "localhost:5000/ns/model",
			wantHost: "localhost:5000", wantNamespace: "ns", wantModel: modelWord, wantTag: tagLatest,
			wantCanonical: "localhost:5000/ns/model:latest",
		},
		{
			name: "leading/trailing spaces trimmed", in: "  laya:en  ",
			wantNamespace: defaultNamespace, wantModel: nameLaya, wantTag: "en",
			wantCanonical: modelLayaEn,
		},
		// Invalid names -> error, no request.
		{name: "empty", in: "", wantErr: true},
		{name: "only spaces", in: "   ", wantErr: true},
		{name: "empty tag", in: "laya:", wantErr: true},
		{name: "empty model", in: "acme/:v1", wantErr: true},
		{name: "bad char in model", in: "la*ya:en", wantErr: true},
		{name: "too many path segments", in: "host.io/ns/extra/model", wantErr: true},
		{name: "model starts with dash", in: "-model", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parseName(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseName(%q) expected error, got %+v", tt.in, p)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseName(%q) unexpected error: %v", tt.in, err)
			}
			if p.host != tt.wantHost || p.namespace != tt.wantNamespace ||
				p.model != tt.wantModel || p.tag != tt.wantTag {
				t.Errorf("parseName(%q) = {host:%q ns:%q model:%q tag:%q}, want {host:%q ns:%q model:%q tag:%q}",
					tt.in, p.host, p.namespace, p.model, p.tag,
					tt.wantHost, tt.wantNamespace, tt.wantModel, tt.wantTag)
			}
			if got := p.canonical(); got != tt.wantCanonical {
				t.Errorf("canonical(%q) = %q, want %q", tt.in, got, tt.wantCanonical)
			}
		})
	}
}

func TestRegistryBaseFromHost(t *testing.T) {
	tests := []struct {
		name string
		in   string
		def  string
		want string
	}{
		{"no host uses default", "laya:en", registryDev, registryDev},
		{"bare host defaults https", "registry.example.com/ns/m", registryDev, "https://registry.example.com"},
		{"explicit http scheme kept", "http://localhost:5000/ns/m", registryDev, "http://localhost:5000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parseName(tt.in)
			if err != nil {
				t.Fatalf("parseName: %v", err)
			}
			if got := p.registryBase(tt.def); got != tt.want {
				t.Errorf("registryBase = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	const goodManifest = `{"schemaVersion":2,"config":{"digest":"abc"}}`
	wantDigest := func(body string) string {
		sum := sha256.Sum256([]byte(body))
		return hex.EncodeToString(sum[:])
	}

	tests := []struct {
		name          string
		modelName     string
		status        int
		body          string
		wantPath      string
		wantErr       bool
		wantErrIs     error
		wantDigest    string
		wantCanonical string
	}{
		{
			name:          "library model resolves to sha256 of body",
			modelName:     modelLayaEn,
			status:        http.StatusOK,
			body:          goodManifest,
			wantPath:      "/v2/library/laya/manifests/en",
			wantDigest:    wantDigest(goodManifest),
			wantCanonical: modelLayaEn,
		},
		{
			name:          "bare name uses latest tag",
			modelName:     "laya",
			status:        http.StatusOK,
			body:          goodManifest,
			wantPath:      "/v2/library/laya/manifests/latest",
			wantCanonical: "laya:latest",
		},
		{
			name:          "namespaced name keeps namespace",
			modelName:     repoOrgModel + ":v1",
			status:        http.StatusOK,
			body:          goodManifest,
			wantPath:      "/v2/org/model/manifests/v1",
			wantCanonical: "org/model:v1",
		},
		{
			name:      "invalid name errors without a request",
			modelName: "la*ya:en",
			wantErr:   true,
		},
		{
			name:      "404 maps to ErrNotFound",
			modelName: "missing:tag",
			status:    http.StatusNotFound,
			body:      `{"error":"not found"}`,
			wantErr:   true,
			wantErrIs: engine.ErrNotFound,
		},
		{
			name:      "500 returns status error",
			modelName: modelLayaEn,
			status:    http.StatusInternalServerError,
			body:      "boom",
			wantErr:   true,
		},
		{
			name:      "non-JSON body is an error",
			modelName: modelLayaEn,
			status:    http.StatusOK,
			body:      "not json",
			wantErr:   true,
		},
		{
			name:      "wrong schemaVersion is an error",
			modelName: modelLayaEn,
			status:    http.StatusOK,
			body:      `{"schemaVersion":1}`,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			e := New(WithRegistryURL(srv.URL))
			ref, err := e.Resolve(context.Background(), tt.modelName)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("Resolve(%q) expected error, got nil", tt.modelName)
				}
				if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("Resolve(%q) error = %v, want errors.Is %v", tt.modelName, err, tt.wantErrIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%q) unexpected error: %v", tt.modelName, err)
			}
			if tt.wantPath != "" && gotPath != tt.wantPath {
				t.Errorf("request path = %q, want %q", gotPath, tt.wantPath)
			}
			if ref.Name != tt.wantCanonical {
				t.Errorf("ref.Name = %q, want canonical %q", ref.Name, tt.wantCanonical)
			}
			if tt.wantDigest != "" && ref.Digest != tt.wantDigest {
				t.Errorf("ref.Digest = %q, want %q", ref.Digest, tt.wantDigest)
			}
		})
	}
}

// TestResolveKnownDigest verifies the digest computation against the value
// recorded for laya:en (sha256 over the exact manifest bytes).
func TestResolveKnownDigest(t *testing.T) {
	// The manifest bytes whose sha256 equals the recorded digest.
	const manifest = `{"schemaVersion":2}`
	sum := sha256.Sum256([]byte(manifest))
	digest := hex.EncodeToString(sum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, manifest)
	}))
	defer srv.Close()

	e := New(WithRegistryURL(srv.URL))
	ref, err := e.Resolve(context.Background(), modelLayaEn)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if ref.Digest != digest {
		t.Errorf("digest = %q, want %q (sha256 of raw body)", ref.Digest, digest)
	}
	// Resolve must return the exact bytes it hashed so the controller can persist
	// them and seed a rebuild later (sha256(Manifest) == Digest).
	if string(ref.Manifest) != manifest {
		t.Errorf("manifest = %q, want the raw body %q", string(ref.Manifest), manifest)
	}
	if sum2 := sha256.Sum256(ref.Manifest); hex.EncodeToString(sum2[:]) != ref.Digest {
		t.Errorf("sha256(Manifest) != Digest")
	}
}

func TestResolveAuthHeader(t *testing.T) {
	// Resolve targets the public registry and does not send auth; ensure no
	// Authorization header is set on the registry request.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Resolve should not send Authorization header, got %q", got)
		}
		_, _ = io.WriteString(w, `{"schemaVersion":2}`)
	}))
	defer srv.Close()

	e := New(WithRegistryURL(srv.URL))
	if _, err := e.Resolve(context.Background(), modelLayaEn); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
}

func TestInspect(t *testing.T) {
	const psBody = `{"models":[{"name":"laya:en","model":"laya:en","size":852602879,
 "digest":"c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d",
 "details":{"parent_model":"","format":"onnx","family":"laya","families":["laya"],
  "parameter_size":"421M","quantization_level":"F32"},
 "expires_at":null,"size_vram":0,"context_length":512,"device":"cpu"}]}`

	const psPinnedFalse = `{"models":[{"name":"kev:en","digest":"deadbeef","device":"cuda",
 "details":{"quantization_level":"F16"},"expires_at":"2026-09-27T17:00:00Z"}]}`

	tests := []struct {
		name    string
		apiKey  string
		status  int
		body    string
		wantErr bool
		verify  func(t *testing.T, got []engine.Loaded)
	}{
		{
			name:   "maps pinned model (expires_at null)",
			status: http.StatusOK,
			body:   psBody,
			verify: func(t *testing.T, got []engine.Loaded) {
				if len(got) != 1 {
					t.Fatalf("len = %d, want 1", len(got))
				}
				m := got[0]
				if m.Name != "laya:en" || m.Digest != "c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d" {
					t.Errorf("name/digest mismatch: %+v", m)
				}
				if m.Device != "cpu" || m.Precision != "F32" {
					t.Errorf("device/precision mismatch: %+v", m)
				}
				if !m.Pinned {
					t.Errorf("expected Pinned=true for expires_at null")
				}
			},
		},
		{
			name:   "expires_at set means not pinned",
			status: http.StatusOK,
			body:   psPinnedFalse,
			verify: func(t *testing.T, got []engine.Loaded) {
				if len(got) != 1 {
					t.Fatalf("len = %d, want 1", len(got))
				}
				if got[0].Pinned {
					t.Errorf("expected Pinned=false when expires_at is set")
				}
				if got[0].Device != "cuda" || got[0].Precision != "F16" {
					t.Errorf("device/precision mismatch: %+v", got[0])
				}
			},
		},
		{
			name:   "empty model list",
			status: http.StatusOK,
			body:   `{"models":[]}`,
			verify: func(t *testing.T, got []engine.Loaded) {
				if len(got) != 0 {
					t.Errorf("len = %d, want 0", len(got))
				}
			},
		},
		{
			name:    "non-2xx is an error",
			status:  http.StatusServiceUnavailable,
			body:    "down",
			wantErr: true,
		},
		{
			name:    "bad JSON is an error",
			status:  http.StatusOK,
			body:    "not json",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/ps" {
					t.Errorf("path = %q, want /api/ps", r.URL.Path)
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			e := New(WithRegistryURL("https://unused"))
			got, err := e.Inspect(context.Background(), srv.URL, tt.apiKey)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Inspect expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Inspect unexpected error: %v", err)
			}
			tt.verify(t, got)
		})
	}
}

func TestInspectAuthHeader(t *testing.T) {
	tests := []struct {
		name       string
		apiKey     string
		wantHeader string
	}{
		{"with key sets bearer", "secret", "Bearer secret"},
		{"empty key sets no header", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != tt.wantHeader {
					t.Errorf("Authorization = %q, want %q", got, tt.wantHeader)
				}
				_, _ = io.WriteString(w, `{"models":[]}`)
			}))
			defer srv.Close()

			e := New()
			if _, err := e.Inspect(context.Background(), srv.URL, tt.apiKey); err != nil {
				t.Fatalf("Inspect: %v", err)
			}
		})
	}
}

func TestWarmup(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantErr     bool
		wantErrText string
	}{
		{
			name:   "success with done_reason load",
			status: http.StatusOK,
			body:   `{"model":"laya:en","done_reason":"load","total_duration":1}`,
		},
		{
			name:        "wrong done_reason is an error",
			status:      http.StatusOK,
			body:        `{"model":"laya:en","done_reason":"stop"}`,
			wantErr:     true,
			wantErrText: "done_reason",
		},
		{
			name:        "error body carries code",
			status:      http.StatusNotFound,
			body:        `{"error":"model not found","code":"MODEL_NOT_FOUND"}`,
			wantErr:     true,
			wantErrText: "MODEL_NOT_FOUND",
		},
		{
			name:        "load failure code",
			status:      http.StatusInternalServerError,
			body:        `{"error":"could not load","code":"MODEL_LOAD_FAILED"}`,
			wantErr:     true,
			wantErrText: "MODEL_LOAD_FAILED",
		},
		{
			name:    "non-2xx without code still errors",
			status:  http.StatusBadGateway,
			body:    "gateway",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotBody decideRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/decide" {
					t.Errorf("path = %q, want /api/decide", r.URL.Path)
				}
				if r.Method != http.MethodPost {
					t.Errorf("method = %q, want POST", r.Method)
				}
				_ = decodeJSON(r.Body, &gotBody)
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			e := New()
			err := e.Warmup(context.Background(), srv.URL, "", modelLayaEn)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Warmup expected error, got nil")
				}
				if tt.wantErrText != "" && !contains(err.Error(), tt.wantErrText) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("Warmup unexpected error: %v", err)
			}
			if gotBody.Model != modelLayaEn {
				t.Errorf("request model = %q, want laya:en", gotBody.Model)
			}
			if gotBody.KeepAlive != -1 {
				t.Errorf("request keep_alive = %d, want -1", gotBody.KeepAlive)
			}
		})
	}
}

func TestWarmupAuthHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
		}
		_, _ = io.WriteString(w, `{"done_reason":"load"}`)
	}))
	defer srv.Close()

	e := New()
	if err := e.Warmup(context.Background(), srv.URL, "tok", modelLayaEn); err != nil {
		t.Fatalf("Warmup: %v", err)
	}
}

// --- helpers ---

func decodeJSON(r io.Reader, v any) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}

// slowHandler blocks until the request context is cancelled or delay elapses,
// then writes body. It lets a test drive the client into a deadline.
func slowHandler(delay time.Duration, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			_, _ = io.WriteString(w, body)
		case <-r.Context().Done():
		}
	}
}

func TestDefaultClientHasNoGlobalTimeout(t *testing.T) {
	e := New()
	if e.httpClient.Timeout != 0 {
		t.Errorf("default client Timeout = %v, want 0 (context-driven)", e.httpClient.Timeout)
	}
	if e.httpClient.Transport == nil {
		t.Errorf("default client should use a configured Transport")
	}
}

func TestWithHTTPClientOverrides(t *testing.T) {
	custom := &http.Client{Timeout: 3 * time.Second}
	e := New(WithHTTPClient(custom))
	if e.httpClient != custom {
		t.Errorf("WithHTTPClient did not override the client")
	}
}

func TestCallerDeadlineIsRespected(t *testing.T) {
	tests := []struct {
		name string
		call func(ctx context.Context, e *Engine, url string) error
	}{
		{
			name: "Resolve",
			call: func(ctx context.Context, e *Engine, url string) error {
				_, err := e.Resolve(ctx, "laya:en")
				return err
			},
		},
		{
			name: "Inspect",
			call: func(ctx context.Context, e *Engine, url string) error {
				_, err := e.Inspect(ctx, url, "")
				return err
			},
		},
		{
			name: "Warmup",
			call: func(ctx context.Context, e *Engine, url string) error {
				return e.Warmup(ctx, url, "", "laya:en")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(slowHandler(5*time.Second, `{"schemaVersion":2}`))
			defer srv.Close()

			// Generous per-method defaults so it's the caller ctx that fires.
			e := New(WithRegistryURL(srv.URL), withTimeouts(time.Minute, time.Minute, time.Minute, time.Minute))
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			err := tt.call(ctx, e, srv.URL)
			if err == nil {
				t.Fatalf("%s: expected a deadline error, got nil", tt.name)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s: error = %v, want errors.Is context.DeadlineExceeded", tt.name, err)
			}
		})
	}
}

func TestPerMethodDefaultDeadlineApplied(t *testing.T) {
	// No deadline on the caller ctx; the short per-method default must fire.
	tests := []struct {
		name string
		opts []Option
		call func(ctx context.Context, e *Engine, url string) error
	}{
		{
			name: "Resolve default",
			opts: []Option{withTimeouts(30*time.Millisecond, time.Minute, time.Minute, time.Minute)},
			call: func(ctx context.Context, e *Engine, url string) error {
				_, err := e.Resolve(ctx, "laya:en")
				return err
			},
		},
		{
			name: "Inspect default",
			opts: []Option{withTimeouts(time.Minute, 30*time.Millisecond, time.Minute, time.Minute)},
			call: func(ctx context.Context, e *Engine, url string) error {
				_, err := e.Inspect(ctx, url, "")
				return err
			},
		},
		{
			name: "Warmup default",
			opts: []Option{withTimeouts(time.Minute, time.Minute, 30*time.Millisecond, time.Minute)},
			call: func(ctx context.Context, e *Engine, url string) error {
				return e.Warmup(ctx, url, "", "laya:en")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(slowHandler(5*time.Second, `{"schemaVersion":2}`))
			defer srv.Close()

			opts := append([]Option{WithRegistryURL(srv.URL)}, tt.opts...)
			e := New(opts...)

			// context.Background() has no deadline: the per-method default applies.
			err := tt.call(context.Background(), e, srv.URL)
			if err == nil {
				t.Fatalf("%s: expected a deadline error from the default, got nil", tt.name)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s: error = %v, want errors.Is context.DeadlineExceeded", tt.name, err)
			}
		})
	}
}

func TestNoDeadlineWhenTimeoutZero(t *testing.T) {
	// withTimeout with d<=0 and no caller deadline returns a context with no deadline.
	ctx, cancel := withTimeout(context.Background(), 0)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Errorf("withTimeout(ctx, 0) should not set a deadline")
	}

	// A caller deadline is preserved (not replaced) by withTimeout.
	parent, pcancel := context.WithTimeout(context.Background(), time.Hour)
	defer pcancel()
	got, gcancel := withTimeout(parent, time.Second)
	defer gcancel()
	d1, _ := parent.Deadline()
	d2, ok := got.Deadline()
	if !ok || !d1.Equal(d2) {
		t.Errorf("withTimeout replaced the caller's deadline: parent=%v got=%v", d1, d2)
	}
}

// Real Ollaya 0.7.3 on a T4 (EKS g4dn) reports "cuda:0"; the gate compares the
// requested class "cuda", so Inspect must strip the ordinal but keep the class.
func TestNormalizeDevice(t *testing.T) {
	tests := []struct{ in, want string }{
		{"cpu", "cpu"},
		{"cuda", "cuda"},
		{"cuda:0", "cuda"},
		{"cuda:3", "cuda"},
		{" CUDA:1 ", "cuda"},
		{"", ""},
		// Must NOT collapse to "cuda": partial CPU offload / unknown forms fail the gate.
		{"cuda:0+cpu", "cuda:0+cpu"},
		{"cuda:0,cpu", "cuda:0,cpu"},
		{"cuda:", "cuda:"},
		{"cpu:0", "cpu:0"},
		{"rocm:0", "rocm:0"},
	}
	for _, tc := range tests {
		if got := normalizeDevice(tc.in); got != tc.want {
			t.Errorf("normalizeDevice(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestInspectNormalizesCUDAOrdinal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"models":[{"name":"laya:en","digest":"sha256:abc","device":"cuda:0",
 "details":{"quantization_level":"fp16"},"expires_at":null}]}`))
	}))
	defer srv.Close()
	got, err := New().Inspect(context.Background(), srv.URL, "")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(got) != 1 || got[0].Device != "cuda" || got[0].Digest != "abc" {
		t.Fatalf("got %+v, want one model on device cuda with bare-hex digest abc", got)
	}
}
