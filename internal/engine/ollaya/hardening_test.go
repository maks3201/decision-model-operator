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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

const goodManifestBody = `{"schemaVersion":2}`

// fastEngine builds an Engine pointed at url with near-zero retry backoffs so
// retry tests don't sleep. It swaps the package retryBackoffs for the duration
// of the test via a helper below (retries use the package-level var).
func fastEngine(url string) *Engine {
	return New(WithRegistryURL(url), withTimeouts(2*time.Second, 2*time.Second, 2*time.Second, 2*time.Second))
}

// withFastBackoffs temporarily shrinks the retry backoffs for a test.
func withFastBackoffs(t *testing.T) {
	t.Helper()
	orig := retryBackoffs
	retryBackoffs = []time.Duration{1 * time.Millisecond, 1 * time.Millisecond}
	t.Cleanup(func() { retryBackoffs = orig })
}

func TestResolveRetriesOn5xxThenSucceeds(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, goodManifestBody)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	ref, err := e.Resolve(context.Background(), "laya:en")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("server calls = %d, want 3 (2 retries)", got)
	}
	if ref.Name != modelLayaEn {
		t.Errorf("ref.Name = %q", ref.Name)
	}
}

func TestResolveExhaustsRetriesOn5xx(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), "laya:en")
	if err == nil {
		t.Fatalf("expected error after exhausting retries")
	}
	if got := atomic.LoadInt32(&calls); got != maxAttempts {
		t.Errorf("server calls = %d, want %d", got, maxAttempts)
	}
}

func TestResolveDoesNotRetry404(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), "laya:en")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("404 must not be retried: calls = %d, want 1", got)
	}
}

func TestResolveDoesNotRetryOther4xx(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), "laya:en")
	if err == nil {
		t.Fatalf("expected error on 400")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("4xx must not be retried: calls = %d, want 1", got)
	}
}

func TestResolveRetriesOn429WithRetryAfter(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, goodManifestBody)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	if _, err := e.Resolve(context.Background(), "laya:en"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (one 429 retry)", got)
	}
}

func TestResolveRespectsContextDuringRetry(t *testing.T) {
	// Backoffs are long here; a short ctx must cut the retry loop.
	orig := retryBackoffs
	retryBackoffs = []time.Duration{time.Second, time.Second}
	t.Cleanup(func() { retryBackoffs = orig })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := e.Resolve(ctx, "laya:en")
	if err == nil {
		t.Fatalf("expected an error when ctx expires during backoff")
	}
}

func TestBodySizeCap(t *testing.T) {
	// A manifest larger than the cap is rejected.
	big := strings.Repeat("x", maxBodyBytes+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), "laya:en")
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected an over-cap error, got %v", err)
	}
}

func TestInspectBodySizeCap(t *testing.T) {
	big := strings.Repeat("y", maxBodyBytes+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()

	e := New()
	_, err := e.Inspect(context.Background(), srv.URL, "")
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected an over-cap error, got %v", err)
	}
}

func TestReadCappedBodyBoundary(t *testing.T) {
	// Exactly maxBodyBytes is accepted; one more byte is rejected.
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, maxBodyBytes))
	}))
	defer okSrv.Close()
	resp, err := http.Get(okSrv.URL) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	body, err := readCappedBody(resp)
	_ = resp.Body.Close()
	if err != nil {
		t.Errorf("exactly maxBodyBytes should be accepted: %v", err)
	}
	if len(body) != maxBodyBytes {
		t.Errorf("len = %d, want %d", len(body), maxBodyBytes)
	}
}

func TestCanonicalName(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"laya", "laya:latest", false},
		{"Laya:EN", "laya:en", false},
		{"laya:en", "laya:en", false},
		{"ollaya.dev/library/laya:en", "laya:en", false},
		{"OLLAYA.DEV/Library/Laya:EN", "laya:en", false},
		{"acme/triage", "acme/triage:latest", false},
		{"localhost:5000/ns/m:t", "localhost:5000/ns/m:t", false},
		{"registry.example.com/ns/laya:en", "registry.example.com/ns/laya:en", false},
		// A namespace that looks like a host is ambiguous and must be rejected so
		// every accepted name round-trips through CanonicalName (idempotence).
		{"ollaya.dev/0./0", "", true},
		{"0./0", "", true},
		{"ollaya.dev/foo.bar/laya:en", "", true},
		{"ollaya.dev/localhost/laya:en", "", true},
		// A host-looking first segment of a two-segment name is the host, kept
		// verbatim (non-default), and round-trips.
		{"foo.bar/laya:en", "foo.bar/library/laya:en", false},
		// With a non-default host the namespace may contain dots: the host is kept.
		{"registry.example.com/my.team/laya:en", "registry.example.com/my.team/laya:en", false},
		{"localhost/laya:en", "localhost/library/laya:en", false},
		{"la*ya", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := CanonicalName(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("CanonicalName(%q) expected error, got %q", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalName(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("CanonicalName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestCanonicalNameEquivalence documents the controller's use: user-written
// forms of the same model canonicalise to the same string Ollaya reports.
func TestCanonicalNameEquivalence(t *testing.T) {
	forms := []string{"Laya:EN", "laya:en", "ollaya.dev/library/laya:en", "OLLAYA.DEV/library/LAYA:en"}
	var canon string
	for i, f := range forms {
		got, err := CanonicalName(f)
		if err != nil {
			t.Fatalf("CanonicalName(%q): %v", f, err)
		}
		if i == 0 {
			canon = got
			continue
		}
		if got != canon {
			t.Errorf("CanonicalName(%q) = %q, want %q (all forms must agree)", f, got, canon)
		}
	}
	if canon != modelLayaEn {
		t.Errorf("canonical = %q, want %q", canon, modelLayaEn)
	}
}

func TestEngineCanonicalNameMethod(t *testing.T) {
	e := New()
	got, err := e.CanonicalName("ollaya.dev/library/Laya:EN")
	if err != nil {
		t.Fatalf("CanonicalName method: %v", err)
	}
	if got != modelLayaEn {
		t.Errorf("method = %q, want %q", got, modelLayaEn)
	}

	// Method and package func must agree.
	pkg, _ := CanonicalName("acme/Triage")
	m, _ := e.CanonicalName("acme/Triage")
	if pkg != m {
		t.Errorf("method (%q) and package func (%q) disagree", m, pkg)
	}

	if _, err := e.CanonicalName("la*ya"); err == nil {
		t.Errorf("invalid name must error via the method too")
	}
}

func TestRegistryHost(t *testing.T) {
	tests := []struct {
		name         string
		registryURL  string // "" -> engine default
		in           string
		wantHost     string
		wantInsecure bool
		wantErr      bool
	}{
		{name: "host-less uses engine default", in: "laya:en", wantHost: "ollaya.dev"},
		{name: "host-less namespaced uses default", in: "acme/triage", wantHost: "ollaya.dev"},
		{name: "explicit host", in: "registry.example.com/ns/m:t", wantHost: "registry.example.com"},
		{name: "explicit host with port", in: "localhost:5000/ns/m", wantHost: "localhost:5000"},
		{name: "http host is insecure", in: "http://localhost:5000/ns/m", wantHost: "localhost:5000", wantInsecure: true},
		{name: "https host", in: "https://registry.example.com/ns/m:t", wantHost: "registry.example.com"},
		{name: "mixed-case host lowercased", in: "LocalHost:5000/ns/m", wantHost: "localhost:5000"},
		{name: "mixed-case dotted host lowercased", in: "Registry.EXAMPLE.com/ns/m:t", wantHost: "registry.example.com"},
		{name: "custom registry URL", registryURL: "https://reg.internal:8443", in: "laya:en", wantHost: "reg.internal:8443"},
		{name: "http registry URL -> insecure", registryURL: "http://reg.internal:5000", in: "laya:en", wantHost: "reg.internal:5000", wantInsecure: true},
		{name: "invalid name errors", in: "la*ya", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e *Engine
			if tt.registryURL != "" {
				e = New(WithRegistryURL(tt.registryURL))
			} else {
				e = New()
			}
			host, insecure, err := e.RegistryHost(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("RegistryHost(%q) expected error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("RegistryHost(%q): %v", tt.in, err)
			}
			if host != tt.wantHost || insecure != tt.wantInsecure {
				t.Errorf("RegistryHost(%q) = (%q, %v), want (%q, %v)", tt.in, host, insecure, tt.wantHost, tt.wantInsecure)
			}
		})
	}
}

func TestHostSegmentValidation(t *testing.T) {
	// These parse as having a host segment; bad hosts must be rejected.
	bad := []string{
		"registry..example.com/ns/m", // empty label
		"reg istry.com/ns/m",         // space -> not a valid label (also has no dot? it does via .com)
		"registry.com:/ns/m",         // empty port
		"registry.com:abc/ns/m",      // non-numeric port
		"../ns/m",                    // path traversal attempt as host
	}
	for _, in := range bad {
		if _, err := parseName(in); err == nil {
			t.Errorf("parseName(%q) should reject an invalid host", in)
		}
	}
	// Valid hosts still parse.
	good := []string{
		"registry.example.com/ns/m",
		"localhost:5000/ns/m",
		"reg-1.example.io:8443/ns/m:t",
	}
	for _, in := range good {
		if _, err := parseName(in); err != nil {
			t.Errorf("parseName(%q) should accept a valid host: %v", in, err)
		}
	}
}

// TestHostIsDefaultPort pins that only the exact default
// authority (ollaya.dev on https, no port or :443) is the default registry; a
// non-default port is a different registry and must NOT be treated as default.
func TestHostIsDefaultPort(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"", true},
		{"ollaya.dev", true},
		{"https://ollaya.dev", true},
		{"https://ollaya.dev:443", true},
		{"http://ollaya.dev:80", true},
		{"OLLAYA.DEV", true},
		{"ollaya.dev:5000", false}, // non-default port -> different registry
		{"https://ollaya.dev:5000", false},
		{"http://ollaya.dev", true}, // host-only check: hostname+port match (transport handled by registryIsDefault)
		{"mirror.corp", false},
		{"registry.example.com:443", false},
	}
	for _, tt := range tests {
		if got := hostIsDefault(tt.host); got != tt.want {
			t.Errorf("hostIsDefault(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

// TestResolveRefusesCrossOriginRedirect pins the registry
// client must not follow a redirect to a different scheme+host+port, so a
// registry on the allow-list cannot bounce the resolve to an unapproved host.
func TestResolveRefusesCrossOriginRedirect(t *testing.T) {
	var evilHits int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&evilHits, 1)
		_, _ = io.WriteString(w, goodManifestBody)
	}))
	defer evil.Close()

	// The allowed registry 302-redirects every manifest request to the evil host.
	allowed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+"/v2/library/laya/manifests/en", http.StatusFound)
	}))
	defer allowed.Close()

	e := New(WithRegistryURL(allowed.URL), withTimeouts(2*time.Second, 2*time.Second, 2*time.Second, 2*time.Second))
	withFastBackoffs(t)
	_, err := e.Resolve(context.Background(), modelLayaEn)
	if err == nil {
		t.Fatalf("Resolve must fail when the registry redirects cross-origin")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error should mention the refused redirect, got %v", err)
	}
	if got := atomic.LoadInt32(&evilHits); got != 0 {
		t.Errorf("evil host was reached %d times; the redirect must be refused", got)
	}
}

// TestResolveFollowsSameOriginRedirect confirms a same-origin redirect (e.g. a
// trailing-slash normalisation) is still followed.
func TestResolveFollowsSameOriginRedirect(t *testing.T) {
	var mux http.ServeMux
	mux.HandleFunc("/v2/library/laya/manifests/en", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/v2/library/laya/manifests/en-canonical", http.StatusFound)
	})
	mux.HandleFunc("/v2/library/laya/manifests/en-canonical", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, goodManifestBody)
	})
	srv := httptest.NewServer(&mux)
	defer srv.Close()

	e := New(WithRegistryURL(srv.URL), withTimeouts(2*time.Second, 2*time.Second, 2*time.Second, 2*time.Second))
	if _, err := e.Resolve(context.Background(), modelLayaEn); err != nil {
		t.Fatalf("same-origin redirect should be followed: %v", err)
	}
}

// TestRuntimeClientRefusesAllRedirects pins that the runtime client
// (Inspect/Warmup/Decide — in-cluster calls to Pod IPs that carry the API key)
// follows NO redirect: a compromised or buggy runtime answering a 3xx must not
// steer the operator (or the API key) at another address (SSRF), nor let a
// forged /api/ps from elsewhere flip the readiness gate.
func TestRuntimeClientRefusesAllRedirects(t *testing.T) {
	var evilHits int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&evilHits, 1)
		// A plausible but forged answer for each endpoint.
		_, _ = io.WriteString(w, `{"models":[{"name":"laya:en","digest":"deadbeef","device":"cpu"}],"done_reason":"load","answers":{}}`)
	}))
	defer evil.Close()

	// The runtime 302-redirects every call to the evil host.
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+r.URL.Path, http.StatusFound)
	}))
	defer runtime.Close()

	e := New(withTimeouts(2*time.Second, 2*time.Second, 2*time.Second, 2*time.Second))

	t.Run("Inspect", func(t *testing.T) {
		if _, err := e.Inspect(context.Background(), runtime.URL, "tok"); err == nil {
			t.Fatalf("Inspect must fail on a redirect")
		} else if !strings.Contains(err.Error(), "redirect") {
			t.Errorf("error should mention the refused redirect, got %v", err)
		}
	})
	t.Run("Warmup", func(t *testing.T) {
		if err := e.Warmup(context.Background(), runtime.URL, "tok", modelLayaEn); err == nil {
			t.Fatalf("Warmup must fail on a redirect")
		} else if !strings.Contains(err.Error(), "redirect") {
			t.Errorf("error should mention the refused redirect, got %v", err)
		}
	})
	t.Run("Decide", func(t *testing.T) {
		if _, err := e.Decide(context.Background(), runtime.URL, "tok", decideReq()); err == nil {
			t.Fatalf("Decide must fail on a redirect")
		} else if !strings.Contains(err.Error(), "redirect") {
			t.Errorf("error should mention the refused redirect, got %v", err)
		}
	})

	if got := atomic.LoadInt32(&evilHits); got != 0 {
		t.Errorf("the redirect target was reached %d times; all redirects must be refused", got)
	}
}

// TestRuntimeRedirectErrorHidesAPIKey confirms the refused-redirect error names
// the status and target host but never leaks the API key.
func TestRuntimeRedirectErrorHidesAPIKey(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer runtime.Close()

	e := New(withTimeouts(2*time.Second, 2*time.Second, 2*time.Second, 2*time.Second))
	const secret = "super-secret-key"
	_, err := e.Inspect(context.Background(), runtime.URL, secret)
	if err == nil {
		t.Fatalf("expected a refused-redirect error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error must not contain the API key: %v", err)
	}
	if !strings.Contains(err.Error(), "169.254.169.254") {
		t.Errorf("error should name the target host, got %v", err)
	}
}

// TestResolveNotFoundWithLargeBody pins that a 404 carrying a body larger than
// the read cap (a CDN/proxy HTML error page) is still classified as ErrNotFound,
// not a "body too large" transient error — the status is checked before the body
// is read.
func TestResolveNotFoundWithLargeBody(t *testing.T) {
	withFastBackoffs(t)
	huge := strings.Repeat("<html>not found</html>", (2<<20)/22) // ~2 MiB, well over maxBodyBytes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, huge)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), modelLayaEn)
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound (a large 404 body must not become a size error)", err)
	}
	if strings.Contains(err.Error(), "exceeds") {
		t.Errorf("a 404 must not be reported as a body-too-large error: %v", err)
	}
}
