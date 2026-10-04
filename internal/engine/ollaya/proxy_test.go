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
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestRuntimeClientHasNoProxy asserts the proxy split at the transport level,
// deterministically (no dependence on the process-global ProxyFromEnvironment
// cache, which other tests may have primed): the runtime client's transport has
// Proxy == nil (never proxies), while the registry client's transport has a
// non-nil Proxy func (honours the environment). The end-to-end test below proves
// the live behaviour with a real proxy.
func TestRuntimeClientHasNoProxy(t *testing.T) {
	e := New()

	rt, ok := e.runtimeClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("runtime client transport is not *http.Transport")
	}
	if rt.Proxy != nil {
		t.Errorf("runtime client transport Proxy must be nil (API-key calls must never be proxied)")
	}

	reg, ok := e.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("registry client transport is not *http.Transport")
	}
	if reg.Proxy == nil {
		t.Errorf("registry client transport Proxy must be set (honours HTTP_PROXY for pulls)")
	}
}

// TestRuntimeCallsBypassProxyEndToEnd points HTTP_PROXY at a recording proxy
// and confirms a runtime call (Inspect) reaches the target server directly and
// the proxy is never contacted. The engine is constructed after t.Setenv.
func TestRuntimeCallsBypassProxyEndToEnd(t *testing.T) {
	var proxyHits int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&proxyHits, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxy.Close()

	var runtimeHits int32
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&runtimeHits, 1)
		if r.URL.Path != "/api/ps" {
			t.Errorf("path = %q, want /api/ps", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"models":[]}`)
	}))
	defer runtime.Close()

	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "") // do not exempt anything; only the proxy-less client should bypass

	e := New()

	if _, err := e.Inspect(context.Background(), runtime.URL, "secret-api-key"); err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if got := atomic.LoadInt32(&runtimeHits); got != 1 {
		t.Errorf("runtime server hits = %d, want 1 (direct)", got)
	}
	if got := atomic.LoadInt32(&proxyHits); got != 0 {
		t.Errorf("proxy was contacted %d times; the runtime client (with the API key) must never use a proxy", got)
	}
}
