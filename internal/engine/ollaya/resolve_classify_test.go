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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Item 42: resolver error classification gaps not already covered by
// hardening_test.go (which covers 404, other-4xx, 429, 5xx retry/exhaust,
// cross-origin redirect, body cap, and a large-body 404).

// 401 and 403 are permanent auth failures: they are not 429 and not 5xx, so the
// retry policy must not retry them, and Resolve surfaces the status (not
// ErrNotFound, since the tag may well exist behind the auth wall).
func TestResolveAuthStatusesNotRetried(t *testing.T) {
	withFastBackoffs(t)
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&calls, 1)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			e := fastEngine(srv.URL)
			_, err := e.Resolve(context.Background(), modelLayaEn)
			if err == nil {
				t.Fatalf("expected an error for status %d", code)
			}
			if errors.Is(err, engine.ErrNotFound) {
				t.Errorf("a %d must not be classified as ErrNotFound: %v", code, err)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Errorf("auth status %d must not be retried: calls = %d, want 1", code, got)
			}
		})
	}
}

// A transport error (the server resets the connection without a response) is
// transient: doRetry retries it up to maxAttempts. The server resets on every
// attempt, so Resolve exhausts the budget and returns an error after exactly
// maxAttempts connection attempts.
func TestResolveRetriesTransportReset(t *testing.T) {
	withFastBackoffs(t)
	var conns int32
	// A raw TCP listener that accepts, counts, then closes the connection
	// immediately — the client sees a reset/EOF with no HTTP response.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
				default:
				}
				return
			}
			atomic.AddInt32(&conns, 1)
			_ = c.Close() // drop immediately: no HTTP response
		}
	}()
	t.Cleanup(func() { close(done) })

	e := fastEngine("http://" + ln.Addr().String())
	_, resErr := e.Resolve(context.Background(), modelLayaEn)
	if resErr == nil {
		t.Fatalf("expected a transport error")
	}
	if errors.Is(resErr, engine.ErrNotFound) {
		t.Errorf("a transport reset must not be ErrNotFound: %v", resErr)
	}
	if got := atomic.LoadInt32(&conns); got != maxAttempts {
		t.Errorf("transport error must be retried: connections = %d, want %d", got, maxAttempts)
	}
}

// A context deadline that fires DURING an in-flight request (the server stalls
// before responding) aborts Resolve with a deadline error — distinct from the
// existing test where the deadline fires during the backoff between retries.
func TestResolveContextDeadlineDuringRequest(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(block)

	e := fastEngine(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := e.Resolve(ctx, modelLayaEn)
	if err == nil {
		t.Fatalf("expected a deadline error while the request was in flight")
	}
	// It must give up near the deadline, not hang for the full resolve timeout.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Resolve did not honour the context deadline during the request: took %v", elapsed)
	}
}

// A malformed-JSON manifest body (valid 200, body is not JSON) is a permanent
// classification error: it is returned immediately after a single request, never
// retried (the status is 2xx, so doRetry already returned the response).
func TestResolveMalformedJSONNotRetried(t *testing.T) {
	withFastBackoffs(t)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.WriteString(w, `{not valid json`)
	}))
	defer srv.Close()

	e := fastEngine(srv.URL)
	_, err := e.Resolve(context.Background(), modelLayaEn)
	if err == nil {
		t.Fatalf("expected an error on a malformed JSON manifest")
	}
	if !strings.Contains(err.Error(), "valid JSON") {
		t.Errorf("error should name the JSON problem, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("a 200 with bad JSON must not be retried: calls = %d, want 1", got)
	}
}
