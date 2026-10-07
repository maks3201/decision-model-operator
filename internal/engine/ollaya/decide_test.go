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
	"encoding/json"
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

// billingResponse is the §7.3 example mapped to the /v1/systemone answer shapes
// (no native laya extras, which are /api/decide only).
const billingResponse = `{
  "model": "laya:en",
  "answers": {
    "department": {
      "type": "choice",
      "choice": "billing",
      "confidence": 0.7781,
      "probabilities": {"billing": 0.8521, "technical": 0.0611, "account": 0.0868}
    },
    "urgency": {
      "type": "score",
      "score": 1.1982,
      "confidence": 0.3418,
      "legend": {"0": "Can wait", "1": "Needs attention this week", "2": "Needs attention today"},
      "probabilities": {"0": 0.1203, "1": 0.5612, "2": 0.3185}
    },
    "refund": {"type": "noul", "noul": 0.9127}
  },
  "usage": {"input_tokens": 118, "output_tokens": 0},
  "state_truncated": false
}`

func decideReq() engine.DecideRequest {
	return engine.DecideRequest{
		Model:     modelLayaEn,
		State:     json.RawMessage(`"I was charged twice"`),
		Questions: json.RawMessage(`{"department":{"type":"choice"}}`),
	}
}

func TestDecideMapsAllAnswerTypes(t *testing.T) {
	var gotBody systemOneRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/systemone" {
			t.Errorf("path = %q, want /v1/systemone", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		_ = decodeJSON(r.Body, &gotBody)
		_, _ = io.WriteString(w, billingResponse)
	}))
	defer srv.Close()

	e := New()
	resp, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	// request passthrough
	if gotBody.Model != modelLayaEn {
		t.Errorf("request model = %q", gotBody.Model)
	}
	if string(gotBody.Questions) != `{"department":{"type":"choice"}}` {
		t.Errorf("questions not passed through verbatim: %s", gotBody.Questions)
	}

	if len(resp.Answers) != 3 {
		t.Fatalf("answers = %d, want 3", len(resp.Answers))
	}

	dep := resp.Answers["department"]
	if dep.Type != "choice" || dep.Choice != "billing" {
		t.Errorf("department = %+v", dep)
	}
	if dep.Confidence != 0.7781 || dep.Probabilities["billing"] != 0.8521 {
		t.Errorf("choice fields = %+v", dep)
	}
	if dep.Noul != nil || dep.Score != nil {
		t.Errorf("choice must not set noul/score: %+v", dep)
	}

	urg := resp.Answers["urgency"]
	if urg.Type != "score" || urg.Score == nil || *urg.Score != 1.1982 {
		t.Errorf("urgency score = %+v", urg)
	}
	if urg.Confidence != 0.3418 {
		t.Errorf("urgency confidence = %v", urg.Confidence)
	}
	// Score answers must carry per-level probabilities (keys "0".."n-1") so the
	// evaluator's score ECE/Brier has data.
	if urg.Probabilities["0"] != 0.1203 || urg.Probabilities["1"] != 0.5612 || urg.Probabilities["2"] != 0.3185 {
		t.Errorf("score probabilities not copied: %+v", urg.Probabilities)
	}
	if urg.Choice != "" || urg.Noul != nil {
		t.Errorf("score must not set choice/noul: %+v", urg)
	}

	ref := resp.Answers["refund"]
	if ref.Type != "noul" || ref.Noul == nil || *ref.Noul != 0.9127 {
		t.Errorf("refund noul = %+v", ref)
	}
	if ref.Confidence != 0 {
		t.Errorf("noul must have no confidence, got %v", ref.Confidence)
	}
}

func TestDecideUnknownTypePassesThroughType(t *testing.T) {
	body := `{"answers":{"q":{"type":"mystery","choice":"x"}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	e := New()
	resp, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	a := resp.Answers["q"]
	if a.Type != "mystery" {
		t.Errorf("type = %q, want mystery", a.Type)
	}
	if a.Choice != "" || a.Probabilities != nil || a.Noul != nil || a.Score != nil {
		t.Errorf("unknown type must carry no values, got %+v", a)
	}
}

func TestDecideEmptyAnswers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"answers":{}}`)
	}))
	defer srv.Close()
	e := New()
	resp, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if len(resp.Answers) != 0 {
		t.Errorf("answers = %d, want 0", len(resp.Answers))
	}
}

func TestDecideErrorCodes(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantText string
	}{
		{"invalid request", 422, `{"error":"bad","code":"INVALID_REQUEST"}`, "INVALID_REQUEST"},
		{"state truncated", 422, `{"error":"trunc","code":"STATE_TRUNCATED"}`, "STATE_TRUNCATED"},
		{"model not found", 404, `{"error":"nope","code":"MODEL_NOT_FOUND"}`, "MODEL_NOT_FOUND"},
		{"internal", 500, `{"error":"boom","code":"INTERNAL"}`, "INTERNAL"},
		{"non-json 5xx", 502, `bad gateway`, "status 502"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()
			e := New()
			_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
			if err == nil {
				t.Fatalf("expected error")
			}
			if !contains(err.Error(), tt.wantText) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantText)
			}
		})
	}
}

// TestDecideScoreProbabilitiesKept checks the mapping layer directly: a score
// answer's per-level probabilities are copied (keys "0".."n-1"), a choice answer
// keeps its per-choice probabilities, and a noul answer carries none. Verified
// against the runtime, a score answer's probabilities look like
// {"0":0.0125,"1":0.0915,"2":0.896} (0-based, summing to ~1).
func TestDecideScoreProbabilitiesKept(t *testing.T) {
	body := `{"answers":{
		"sc":{"type":"score","score":1.88,"confidence":0.84,"probabilities":{"0":0.0125,"1":0.0915,"2":0.896}},
		"ch":{"type":"choice","choice":"a","confidence":0.5,"probabilities":{"a":0.6,"b":0.4}},
		"nl":{"type":"noul","noul":0.3,"probabilities":{"ignored":1.0}}
	}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	e := New()
	resp, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	sc := resp.Answers["sc"]
	if sc.Probabilities["0"] != 0.0125 || sc.Probabilities["1"] != 0.0915 || sc.Probabilities["2"] != 0.896 {
		t.Errorf("score probabilities = %+v, want 0/1/2 kept", sc.Probabilities)
	}
	if sc.Score == nil || *sc.Score != 1.88 {
		t.Errorf("score value = %+v", sc.Score)
	}

	ch := resp.Answers["ch"]
	if ch.Choice != "a" || ch.Probabilities["a"] != 0.6 || ch.Probabilities["b"] != 0.4 {
		t.Errorf("choice fields = %+v", ch)
	}

	// noul maps no probabilities (the contract field is for choice/score).
	if nl := resp.Answers["nl"]; nl.Probabilities != nil {
		t.Errorf("noul must carry no probabilities, got %+v", nl.Probabilities)
	}
}

// TestDecideRejectedStatusClassification pins which statuses wrap
// engine.ErrRequestRejected (permanent) and which stay transient. Status codes
// and bodies match the real runtime (0.10.0): 404 MODEL_NOT_FOUND, 422
// INVALID_REQUEST, 400 INVALID_JSON are rejected; 401 UNAUTHORIZED is not.
func TestDecideRejectedStatusClassification(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		rejected bool
	}{
		{"400 invalid json", 400, `{"error":"invalid JSON","code":"INVALID_JSON"}`, true},
		{"404 model not found", 404, `{"error":"not found","code":"MODEL_NOT_FOUND"}`, true},
		{"413 payload too large", 413, `{"error":"too big","code":"PAYLOAD_TOO_LARGE"}`, true},
		{"422 invalid request", 422, `{"error":"bad field","code":"INVALID_REQUEST"}`, true},
		{"418 other 4xx", 418, `{"error":"teapot","code":"TEAPOT"}`, true},
		{"400 unparseable body", 400, `not json at all`, true},
		{"401 unauthorized", 401, `{"error":"no key","code":"UNAUTHORIZED"}`, false},
		{"403 forbidden", 403, `{"error":"nope","code":"FORBIDDEN"}`, false},
		{"408 request timeout", 408, `{"error":"slow","code":"TIMEOUT"}`, false},
		{"409 conflict", 409, `{"error":"busy","code":"CONFLICT"}`, false},
		{"429 rate limited", 429, `{"error":"slow down","code":"RATE_LIMITED"}`, false},
		{"500 internal", 500, `{"error":"boom","code":"INTERNAL"}`, false},
		{"502 non-json", 502, `bad gateway`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()
			e := New()
			_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
			if err == nil {
				t.Fatalf("expected error")
			}
			if got := errors.Is(err, engine.ErrRequestRejected); got != tt.rejected {
				t.Errorf("errors.Is(err, ErrRequestRejected) = %v, want %v (err: %v)", got, tt.rejected, err)
			}
		})
	}
}

func TestDecideQueueFullRetriesOnce(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"full","code":"QUEUE_FULL"}`)
			return
		}
		_, _ = io.WriteString(w, billingResponse)
	}))
	defer srv.Close()

	e := New()
	resp, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err != nil {
		t.Fatalf("Decide after queue retry: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (one QUEUE_FULL retry)", got)
	}
	if len(resp.Answers) != 3 {
		t.Errorf("answers = %d, want 3", len(resp.Answers))
	}
}

func TestDecideQueueFullRetriesOnlyOnce(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"full","code":"QUEUE_FULL"}`)
	}))
	defer srv.Close()

	e := New()
	_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err == nil {
		t.Fatalf("expected error after retry still 503")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (retry exactly once)", got)
	}
	if !contains(err.Error(), "QUEUE_FULL") {
		t.Errorf("error should carry QUEUE_FULL: %v", err)
	}
}

func TestDecideNonQueueFull503NotRetried(t *testing.T) {
	// A 503 whose code is not QUEUE_FULL (e.g. the server draining) must be
	// returned immediately, not retried as if the queue were full.
	// The error must surface the real code.
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"draining","code":"SERVER_SHUTTING_DOWN"}`)
	}))
	defer srv.Close()

	e := New()
	_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err == nil {
		t.Fatalf("expected error on non-QUEUE_FULL 503")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (no retry for a non-QUEUE_FULL 503)", got)
	}
	if !contains(err.Error(), "SERVER_SHUTTING_DOWN") {
		t.Errorf("error should carry the real code, got %v", err)
	}
}

func TestDecide503NoBodyNotRetried(t *testing.T) {
	// A bare 503 with no parseable code is not QUEUE_FULL -> returned immediately.
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `service unavailable`)
	}))
	defer srv.Close()

	e := New()
	_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err == nil {
		t.Fatalf("expected error on bare 503")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (no retry for a code-less 503)", got)
	}
	if !contains(err.Error(), "status 503") {
		t.Errorf("error should surface status 503, got %v", err)
	}
}

func TestDecideAuthHeader(t *testing.T) {
	tests := []struct {
		name       string
		apiKey     string
		wantHeader string
	}{
		{"with key", "tok", "Bearer tok"},
		{"without key", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != tt.wantHeader {
					t.Errorf("Authorization = %q, want %q", got, tt.wantHeader)
				}
				_, _ = io.WriteString(w, `{"answers":{}}`)
			}))
			defer srv.Close()
			e := New()
			if _, err := e.Decide(context.Background(), srv.URL, tt.apiKey, decideReq()); err != nil {
				t.Fatalf("Decide: %v", err)
			}
		})
	}
}

func TestDecideBodySizeCap(t *testing.T) {
	big := `{"answers":{}}` + strings.Repeat(" ", maxBodyBytes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()
	e := New()
	_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err == nil || !contains(err.Error(), "exceeds") {
		t.Fatalf("expected over-cap error, got %v", err)
	}
}

func TestDecideDefaultDeadlineApplied(t *testing.T) {
	srv := httptest.NewServer(slowHandler(5*time.Second, billingResponse))
	defer srv.Close()
	// Short decide default; no caller deadline.
	e := New(withTimeouts(time.Minute, time.Minute, time.Minute, 30*time.Millisecond))
	_, err := e.Decide(context.Background(), srv.URL, "", decideReq())
	if err == nil {
		t.Fatalf("expected a deadline error from the default")
	}
}
