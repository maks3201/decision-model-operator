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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// systemOneRequest is the /v1/systemone request body. State and Questions are
// passed through verbatim (any JSON) per the contract.
type systemOneRequest struct {
	Model     string          `json:"model"`
	State     json.RawMessage `json:"state,omitempty"`
	Questions json.RawMessage `json:"questions,omitempty"`
}

// systemOneResponse is the subset of the TypeSafe /v1/systemone response we map
// (docs/api.md §5.4). Each answer carries an explicit "type" discriminator.
type systemOneResponse struct {
	Answers map[string]rawAnswer `json:"answers"`
	// Error/Code are only present on error bodies (§4).
	Error string `json:"error"`
	Code  string `json:"code"`
}

// rawAnswer is one answer as returned on the wire.
type rawAnswer struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice"`
	Probabilities map[string]float64 `json:"probabilities"`
	Noul          *float64           `json:"noul"`
	Score         *float64           `json:"score"`
	Confidence    float64            `json:"confidence"`
}

// Decide sends one System-1 decision request to the runtime at baseURL
// (POST /v1/systemone) and maps the answers per docs/api.md §5.4.
func (e *Engine) Decide(ctx context.Context, baseURL, apiKey string, req engine.DecideRequest) (engine.DecideResponse, error) {
	ctx, cancel := withTimeout(ctx, e.timeouts.decide)
	defer cancel()

	url := strings.TrimRight(baseURL, "/") + "/v1/systemone"
	payload, err := json.Marshal(systemOneRequest{
		Model:     req.Model,
		State:     req.State,
		Questions: req.Questions,
	})
	if err != nil {
		return engine.DecideResponse{}, fmt.Errorf("ollaya: marshal decide request: %w", err)
	}

	newReq := func() (*http.Request, error) {
		r, rerr := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if rerr != nil {
			return nil, rerr
		}
		r.Header.Set("Content-Type", "application/json")
		setAuth(r, apiKey)
		return r, nil
	}

	// 503 QUEUE_FULL is retried once after Retry-After; other statuses are not
	// retried here (the controller's evaluator decides what to do with them).
	resp, err := e.doDecideWithQueueRetry(ctx, newReq)
	if err != nil {
		return engine.DecideResponse{}, fmt.Errorf("ollaya: decide %q: %w", req.Model, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readCappedBody(resp)
	if err != nil {
		return engine.DecideResponse{}, fmt.Errorf("ollaya: read decide body: %w", err)
	}

	var sr systemOneResponse
	if uerr := json.Unmarshal(body, &sr); uerr != nil {
		// On a non-2xx with an unparseable body, still surface the status.
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return engine.DecideResponse{}, fmt.Errorf("ollaya: decide %q: unexpected status %d", req.Model, resp.StatusCode)
		}
		return engine.DecideResponse{}, fmt.Errorf("ollaya: decide %q: invalid JSON: %w", req.Model, uerr)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if sr.Code != "" {
			return engine.DecideResponse{}, fmt.Errorf("ollaya: decide %q failed (status %d, code %s): %s",
				req.Model, resp.StatusCode, sr.Code, sr.Error)
		}
		return engine.DecideResponse{}, fmt.Errorf("ollaya: decide %q: unexpected status %d", req.Model, resp.StatusCode)
	}

	return engine.DecideResponse{Answers: mapAnswers(sr.Answers)}, nil
}

// doDecideWithQueueRetry issues the request once; on a 503 whose body carries the
// error code QUEUE_FULL it waits for Retry-After (bounded) and retries exactly
// once. Any other response — including a 503 that is NOT QUEUE_FULL — is returned
// as-is for the caller to interpret (a non-QUEUE_FULL 503,
// e.g. the server shutting down, must not be retried as if the queue were full).
func (e *Engine) doDecideWithQueueRetry(ctx context.Context, newReq func() (*http.Request, error)) (*http.Response, error) {
	for attempt := 0; attempt < 2; attempt++ {
		req, err := newReq()
		if err != nil {
			return nil, err
		}
		resp, err := e.runtimeClient.Do(req)
		if err != nil {
			return nil, err
		}
		if attempt == 0 && resp.StatusCode == http.StatusServiceUnavailable {
			// Peek the body to read the error code; only QUEUE_FULL is retried.
			body, rerr := readCappedBody(resp)
			_ = resp.Body.Close()
			if rerr != nil {
				return nil, rerr
			}
			if isQueueFull(body) {
				if serr := sleepCtx(ctx, retryAfter(resp)); serr != nil {
					return nil, serr
				}
				continue
			}
			// Not QUEUE_FULL: hand the response back with its body restored so the
			// caller sees the real status and code.
			resp.Body = io.NopCloser(bytes.NewReader(body))
			return resp, nil
		}
		return resp, nil
	}
	// Unreachable: the loop either returns a response or an error above.
	return nil, fmt.Errorf("ollaya: decide: exhausted queue retry")
}

// isQueueFull reports whether a decide response body carries the error code
// QUEUE_FULL (docs/api.md §4 error shape {"error":...,"code":"QUEUE_FULL"}). A
// body that does not parse, or carries any other code, returns false.
func isQueueFull(body []byte) bool {
	var e struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		return false
	}
	return e.Code == "QUEUE_FULL"
}

// mapAnswers converts wire answers to contract Answers per §5.4. Unknown types
// yield an Answer with only Type set (no error).
func mapAnswers(raw map[string]rawAnswer) map[string]engine.Answer {
	if raw == nil {
		return map[string]engine.Answer{}
	}
	out := make(map[string]engine.Answer, len(raw))
	for id, a := range raw {
		ans := engine.Answer{Type: a.Type}
		switch a.Type {
		case "choice":
			ans.Choice = a.Choice
			ans.Probabilities = a.Probabilities
			ans.Confidence = a.Confidence
		case "score":
			ans.Score = a.Score
			ans.Confidence = a.Confidence
		case "noul":
			ans.Noul = a.Noul
			// noul has no confidence (§5.4); leave Confidence zero.
		default:
			// Unknown type: Type only, no values, no error.
		}
		out[id] = ans
	}
	return out
}
