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
	"os/exec"
	"strings"
	"testing"
)

// pullCaseFromScript extracts the real `case "$1" in … esac` block from
// classify_pull_failure() in prefetchScript so the test classifies the exact
// strings the shipped script classifies (no duplicated patterns).
func pullCaseFromScript(t *testing.T) string {
	t.Helper()
	const start = `case "$1" in`
	i := strings.Index(prefetchScript, start)
	if i < 0 {
		t.Fatalf("prefetchScript has no classify_pull_failure case")
	}
	rest := prefetchScript[i:]
	j := strings.Index(rest, "esac")
	if j < 0 {
		t.Fatalf("could not find the end of the classify case")
	}
	return rest[:j+len("esac")]
}

// runPullCase feeds pullErr through the real fail() + classify case and returns
// the fail() exit code (the prefetch exit the Job reports). No network.
func runPullCase(t *testing.T, pullErr string) int {
	t.Helper()
	prog := "set -u\n" +
		failFuncFromScript(t) + "\n" +
		pullCaseFromScript(t) + "\n"
	cmd := exec.Command("/bin/sh", "-c", prog, "sh", pullErr)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected fail() to exit non-zero for %q; output=%s", pullErr, out)
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("run pull case for %q: %v (output=%s)", pullErr, err, out)
	}
	return ee.ExitCode()
}

// These are the real `ollaya pull` stderr strings observed against
// ghcr.io/ollaya-dev/ollaya:0.12.1, plus the 0.13.0 digest-pull strings from
// spike 011. The script must map availability errors to Transient (exit 1), a
// missing tag or an unknown digest to ModelNotFound (exit 3), a digest the
// registry cannot honour (DIGEST_MISMATCH) to DigestMismatch (exit 4),
// auth/space/permission and anything unrecognised to Unknown (exit 2) — never a
// blind Transient catch-all.
func TestPrefetchPullErrorClassification(t *testing.T) {
	tests := []struct {
		name    string
		pullErr string
		want    int // prefetch exit: 1 Transient, 2 Unknown, 3 ModelNotFound
	}{
		// availability -> Transient (1)
		{"connection refused / DNS / reset (transport)", "Error: error sending request for url (http://127.0.0.1:1/v2/library/laya/manifests/en)", prefetchExitTransient},
		{"503 service unavailable", "Error: HTTP status server error (503 Service Unavailable) for url (http://mirror:8080/v2/library/laya/manifests/en)", prefetchExitTransient},
		{"500 internal server error", "Error: HTTP status server error (500 Internal Server Error) for url (http://mirror/v2/...)", prefetchExitTransient},
		{"502 bad gateway", "Error: HTTP status server error (502 Bad Gateway) for url (http://mirror/v2/...)", prefetchExitTransient},
		{"429 too many requests", "Error: HTTP status client error (429 Too Many Requests) for url (http://ollaya.dev/v2/...)", prefetchExitTransient},
		{"408 request timeout", "Error: HTTP status client error (408 Request Timeout) for url (http://ollaya.dev/v2/...)", prefetchExitTransient},
		{"operation timed out", "Error: operation timed out after 30s", prefetchExitTransient},
		// missing tag -> ModelNotFound (3)
		{"model not found in registry", `Error: model "laya:nope" not found in registry ollaya.dev`, prefetchExitModelNotFound},
		// 0.13.0 digest pull: unknown digest -> MODEL_NOT_FOUND / ModelNotFound (3)
		{"unknown digest (0.13.0)", `Error: model "laya:en@sha256:0000000000000000000000000000000000000000000000000000000000000000" not found in registry ollaya.dev: it has no manifest with that digest`, prefetchExitModelNotFound},
		// 0.13.0 digest pull: registry served the wrong bytes for the digest ->
		// DigestMismatch (4), permanent (retrying the same digest cannot help).
		{"digest mismatch CLI message (0.13.0)", `Error: the registry served manifest sha256:f6888fde... for "laya:en@sha256:aaaa", not the one its digest names; nothing was stored`, prefetchExitDigestMismatch},
		{"digest mismatch code (0.13.0)", `Error: DIGEST_MISMATCH`, prefetchExitDigestMismatch},
		// auth / space / permission / unknown -> Unknown (2)
		{"401 unauthorized", "Error: HTTP status client error (401 Unauthorized) for url (http://ollaya.dev/v2/...)", prefetchExitUnknown},
		{"403 forbidden", "Error: HTTP status client error (403 Forbidden) for url (http://ollaya.dev/v2/...)", prefetchExitUnknown},
		{"no space left on device", "Error: write /models/blobs/sha256-ab: no space left on device", prefetchExitUnknown},
		{"permission denied", "Error: open /models/manifests/...: permission denied", prefetchExitUnknown},
		{"400 bad request (unrecognised 4xx)", "Error: HTTP status client error (400 Bad Request) for url (http://ollaya.dev/v2/...)", prefetchExitUnknown},
		{"wholly unrecognised error", "Error: something nobody has seen before", prefetchExitUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runPullCase(t, tt.pullErr); got != tt.want {
				t.Errorf("pull error %q -> exit %d, want %d", tt.pullErr, got, tt.want)
			}
		})
	}
}
