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

// failFuncFromScript extracts the real fail() shell function from prefetchScript
// so the executable tests run exactly the code the Job runs (not a copy). It
// returns the text from "fail() {" up to and including its closing "}".
func failFuncFromScript(t *testing.T) string {
	t.Helper()
	const start = "fail() {"
	i := strings.Index(prefetchScript, start)
	if i < 0 {
		t.Fatalf("prefetchScript has no fail() function")
	}
	rest := prefetchScript[i:]
	// The function body ends at the first line that is exactly "}".
	j := strings.Index(rest, "\n}\n")
	if j < 0 {
		t.Fatalf("could not find the end of fail()")
	}
	return rest[:j+len("\n}")]
}

// runFail runs the real fail() with the given args in /bin/sh and returns its
// combined output (stdout+stderr). fail() prints the same "reason:"/"detail:"
// lines to stdout that it writes to /dev/termination-log, and the Job's
// TerminationMessageFallbackToLogsOnError makes the controller read exactly
// those log lines when the file is empty (as it is on a plain host). So the
// captured stdout is the message the controller classifies. No network.
func runFail(t *testing.T, reason, exit, detail string) string {
	t.Helper()
	// Call fail with "$@" so the detail (which may be empty) is passed exactly as
	// the script passes it. set -eu matches the script's own options.
	prog := "set -eu\n" + failFuncFromScript(t) + "\nfail \"$@\"\n"
	cmd := exec.Command("/bin/sh", "-c", prog, "sh", reason, exit, detail)
	out, err := cmd.CombinedOutput()
	// fail() always exit's non-zero; that is expected, not a test failure.
	if err == nil {
		t.Fatalf("fail() exited 0, want non-zero")
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if got := ee.ExitCode(); strings.TrimSpace(exit) != "" && got != atoiOrFatal(t, exit) {
			t.Errorf("fail() exit code = %d, want %s", got, exit)
		}
	}
	return string(out)
}

func atoiOrFatal(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, r := range strings.TrimSpace(s) {
		if r < '0' || r > '9' {
			t.Fatalf("exit %q is not numeric", s)
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// TestPrefetchFailWritesDetailAndClassifies runs the real fail() for the two
// permanent branches with a two-short-digest detail, then checks that
// classifyPrefetchFailure maps the produced message to the right reason and that
// both short digests appear in the detail line. This is the end-to-end tie
// between the script's output and the controller's classification, no network.
func TestPrefetchFailWritesDetailAndClassifies(t *testing.T) {
	const wantShort = "c305a9276531" // laya:en recorded digest, first 12 hex
	const gotShort = "deadbeef0000"  // a different digest, first 12 hex

	tests := []struct {
		name   string
		reason string
		exit   string
		detail string
		wantR  string
		wantP  bool
	}{
		{
			name:   "tag moved carries both short digests",
			reason: PrefetchReasonUpstreamTagMoved,
			exit:   prefetchExitTagMovedStr,
			detail: "recorded " + wantShort + ", registry now serves " + gotShort,
			wantR:  PrefetchReasonUpstreamTagMoved,
			wantP:  true,
		},
		{
			name:   "digest mismatch carries both short digests",
			reason: PrefetchReasonDigestMismatch,
			exit:   prefetchExitDigestMismatchStr,
			detail: "recorded " + wantShort + ", got " + gotShort,
			wantR:  PrefetchReasonDigestMismatch,
			wantP:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := runFail(t, tt.reason, tt.exit, tt.detail)

			// The classifier maps the produced message to the right reason.
			reason, permanent := classifyPrefetchFailure(msg, 0)
			if reason != tt.wantR || permanent != tt.wantP {
				t.Errorf("classify(%q) = (%q, %v), want (%q, %v)", msg, reason, permanent, tt.wantR, tt.wantP)
			}
			// The "reason: <X>" line is present (what the classifier matches).
			if !strings.Contains(msg, "reason: "+tt.wantR) {
				t.Errorf("message %q missing %q", msg, "reason: "+tt.wantR)
			}
			// Both short digests appear, in a detail: line.
			if !strings.Contains(msg, "detail: ") {
				t.Errorf("message %q has no detail line", msg)
			}
			if !strings.Contains(msg, wantShort) || !strings.Contains(msg, gotShort) {
				t.Errorf("message %q must contain both short digests %q and %q", msg, wantShort, gotShort)
			}
			// Small: well under the 4096-byte termination-message limit.
			if len(msg) > 4096 {
				t.Errorf("message is %d bytes, must be < 4096", len(msg))
			}
		})
	}
}

// TestPrefetchFailWithoutDetailHasNoDetailLine pins that an empty detail (the
// transient / model-not-found calls, which pass no $3) produces no detail line,
// so the message is exactly the reason the classifier already handled before.
func TestPrefetchFailWithoutDetailHasNoDetailLine(t *testing.T) {
	msg := runFail(t, PrefetchReasonTransient, prefetchExitTransientStr, "")
	if strings.Contains(msg, "detail:") {
		t.Errorf("message %q must have no detail line when no detail is passed", msg)
	}
	if reason, permanent := classifyPrefetchFailure(msg, 0); reason != PrefetchReasonTransient || permanent {
		t.Errorf("classify(%q) = (%q, %v), want (Transient, false)", msg, reason, permanent)
	}
}
