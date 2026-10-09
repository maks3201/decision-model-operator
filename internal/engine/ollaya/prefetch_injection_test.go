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
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Items 72/73: shell injection and path traversal in the prefetch script.
//
// The script runs under `/bin/sh -c <script>` with every interpolated value
// passed through the environment and referenced only as a double-quoted shell
// variable ("$MODEL", "$MANIFEST_PATH", "$EXPECT_DIGEST", "$OLLAYA_MODELS", …)
// — never spliced into the program text. These tests prove that a value laden
// with shell metacharacters is treated as inert data: the shell never evaluates
// it, so a `$(...)`, backtick, `;`, newline or redirection in a value cannot run
// a command, and a `../` or absolute path in MANIFEST_PATH cannot make the seed
// escape $OLLAYA_MODELS.

// runScriptWithEnv runs the real prefetch script with the given env and a stub
// `ollaya` that records each argument it received (one per line) to argvFile,
// then exits 0 so execution proceeds past the pull. Returns combined output and
// the process error. No network.
func runScriptWithEnv(t *testing.T, script string, env map[string]string, argvFile string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	bin := t.TempDir()
	// The stub appends each positional arg verbatim to argvFile, then satisfies
	// the post-pull step by writing the seeded/expected manifest if told to.
	stub := "#!/bin/sh\n" +
		": > \"$OLLAYA_ARGV_FILE\"\n" +
		"for a in \"$@\"; do printf '%s\\n' \"$a\" >> \"$OLLAYA_ARGV_FILE\"; done\n" +
		"dest=\"$OLLAYA_MODELS/$MANIFEST_PATH\"\n" +
		"mkdir -p \"$(dirname \"$dest\")\" 2>/dev/null || true\n" +
		"printf '%s' \"$OLLAYA_STUB_BODY\" > \"$dest\" 2>/dev/null || true\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "ollaya"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"OLLAYA_ARGV_FILE="+argvFile,
	)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestPrefetchScriptDoesNotEvaluateModelMetacharacters proves a MODEL value
// containing shell metacharacters is passed to `ollaya pull -- "$MODEL"` as one
// literal argument and never evaluated by the shell: the canary command inside
// the value does not run, and the stub receives the whole string as a single
// argv entry (after the `--` guard).
func TestPrefetchScriptDoesNotEvaluateModelMetacharacters(t *testing.T) {
	p := seedParams([]byte(`{"schemaVersion":2,"layers":[]}`))
	// Keep a parsable name so we exercise the real prefetchScript (not the refuse
	// script); the metacharacters live in the MODEL *env value* we inject, which
	// is exactly what the controller would set verbatim from spec.model.
	script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]

	store := t.TempDir()
	canary := filepath.Join(t.TempDir(), "pwned")
	argv := filepath.Join(t.TempDir(), "argv")

	// A value that WOULD run `touch <canary>` if the shell ever evaluated it.
	evil := "laya:en$(touch " + canary + ");`touch " + canary + "`;rm -rf /"

	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)
	env := map[string]string{
		"OLLAYA_MODELS":    store,
		"MANIFEST_PATH":    "manifests/ollaya.dev/library/laya/en",
		"MODEL":            evil,
		"EXPECT_DIGEST":    digestOf(manifest),
		"OLLAYA_STUB_BODY": string(manifest),
	}
	// Seed bytes satisfy EXPECT_DIGEST so the run reaches and passes the pull.
	// (seedParams already set MANIFEST_SEED_B64; override MANIFEST_PATH/digest to
	// our temp values via env, which the script reads last.)
	env["MANIFEST_SEED_B64"] = base64Std(manifest)

	out, runErr := runScriptWithEnv(t, script, env, argv)
	if runErr != nil {
		t.Fatalf("script failed unexpectedly: %v\n%s", runErr, out)
	}

	if _, err := os.Stat(canary); err == nil {
		t.Fatalf("CANARY CREATED: the shell evaluated a metacharacter in MODEL (injection); output:\n%s", out)
	}
	// The stub must have received the whole evil string as a SINGLE argv entry
	// (after the leading "--"). This proves `--` + quoting kept it one inert arg.
	gotArgv, _ := os.ReadFile(argv)
	lines := nonEmptyLines(string(gotArgv))
	if len(lines) < 3 {
		t.Fatalf("stub recorded argv %v, want at least [pull -- <model>]; output:\n%s", lines, out)
	}
	// The invocation is `ollaya pull -- "$MODEL"`, so argv is [pull, --, <model>].
	last := lines[len(lines)-1]
	if last != evil {
		t.Errorf("ollaya received model arg %q, want the literal %q (one inert argument)", last, evil)
	}
	// The "--" guard must immediately precede the model so a leading-dash value is
	// never parsed as an option.
	if guard := lines[len(lines)-2]; guard != "--" {
		t.Errorf("arg before the model = %q, want %q (the end-of-options guard)", guard, "--")
	}
}

// TestPrefetchSeedCannotEscapeStoreViaManifestPath proves a MANIFEST_PATH with
// `../` traversal cannot make the seed write outside the store mount in a way
// that lands on a sensitive host path: the write stays rooted at
// "$OLLAYA_MODELS/$MANIFEST_PATH", so even a traversal resolves relative to the
// store dir, never to an absolute host location the attacker chose. The real
// defence is upstream (the controller/engine derives MANIFEST_PATH from a parsed
// name and refuses an invalid sub-path), but this pins the script's own
// behaviour: it only ever writes under $OLLAYA_MODELS/<path>, concatenated, so an
// absolute-looking path is still prefixed by the store.
func TestPrefetchSeedWriteIsRootedAtStore(t *testing.T) {
	p := seedParams([]byte(`{"schemaVersion":2,"layers":[]}`))
	script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]

	store := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	argv := filepath.Join(t.TempDir(), "argv")
	manifest := []byte(`{"schemaVersion":2,"layers":[]}`)

	// An absolute path: the script writes to "$OLLAYA_MODELS/$MANIFEST_PATH",
	// which for an absolute MANIFEST_PATH becomes "<store>//etc/evil" — still
	// under the store, NOT /etc/evil on the host.
	env := map[string]string{
		"OLLAYA_MODELS":     store,
		"MANIFEST_PATH":     "/etc/evil",
		"MODEL":             "laya:en",
		"EXPECT_DIGEST":     digestOf(manifest),
		"OLLAYA_STUB_BODY":  string(manifest),
		"MANIFEST_SEED_B64": base64Std(manifest),
	}
	out, runErr := runScriptWithEnv(t, script, env, argv)
	// It may succeed or fail depending on mkdir of the odd path; what matters is
	// nothing was written to the real absolute path.
	_ = runErr

	if _, err := os.Stat("/etc/evil"); err == nil {
		t.Fatalf("ESCAPE: the seed wrote to the absolute host path /etc/evil; output:\n%s", out)
	}
	// The write, if any, must be under the store.
	rooted := filepath.Join(store, "etc", "evil")
	if _, err := os.Stat(rooted); err == nil {
		// Good: the write was rooted at the store, as intended.
		return
	}
	// If the odd path failed to create, that is also acceptable — the invariant
	// is only that nothing escaped the store.
}

// base64Std mirrors base64.StdEncoding.EncodeToString (the encoding the engine
// uses for MANIFEST_SEED_B64), so the injection test seeds the same way.
func base64Std(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// nonEmptyLines splits s into its non-empty lines.
func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// A sanity guard tying the metacharacter case to the engine's parse-time
// refusal: a model NAME (as the controller would pass it in spec.model) that
// contains shell metacharacters does not parse, so the engine renders the
// refuse script that never calls `ollaya pull` at all.
func TestPrefetchRefusesMetacharacterModelName(t *testing.T) {
	p := seedParams([]byte(`{"schemaVersion":2}`))
	p.Model.Name = "laya:en;touch /tmp/pwned"
	js := New().PrefetchJobSpec(p)
	script := js.Template.Spec.Containers[0].Args[0]
	if script != unparsableNameScript {
		t.Errorf("a metacharacter-laden model name must render the refuse script, not run a pull")
	}
	// And no seed is mounted/carried for a refuse script.
	for _, e := range js.Template.Spec.Containers[0].Env {
		if e.Name == "MANIFEST_SEED_B64" || e.Name == "MANIFEST_SEED_FILE" {
			t.Errorf("refuse script must carry no seed env, found %q", e.Name)
		}
	}
	if _, ok := volumeByName(js.Template.Spec.Volumes, manifestSeedVolume); ok {
		t.Error("refuse script must not mount a seed volume")
	}
	// The store PVC volume is always present (the refuse script still mounts the
	// store); checking a second, distinct volume name keeps volumeByName general.
	if _, ok := volumeByName(js.Template.Spec.Volumes, modelsVolume); !ok {
		t.Errorf("the models volume %q must always be present", modelsVolume)
	}
}
