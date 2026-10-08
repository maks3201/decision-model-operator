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
	"strings"
	"testing"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

const (
	hash10 = "cccccccccc"       // 10 hex (legacy width)
	hash16 = "cccccccccccccccc" // 16 hex (64-bit width)
)

// TestRevHashREWidths pins that the revision-hash validator accepts exactly 10
// or exactly 16 lowercase hex and nothing else. The 16-hex case is the one that
// regressed: the 64-bit revision hash in a newer line is 16 hex, and the old
// `^[a-f0-9]{10}$` refused it, so PrefetchJobSpec rendered the refuse script for
// every new revision.
func TestRevHashREWidths(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"10 hex accepted", "0123456789", true},
		{"16 hex accepted", "0123456789abcdef", true},
		{"all-f 10", "ffffffffff", true},
		{"all-f 16", "ffffffffffffffff", true},
		{"9 rejected", "012345678", false},
		{"11 rejected", "0123456789a", false},
		{"15 rejected", "0123456789abcde", false},
		{"17 rejected", "0123456789abcdef0", false},
		{"uppercase rejected", "ABCDEF0123", false},
		{"mixed case rejected", "0123456789ABCDEF", false},
		{"dotdot rejected", "..", false},
		{"slash rejected", "a/b", false},
		{"empty rejected", "", false},
		{"non-hex rejected", "gggggggggg", false},
		{"path-ish 10 with dot rejected", "abcde.fghi", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := revHashRE.MatchString(tt.input); got != tt.want {
				t.Errorf("revHashRE.MatchString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestPrefetchJobSpec16HexUsesPullScript is the regression guard: a 16-hex
// StoreSubPath must render the normal pull script (ollaya pull), NOT the
// refuse-and-exit-1 script. Before the fix this rendered invalidSubPathScript
// and the Job failed every attempt.
func TestPrefetchJobSpec16HexUsesPullScript(t *testing.T) {
	for _, h := range []string{hash10, hash16} {
		t.Run(h, func(t *testing.T) {
			p := baseParams()
			p.StoreSubPath = h
			p.KeepStoreSubPaths = []string{h}
			script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]
			if script == invalidSubPathScript {
				t.Fatalf("StoreSubPath %q (len %d) rendered the refuse script; want the pull script", h, len(h))
			}
			if !strings.Contains(script, `ollaya pull -- "$MODEL"`) {
				t.Errorf("expected the pull script for subPath %q", h)
			}
		})
	}
}

// TestPrefetchJobSpecInvalidSubPathStillRefused pins that widths outside {10,16}
// (and non-hex) still render the refuse script — the guard is not loosened to
// "any hex".
func TestPrefetchJobSpecInvalidSubPathStillRefused(t *testing.T) {
	for _, bad := range []string{"abcde", "0123456789a", "0123456789abcde", "0123456789abcdef0", "manifests"} {
		p := baseParams()
		p.StoreSubPath = bad
		script := New().PrefetchJobSpec(p).Template.Spec.Containers[0].Args[0]
		if script != invalidSubPathScript {
			t.Errorf("StoreSubPath %q should render the refuse script", bad)
		}
	}
}

// TestPrefetchJob16HexPruneEnabled pins that pruning works for 16-hex revisions
// exactly as for 10-hex: KEEP_SUBPATHS carries the 16-hex entries and the script
// has a 16-class case glob so a 16-hex sibling dir is recognised (and thus
// eligible for keep/remove), while legacy dirs are still never matched.
func TestPrefetchJob16HexPruneEnabled(t *testing.T) {
	keepA := "aaaaaaaaaaaaaaaa" // 16 hex
	p := baseParams()
	p.StoreSubPath = hash16
	p.KeepStoreSubPaths = []string{keepA, hash16}
	spec := New().PrefetchJobSpec(p).Template.Spec
	c := spec.Containers[0]
	env := envMap(c.Env)

	if env["KEEP_SUBPATHS"].Value != keepA+" "+hash16 {
		t.Errorf("KEEP_SUBPATHS = %q, want %q", env["KEEP_SUBPATHS"].Value, keepA+" "+hash16)
	}
	if env["STORE_ROOT"].Value != storeRootMount {
		t.Errorf("STORE_ROOT = %q, want %q", env["STORE_ROOT"].Value, storeRootMount)
	}
	script := c.Args[0]
	// Both case globs present: 10 classes and 16 classes.
	tenGlob := strings.Repeat("[0-9a-f]", 10)
	sixteenGlob := strings.Repeat("[0-9a-f]", 16)
	if !strings.Contains(script, tenGlob+")") {
		t.Errorf("prune case missing the 10-hex glob")
	}
	if !strings.Contains(script, sixteenGlob+")") {
		t.Errorf("prune case missing the 16-hex glob")
	}
}

// TestPruneKeepList16Hex exercises pruneKeepList directly for the 16-hex width:
// a valid 16-hex sub-path + keep list enables pruning; a mixed-width valid list
// is fine (both widths are valid hashes); an invalid-width entry disables it.
func TestPruneKeepList16Hex(t *testing.T) {
	tests := []struct {
		name        string
		sub         string
		keep        []string
		wantEnabled bool
	}{
		{"16-hex sub + 16-hex keep", hash16, []string{hash16}, true},
		{"16-hex sub + mixed-width valid keep", hash16, []string{hash10, hash16}, true},
		{"16-hex sub + invalid-width keep disables", hash16, []string{"0123456789abcde"}, false},
		{"invalid-width sub disables", "0123456789abcde", []string{hash16}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := engine.Params{StoreSubPath: tt.sub, KeepStoreSubPaths: tt.keep}
			_, enabled := pruneKeepList(p)
			if enabled != tt.wantEnabled {
				t.Errorf("pruneKeepList enabled = %v, want %v", enabled, tt.wantEnabled)
			}
		})
	}
}
