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
)

// Item 71: fuzz the full name parser (parseName) and RegistryHost, distinct from
// FuzzCanonicalName which fuzzes the higher-level CanonicalName. The invariants:
//   - parseName never panics on arbitrary input (unicode, "..", slashes, empty
//     tag, host:port, malformed digest-looking tags).
//   - an accepted name round-trips: its canonical() re-parses to the same
//     canonical string (so every stored name is stable).
//   - the derived on-disk manifest path never contains a ".." path segment, so a
//     crafted name cannot make the prefetch Job write outside the store.
//   - RegistryHost never panics and never returns an empty host without an error.
func FuzzParseName(f *testing.F) {
	seeds := []string{
		"laya:en",
		"LAYA:EN",
		"library/laya:en",
		"ollaya.dev/library/laya:en",
		"registry.example.com:5000/team/nli:v1",
		"https://ollaya.dev/laya:en",
		"http://localhost:5000/ns/m",
		"laya",
		":en",
		"laya:",
		"a/b/c/d:e",
		"host..x/laya:en",
		"../../etc/passwd:tag",
		"laya:../../../evil",
		"ns/../m:t",
		"ollaya.dev/ns/m:@sha256:deadbeef",
		"name:tag:extra",
		"laya:en\n",
		"laya\t:en",
		"ПÑ€Ð¸Ð²ÐµÑ‚/Ð¼Ð¾Ð´ÐµÐ»ÑŒ:Ñ‚ÐµÐ³", // unicode namespace/model/tag
		"🙂:🙂",
		"registry.com:/ns/m",
		"registry.com:99999/ns/m",
		"",
		"///",
		":",
		"a:b:c:d",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, name string) {
		p, err := parseName(name)
		if err != nil {
			return // rejected names carry no further invariant
		}

		// An accepted name must round-trip through its canonical form.
		canon := p.canonical()
		if canon == "" {
			t.Fatalf("parseName(%q) accepted but canonical() is empty", name)
		}
		p2, err := parseName(canon)
		if err != nil {
			t.Fatalf("canonical %q (from %q) does not re-parse: %v", canon, name, err)
		}
		if again := p2.canonical(); again != canon {
			t.Fatalf("not idempotent: %q -> %q -> %q", name, canon, again)
		}

		// The on-disk manifest path must never contain a ".." segment: that is
		// what keeps a crafted name from escaping $OLLAYA_MODELS when the Job
		// writes "$OLLAYA_MODELS/$MANIFEST_PATH".
		path := p.manifestDiskPath("ollaya.dev")
		for _, seg := range strings.Split(path, "/") {
			if seg == ".." {
				t.Fatalf("manifest path %q (from %q) contains a .. segment", path, name)
			}
		}

		// RegistryHost on the same accepted name must not panic and must not
		// yield an empty host without an error.
		e := New()
		host, _, hErr := e.RegistryHost(name)
		if hErr == nil && host == "" {
			t.Fatalf("RegistryHost(%q) returned an empty host with no error", name)
		}
	})
}
