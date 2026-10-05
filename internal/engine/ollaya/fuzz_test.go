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

import "testing"

// FuzzCanonicalName checks that model-name canonicalisation never panics and is
// idempotent: a canonical name canonicalises to itself.
func FuzzCanonicalName(f *testing.F) {
	for _, s := range []string{
		"laya:en", "LAYA:EN", "library/laya:en", "ollaya.dev/library/laya:en",
		"registry.example.com:5000/team/nli:v1", "https://ollaya.dev/laya:en",
		"laya", ":en", "a/b/c/d:e", "host..x/laya:en", "",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		got, err := CanonicalName(name)
		if err != nil {
			return
		}
		again, err := CanonicalName(got)
		if err != nil {
			t.Fatalf("CanonicalName(%q) = %q, which is rejected: %v", name, got, err)
		}
		if again != got {
			t.Fatalf("not idempotent: %q -> %q -> %q", name, got, again)
		}
	})
}
