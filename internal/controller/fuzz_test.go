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

package controller

import "testing"

// FuzzParseDataset feeds arbitrary bytes to the golden-dataset parser (user
// controlled via a ConfigMap/Secret): it must never panic and must honour the
// case cap.
func FuzzParseDataset(f *testing.F) {
	f.Add([]byte(`{"state":"x","questions":{"q":{"type":"choice","options":["a","b"]}},"expected":{"q":"a"}}`), 0)
	f.Add([]byte("{\"state\":1}\n\n{\"expected\":{\"q\":true}}\n"), 1)
	f.Add([]byte("not json"), 5)
	f.Add([]byte{}, 0)
	f.Fuzz(func(t *testing.T, raw []byte, maxCases int) {
		cases, err := parseDataset(raw, maxCases)
		if err != nil {
			return
		}
		if maxCases > 0 && len(cases) > maxCases {
			t.Fatalf("parseDataset returned %d cases, cap %d", len(cases), maxCases)
		}
	})
}

// FuzzParseProxySetting checks that proxy values from the environment never
// panic the parser.
func FuzzParseProxySetting(f *testing.F) {
	for _, s := range []string{
		"proxy:3128", "http://proxy:3128", "http://user:pass@proxy:3128",
		"https://[::1]:8443", "socks5://p:1080", "http://a b:p@host", "", "://",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v string) {
		_, _ = parseProxySetting(v)
	})
}
