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

package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseWatchNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{name: "empty is all namespaces", raw: "", want: nil},
		{name: "whitespace only is all namespaces", raw: "   ", want: nil},
		{name: "single", raw: "prod", want: []string{"prod"}},
		{name: "multiple", raw: "a,b,c", want: []string{"a", "b", "c"}},
		{name: "trims surrounding whitespace", raw: " a , b ,c ", want: []string{"a", "b", "c"}},
		{name: "dedupes keeping first", raw: "a,b,a,c,b", want: []string{"a", "b", "c"}},
		{name: "dns-1123 with hyphen and digits", raw: "ns-1,team-42", want: []string{"ns-1", "team-42"}},
		{name: "empty item between commas", raw: "a,,b", wantErr: true},
		{name: "trailing comma", raw: "a,b,", wantErr: true},
		{name: "leading comma", raw: ",a", wantErr: true},
		{name: "uppercase rejected", raw: "Prod", wantErr: true},
		{name: "underscore rejected", raw: "my_ns", wantErr: true},
		{name: "leading hyphen rejected", raw: "-ns", wantErr: true},
		{name: "too long rejected", raw: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWatchNamespaces(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseWatchNamespaces(%q) = %v, want error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseWatchNamespaces(%q) unexpected error: %v", tt.raw, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseWatchNamespaces(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestWatchNamespacesCacheDefaults(t *testing.T) {
	// Unset (cluster-wide): nil map, so the manager cache stays cluster-scoped.
	if got := watchNamespacesCacheDefaults(nil); got != nil {
		t.Fatalf("watchNamespacesCacheDefaults(nil) = %v, want nil (cluster-wide)", got)
	}
	if got := watchNamespacesCacheDefaults([]string{}); got != nil {
		t.Fatalf("watchNamespacesCacheDefaults([]) = %v, want nil (cluster-wide)", got)
	}

	// Set: exactly one cache.Config entry per watched namespace.
	ns := []string{"a", "b"}
	got := watchNamespacesCacheDefaults(ns)
	if len(got) != len(ns) {
		t.Fatalf("watchNamespacesCacheDefaults(%v) has %d entries, want %d", ns, len(got), len(ns))
	}
	for _, n := range ns {
		if _, ok := got[n]; !ok {
			t.Fatalf("watchNamespacesCacheDefaults(%v) missing namespace %q", ns, n)
		}
	}
}

func TestValidateHFEndpoint(t *testing.T) {
	tests := []struct {
		name          string
		raw           string
		allowInsecure bool
		wantErr       bool
	}{
		{name: "valid https", raw: "https://hf-mirror.example.com"},
		{name: "valid https with path", raw: "https://mirror.example.com/models"},
		{name: "valid https with port", raw: "https://mirror.example.com:8443"},
		{name: "http rejected by default", raw: "http://mirror.example.com", wantErr: true},
		{name: "http allowed with insecure flag", raw: "http://mirror.example.com", allowInsecure: true},
		{name: "empty rejected", raw: "", wantErr: true},
		{name: "not a URL rejected", raw: "::::", wantErr: true},
		{name: "relative rejected", raw: "mirror.example.com/models", wantErr: true},
		{name: "no host rejected", raw: "https://", wantErr: true},
		{name: "userinfo rejected", raw: "https://user:pass@mirror.example.com", wantErr: true},
		{
			name: "userinfo rejected even with insecure", raw: "http://user@mirror.example.com",
			allowInsecure: true, wantErr: true,
		},
		{name: "query rejected", raw: "https://mirror.example.com/?token=x", wantErr: true},
		{name: "fragment rejected", raw: "https://mirror.example.com/#frag", wantErr: true},
		{name: "ftp scheme rejected", raw: "ftp://mirror.example.com", wantErr: true},
		{name: "scheme-only rejected", raw: "https:///models", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHFEndpoint(tt.raw, tt.allowInsecure)
			if tt.wantErr && err == nil {
				t.Fatalf("validateHFEndpoint(%q, %v) = nil, want error", tt.raw, tt.allowInsecure)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateHFEndpoint(%q, %v) unexpected error: %v", tt.raw, tt.allowInsecure, err)
			}
		})
	}
}

func TestWarnSkippedProxyEnv(t *testing.T) {
	type call struct {
		msg string
		kv  []any
	}
	run := func(skipped []string) (calls []call) {
		warnSkippedProxyEnv(func(msg string, kv ...any) { calls = append(calls, call{msg, kv}) }, skipped)
		return calls
	}

	if got := run(nil); len(got) != 0 {
		t.Errorf("nothing skipped must log nothing, got %v", got)
	}
	if got := run([]string{}); len(got) != 0 {
		t.Errorf("empty skipped must log nothing, got %v", got)
	}

	got := run([]string{"HTTPS_PROXY", "https_proxy"})
	if len(got) != 1 {
		t.Fatalf("expected exactly ONE warning, got %d: %v", len(got), got)
	}
	// The warning carries key names only, as a structured value.
	if len(got[0].kv) != 2 || got[0].kv[0] != "variables" {
		t.Fatalf("expected a single structured field 'variables', got %v", got[0].kv)
	}
	if !reflect.DeepEqual(got[0].kv[1], []string{"HTTPS_PROXY", "https_proxy"}) {
		t.Errorf("variables = %v, want the skipped key names", got[0].kv[1])
	}
	// Nothing in the message can be a credential: it is a constant sentence.
	for _, bad := range []string{"@", "://", "s3cr3t"} {
		if strings.Contains(got[0].msg, bad) {
			t.Errorf("warning message contains %q: %q", bad, got[0].msg)
		}
	}
}
