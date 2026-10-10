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
	neturl "net/url"
	"strings"
	"testing"
)

// effectivePort decides the port used for the redirect same-origin check
// (sameOrigin): an explicit port wins, else the scheme default, else "" for an
// unknown scheme. The unknown-scheme branch was previously uncovered; a wrong
// result there could make two different-origin URLs compare equal and allow a
// redirect the client should refuse.
func TestEffectivePort(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"https default", "https://host/path", defaultPortTLS},
		{"http default", "http://host/path", defaultPortHTTP},
		{"explicit port wins over https default", "https://host:8443/x", "8443"},
		{"explicit port wins over http default", "http://host:8080/x", "8080"},
		{"explicit port on an unknown scheme", "ftp://host:21/x", "21"},
		{"unknown scheme, no port -> empty", "ftp://host/x", ""},
		{"scheme-less, no port -> empty", "//host/x", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := neturl.Parse(tt.raw)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.raw, err)
			}
			if got := effectivePort(u); got != tt.want {
				t.Errorf("effectivePort(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// sameOrigin treats :443/https and :80/http as the scheme defaults, and a
// different scheme/host/port as a different origin. This exercises effectivePort
// through its real caller (the redirect guard).
func TestSameOrigin(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "https://h/x", "https://h/y", true},
		{"explicit 443 == implicit https", "https://h:443/x", "https://h/y", true},
		{"explicit 80 == implicit http", "http://h:80/x", "http://h/y", true},
		{"host differs", "https://a/x", "https://b/x", false},
		{"scheme differs", "https://h/x", "http://h/x", false},
		{"port differs", "https://h:8443/x", "https://h/x", false},
		{"hostname case-insensitive", "https://HOST/x", "https://host/x", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _ := neturl.Parse(tt.a)
			b, _ := neturl.Parse(tt.b)
			if got := sameOrigin(a, b); got != tt.want {
				t.Errorf("sameOrigin(%q,%q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// InvalidRuntimeVersionError.Error formats the message the controller surfaces on
// an InvalidRuntimeVersion / RuntimeVersionUnsupported condition; it must name the
// offending version and the reason.
func TestInvalidRuntimeVersionErrorMessage(t *testing.T) {
	err := &InvalidRuntimeVersionError{Version: "0.1.0", Reason: "older than the minimum supported version 0.7.3"}
	msg := err.Error()
	for _, want := range []string{"0.1.0", "older than the minimum supported version 0.7.3", "invalid runtime version"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, want it to contain %q", msg, want)
		}
	}
	// A real validation error round-trips through Error() with the version named.
	verr := ValidateRuntimeVersion("0.1.0")
	if verr == nil || !strings.Contains(verr.Error(), "0.1.0") {
		t.Errorf("ValidateRuntimeVersion(0.1.0).Error() = %v, want it to name 0.1.0", verr)
	}
}
