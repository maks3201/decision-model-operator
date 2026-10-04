//go:build integration

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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// wantLayaEnDigest is the recorded digest for laya:en (2026-09-27).
const wantLayaEnDigest = "c305a9276531a47000bf93559d2c94f1ed6cbb67055c9151084682dad7655e9d"

// TestIntegrationResolveLayaEn resolves laya:en against the real ollaya.dev
// registry and checks the recorded digest. Run with:
//
//	go test -tags integration ./internal/engine/ollaya/ -run Integration -v
func TestIntegrationResolveLayaEn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e := New()
	ref, err := e.Resolve(ctx, "laya:en")
	if err != nil {
		t.Fatalf("Resolve(laya:en) against real registry: %v", err)
	}
	if ref.Digest != wantLayaEnDigest {
		// Do not fail silently: surface the new value so the record can be updated.
		t.Fatalf("laya:en digest changed upstream: got %q, recorded %q (update the recorded value)",
			ref.Digest, wantLayaEnDigest)
	}
	t.Logf("laya:en -> name=%q digest=%s", ref.Name, ref.Digest)
}

// TestIntegrationResolveNotFound resolves a model that does not exist and
// expects engine.ErrNotFound.
func TestIntegrationResolveNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e := New()
	_, err := e.Resolve(ctx, "definitely-not-a-real-model:nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Resolve(nonexistent) error = %v, want errors.Is engine.ErrNotFound", err)
	}
	t.Logf("nonexistent model correctly returned ErrNotFound: %v", err)
}
