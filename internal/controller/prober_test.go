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

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

// proberFakeEngine is a minimal engine.Engine for exercising the default
// engineProber. It returns a fixed set of loaded models from Inspect.
type proberFakeEngine struct {
	loaded []engine.Loaded
}

func (proberFakeEngine) Name() string { return "ollaya" }
func (proberFakeEngine) Resolve(context.Context, string) (engine.ModelRef, error) {
	return engine.ModelRef{}, nil
}
func (proberFakeEngine) ServingPodSpec(engine.Params) corev1.PodSpec   { return corev1.PodSpec{} }
func (proberFakeEngine) PrefetchJobSpec(engine.Params) batchv1.JobSpec { return batchv1.JobSpec{} }
func (proberFakeEngine) ServicePort() int32                            { return 11435 }
func (e proberFakeEngine) Inspect(context.Context, string, string) ([]engine.Loaded, error) {
	return e.loaded, nil
}
func (proberFakeEngine) Warmup(context.Context, string, string, string) error { return nil }

// canonicalProberEngine adds the optional CanonicalName capability.
type canonicalProberEngine struct{ proberFakeEngine }

func (canonicalProberEngine) CanonicalName(name string) (string, error) {
	return strings.ToLower(name), nil
}

func readyPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p0"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.1"},
	}
}

// probeUntilReady drives the async prober until warmup completes (the fake
// engine's Warmup returns immediately), then returns the final Probe result.
func probeUntilReady(p Prober, pod *corev1.Pod, eng engine.Engine, model string) (engine.Loaded, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		loaded, err := p.Probe(context.Background(), pod, eng, "", model)
		if !errors.Is(err, errWarmupInProgress) {
			return loaded, err
		}
		if time.Now().After(deadline) {
			return engine.Loaded{}, err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestProberCanonicalMatch verifies that with an engine exposing CanonicalName,
// a differently-cased spec model matches the /api/ps entry; without it, the
// plain compare fails on a case difference but succeeds on an exact match.
func TestProberCanonicalMatch(t *testing.T) {
	loaded := engine.Loaded{Name: "laya:en", Digest: "abc", Device: "cpu", Precision: "F32"}

	t.Run("canonical engine matches different case", func(t *testing.T) {
		eng := canonicalProberEngine{proberFakeEngine{loaded: []engine.Loaded{loaded}}}
		got, err := probeUntilReady(NewProber(nil), readyPod(), eng, "Laya:EN")
		if err != nil {
			t.Fatalf("expected match, got error: %v", err)
		}
		if got.Digest != "abc" {
			t.Fatalf("expected loaded entry, got %+v", got)
		}
	})

	t.Run("plain compare fails on case difference", func(t *testing.T) {
		eng := proberFakeEngine{loaded: []engine.Loaded{loaded}}
		if _, err := probeUntilReady(NewProber(nil), readyPod(), eng, "Laya:EN"); err == nil {
			t.Fatal("expected no match without canonicalization")
		}
	})

	t.Run("plain compare matches exact name", func(t *testing.T) {
		eng := proberFakeEngine{loaded: []engine.Loaded{loaded}}
		if _, err := probeUntilReady(NewProber(nil), readyPod(), eng, "laya:en"); err != nil {
			t.Fatalf("expected exact match, got error: %v", err)
		}
	})
}

// PruneWarm must only drop entries belonging to the DecisionModel being
// reconciled — reconciling DM A must not reset DM B's warm state.
func TestPruneWarmScopedPerDecisionModel(t *testing.T) {
	p := NewProber(nil).(*engineProber)
	// Two DMs; A has a live Pod (a1) and a stale one (a2); B has one Pod (b1).
	p.warm = map[types.UID]*warmState{
		"a1": {done: true, owner: "ns/A"},
		"a2": {done: true, owner: "ns/A"},
		"b1": {done: true, owner: "ns/B"},
	}

	// Reconcile DM A: only a1 is live.
	p.PruneWarm("ns/A", map[types.UID]bool{"a1": true})

	if _, ok := p.warm["a1"]; !ok {
		t.Error("a1 (live, DM A) must be kept")
	}
	if _, ok := p.warm["a2"]; ok {
		t.Error("a2 (stale, DM A) must be pruned")
	}
	if _, ok := p.warm["b1"]; !ok {
		t.Error("b1 (DM B) must be untouched when reconciling DM A")
	}
}
