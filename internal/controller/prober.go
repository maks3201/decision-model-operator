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
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// inspectTimeout bounds the synchronous /api/ps inspect done in Reconcile so a
// single Pod can never hold a worker for long.
const inspectTimeout = 5 * time.Second

// warmupBaseTimeout bounds a single background warmup attempt.
const warmupBaseTimeout = 2 * time.Minute

// errWarmupInProgress signals that the model is still being warmed on the Pod;
// the controller should not treat this as a mismatch — just requeue.
var errWarmupInProgress = errors.New("warmup in progress")

// errModelNotLoaded is returned by Reinspect when the runtime answered but the
// expected model is no longer listed — a definitive model loss that must flip
// the readiness gate False. It is distinct from a transport/auth error (e.g. a
// 401 during an API-key rotation), which is transient and must not drop a
// healthy Pod from the Service.
var errModelNotLoaded = errors.New("model no longer loaded")

// Prober warms up (asynchronously) and inspects a single serving Pod, returning
// the engine's view of the given model as loaded in that Pod. It is an
// interface so tests can inject a fake without a running runtime.
type Prober interface {
	Probe(ctx context.Context, pod *corev1.Pod, eng engine.Engine, apiKey, model string) (engine.Loaded, error)
}

// warmPruner is an optional Prober capability: drop cached warm state for Pods
// of a given DecisionModel (owner "<namespace>/<name>") not in the live set, so
// the map does not leak across rollouts without touching other DMs.
type warmPruner interface {
	PruneWarm(owner string, live map[types.UID]bool)
}

// engineProber is the default Prober. It performs Warmup asynchronously in a
// goroutine keyed by Pod UID (so a slow model never blocks the reconcile
// worker) and does a short, synchronous Inspect once warmup has completed.
type engineProber struct {
	baseCtx func() context.Context
	mu      sync.Mutex
	warm    map[types.UID]*warmState
}

type warmState struct {
	done bool
	err  error
	// owner is "<namespace>/<decisionmodel.io/name>" of the Pod this entry belongs
	// to, so PruneWarm only touches the DecisionModel being reconciled.
	owner string
}

// NewProber returns the default engine-backed Prober. baseCtx supplies the
// manager-lifetime context for background warmups (nil -> context.Background).
func NewProber(baseCtx func() context.Context) Prober {
	if baseCtx == nil {
		baseCtx = context.Background
	}
	return &engineProber{baseCtx: baseCtx, warm: map[types.UID]*warmState{}}
}

// canonicalizer is an optional engine capability: it normalises a model name so
// that equivalent forms (e.g. "Laya:EN" and "laya:en") compare equal. The
// engine.Engine contract does not require it, so it is detected via assertion.
type canonicalizer interface {
	CanonicalName(name string) (string, error)
}

// canonicalName normalises name via the engine when it implements
// CanonicalName; otherwise it returns name unchanged (plain compare fallback).
func canonicalName(eng engine.Engine, name string) string {
	if c, ok := eng.(canonicalizer); ok {
		if canon, err := c.CanonicalName(name); err == nil {
			return canon
		}
	}
	return name
}

// podBaseURL builds the http base URL for a Pod IP and port. net.JoinHostPort
// brackets IPv6 literals (fd00::123 -> [fd00::123]:11435).
func podBaseURL(ip string, port int32) string {
	return "http://" + net.JoinHostPort(ip, strconv.Itoa(int(port)))
}

// Probe ensures a background warmup for the Pod, then (once warmup completed)
// does a short synchronous Inspect and returns the entry matching model
// (compared in canonical form). While warmup is still running it returns
// errWarmupInProgress without blocking the worker.
func (p *engineProber) Probe(
	ctx context.Context,
	pod *corev1.Pod,
	eng engine.Engine,
	apiKey, model string,
) (engine.Loaded, error) {
	if pod.Status.PodIP == "" {
		return engine.Loaded{}, fmt.Errorf("pod %s/%s has no PodIP", pod.Namespace, pod.Name)
	}
	baseURL := podBaseURL(pod.Status.PodIP, eng.ServicePort())

	// Kick off / observe the async warmup keyed by Pod UID.
	owner := pod.Namespace + "/" + pod.Labels[decisionmodelv1alpha1.LabelName]
	st := p.ensureWarm(pod.UID, owner, eng, baseURL, apiKey, model, warmupTimeoutFrom(ctx))
	if !st.done {
		return engine.Loaded{}, errWarmupInProgress
	}
	if st.err != nil {
		// Warmup failed; allow a retry on the next probe and surface the error.
		p.forget(pod.UID)
		return engine.Loaded{}, fmt.Errorf("warmup %q on %s: %w", model, pod.Name, st.err)
	}

	inspectCtx, cancel := context.WithTimeout(ctx, inspectTimeout)
	defer cancel()
	loaded, err := eng.Inspect(inspectCtx, baseURL, apiKey)
	if err != nil {
		return engine.Loaded{}, fmt.Errorf("inspect %s: %w", pod.Name, err)
	}
	want := canonicalName(eng, model)
	for _, l := range loaded {
		if canonicalName(eng, l.Name) == want {
			return l, nil
		}
	}
	return engine.Loaded{}, fmt.Errorf("model %q not loaded on %s", model, pod.Name)
}

// Reinspect implements the optional reinspector capability: an Inspect-only
// check of a Pod that is already serving (no warmup). When the model is no
// longer listed the Pod's warm state is dropped, so the next Probe runs the
// normal warmup again and reloads it. A transport error keeps the warm state:
// the runtime is unreachable, not necessarily empty, and a plain Inspect is
// enough to recover once it answers again.
func (p *engineProber) Reinspect(
	ctx context.Context,
	pod *corev1.Pod,
	eng engine.Engine,
	apiKey, model string,
) (engine.Loaded, error) {
	if pod.Status.PodIP == "" {
		return engine.Loaded{}, fmt.Errorf("pod %s/%s has no PodIP", pod.Namespace, pod.Name)
	}
	baseURL := podBaseURL(pod.Status.PodIP, eng.ServicePort())
	inspectCtx, cancel := context.WithTimeout(ctx, inspectTimeout)
	defer cancel()
	loaded, err := eng.Inspect(inspectCtx, baseURL, apiKey)
	if err != nil {
		return engine.Loaded{}, fmt.Errorf("inspect %s: %w", pod.Name, err)
	}
	want := canonicalName(eng, model)
	for _, l := range loaded {
		if canonicalName(eng, l.Name) == want {
			return l, nil
		}
	}
	p.forget(pod.UID)
	return engine.Loaded{}, fmt.Errorf("%w: model %q on %s", errModelNotLoaded, model, pod.Name)
}

// ensureWarm returns the warm state for a Pod UID, starting a background warmup
// the first time it is seen.
func (p *engineProber) ensureWarm(
	uid types.UID, owner string, eng engine.Engine, baseURL, apiKey, model string, timeout time.Duration,
) warmState {
	p.mu.Lock()
	if st, ok := p.warm[uid]; ok {
		snapshot := *st
		p.mu.Unlock()
		return snapshot
	}
	st := &warmState{owner: owner}
	p.warm[uid] = st
	p.mu.Unlock()

	go func() {
		wctx, cancel := context.WithTimeout(p.baseCtx(), timeout)
		defer cancel()
		err := eng.Warmup(wctx, baseURL, apiKey, model)
		p.mu.Lock()
		st.done = true
		st.err = err
		p.mu.Unlock()
	}()
	return warmState{}
}

// forget drops a Pod's warm state so a subsequent probe re-attempts warmup.
func (p *engineProber) forget(uid types.UID) {
	p.mu.Lock()
	delete(p.warm, uid)
	p.mu.Unlock()
}

// PruneWarm drops warm state for Pods of the given DecisionModel (owner =
// "<namespace>/<name>") that are not in live, so the map does not grow without
// bound as that DM's Pods are replaced across rollouts. It only ever touches
// entries belonging to that DM, so reconciling one DecisionModel never resets
// another DM's warmups. It is an optional Prober capability detected
// by type assertion.
func (p *engineProber) PruneWarm(owner string, live map[types.UID]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for uid, st := range p.warm {
		if st.owner == owner && !live[uid] {
			delete(p.warm, uid)
		}
	}
}
