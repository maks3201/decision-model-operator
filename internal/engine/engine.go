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

// Package engine is the contract between the DecisionModel controller and a
// model runtime (Ollaya first). It deliberately does not import the CRD types:
// the controller maps DecisionModel spec -> Params, the engine maps Params ->
// Kubernetes objects and talks to running Pods over HTTP.
//
// This file is the engine contract: every engine implements it, so signature
// changes need a dedicated, reviewed pull request.
package engine

import (
	"context"
	"encoding/json"
	"errors"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// Device values accepted in Params.Device.
const (
	DeviceCPU  = "cpu"
	DeviceCUDA = "cuda"
)

// ModelRef is a model name pinned to an immutable digest.
type ModelRef struct {
	// Name in engine terms, e.g. "laya:en".
	Name string
	// Digest is bare lowercase hex sha256 of the model manifest (Ollaya format,
	// the same value /api/ps reports). Empty means "not resolved yet".
	Digest string
}

// Params is everything an engine needs to build workloads for one revision.
type Params struct {
	Model  ModelRef
	Device string // DeviceCPU | DeviceCUDA
	// Image overrides the engine default image (optional).
	Image string
	// CacheClaimName is the PVC holding the model store.
	CacheClaimName string
	// StoreSubPath isolates this revision's model store inside the PVC
	// (mounted via volumeMount.subPath). The controller sets it to the revision
	// hash, so a tag that moved upstream cannot overwrite the manifest a stable
	// revision depends on. Empty means the PVC root (legacy).
	StoreSubPath string
	// KeepStoreSubPaths lists sub-paths that must survive store cleanup (stable
	// and candidate revisions). The prefetch Job may delete any other top-level
	// sub-path of the store after a successful pull.
	KeepStoreSubPaths []string
	// APIKey, if set, is injected into the runtime and must be used by clients.
	APIKey *corev1.SecretKeySelector
	// DownloadToken, if set, is a credential for weight downloads (e.g. a Hugging
	// Face token for private or gated repositories, or a mirror). It is injected
	// into the prefetch Job only, never into serving Pods.
	DownloadToken *corev1.SecretKeySelector
	// Resources for the serving container. The engine adds nvidia.com/gpu for CUDA
	// if absent.
	Resources corev1.ResourceRequirements
}

// Loaded describes one model resident in a running runtime (from /api/ps).
type Loaded struct {
	Name      string
	Digest    string
	Device    string // actual device, e.g. "cpu", "cuda"
	Precision string // e.g. "F32", "F16"
	Pinned    bool   // true if it will not be evicted by keep_alive (expires_at == null)
}

// ErrNotFound is returned by Resolve when the model/tag does not exist.
var ErrNotFound = errors.New("model not found")

// Engine builds Kubernetes objects for a runtime and inspects running Pods.
type Engine interface {
	// Name of the engine, matches DecisionModel spec.engine (e.g. "ollaya").
	Name() string

	// Resolve turns a model name ("laya:en") into a pinned ModelRef without a
	// running runtime (e.g. via registry HTTP). Returns ErrNotFound if absent.
	Resolve(ctx context.Context, name string) (ModelRef, error)

	// ServingPodSpec returns the PodSpec for serving Pods. The model store is
	// mounted read-only. It must include a container port named "http" and a
	// liveness probe. Model readiness is NOT expressed via probes: the
	// controller uses a Pod readiness gate driven by Inspect/Warmup.
	ServingPodSpec(p Params) corev1.PodSpec

	// PrefetchJobSpec returns a Job that downloads p.Model into the store
	// (read-write mount). Must be idempotent: succeed fast if already present,
	// and fail if the downloaded digest != p.Model.Digest.
	PrefetchJobSpec(p Params) batchv1.JobSpec

	// ServicePort is the port the runtime listens on (the "http" port).
	ServicePort() int32

	// Inspect lists models loaded in the runtime at baseURL (e.g. http://10.0.0.5:11435).
	Inspect(ctx context.Context, baseURL, apiKey string) ([]Loaded, error)

	// Warmup loads model into memory and pins it (Ollaya: keep_alive=-1).
	Warmup(ctx context.Context, baseURL, apiKey, model string) error
}

// Decider is an optional engine capability used by the rollout evaluator.
// The controller detects it with a type assertion; engines without it cannot run
// eval-gated rollouts.
type Decider interface {
	// Decide sends one Jev/System-1 decision request (POST /v1/systemone) to the
	// runtime at baseURL.
	Decide(ctx context.Context, baseURL, apiKey string, req DecideRequest) (DecideResponse, error)
}

// DecideRequest is a TypeSafe /v1/systemone request. State and Questions are passed
// through verbatim (any JSON), so the engine does not need to model the question schema.
type DecideRequest struct {
	Model     string
	State     json.RawMessage
	Questions json.RawMessage
}

// Answer is one typed answer. Exactly one of Choice / Noul / Score is set, matching Type.
type Answer struct {
	Type          string             // "choice" | "noul" | "score"
	Choice        string             // for choice
	Probabilities map[string]float64 // for choice
	Noul          *float64           // probability of "yes" for noul
	Score         *float64           // expected level for score
	Confidence    float64
}

// DecideResponse maps question id -> answer.
type DecideResponse struct {
	Answers map[string]Answer
}

// RegistryHoster is an optional engine capability: it reports which registry host a
// model name targets, using the SAME parser the engine uses for Resolve and prefetch.
// The controller's registry allow-list must rely on it and fail closed: an engine
// without it may only use its default registry.
type RegistryHoster interface {
	// RegistryHost returns the lowercase host[:port] the name resolves against (the
	// engine default when the name has no host) and whether plain http:// is used.
	RegistryHost(name string) (host string, insecure bool, err error)
}
