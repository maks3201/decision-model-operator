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
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// registryConfigurer is an optional engine capability: it reports the engine's
// configured registry base URL and its built-in default. It is a structural
// interface the controller type-asserts, NOT part of the engine contract, so a
// new engine need not implement it (then no registry is recorded and rendering
// falls back to the engine's own default, today's behaviour).
type registryConfigurer interface {
	// ConfiguredRegistry is the registry --ollaya-registry set, or the engine
	// default when unset. Always a concrete URL (never "").
	ConfiguredRegistry() string
	// RecordedRegistry is the registry base URL a NEW candidate for model is
	// actually served/prefetched from: the name's own host for an explicit-host
	// name (host/[ns/]model:tag), else ConfiguredRegistry for a host-less name.
	// Always a concrete URL (never ""). Recording the true host (not the flag) for
	// an explicit-host name makes status.<revision>.registry truthful and lets the
	// store-recovery allow-list guard check the real source.
	RecordedRegistry(model string) string
	// DefaultRegistry is the engine's built-in default registry, independent of
	// the flag — the registry a stable with no recorded/live registry runs on.
	DefaultRegistry() string
}

// recordedCandidateRegistry is the registry base URL to record on a new candidate
// revision (RevisionStatus.Registry) for model: the registry the candidate is
// actually served/prefetched from (the name's own host for an explicit-host name,
// else the engine's configured registry) when the engine reports one, else "".
// Recording it (the explicit default included) lets the stable revision render its
// serving Pods / prefetch Job from the registry it was prefetched against, so a
// later change of the operator's default registry never re-renders a running
// stable. Rendering a default-registry revision stays byte-identical because the
// engine suppresses the env for its default.
func recordedCandidateRegistry(eng engine.Engine, model string) string {
	if rc, ok := eng.(registryConfigurer); ok {
		return rc.RecordedRegistry(model)
	}
	return ""
}

// engineDefaultRegistry is the engine's built-in default registry URL, or "" when
// the engine does not report one. Used to adopt the default onto a legacy stable
// whose live serving Pod carries no registry env.
func engineDefaultRegistry(eng engine.Engine) string {
	if rc, ok := eng.(registryConfigurer); ok {
		return rc.DefaultRegistry()
	}
	return ""
}
