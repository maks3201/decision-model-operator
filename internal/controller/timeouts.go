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
	"time"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// Effective rollout timeouts. The package constants cacheTimeout,
// startTimeout and evalDeadline are the defaults; spec.rollout.timeouts
// overrides them per DecisionModel. None of them is part of the revision hash.

func rolloutTimeouts(dm *decisionmodelv1alpha1.DecisionModel) *decisionmodelv1alpha1.RolloutTimeouts {
	if dm.Spec.Rollout == nil {
		return nil
	}
	return dm.Spec.Rollout.Timeouts
}

// cachingTimeout bounds the Caching phase.
func cachingTimeout(dm *decisionmodelv1alpha1.DecisionModel) time.Duration {
	if t := rolloutTimeouts(dm); t != nil && t.Caching != nil {
		return t.Caching.Duration
	}
	return cacheTimeout
}

// startingTimeout bounds the Starting phase.
func startingTimeout(dm *decisionmodelv1alpha1.DecisionModel) time.Duration {
	if t := rolloutTimeouts(dm); t != nil && t.Starting != nil {
		return t.Starting.Duration
	}
	return startTimeout
}

// evaluatingTimeout bounds the Evaluating phase and each evaluation run.
func evaluatingTimeout(dm *decisionmodelv1alpha1.DecisionModel) time.Duration {
	if t := rolloutTimeouts(dm); t != nil && t.Evaluating != nil {
		return t.Evaluating.Duration
	}
	return evalDeadline
}

// warmupTimeoutKey carries the background-warmup bound from the controller to
// the prober through the context, so the Prober interface stays unchanged.
type warmupTimeoutKey struct{}

// withWarmupTimeout returns ctx carrying the warmup bound for dm: the explicit
// spec.rollout.timeouts.starting when set (a model that needs 20 minutes to load
// from a cold node cannot be warmed inside the 2-minute default), otherwise
// nothing, so the prober keeps its own default and existing behaviour is
// unchanged. Bounding the warmup by the Starting timeout is deliberate: a warmup
// that outlives the phase it belongs to could only finish after the candidate was
// already rolled back.
func withWarmupTimeout(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) context.Context {
	if t := rolloutTimeouts(dm); t != nil && t.Starting != nil {
		return context.WithValue(ctx, warmupTimeoutKey{}, t.Starting.Duration)
	}
	return ctx
}

// warmupTimeoutFrom returns the warmup bound from ctx, or the default.
func warmupTimeoutFrom(ctx context.Context) time.Duration {
	if d, ok := ctx.Value(warmupTimeoutKey{}).(time.Duration); ok && d > 0 {
		return d
	}
	return warmupBaseTimeout
}
