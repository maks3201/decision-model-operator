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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Runtime-version policy values for --runtime-version-policy.
const (
	// RuntimeVersionPinned reuses the stable revision's recorded runtime version
	// when spec.runtimeVersion is unset, so an operator upgrade that changes the
	// default runtime image does not start a rollout. This is the default.
	RuntimeVersionPinned = "Pinned"
	// RuntimeVersionFollowOperator uses the engine default runtime version when
	// spec.runtimeVersion is unset, so an operator upgrade rolls every
	// DecisionModel onto the new default (bounded by --max-concurrent-rollouts).
	RuntimeVersionFollowOperator = "FollowOperator"
)

// errInvalidRuntimeVersion is returned when spec.runtimeVersion fails the
// engine's validation (below the minimum supported version). The CEL pattern
// rejects malformed strings; this catches the version floor.
var errInvalidRuntimeVersion = errors.New("invalid runtime version")

// errRuntimeVersionUnsupported is returned when spec.runtimeVersion is set but
// the engine does not implement the RuntimeVersioner capability (it cannot pin a
// runtime version).
var errRuntimeVersionUnsupported = errors.New("engine does not support runtimeVersion")

// ValidRuntimeVersionPolicy reports whether p is an accepted policy value.
func ValidRuntimeVersionPolicy(p string) bool {
	return p == RuntimeVersionPinned || p == RuntimeVersionFollowOperator
}

// runtimeVersionPolicyOrDefault returns the configured policy or the default
// (Pinned) when unset.
func (r *DecisionModelReconciler) runtimeVersionPolicyOrDefault() string {
	if r.RuntimeVersionPolicy == "" {
		return RuntimeVersionPinned
	}
	return r.RuntimeVersionPolicy
}

// runtimeVersioner returns the engine's RuntimeVersioner capability, or nil when
// the engine cannot pin a runtime version.
func runtimeVersioner(eng engine.Engine) engine.RuntimeVersioner {
	if rv, ok := eng.(engine.RuntimeVersioner); ok {
		return rv
	}
	return nil
}

// validateRuntimeVersion checks spec.runtimeVersion against the engine's rules.
// Empty is valid (follows the policy). A non-empty version with an engine that
// cannot pin versions is errRuntimeVersionUnsupported. Otherwise the engine's
// own validation applies (wrapped in errInvalidRuntimeVersion).
func validateRuntimeVersion(eng engine.Engine, version string) error {
	if version == "" {
		return nil
	}
	rv := runtimeVersioner(eng)
	if rv == nil {
		return fmt.Errorf("%w: engine %q cannot pin spec.runtimeVersion", errRuntimeVersionUnsupported, eng.Name())
	}
	if err := rv.ValidateRuntimeVersion(version); err != nil {
		return errors.Join(errInvalidRuntimeVersion, err)
	}
	return nil
}

// defaultRuntimeVersion is the engine's default runtime version, or "" when the
// engine cannot pin versions.
func defaultRuntimeVersion(eng engine.Engine) string {
	if rv := runtimeVersioner(eng); rv != nil {
		return rv.DefaultRuntimeVersion()
	}
	return ""
}

// recordedCandidateRuntimeVersion is the runtime version to record on a new
// candidate revision. It is "" when the image is user-set (spec.image; version
// unknown) or the engine cannot pin versions. Otherwise it is the effective
// version, or the engine default when the effective version is empty (so the
// recorded value is the concrete version actually served — this is what Pinned
// reuse and runtimeUpdateAvailable read).
func recordedCandidateRuntimeVersion(eng engine.Engine, dm *decisionmodelv1alpha1.DecisionModel, effVer string) string {
	if dm.Spec.Image != "" || runtimeVersioner(eng) == nil {
		return ""
	}
	if effVer != "" {
		return effVer
	}
	return defaultRuntimeVersion(eng)
}

// effectiveRuntimeVersion resolves the runtime version a candidate should be
// built with, honouring the policy:
//   - engine without the capability      -> "" (no pinning; validateRuntimeVersion
//     has already rejected a set spec.runtimeVersion)
//   - spec.runtimeVersion set            -> that version
//   - spec.image set                     -> "" (image override; version unknown)
//   - Pinned and a stable exists         -> the stable's recorded version
//     (derived from its recorded image when the version field is empty, e.g. a
//     stable promoted before this field existed; "" when it cannot be derived)
//   - no stable, or FollowOperator       -> "" (engine default)
//
// Returning "" means "the engine default", resolved by the engine when it builds
// the image.
func (r *DecisionModelReconciler) effectiveRuntimeVersion(
	eng engine.Engine,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
) string {
	if runtimeVersioner(eng) == nil {
		return ""
	}
	if dm.Spec.RuntimeVersion != "" {
		return dm.Spec.RuntimeVersion
	}
	if dm.Spec.Image != "" {
		return ""
	}
	if r.runtimeVersionPolicyOrDefault() == RuntimeVersionPinned && stable != nil {
		return recordedRuntimeVersion(eng, stable)
	}
	return ""
}

// recordedRuntimeVersion returns the runtime version a recorded revision runs,
// from its RuntimeVersion field or, for a revision recorded before that field
// existed, derived from its image via the engine. Empty when it cannot be
// determined (e.g. a user-set spec.image, or no capability).
func recordedRuntimeVersion(eng engine.Engine, rev *decisionmodelv1alpha1.RevisionStatus) string {
	if rev == nil {
		return ""
	}
	if rev.RuntimeVersion != "" {
		return rev.RuntimeVersion
	}
	if rv := runtimeVersioner(eng); rv != nil {
		return rv.RuntimeVersionFromImage(rev.Image)
	}
	return ""
}

// runtimeUpdateAvailable reports whether a newer engine default runtime version
// exists than the one a revision is pinned to. It is false when the pinned
// version is unknown (user image / no capability) or already at/above the default.
func runtimeUpdateAvailable(eng engine.Engine, pinned string) bool {
	rv := runtimeVersioner(eng)
	if rv == nil || pinned == "" {
		return false
	}
	return rv.CompareRuntimeVersions(pinned, rv.DefaultRuntimeVersion()) < 0
}

// reconcileRuntimeUpdate maintains the RuntimeUpdateAvailable condition and emits
// a Normal Event once per newly observed default. The version a DecisionModel
// "runs" is its stable's recorded version (or the candidate's for a first
// rollout). A DecisionModel that pins spec.runtimeVersion or spec.image opts out
// (no nudge); so does one already at or above the default, or an engine without
// the capability.
func (r *DecisionModelReconciler) reconcileRuntimeUpdate(
	ctx context.Context,
	eng engine.Engine,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable, candidate *decisionmodelv1alpha1.RevisionStatus,
) {
	running := recordedRuntimeVersion(eng, stable)
	if running == "" {
		running = recordedRuntimeVersion(eng, candidate)
	}
	if !runtimeUpdateAvailable(eng, running) {
		// At/above the default or version unknown: ensure the condition is not
		// left stale True from an earlier, older version.
		if meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable) != nil {
			meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable)
		}
		return
	}
	def := defaultRuntimeVersion(eng)
	msg := fmt.Sprintf("runtime %s available (running %s); set spec.runtimeVersion: %s to adopt it", def, running, def)
	// Announce once per new default: only when the condition is absent or its
	// message names a different default version.
	cur := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable)
	if cur == nil || cur.Message != msg {
		r.event(ctx, dm, corev1.EventTypeNormal, eventRuntimeUpdate, "%s", msg)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable,
		Status:  metav1.ConditionTrue,
		Reason:  reasonRuntimeUpdateAvailable,
		Message: msg,
	})
}
