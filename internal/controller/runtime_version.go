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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
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

// validateRuntimeVersion checks spec.runtimeVersion against the engine's rules.
// Empty is valid (follows the policy). Returns errInvalidRuntimeVersion wrapping
// the engine message so the controller can surface a Degraded condition.
func validateRuntimeVersion(version string) error {
	if err := ollaya.ValidateRuntimeVersion(version); err != nil {
		return errors.Join(errInvalidRuntimeVersion, err)
	}
	return nil
}

// defaultRuntimeVersion is the engine's default runtime version for this
// operator build.
func defaultRuntimeVersion() string { return ollaya.DefaultRuntimeVersion }

// recordedCandidateRuntimeVersion is the runtime version to record on a new
// candidate revision. It is "" when the image is user-set (spec.image; version
// unknown). Otherwise it is the effective version, or the engine default when
// the effective version is empty (so the recorded value is the concrete version
// actually served — this is what Pinned reuse and runtimeUpdateAvailable read).
func recordedCandidateRuntimeVersion(dm *decisionmodelv1alpha1.DecisionModel, effVer string) string {
	if dm.Spec.Image != "" {
		return ""
	}
	if effVer != "" {
		return effVer
	}
	return defaultRuntimeVersion()
}

// effectiveRuntimeVersion resolves the runtime version a candidate should be
// built with, honouring the policy:
//   - spec.runtimeVersion set            -> that version
//   - spec.image set                     -> "" (image override; version unknown)
//   - Pinned and a stable exists         -> the stable's recorded version
//     (derived from its recorded image when the version field is empty, e.g. a
//     stable promoted before this field existed; "" when it cannot be derived,
//     which leaves the engine default)
//   - no stable, or FollowOperator       -> "" (engine default)
//
// Returning "" means "the engine default", resolved by the engine when it builds
// the image.
func (r *DecisionModelReconciler) effectiveRuntimeVersion(
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
) string {
	if dm.Spec.RuntimeVersion != "" {
		return dm.Spec.RuntimeVersion
	}
	if dm.Spec.Image != "" {
		return ""
	}
	if r.runtimeVersionPolicyOrDefault() == RuntimeVersionPinned && stable != nil {
		return recordedRuntimeVersion(stable)
	}
	return ""
}

// recordedRuntimeVersion returns the runtime version a recorded revision runs,
// from its RuntimeVersion field or, for a revision recorded before that field
// existed, derived from its image when the image is a default engine tag. Empty
// when it cannot be determined (e.g. a user-set spec.image).
func recordedRuntimeVersion(rev *decisionmodelv1alpha1.RevisionStatus) string {
	if rev == nil {
		return ""
	}
	if rev.RuntimeVersion != "" {
		return rev.RuntimeVersion
	}
	return runtimeVersionFromImage(rev.Image)
}

// runtimeVersionFromImage extracts a MAJOR.MINOR.PATCH version from a default
// engine image tag (ghcr.io/ollaya-dev/ollaya:<ver>[-cuda]); "" when the image
// is empty or not a recognised default tag (a user image carries no known
// version).
func runtimeVersionFromImage(image string) string {
	const prefix = "ghcr.io/ollaya-dev/ollaya:"
	if !strings.HasPrefix(image, prefix) {
		return ""
	}
	tag := strings.TrimPrefix(image, prefix)
	tag = strings.TrimSuffix(tag, "-cuda")
	if ollaya.ValidateRuntimeVersion(tag) != nil {
		return ""
	}
	return tag
}

// runtimeUpdateAvailable reports whether a newer engine default runtime version
// exists than the one a revision is pinned to. It is false when the pinned
// version is unknown (user image) or already at/above the default.
func runtimeUpdateAvailable(pinned string) bool {
	if pinned == "" {
		return false
	}
	return compareRuntimeVersions(pinned, defaultRuntimeVersion()) < 0
}

// reconcileRuntimeUpdate maintains the RuntimeUpdateAvailable condition and emits
// a Normal Event once per newly observed default. The version a DecisionModel
// "runs" is its stable's recorded version (or the candidate's for a first
// rollout). A DecisionModel that pins spec.runtimeVersion or spec.image opts out
// (no nudge); so does one already at or above the default.
func (r *DecisionModelReconciler) reconcileRuntimeUpdate(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable, candidate *decisionmodelv1alpha1.RevisionStatus,
) {
	running := recordedRuntimeVersion(stable)
	if running == "" {
		running = recordedRuntimeVersion(candidate)
	}
	def := defaultRuntimeVersion()
	if !runtimeUpdateAvailable(running) {
		// At/above the default or version unknown: ensure the condition is not
		// left stale True from an earlier, older version.
		if meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable) != nil {
			meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable)
		}
		return
	}
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

// compareRuntimeVersions compares two MAJOR.MINOR.PATCH strings numerically,
// returning -1, 0 or 1. A string that is not a valid runtime version sorts as
// "unknown" and compares equal (so it never claims an update is available).
func compareRuntimeVersions(a, b string) int {
	ap, aok := parseRuntimeVersion(a)
	bp, bok := parseRuntimeVersion(b)
	if !aok || !bok {
		return 0
	}
	for i := 0; i < 3; i++ {
		if ap[i] != bp[i] {
			if ap[i] < bp[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// parseRuntimeVersion splits a validated MAJOR.MINOR.PATCH string into three
// ints. ok is false when it is not that form.
func parseRuntimeVersion(v string) ([3]int, bool) {
	var out [3]int
	if ollaya.ValidateRuntimeVersion(v) != nil {
		return out, false
	}
	parts := strings.SplitN(v, ".", 3)
	if len(parts) != 3 {
		return out, false
	}
	for i := 0; i < 3; i++ {
		n := 0
		for _, c := range parts[i] {
			if c < '0' || c > '9' {
				return out, false
			}
			n = n*10 + int(c-'0')
		}
		out[i] = n
	}
	return out, true
}
