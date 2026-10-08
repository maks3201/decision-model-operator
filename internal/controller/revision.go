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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
)

// revisionInputs is the canonical subset of a DecisionModelSpec (plus the
// resolved digest and engine image) that identifies a revision. Fields are
// ordered and JSON tags are fixed so the marshaled form is stable across runs.
// Anything not present here (replicas, cache, auth, rollout) must not affect the
// revision hash.
//
// Placement is the hash of spec.scheduling. It is omitted when empty, so the
// hash of a DecisionModel without scheduling is byte-identical to what earlier
// operator versions computed.
type revisionInputs struct {
	Engine    string                      `json:"engine"`
	Model     string                      `json:"model"`
	Digest    string                      `json:"digest"`
	Device    string                      `json:"device"`
	Image     string                      `json:"image"`
	Resources corev1.ResourceRequirements `json:"resources"`
	Placement string                      `json:"placement,omitempty"`
}

// revisionHashWidth is the number of hex chars in a new revision hash (64 bits).
// Earlier operator versions used legacyRevisionHashWidth (40 bits); a revision
// recorded with the old width keeps its name — the two share a prefix because
// the wider hash is a longer prefix of the same sha256, so recognising a legacy
// revision is just a width-10 comparison (legacyRevisionHash / recomputeHashWidth).
const (
	revisionHashWidth       = 16
	legacyRevisionHashWidth = 10
)

// hashInputs returns the first revisionHashWidth hex chars of the sha256 over the
// canonical JSON (new revisions). A revision recorded at legacyRevisionHashWidth
// is still recognised by legacyRevisionHash, which hashes the same bytes to the
// old width (the two share a prefix).
func hashInputs(in revisionInputs) string {
	return hashInputsWidth(in, revisionHashWidth)
}

// hashInputsWidth hashes the canonical JSON of in and returns the first width hex
// chars. The sha256 is independent of width, so a wider hash is a longer prefix
// of a narrower one for the same inputs.
func hashInputsWidth(in revisionInputs, width int) string {
	// json.Marshal of a struct is deterministic: fields are emitted in declaration
	// order and map keys (inside ResourceRequirements) are sorted. The inputs are
	// plain data types that always marshal; an error is a programming error.
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:width]
}

// placementHash is the short hash of a SchedulingSpec, or "" when it carries no
// scheduling at all (nil, or every field empty), so that "no scheduling" and
// "empty scheduling" are the same placement.
func placementHash(s *decisionmodelv1alpha1.SchedulingSpec) string {
	if s == nil ||
		(len(s.NodeSelector) == 0 && len(s.Tolerations) == 0 && s.Affinity == nil && s.RuntimeClassName == nil) {
		return ""
	}
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:10]
}

// placementNone is the recorded placement of a revision created without any
// scheduling. It is distinct from the empty string, which in a recorded revision
// means "created before placement was recorded" (a legacy stable, see
// adoptLegacyStable). Without this, adding scheduling to a modern stable would be
// indistinguishable from a legacy one and would be adopted in place.
const placementNone = "none"

// placementRecord is the value stored in RevisionStatus.Placement for a spec.
func placementRecord(s *decisionmodelv1alpha1.SchedulingSpec) string {
	if h := placementHash(s); h != "" {
		return h
	}
	return placementNone
}

// placementInput converts a recorded placement back to the value that enters the
// revision hash ("" for no scheduling, so the hash stays what it always was).
func placementInput(recorded string) string {
	if recorded == placementNone {
		return ""
	}
	return recorded
}

// RevisionHash returns a stable short hash identifying a revision.
//
// It depends on engine, model, digest, device, image, resources and the
// placement (spec.scheduling). It is invariant under changes to replicas, cache,
// auth and rollout settings, which are not part of a revision.
//
// resolvedDigest is the digest the operator resolved for the model; when the
// spec pins a digest it should equal spec.Digest. image is the fully resolved
// engine image used for serving.
func RevisionHash(spec decisionmodelv1alpha1.DecisionModelSpec, resolvedDigest string, image string) string {
	return hashInputs(revisionInputs{
		Engine:    spec.Engine,
		Model:     spec.Model,
		Digest:    resolvedDigest,
		Device:    spec.Device,
		Image:     image,
		Resources: spec.Resources,
		Placement: placementHash(spec.Scheduling),
	})
}

// legacyRevisionHash is RevisionHash as computed before placement became a
// revision input AND at the earlier 40-bit width. It is only used to recognise a
// stable revision that was created by an earlier operator version (see
// adoptLegacyStable). The width is legacyRevisionHashWidth so it matches the
// 10-hex name such a stable was recorded under.
func legacyRevisionHash(spec decisionmodelv1alpha1.DecisionModelSpec, resolvedDigest string, image string) string {
	return hashInputsWidth(revisionInputs{
		Engine:    spec.Engine,
		Model:     spec.Model,
		Digest:    resolvedDigest,
		Device:    spec.Device,
		Image:     image,
		Resources: spec.Resources,
	}, legacyRevisionHashWidth)
}

// revisionHashFromStatus recomputes the revision hash from a recorded
// RevisionStatus, using the same inputs as RevisionHash. It lets the controller
// verify a rendered Pod still hashes to that revision. The width matches the
// recorded hash (legacyRevisionHashWidth for a revision recorded by an older
// operator, revisionHashWidth otherwise), so verification works for both widths.
// Returns "" for a nil revision.
func revisionHashFromStatus(rev *decisionmodelv1alpha1.RevisionStatus) string {
	if rev == nil {
		return ""
	}
	width := revisionHashWidth
	if len(rev.Hash) == legacyRevisionHashWidth {
		width = legacyRevisionHashWidth
	}
	return hashInputsWidth(revisionInputs{
		Engine:    rev.Engine,
		Model:     rev.Model,
		Digest:    rev.Digest,
		Device:    rev.Device,
		Image:     rev.Image,
		Resources: rev.Resources,
		Placement: placementInput(rev.Placement),
	}, width)
}

// sameIdentity reports whether a recorded revision is the revision that the
// current spec would create: equal engine, model, digest, device, image,
// resources and placement. It compares the recorded fields rather than hash
// names, so a legacy stable that keeps its old hash name (adoptLegacyStable) is
// still recognised as the current revision.
//
// engineRendered says whether the serving image is the engine's own default
// (spec.image unset on the DM). Only then is the serving image compared with
// sameServingImage, so a stable an older operator recorded as the tag-only form
// (repo:tag) is recognised as the same revision this build computes as
// repo:tag@sha256:<index> for the SAME tag (identity recognition, not admission:
// without it an upgrade would roll every quiet DecisionModel). When the user set
// spec.image the image is compared EXACTLY: a user who edits spec.image from
// repo:tag to repo:tag@sha256:X (or removes the pin) is changing the revision and
// a new one must start so the pin takes effect.
func sameIdentity(rev, want *decisionmodelv1alpha1.RevisionStatus, engineRendered bool) bool {
	imageMatches := rev != nil && want != nil && rev.Image == want.Image
	if engineRendered && rev != nil && want != nil {
		imageMatches = sameServingImage(rev.Image, want.Image)
	}
	return rev != nil && want != nil &&
		rev.Engine == want.Engine && rev.Model == want.Model && rev.Digest == want.Digest &&
		rev.Device == want.Device && imageMatches && rev.Placement == want.Placement &&
		equality.Semantic.DeepEqual(rev.Resources, want.Resources)
}

// splitImagePin splits a serving image reference into its repo:tag part and its
// @sha256:<hex> pin (if any). "repo:tag@sha256:abc" -> ("repo:tag", "sha256:abc");
// "repo:tag" -> ("repo:tag", ""). It splits on the first "@" so a tag can never
// contain one.
func splitImagePin(image string) (tag, pin string) {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		return image[:i], image[i+1:]
	}
	return image, ""
}

// sameServingImage reports whether a recorded revision image and a freshly
// computed image denote the same revision. They match when equal, or when they
// have the same repo:tag and AT MOST ONE side carries an @sha256 pin — i.e. an
// older release recorded the tag-only form and this build adds the pinned digest
// of the SAME tag (or vice versa). Two DIFFERENT explicit digests of the same tag
// are not collapsed (a deliberate re-pin is a real revision change); a different
// tag is never collapsed (a version bump rolls, as it should).
func sameServingImage(recorded, computed string) bool {
	if recorded == computed {
		return true
	}
	rTag, rPin := splitImagePin(recorded)
	cTag, cPin := splitImagePin(computed)
	if rTag != cTag || rTag == "" {
		return false
	}
	// Same repo:tag: collapse only when at least one side is unpinned (the
	// tag-only form an older release recorded, now rendered with its digest).
	return rPin == "" || cPin == ""
}

// adoptLegacyStable records the current placement on a stable revision that was
// created before placement was recorded (RevisionStatus.Placement == ""), provided
// it is the revision the current spec describes under the previous hash formula.
// That hash equality already proves engine, model, digest, device, image and
// resources are the current ones, so placement and resources are backfilled.
// The image is backfilled ONLY when it was never recorded (empty): a stable that
// already recorded a tag-only image keeps it verbatim, so adoption never swaps a
// tag-form image for this build's digest-pinned form and never rolls the running
// stable. Nothing else changes: the stable keeps its hash name, Deployment and
// store. No-op for a revision that already has a recorded placement, and for a
// stable the current spec no longer matches (that is an ordinary new candidate).
func adoptLegacyStable(stable, current *decisionmodelv1alpha1.RevisionStatus, legacyHash string) {
	if stable == nil || stable.Placement != "" || stable.Hash != legacyHash {
		return
	}
	stable.Placement = current.Placement
	if stable.Image == "" {
		stable.Image = current.Image
	}
	stable.Resources = *current.Resources.DeepCopy()
}
