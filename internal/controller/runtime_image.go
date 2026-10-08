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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// runtimeImagePinner returns the engine's RuntimeImagePinner capability, or nil
// when the engine cannot report whether it pins the runtime image to a digest.
// An engine without the capability is treated as unable to pin (fail closed).
func runtimeImagePinner(eng engine.Engine) engine.RuntimeImagePinner {
	if p, ok := eng.(engine.RuntimeImagePinner); ok {
		return p
	}
	return nil
}

// candidateImageMessage names the runtime image the candidate would run, for a
// condition/Event message. A user spec.image override is named verbatim; an
// engine-rendered image is described by its (effective) runtime version and
// device so the message is useful without leaking anything.
func candidateImageMessage(dm *decisionmodelv1alpha1.DecisionModel, effVer, device string) string {
	if dm.Spec.Image != "" {
		return fmt.Sprintf("spec.image %q", dm.Spec.Image)
	}
	v := effVer
	if v == "" {
		v = "the engine default version"
	} else {
		v = "runtime version " + v
	}
	return fmt.Sprintf("the runtime image for %s on %s", v, device)
}

// candidateRuntimeImagePinned reports whether the runtime image a candidate
// would run is pinned to an immutable digest.
//
//   - spec.image set (allowed via --allow-image-override): pinned iff the
//     reference carries an @sha256: digest. The operator does not know the engine
//     renders it, so the digest in the string is the only signal.
//   - otherwise: the engine renders the image; ask RuntimeImagePinner for the
//     effective version and device. An engine without the capability is unpinned.
func candidateRuntimeImagePinned(
	eng engine.Engine,
	dm *decisionmodelv1alpha1.DecisionModel,
	effVer, device string,
) bool {
	if dm.Spec.Image != "" {
		return strings.Contains(dm.Spec.Image, "@sha256:")
	}
	pinner := runtimeImagePinner(eng)
	if pinner == nil {
		return false
	}
	return pinner.RuntimeImagePinned(effVer, device)
}

// recordRuntimeImagePinned stamps ImagePinned=false on a candidate whose runtime
// image is an unpinned mutable tag (only reachable with
// --allow-unpinned-runtime-images, since preflightRuntimeImage refuses it
// otherwise) and emits one Warning Event per revision. A pinned candidate leaves
// ImagePinned nil (the default-policy invariant: an admitted revision is pinned).
//
// The Event dedupes on the already-recorded revision: it fires only when neither
// the current stable nor the previously recorded candidate for this hash already
// carries ImagePinned=false, so a steady unpinned revision warns once, not on
// every reconcile, and the warning survives a restart (it reads status, not RAM).
func (r *DecisionModelReconciler) recordRuntimeImagePinned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	effVer string,
) {
	device := deviceOrDefault(dm.Spec.Device)
	if candidateRuntimeImagePinned(eng, dm, effVer, device) {
		return
	}
	alreadyRecorded := revisionUnpinned(dm.Status.StableRevision, candidate.Hash) ||
		revisionUnpinned(dm.Status.CandidateRevision, candidate.Hash)
	pinned := false
	candidate.ImagePinned = &pinned
	if !alreadyRecorded {
		r.event(ctx, dm, corev1.EventTypeWarning, eventUnpinnedRuntimeImage,
			"admitting %s with a mutable (unpinned) runtime image because "+
				"--allow-unpinned-runtime-images is set; this revision is not "+
				"guaranteed to run one fixed set of bytes",
			candidateImageMessage(dm, effVer, device))
	}
}

// revisionUnpinned reports whether a recorded revision is the one identified by
// hash and already carries ImagePinned=false.
func revisionUnpinned(rev *decisionmodelv1alpha1.RevisionStatus, hash string) bool {
	return rev != nil && rev.Hash == hash && rev.ImagePinned != nil && !*rev.ImagePinned
}
