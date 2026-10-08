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
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// manifestKey is the binaryData key under which a revision's raw model manifest
// bytes are stored in its manifest ConfigMap.
const manifestKey = "manifest"

// manifestConfigMapName is the per-revision manifest ConfigMap name. It is not a
// Pod-label value, so the 63-char limit does not apply (ConfigMap names allow 253):
// <dm (<=43)>-manifest-<rev (<=16)> is well within that.
func manifestConfigMapName(dm *decisionmodelv1alpha1.DecisionModel, rev string) string {
	return dm.Name + "-manifest-" + rev
}

// digestOf returns the bare lowercase hex sha256 of b (the Ollaya manifest
// digest form), matching engine.ModelRef.Digest.
func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ensureManifestConfigMap persists the raw model manifest for a revision as an
// owned, revision-labelled ConfigMap (binaryData), so a lost store can later be
// rebuilt to EXACTLY the recorded digest even if the upstream tag moved. It is
// idempotent and runs before the prefetch Job (persist-then-act): the durable
// bytes exist before the Job that seeds them.
//
//   - If an owned ConfigMap already holds bytes whose sha256 == digest, it is a
//     no-op (recovery / a resumed candidate reuses it).
//   - Otherwise it persists THIS reconcile's resolved manifest (stashed by
//     resolveDigest), and only when sha256(manifest) == digest. When Resolve was
//     short-circuited (hard-pinned spec.digest, or a digest reused from status) or
//     returned no manifest, or the bytes do not match the recorded digest (the tag
//     moved), NOTHING is written and the prefetch falls back to pull-by-tag +
//     verify (today's behaviour; a moved tag then surfaces as UpstreamTagMoved).
//
// It never calls Resolve itself (so it adds no registry round-trips and does not
// resolve a hard-pinned digest), and never fails the reconcile beyond a genuine
// ConfigMap API error worth a requeue.
func (r *DecisionModelReconciler) ensureManifestConfigMap(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev, digest string,
) error {
	if digest == "" {
		return nil
	}
	// Already persisted and valid?
	if b := r.manifestBytes(ctx, dm, rev, digest); b != nil {
		return nil
	}
	// Use this reconcile's resolved manifest; persist only when it hashes to the
	// recorded digest. A moved tag / hard pin / no-manifest Resolve means we cannot
	// persist and the prefetch falls back to pull-by-tag.
	st := reconcileStateFrom(ctx)
	if st == nil || len(st.resolvedManifest) == 0 || digestOf(st.resolvedManifest) != digest {
		return nil
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      manifestConfigMapName(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		BinaryData: map[string][]byte{manifestKey: st.resolvedManifest},
	}
	if err := controllerutil.SetControllerReference(dm, cm, r.Scheme); err != nil {
		return err
	}
	// Create directly (no createOrAdopt): configmaps have get/create/delete RBAC
	// only — no patch — so we never relabel/adopt. On AlreadyExists, verify through
	// the uncached APIReader that the existing object is ours (owner UID); a
	// foreign ConfigMap with our name is a conflict (never overwritten). An object
	// that is ours is a benign re-create (idempotent).
	if err := r.Create(ctx, cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		rdr, rerr := r.reader()
		if rerr != nil {
			return rerr
		}
		live := &corev1.ConfigMap{}
		if gerr := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: cm.Name}, live); gerr != nil {
			return gerr
		}
		if !ownedBy(live, dm) {
			return r.conflictIfNotOwned(ctx, dm, live, "ConfigMap")
		}
	}
	return nil
}

// manifestBytes returns the persisted manifest for a revision, or nil when there
// is none we can trust. It reads the ConfigMap through the UNCACHED APIReader
// (configmaps carry get-only RBAC and are not watched/cached — the same rule as
// Secrets and dataset ConfigMaps), then requires BOTH: our controller
// OwnerReference UID (a foreign ConfigMap with the same name is not trusted —
// labels are user-settable) AND sha256(bytes) == digest (a tampered or stale body
// is treated as missing). nil callers fall back to pull-by-tag.
func (r *DecisionModelReconciler) manifestBytes(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev, digest string,
) []byte {
	if digest == "" {
		return nil
	}
	rdr, err := r.reader()
	if err != nil {
		return nil
	}
	cm := &corev1.ConfigMap{}
	if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: manifestConfigMapName(dm, rev)}, cm); err != nil {
		return nil
	}
	if !ownedBy(cm, dm) {
		return nil // foreign object with our name: not trusted
	}
	b := cm.BinaryData[manifestKey]
	if len(b) == 0 || digestOf(b) != digest {
		return nil // absent / tampered / stale -> treat as missing
	}
	return b
}

// seedManifest sets params.Model.Manifest from the persisted manifest for rev,
// so the prefetch Job can rebuild exactly the recorded digest. A
// missing/foreign/stale ConfigMap leaves Manifest empty (pull-by-tag fallback).
// It only READS: the ConfigMap is written earlier (persistCandidateManifest, in
// the same reconcile as Resolve, before the digest is recorded in status) so the
// bytes can never be lost. Called on the prefetch paths only — serving Pods do
// not need the manifest.
func (r *DecisionModelReconciler) seedManifest(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	params engine.Params,
	rev, digest string,
) engine.Params {
	params.Model.Manifest = r.manifestBytes(ctx, dm, rev, digest)
	return params
}

// persistCandidateManifest writes the candidate revision's manifest as an owned
// ConfigMap in the SAME reconcile that resolved it, BEFORE the digest is recorded
// in status. Returning an error here aborts the reconcile without a status write,
// so the next reconcile resolves again and retries — the bytes can never be lost
// (unlike persisting only when the prefetch Job runs, which can be a later
// reconcile that no longer has the resolved manifest). It is a no-op when the
// manifest cannot be persisted (Resolve short-circuited / no manifest / moved tag
// / hard pin): then the prefetch falls back to pull-by-tag.
func (r *DecisionModelReconciler) persistCandidateManifest(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev, digest string,
) error {
	return r.ensureManifestConfigMap(ctx, dm, rev, digest)
}

// deleteManifestConfigMapIfOwned deletes a revision's manifest ConfigMap by name
// when it exists and is owned by this DM. It reads through the uncached APIReader
// (configmaps are get-only, not cached), so GC never needs a ConfigMap informer.
// A NotFound is ignored (already gone); a foreign ConfigMap with the same name is
// left untouched.
func (r *DecisionModelReconciler) deleteManifestConfigMapIfOwned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) error {
	rdr, err := r.reader()
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: manifestConfigMapName(dm, rev)}
	if gerr := rdr.Get(ctx, key, cm); gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return nil
		}
		return gerr
	}
	if !ownedBy(cm, dm) {
		return nil
	}
	if derr := r.Delete(ctx, cm); derr != nil && !apierrors.IsNotFound(derr) {
		return derr
	}
	return nil
}
