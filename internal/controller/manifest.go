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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	batchv1 "k8s.io/api/batch/v1"

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
// owned, revision-labelled ConfigMap (binaryData), so a lost store can be rebuilt
// from the recorded bytes rather than by re-resolving the tag. When the upstream
// tag still serves the recorded bytes this reaches EXACTLY the recorded digest;
// when the tag has MOVED, `ollaya pull <tag>` overwrites the seed with the moved
// bytes (ollaya-dev/ollaya#64), so the rebuild surfaces as UpstreamTagMoved rather
// than silently serving the wrong model. It is idempotent and runs before the
// prefetch Job (persist-then-act): the durable bytes exist before the Job that
// seeds them.
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
	// Not valid (missing, tampered, stale, or holding a different digest). Can we
	// (re)write it from THIS reconcile's verified resolved manifest?
	st := reconcileStateFrom(ctx)
	haveVerified := st != nil && len(st.resolvedManifest) > 0 && digestOf(st.resolvedManifest) == digest
	if !haveVerified {
		// No verified bytes this reconcile. If an owned-but-invalid ConfigMap
		// exists, warn ONCE (do not loop deletes) and leave it — the prefetch
		// falls back to pull-by-tag + verify. A moved tag / hard pin / reused
		// digest simply has nothing to persist.
		r.warnInvalidManifestOnce(ctx, dm, rev, digest)
		return nil
	}
	// We have verified bytes. Persist them, repairing an owned-but-invalid existing
	// ConfigMap by delete-then-recreate (ConfigMaps have no patch RBAC).
	return r.writeManifestConfigMap(ctx, dm, rev, st.resolvedManifest)
}

// writeManifestConfigMap creates the per-revision manifest ConfigMap from the
// given (already digest-verified) bytes. On AlreadyExists it checks ownership: a
// foreign ConfigMap with our name is a conflict (never overwritten); an owned one
// is invalid (the caller only reaches here when manifestBytes returned nil), so it
// is repaired by delete-then-recreate so exact-digest recovery is restored rather
// than silently degrading to pull-by-tag. The delete carries a UID+resourceVersion
// precondition so it can only remove the exact object we inspected, never a
// replacement a concurrent writer created in between.
//
// A prefetch Job for the revision may already mount this ConfigMap. Deleting and
// recreating the ConfigMap under the same name keeps a running Job Pod's existing
// mount valid, but a Job Pod created in the delete→recreate gap (a retry/backoff
// restart) could fail to mount. So when a Job exists, after recreating the
// ConfigMap we delete the Job too (option b): it is recreated by the normal
// prefetch path and re-mounts the now-correct ConfigMap, closing the race.
func (r *DecisionModelReconciler) writeManifestConfigMap(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
	manifest []byte,
) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      manifestConfigMapName(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		BinaryData: map[string][]byte{manifestKey: manifest},
	}
	if err := controllerutil.SetControllerReference(dm, cm, r.Scheme); err != nil {
		return err
	}
	err := r.Create(ctx, cm)
	if err == nil || !apierrors.IsAlreadyExists(err) {
		return err
	}
	// AlreadyExists: inspect the live object through the uncached APIReader.
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
	// Delete the exact live object (UID + resourceVersion precondition), then
	// recreate it so the stored bytes match the recorded digest again. The
	// precondition fails the delete if a concurrent writer replaced the object
	// since the Get above, so we never delete someone else's newer ConfigMap.
	precond := client.Preconditions{UID: &live.UID, ResourceVersion: &live.ResourceVersion}
	if derr := r.Delete(ctx, live, precond); derr != nil && !apierrors.IsNotFound(derr) {
		return derr
	}
	r.event(ctx, dm, corev1.EventTypeNormal, eventManifestRepaired,
		"repaired the model manifest for revision %s (recreated from the verified manifest)", rev)
	if cerr := r.Create(ctx, cm); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
		return cerr
	}
	// If a prefetch Job for this revision already exists it may have been created
	// against the now-replaced ConfigMap; delete it so it is recreated and mounts
	// the repaired ConfigMap, rather than a Pod retry racing the delete→recreate
	// gap. Idempotent: a NotFound is fine, and deleting an owned Job is safe (the
	// prefetch is restartable). Only our Job is touched.
	if derr := r.deletePrefetchJobIfOwned(ctx, dm, rev); derr != nil {
		return derr
	}
	return nil
}

// deletePrefetchJobIfOwned deletes the prefetch Job for a revision when it exists
// and is owned by this DM (propagation Background so its Pods go too). Read
// through the uncached APIReader so a just-created Job is seen. A NotFound is
// ignored; a foreign Job with our name is left untouched.
func (r *DecisionModelReconciler) deletePrefetchJobIfOwned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) error {
	rdr, err := r.reader()
	if err != nil {
		return err
	}
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}
	if gerr := rdr.Get(ctx, key, job); gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return nil
		}
		return gerr
	}
	if !ownedBy(job, dm) {
		return nil
	}
	policy := metav1.DeletePropagationBackground
	precond := client.Preconditions{UID: &job.UID, ResourceVersion: &job.ResourceVersion}
	if derr := r.Delete(ctx, job, precond, &client.DeleteOptions{PropagationPolicy: &policy}); derr != nil &&
		!apierrors.IsNotFound(derr) && !apierrors.IsConflict(derr) {
		return derr
	}
	return nil
}

// warnInvalidManifestOnce emits a single Warning when an owned manifest ConfigMap
// exists but fails the digest check and this reconcile has no verified bytes to
// repair it with — the exact-digest recovery is degraded to pull-by-tag. It warns
// only when the ConfigMap exists and is ours and invalid, so a normal
// "no manifest persisted" case (hard pin / reused digest) stays quiet, and it does
// not loop deletes.
func (r *DecisionModelReconciler) warnInvalidManifestOnce(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev, digest string,
) {
	rdr, err := r.reader()
	if err != nil {
		return
	}
	cm := &corev1.ConfigMap{}
	if gerr := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: manifestConfigMapName(dm, rev)}, cm); gerr != nil {
		return // absent or unreadable: nothing to warn about
	}
	if !ownedBy(cm, dm) {
		return
	}
	b := cm.BinaryData[manifestKey]
	if len(b) > 0 && digestOf(b) == digest {
		return // actually valid (raced with a repair) — no warning
	}
	r.event(ctx, dm, corev1.EventTypeWarning, eventManifestInvalid,
		"model manifest for revision %s is corrupt and cannot be repaired this reconcile "+
			"(no verified manifest available); falling back to pull-by-tag", rev)
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

// seedManifest points the prefetch Job at the persisted manifest for rev so it
// can rebuild from the recorded bytes (reaching the recorded digest when the tag
// still serves them; a moved tag overwrites the seed and surfaces as
// UpstreamTagMoved). When an owned, digest-verified
// ConfigMap exists it sets params.ManifestConfigMap (the Job mounts it read-only)
// AND params.Model.Manifest (kept for the env/seed fallback and for engines
// that read the bytes directly). A missing/foreign/stale ConfigMap leaves both
// empty (pull-by-tag fallback). It only READS: the ConfigMap is written on the
// admitted candidate path (persistCandidateManifest) before the prefetch Job, so
// the bytes exist. Called on the prefetch paths only — serving Pods do not need
// the manifest.
func (r *DecisionModelReconciler) seedManifest(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	params engine.Params,
	rev, digest string,
) engine.Params {
	b := r.manifestBytes(ctx, dm, rev, digest)
	if b == nil {
		return params
	}
	params.Model.Manifest = b
	// Mount the ConfigMap instead of carrying the bytes in an env var: the
	// manifest can exceed the 128 KiB argv/env limit. Verified above (manifestBytes
	// checked ownership + digest), so pointing the Job at it is safe.
	params.ManifestConfigMap = &corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: manifestConfigMapName(dm, rev)},
		Key:                  manifestKey,
	}
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
	// Delete the exact object we inspected (UID + resourceVersion precondition),
	// so a concurrent recreate under the same name is not deleted by mistake.
	precond := client.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}
	if derr := r.Delete(ctx, cm, precond); derr != nil && !apierrors.IsNotFound(derr) {
		return derr
	}
	return nil
}
