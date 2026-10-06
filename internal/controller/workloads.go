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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// paramsFor builds engine.Params for the current spec at revision rev. When rev
// is non-empty the model store is this revision's own PVC:
// CacheClaimName = storeNameRev(dm, rev), StoreSubPath = rev, and
// KeepStoreSubPaths = [rev] (nothing else lives in a per-revision PVC, so prune
// is a no-op). This path renders from the live spec and is used for the current
// candidate; a recorded stable revision is rendered via paramsForRevision so a
// pending candidate's spec can never rewrite the stable Pod template (Bug 2).
func (r *DecisionModelReconciler) paramsFor(
	dm *decisionmodelv1alpha1.DecisionModel,
	digest, image, rev string,
) engine.Params {
	p := engine.Params{
		Model:          engine.ModelRef{Name: dm.Spec.Model, Digest: digest},
		Device:         deviceOrDefault(dm.Spec.Device),
		Image:          image,
		CacheClaimName: storeName(dm),
		Resources:      dm.Spec.Resources,
	}
	if rev != "" {
		p.CacheClaimName = storeNameRev(dm, rev)
		p.StoreSubPath = rev
		p.KeepStoreSubPaths = []string{rev}
	}
	if dm.Spec.Auth != nil {
		p.APIKey = dm.Spec.Auth.APIKeySecretRef
	}
	if dm.Spec.Cache != nil {
		p.DownloadToken = dm.Spec.Cache.DownloadTokenSecretRef
	}
	return p
}

// paramsForRevision builds engine.Params from a recorded revision's own fields
// (model, digest, device, image, resources) rather than the live spec, so a
// stable revision is always rendered as it was promoted even while a different
// candidate is pending or failed. Non-revision inputs (auth) still
// come from the spec. claimName selects the store PVC: for a legacy stable whose
// live Deployment still mounts the shared <dm>-store it is that claim; otherwise
// it is the per-revision PVC storeNameRev(dm, rev.Hash).
func (r *DecisionModelReconciler) paramsForRevision(
	dm *decisionmodelv1alpha1.DecisionModel,
	rev *decisionmodelv1alpha1.RevisionStatus,
	claimName string,
) engine.Params {
	p := engine.Params{
		Model:             engine.ModelRef{Name: rev.Model, Digest: rev.Digest},
		Device:            deviceOrDefault(rev.Device),
		Image:             rev.Image,
		CacheClaimName:    claimName,
		Resources:         rev.Resources,
		StoreSubPath:      rev.Hash,
		KeepStoreSubPaths: []string{rev.Hash},
	}
	if dm.Spec.Auth != nil {
		p.APIKey = dm.Spec.Auth.APIKeySecretRef
	}
	if dm.Spec.Cache != nil {
		p.DownloadToken = dm.Spec.Cache.DownloadTokenSecretRef
	}
	return p
}

// stableParams builds engine.Params to render a recorded stable revision. It
// prefers the revision's own recorded fields; for a legacy revision
// whose status predates image/resources it falls back to the live Deployment's
// serving container so the stable Pod template is not rewritten from the current
// spec (which may describe a different candidate). claimName is the store PVC the
// stable revision mounts (per-revision or legacy shared).
func (r *DecisionModelReconciler) stableParams(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claimName string,
) engine.Params {
	rev := stable.DeepCopy()
	if rev.Image == "" || len(rev.Resources.Limits)+len(rev.Resources.Requests) == 0 {
		// Legacy status: recover image/resources from the live Deployment's
		// serving container rather than the (possibly newer) spec.
		dep := &appsv1.Deployment{}
		key := types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, stable.Hash)}
		if err := r.Get(ctx, key, dep); err == nil {
			if cs := dep.Spec.Template.Spec.Containers; len(cs) > 0 {
				if rev.Image == "" {
					rev.Image = cs[0].Image
				}
				if len(rev.Resources.Limits)+len(rev.Resources.Requests) == 0 {
					rev.Resources = cs[0].Resources
				}
			}
		}
	}
	return r.paramsForRevision(dm, rev, claimName)
}

// conflictIfNotOwned returns errResourceConflict (and records Degraded +
// an Event) when an existing object located by name is not controlled by this
// DM. Callers invoke it only after a successful Get, so the object exists; on a
// genuine conflict the caller must not create, update or delete the object.
func (r *DecisionModelReconciler) conflictIfNotOwned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	obj client.Object,
	kind string,
) error {
	if ownedBy(obj, dm) {
		return nil
	}
	msg := fmt.Sprintf("%s %q exists but is not owned by this DecisionModel; refusing to modify it",
		kind, obj.GetName())
	// Emit the Warning only on the transition into ResourceConflict so a backoff
	// loop (resourceConflictRequeue) does not spam identical Events.
	deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
	already := deg != nil && deg.Status == metav1.ConditionTrue && deg.Reason == reasonResourceConflict
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonResourceConflict,
		Message: msg,
	})
	if !already {
		r.event(ctx, dm, corev1.EventTypeWarning, eventResourceConflict, "%s", msg)
	}
	return errResourceConflict
}

// foreignServiceBlocks reports whether the Service named <dm> is occupied by an
// object this DM does not own. It reads through the uncached APIReader because a
// foreign unlabelled Service is invisible to the label-filtered cache.
// When blocked it sets Degraded=ResourceConflict (Event on transition) and
// Ready=False/ResourceConflict so the model is never advertised Ready while
// traffic would flow to a foreign Service, and returns resourceConflictRequeue.
// A missing Service, our own Service, or an APIReader error that is not NotFound
// (surfaced to the caller) does not block.
func (r *DecisionModelReconciler) foreignServiceBlocks(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) (blocked bool, err error) {
	reader, rerr := r.reader()
	if rerr != nil {
		return false, rerr
	}
	svc := &corev1.Service{}
	gerr := reader.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: dm.Name}, svc)
	switch {
	case apierrors.IsNotFound(gerr):
		return false, nil
	case gerr != nil:
		return false, gerr
	case ownedBy(svc, dm):
		return false, nil
	}
	// Foreign Service: conflict condition + Ready=False (not a rollout failure, so
	// no failedRevision — the candidate keeps running and converges once removed).
	_ = r.conflictIfNotOwned(ctx, dm, svc, "Service")
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reasonResourceConflict,
		Message: fmt.Sprintf("Service %q is not owned by this DecisionModel; traffic would not reach our Pods", dm.Name),
	})
	return true, nil
}

// createOrAdopt creates obj and, on an AlreadyExists, closes the hole the
// label-filtered manager cache opens: a cached r.Get only sees
// objects carrying decisionmodel.io/name, so a foreign *unlabelled* object with
// our name reads as NotFound, the create path runs, and Create returns
// AlreadyExists — which client.IgnoreAlreadyExists would otherwise swallow
// without ever checking ownership (worst case: prefetch into a foreign PVC, or
// Ready behind a foreign Service). On AlreadyExists it re-reads the live object
// through the uncached APIReader and:
//   - not ours             -> conflictIfNotOwned (errResourceConflict; nothing
//     is mounted/selected and the caller backs off);
//   - ours but unlabelled  -> patch decisionmodel.io/name back so the object
//     re-enters the cache and later reconciles see it normally;
//   - ours and labelled     -> a benign cache lag; treated as success.
//
// It requires no new RBAC: APIReader get is already granted.
func (r *DecisionModelReconciler) createOrAdopt(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	obj client.Object,
	kind string,
) error {
	err := r.Create(ctx, obj)
	if err == nil || !apierrors.IsAlreadyExists(err) {
		return err
	}
	reader, rerr := r.reader()
	if rerr != nil {
		return rerr
	}
	live := obj.DeepCopyObject().(client.Object)
	if gerr := reader.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, live); gerr != nil {
		// The object vanished between Create and this Get: let the caller retry.
		return gerr
	}
	if !ownedBy(live, dm) {
		return r.conflictIfNotOwned(ctx, dm, live, kind)
	}
	if live.GetLabels()[decisionmodelv1alpha1.LabelName] == dm.Name {
		return nil // ours and already in the cache's selector: benign cache lag.
	}
	// Ours but someone stripped the cache-selector label: patch it back so the
	// object is visible to the cache again.
	patched := live.DeepCopyObject().(client.Object)
	labels := patched.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[decisionmodelv1alpha1.LabelName] = dm.Name
	patched.SetLabels(labels)
	return r.Patch(ctx, patched, client.MergeFrom(live))
}

// deploymentExists reports whether the serving Deployment for a revision exists.
func (r *DecisionModelReconciler) deploymentExists(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (bool, error) {
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev)}, dep)
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// ensureDeployment creates or updates the serving Deployment for a revision.
//
// frozen is true for the stable revision: when its Deployment already exists,
// placement and resources are taken from the live object instead of being
// re-rendered, so they only ever change through a new revision. The
// candidate path passes false and renders everything from the spec.
func (r *DecisionModelReconciler) ensureDeployment(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	params engine.Params,
	rev string,
	frozen bool,
	apiKeyChecksum string,
	freezeTemplate bool,
) error {
	labels := revisionLabels(dm, rev)
	desired := desiredReplicas(dm)

	dep := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev)}
	getErr := r.Get(ctx, key, dep)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return getErr
	}
	if getErr == nil && !ownedBy(dep, dm) {
		return r.conflictIfNotOwned(ctx, dm, dep, "Deployment")
	}

	// API-key checksum. The checksum of the current key value is
	// recorded on the *Deployment* annotation on every reconcile. It is mirrored
	// into the Pod *template* (which rolls the Pods) only when a rotation is
	// observed, i.e. when a non-empty recorded checksum differs from the current
	// one. Consequences:
	//   - the first observation of an existing stable (operator upgrade, or a
	//     Deployment created before this feature) only records the checksum and
	//     keeps the template's existing checksum, so nothing rolls (upgrade-roll class avoided);
	//   - once a rotation has rolled the template, the template keeps carrying
	//     the rotated checksum on subsequent reconciles (no flapping);
	//   - an empty key (no auth) records and mirrors nothing.
	desiredChecksum := apiKeyChecksum
	tmplChecksum := desiredChecksum // create path: new Deployment, no Pods to roll
	if getErr == nil && desiredChecksum != "" {
		recorded := dep.Annotations[apiKeyChecksumAnnotation]
		liveTmpl := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]
		switch {
		case recorded == "":
			// First observation: keep whatever the live template has (empty for a
			// legacy Deployment), only record below. No roll.
			tmplChecksum = liveTmpl
		case recorded != desiredChecksum:
			// Rotation: mirror into the template to roll the Pods.
			tmplChecksum = desiredChecksum
		default:
			// Steady state: preserve the template's current checksum.
			tmplChecksum = liveTmpl
		}
	}
	var tmplAnnotations map[string]string
	if tmplChecksum != "" {
		tmplAnnotations = map[string]string{apiKeyChecksumAnnotation: tmplChecksum}
	}

	podSpec := eng.ServingPodSpec(params)
	podSpec.ReadinessGates = append(podSpec.ReadinessGates, corev1.PodReadinessGate{
		ConditionType: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate),
	})
	applyScheduling(&podSpec, dm.Spec.Scheduling)
	applyRuntimeClass(&podSpec, dm.Spec.Scheduling)
	applyGPUToleration(&podSpec, params.Device)
	applyCUDAArch(&podSpec, params.Device)
	applySELinuxLevel(&podSpec, dm)
	if frozen && getErr == nil {
		freezeFromLive(&podSpec, &dep.Spec.Template.Spec)
	}

	// The strategy keeps any in-place template change from deadlocking on the
	// store or on GPUs.
	strategy := deploymentStrategy(r.storeAccessModes(ctx, dm, params.CacheClaimName), params.Device)

	// A hash of the desired template + replicas + strategy lets us skip no-op Updates.
	desiredHash := deploymentSpecHash(desired, podSpec, strategy, tmplAnnotations)

	if apierrors.IsNotFound(getErr) {
		depAnnotations := map[string]string{specHashAnnotation: desiredHash}
		if desiredChecksum != "" {
			depAnnotations[apiKeyChecksumAnnotation] = desiredChecksum
		}
		dep = &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   dm.Namespace,
				Name:        revisionName(dm, rev),
				Labels:      labels,
				Annotations: depAnnotations,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &desired,
				Strategy: strategy,
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: tmplAnnotations},
					Spec:       podSpec,
				},
			},
		}
		if err := controllerutil.SetControllerReference(dm, dep, r.Scheme); err != nil {
			return err
		}
		return r.createOrAdopt(ctx, dm, dep, "Deployment")
	}

	// While the stable store PVC is Terminating, do not touch the Pod template:
	// a template change (key rotation, drift restore) with the Recreate strategy
	// would stop the serving Pod first and leave the stable down against a dying
	// volume. Replicas may still be applied; template, annotations
	// and strategy are left as-is until recovery repopulates the store.
	if freezeTemplate && getErr == nil {
		if dep.Spec.Replicas != nil && *dep.Spec.Replicas == desired {
			return nil
		}
		dep.Spec.Replicas = &desired
		return r.Update(ctx, dep)
	}

	// Skip the write only when the desired spec is unchanged (hash matches), the
	// live object has not drifted from the desired state for the fields we own,
	// and the recorded API-key checksum is current. The hash alone covers only
	// the desired values, so an out-of-band change to the live object (e.g.
	// `kubectl scale --replicas=0`, a removed readiness gate, an edited template)
	// would otherwise never be reverted. The API server defaults many
	// PodSpec fields, so the template is compared with DeepDerivative
	// (desired ⊆ live) rather than DeepEqual.
	checksumCurrent := desiredChecksum == "" || dep.Annotations[apiKeyChecksumAnnotation] == desiredChecksum
	if dep.Annotations[specHashAnnotation] == desiredHash && checksumCurrent &&
		deploymentMatchesDesired(dep, desired, labels, podSpec, strategy, tmplAnnotations) {
		return nil
	}

	// Keep replicas, template and scheduling in sync (in-place update).
	// TODO(autoscaling): once HPA/KEDA may own spec.replicas, stop enforcing it
	// here and reconcile only the template.
	if dep.Annotations == nil {
		dep.Annotations = map[string]string{}
	}
	dep.Annotations[specHashAnnotation] = desiredHash
	if desiredChecksum != "" {
		dep.Annotations[apiKeyChecksumAnnotation] = desiredChecksum
	}
	dep.Spec.Replicas = &desired
	dep.Spec.Strategy = strategy
	dep.Spec.Template.Labels = labels
	// Restore the desired template annotations, preserving a user rollout-restart
	// (kubectl.kubernetes.io/restartedAt) so drift repair does not undo it.
	dep.Spec.Template.Annotations = mergeTemplateAnnotations(tmplAnnotations, dep.Spec.Template.Annotations)
	dep.Spec.Template.Spec = podSpec
	return r.Update(ctx, dep)
}

// ensurePDB reconciles the PodDisruptionBudget for a revision's serving Pods.
// A PDB is only useful with more than one replica: with replicas <= 1 a
// maxUnavailable:1 PDB would still block a voluntary eviction of the single Pod
// and so wedge node drains forever, so we delete any PDB in that case. With
// replicas > 1 we create/own a PDB (maxUnavailable: 1) selecting the revision's
// Pods so a node drain cannot take down all serving replicas at once.
// The PDB is owned by the DecisionModel and carries the revision labels so
// gcRevisions collects it by the same rules as the Deployment/Job/PVC.
func (r *DecisionModelReconciler) ensurePDB(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) error {
	key := types.NamespacedName{Namespace: dm.Namespace, Name: pdbName(dm, rev)}
	pdb := &policyv1.PodDisruptionBudget{}
	getErr := r.Get(ctx, key, pdb)
	if getErr == nil && !ownedBy(pdb, dm) {
		return r.conflictIfNotOwned(ctx, dm, pdb, "PodDisruptionBudget")
	}
	if desiredReplicas(dm) <= 1 {
		// No PDB wanted. Read the cache first and only issue a Delete when one
		// actually exists: a steady-state single-replica DM must not send a Delete
		// to the API server on every reconcile. A PDB that the (lagging) cache does
		// not know yet is caught by the next reconcile, which the PDB watch
		// triggers on its create event.
		if getErr != nil {
			return client.IgnoreNotFound(getErr)
		}
		if pdb.DeletionTimestamp != nil {
			return nil // already being deleted
		}
		if err := r.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	labels := revisionLabels(dm, rev)
	maxUnavailable := intstr.FromInt32(1)

	if apierrors.IsNotFound(getErr) {
		pdb = &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: dm.Namespace,
				Name:      pdbName(dm, rev),
				Labels:    labels,
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector:       &metav1.LabelSelector{MatchLabels: labels},
			},
		}
		if err := controllerutil.SetControllerReference(dm, pdb, r.Scheme); err != nil {
			return err
		}
		return r.createOrAdopt(ctx, dm, pdb, "PodDisruptionBudget")
	}
	if getErr != nil {
		return getErr
	}

	unchanged := pdb.Spec.MaxUnavailable != nil && pdb.Spec.MaxUnavailable.IntValue() == 1 &&
		pdb.Spec.MinAvailable == nil &&
		pdb.Spec.Selector != nil && labelsEqual(pdb.Spec.Selector.MatchLabels, labels)
	if unchanged {
		return nil
	}
	pdb.Spec.MaxUnavailable = &maxUnavailable
	pdb.Spec.MinAvailable = nil
	pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	return r.Update(ctx, pdb)
}

// ensureService creates or updates the Service selecting the given revision.
func (r *DecisionModelReconciler) ensureService(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	rev string,
) error {
	selector := revisionLabels(dm, rev)
	port := eng.ServicePort()

	svc := &corev1.Service{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: dm.Name}
	getErr := r.Get(ctx, key, svc)
	if getErr == nil && !ownedBy(svc, dm) {
		return r.conflictIfNotOwned(ctx, dm, svc, "Service")
	}
	if apierrors.IsNotFound(getErr) {
		svc = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: dm.Namespace,
				Name:      dm.Name,
				Labels:    map[string]string{decisionmodelv1alpha1.LabelName: dm.Name},
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selector,
				Ports: []corev1.ServicePort{{
					Name:       portNameHTTP,
					Protocol:   corev1.ProtocolTCP,
					Port:       port,
					TargetPort: intstrFromInt32(port),
				}},
			},
		}
		if err := controllerutil.SetControllerReference(dm, svc, r.Scheme); err != nil {
			return err
		}
		return r.createOrAdopt(ctx, dm, svc, "Service")
	}
	if getErr != nil {
		return getErr
	}

	desiredPort := corev1.ServicePort{Name: portNameHTTP, Protocol: corev1.ProtocolTCP, Port: port, TargetPort: intstrFromInt32(port)}
	unchanged := svc.Spec.Type == corev1.ServiceTypeClusterIP &&
		labelsEqual(svc.Spec.Selector, selector) &&
		len(svc.Spec.Ports) == 1 && svc.Spec.Ports[0] == desiredPort
	if unchanged {
		return nil
	}
	svc.Spec.Type = corev1.ServiceTypeClusterIP
	svc.Spec.Selector = selector
	svc.Spec.Ports = []corev1.ServicePort{desiredPort}
	return r.Update(ctx, svc)
}
