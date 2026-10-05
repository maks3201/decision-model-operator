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
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Progress timeouts for the state machine. Kept in one place; evaluated via r.Now().
const (
	// cacheTimeout bounds how long a revision may stay in Caching.
	cacheTimeout = 30 * time.Minute
	// startTimeout bounds how long a candidate may stay in Starting before promotion.
	startTimeout = 10 * time.Minute
	// probeRequeue is how often we re-probe Pods while not all are model-ready.
	probeRequeue = 10 * time.Second
	// notFoundRequeue avoids a retry storm when the model does not exist.
	notFoundRequeue = 5 * time.Minute
	// promoteGrace defers deletion of the old revision after a promotion so the
	// new revision's EndpointSlices are populated before the old Pods disappear,
	// avoiding a brief endpoint gap.
	promoteGrace = 30 * time.Second
)

// specHashAnnotation stores a hash of the desired Deployment spec so no-op
// reconciles skip the Update call.
const specHashAnnotation = "decisionmodel.io/spec-hash"

// apiKeyChecksumAnnotation stores a hash of the engine API key value on the
// serving Pod template so a Secret value rotation rolls the Pods.
const apiKeyChecksumAnnotation = "decisionmodel.io/apikey-checksum"

// storeRecoveringAnnotation marks a store PVC that was recreated by lost-store
// recovery and is not yet repopulated. It is set at PVC Create time (no extra
// write) and removed once a Complete prefetch Job created at/after the PVC
// exists. Being in the cluster, it survives an operator restart, unlike an
// in-memory marker.
const storeRecoveringAnnotation = "decisionmodel.io/store-recovering"

// annotationTrue is the canonical "true" value for the operator's boolean
// label/annotation markers.
const annotationTrue = "true"

// portNameHTTP is the name of the serving container/Service port.
const portNameHTTP = "http"

// Condition reasons.
const (
	reasonUnknownEngine      = "UnknownEngine"
	reasonModelNotFound      = "ModelNotFound"
	reasonResolved           = "Resolved"
	reasonResolveFailed      = "ResolveFailed"
	reasonCaching            = "Caching"
	reasonCached             = "Cached"
	reasonPrefetchFailed     = "PrefetchFailed"
	reasonStarting           = "Starting"
	reasonModelReady         = "ModelReady"
	reasonReady              = "Ready"
	reasonDigestMismatch     = "DigestMismatch"
	reasonDeviceMismatch     = "DeviceMismatch"
	reasonNotPinned          = "NotPinned"
	reasonProbeError         = "ProbeError"
	reasonStartTimeout       = "StartTimeout"
	reasonCacheTimeout       = "CacheTimeout"
	reasonCacheNotShareable  = "CacheNotShareable"
	reasonCacheSpecImmutable = "CacheSpecImmutable"

	reasonEvaluationUnsupported = "EvaluationUnsupported"
	reasonDatasetInvalid        = "DatasetInvalid"
	reasonEvaluationTimeout     = "EvaluationTimeout"
	reasonEvaluationFailed      = "EvaluationFailed"
	reasonEvaluating            = "Evaluating"
	reasonEvaluated             = "Evaluated"
	reasonBaselineUnavailable   = "BaselineUnavailable"

	reasonReplicasNotModelReady = "ReplicasNotModelReady"
	reasonNoModelReadyPods      = "NoModelReadyPods"

	reasonInvalidModelName        = "InvalidModelName"
	reasonRegistryNotAllowed      = "RegistryNotAllowed"
	reasonImageOverrideNotAllowed = "ImageOverrideNotAllowed"
	reasonSecretNotAllowed        = "SecretNotAllowed"
	reasonResourceConflict        = "ResourceConflict"
	reasonStoreLost               = "StoreLost"
	reasonStorePrefetchFailed     = "StorePrefetchFailed"
	reasonStoreTerminating        = "StoreTerminating"
)

// maxStoreRecoverAttempts bounds how many times a lost-store recovery recreates a
// failed prefetch Job before giving up with Degraded=StorePrefetchFailed.
const maxStoreRecoverAttempts = 3

// defaultMaxConcurrent is the default MaxConcurrentReconciles.
const defaultMaxConcurrent = 4

// defaultAllowedRegistry is the registry host allowed when none is configured.
const defaultAllowedRegistry = "ollaya.dev"

// Event reasons (kept distinct from condition reasons for clarity).
const (
	eventResolved          = "Resolved"
	eventPrefetchStarted   = "PrefetchStarted"
	eventCached            = "Cached"
	eventRevisionStarting  = "RevisionStarting"
	eventPromoted          = "Promoted"
	eventRolledBack        = "RolledBack"
	eventFailed            = "Failed"
	eventProbeMismatch     = "ProbeMismatch"
	eventEvaluationStarted = "EvaluationStarted"
	eventEvaluationPassed  = "EvaluationPassed"
	eventEvaluationFailed  = "EvaluationFailed"
	eventResourceConflict  = "ResourceConflict"
	eventStoreLost         = "StoreLost"
	eventStorePrefetchFail = "StorePrefetchFailed"
	eventStoreTerminating  = "StoreTerminating"
)

// DecisionModelReconciler reconciles a DecisionModel object.
type DecisionModelReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader is an uncached reader used for Secrets and dataset ConfigMaps, so
	// the operator does not start cluster-wide informers on them (RBAC get only).
	APIReader client.Reader
	// Engines maps spec.engine -> implementation.
	Engines map[string]engine.Engine
	// Prober probes serving Pods; defaults to the engine-backed prober.
	Prober Prober
	// Recorder emits events.k8s.io/v1 Events; may be nil (events are then skipped).
	Recorder events.EventRecorder
	// Now is the clock used for timeouts; defaults to time.Now.
	Now func() time.Time
	// AllowedRegistries is the set of registry hosts a model may resolve from.
	// Empty means the default {"ollaya.dev"}.
	AllowedRegistries []string
	// AllowInsecureRegistries permits http:// registries.
	AllowInsecureRegistries bool
	// AllowImageOverride permits spec.image.
	AllowImageOverride bool
	// MaxConcurrentReconciles bounds parallel reconciles. 0 -> defaultMaxConcurrent.
	MaxConcurrentReconciles int
	// WatchNamespaces, when non-empty, restricts reconciliation to DecisionModels
	// in these namespaces (namespace-scoped mode). Empty means all
	// namespaces (cluster-wide, the default). It mirrors the manager cache's
	// DefaultNamespaces and acts as defence in depth: even if the cache were
	// misconfigured, a DM outside the set is never reconciled. Membership is
	// tested via watchedNamespace; keep this in sync with the cache options.
	WatchNamespaces []string
	// BaseContext is a manager-lifetime context for background eval goroutines;
	// defaults to context.Background when unset.
	BaseContext context.Context
	// PrefetchProxyEnv is the proxy environment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY
	// and lower-case variants) copied to the prefetch container so `ollaya pull`
	// works in proxied clusters. Set once at startup from the operator's
	// own environment via ProxyEnvFromEnviron; never derived from a DecisionModel.
	// It never contains a proxy URL with credentials: those stay with the operator
	// and are not written into tenant Jobs.
	PrefetchProxyEnv []corev1.EnvVar
	// evals holds in-flight/finished evaluation results, keyed by evalKey.
	evals     *evalStore
	evalsOnce sync.Once
	// regate remembers when each serving Pod was last re-inspected.
	// In memory only; guarded by regateMu.
	regateMu sync.Mutex
	regate   regateTracker
	// storeRecover tracks, per DecisionModel UID, the bounded failed-prefetch retry
	// count for lost-store recovery: the UID of the last failed prefetch Job
	// already counted (idempotent counting) and whether the bound is exhausted.
	// This is a bound, not a safety signal — the restart-safe
	// "recovering" state lives on the store PVC annotation. In memory only;
	// guarded by storeRecoverMu.
	storeRecoverMu sync.Mutex
	storeRecover   map[types.UID]*storeRecoverState
}

// storeRecoverState is the per-DecisionModel lost-store recovery bookkeeping.
type storeRecoverState struct {
	attempts      int       // failed prefetch Jobs counted so far
	lastFailedJob types.UID // UID of the last failed Job already counted (idempotency)
	exhausted     bool      // retries exhausted; StorePrefetchFailed already emitted
}

// watchedNamespace reports whether the reconciler should act on objects in ns.
// An empty WatchNamespaces set means cluster-wide (all namespaces), matching the
// default manager cache; otherwise only listed namespaces are watched.
func (r *DecisionModelReconciler) watchedNamespace(ns string) bool {
	if len(r.WatchNamespaces) == 0 {
		return true
	}
	for _, n := range r.WatchNamespaces {
		if n == ns {
			return true
		}
	}
	return false
}

// evalStoreOrInit lazily initialises the evaluation result store.
func (r *DecisionModelReconciler) evalStoreOrInit() *evalStore {
	r.evalsOnce.Do(func() {
		if r.evals == nil {
			r.evals = newEvalStore()
		}
	})
	return r.evals
}

// +kubebuilder:rbac:groups=decisionmodel.io,resources=decisionmodels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=decisionmodel.io,resources=decisionmodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=decisionmodel.io,resources=decisionmodels/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives the state machine: Resolving -> Caching -> Starting ->
// Promoting -> Ready, with Failed / RolledBack on failure. It is level-triggered:
// every call recomputes the desired phase from actual cluster state.
func (r *DecisionModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Namespace-scoped mode: never reconcile a DecisionModel outside the
	// watched set. The manager cache is already scoped to these namespaces, so
	// this is defence in depth against a misconfigured cache; we return without
	// any read or write and log at V(1).
	if !r.watchedNamespace(req.Namespace) {
		logf.FromContext(ctx).V(1).Info("ignoring DecisionModel outside watched namespaces",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, nil
	}

	var dm decisionmodelv1alpha1.DecisionModel
	if err := r.Get(ctx, req.NamespacedName, &dm); err != nil {
		if apierrors.IsNotFound(err) {
			// DM gone: cancel and drop any evaluations we were running for it,
			// and delete its exported metric series so they stop being scraped.
			r.evalStoreOrInit().forgetExcept(req.Namespace, req.Name, "")
			deleteMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A durable promotion makes its approval annotation obsolete: remove it before
	// taking the status-patch base so the optimistic lock uses the fresh version.
	if err := r.consumePromoteApproval(ctx, &dm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Capture the object as the merge base for status patches.
	ctx = withPatchBase(ctx, dm.DeepCopy())

	// Prune the prober's per-Pod warm cache against the DM's live Pods, so it does
	// not grow without bound as Pods are replaced across rollouts.
	r.pruneProberWarm(ctx, &dm)

	// 1. Resolve the engine.
	eng, ok := r.Engines[engineOrDefault(dm.Spec.Engine)]
	if !ok {
		setStatusCondition(&dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonUnknownEngine,
			Message: fmt.Sprintf("unknown engine %q", dm.Spec.Engine),
		})
		r.setPhase(ctx, &dm, decisionmodelv1alpha1.PhaseFailed)
		return r.finish(ctx, &dm, ctrl.Result{}, nil)
	}

	// Security guards: validate the model name, registry allow-list,
	// and image-override policy before touching the registry or any workload.
	if res, err, done := r.guardSecurity(ctx, &dm, eng); done {
		return res, err
	}

	// 2. Resolve the model to a digest.
	digest, done, res, err := r.reconcileResolve(ctx, &dm, eng)
	if done {
		return res, err
	}

	// 3. Compute the revision and candidate identity.
	image := servingImage(eng, r.paramsFor(&dm, digest, "", ""))
	rev := RevisionHash(dm.Spec, digest, image)
	params := r.paramsFor(&dm, digest, image, rev)
	candidate := &decisionmodelv1alpha1.RevisionStatus{
		Hash:   rev,
		Engine: engineOrDefault(dm.Spec.Engine),
		Model:  dm.Spec.Model,
		Digest: digest,
		Device: deviceOrDefault(dm.Spec.Device),
		// Record the model-affecting render inputs so the stable revision is later
		// rendered from its own state, not a (possibly newer) spec.
		Image:     image,
		Resources: dm.Spec.Resources,
		Placement: placementRecord(dm.Spec.Scheduling),
	}
	stable := dm.Status.StableRevision
	// A stable created before placement was recorded keeps its old hash name and
	// workloads; just record the placement it is running with. This runs
	// on the first reconcile after an operator upgrade, so no candidate is started
	// and nothing is rolled. From then on a change of scheduling differs from the
	// recorded placement and starts a new revision.
	adoptLegacyStable(stable, candidate, legacyRevisionHash(dm.Spec, digest, image))
	isStable := stable != nil && (stable.Hash == rev || sameIdentity(stable, candidate))

	// Cancel any evaluations running for a revision of this DM that is neither the
	// current candidate nor the current stable (e.g. after a spec/model change).
	keepRevs := []string{rev}
	if stable != nil {
		keepRevs = append(keepRevs, stable.Hash)
	}
	r.evalStoreOrInit().forgetExcept(dm.Namespace, dm.Name, keepRevs...)

	// 4. Ensure the model store PVC for the revision we are about to act on and
	// evaluate the cache-sharing guard against it (one PVC per revision).
	// On the stable path reuse the stable revision's existing claim (per-revision
	// PVC, or the legacy shared <dm>-store until the next promotion); on the
	// candidate path create the candidate's own per-revision PVC.
	var pvc *corev1.PersistentVolumeClaim
	if isStable {
		pvc, err = r.ensureStoreForStable(ctx, &dm, stable)
	} else {
		pvc, err = r.ensurePVC(ctx, &dm, rev)
	}
	// A terminating claim is a wait, not a failure, and it only matters to the flow
	// that would build on it (the candidate flow below). The retry bookkeeping and
	// the failed-revision guard do not need the claim, so they still run; without
	// this a `decisionmodel.io/retry` would appear to do nothing until the old
	// claim finished terminating.
	storeTerminating := errors.Is(err, errStoreTerminating)
	// A lost stable store (missing or terminating PVC) is not a failure either: the
	// stable path routes it to recoverStableStore, which recreates the PVC with the
	// recovering annotation and reprefetches before serving. There is
	// no PVC to run the cache-sharing guard against, so skip it in that case.
	storeLost := errors.Is(err, errStoreLostRecovering)
	if err != nil && !storeTerminating && !storeLost {
		return r.finish(ctx, &dm, ctrl.Result{}, err)
	}
	cacheDegraded := false
	if !storeTerminating && !storeLost {
		var cacheErr error
		cacheDegraded, cacheErr = r.guardCacheSharing(ctx, &dm, pvc)
		if cacheErr != nil {
			// A transient cache-reconcile error (e.g. a PVC expansion Patch that
			// failed for a non-policy reason): back off via the workqueue rather
			// than parking it as an immutable-spec condition.
			return r.finish(ctx, &dm, ctrl.Result{}, cacheErr)
		}
	}

	apiKey, err := r.apiKey(ctx, &dm)
	if err != nil {
		if errors.Is(err, errSecretNotAllowed) {
			// The API-key Secret is missing the opt-in label (confused-deputy
			// guard). This is NOT terminal: there is no Secret watch, so adding the
			// label would otherwise never retrigger. Keep Ready=False/SecretNotAllowed
			// and requeue at the regate interval so the fix is picked up within ~60s.
			// setStatusCondition is idempotent (no LastTransitionTime churn when the
			// status/reason are unchanged), so repeated requeues cause no churn.
			setStatusCondition(&dm, metav1.Condition{
				Type:    decisionmodelv1alpha1.ConditionReady,
				Status:  metav1.ConditionFalse,
				Reason:  reasonSecretNotAllowed,
				Message: err.Error(),
			})
			r.setPhase(ctx, &dm, decisionmodelv1alpha1.PhaseDegraded)
			return r.finish(ctx, &dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
		}
		return r.finish(ctx, &dm, ctrl.Result{}, err)
	}

	// a new decisionmodel.io/retry token clears a failed revision and
	// resets an exhausted stable-store recovery (consumed once); a stale/absent
	// token clears failedRevision only when the spec returned to the stable revision.
	r.applyRetryToken(&dm, isStable)

	// Do not automatically retry a revision that already failed. A spec change
	// produces a new revision hash, which clears this guard. This path is
	// authoritative for phase/conditions so a stray/stale write cannot leave the
	// DM stuck: it never runs the candidate/caching flow.
	if dm.Status.FailedRevision != nil && dm.Status.FailedRevision.Hash == rev && !isStable {
		return r.reconcileFailedRevision(ctx, &dm, eng, stable, apiKey, cacheDegraded)
	}

	// 9. In-place path: revision already stable.
	if isStable {
		// A lost or terminating stable store routes to recoverStableStore inside
		// the stable path (annotated recreate + reprefetch, or StoreTerminating
		// while the PVC is still mounted); it must not render Pods against a
		// missing/dying volume. cacheDegraded is false here (no PVC
		// to guard); recovery owns the Degraded condition.
		return r.reconcileStablePath(ctx, &dm, eng, stable, apiKey, cacheDegraded)
	}

	// Candidate revision (new spec or first creation). It builds on this
	// revision's store claim, so it waits while the previous claim of the same
	// revision is still being deleted; once that is NotFound the next reconcile
	// creates a fresh one.
	if storeTerminating {
		return r.finish(ctx, &dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
	}
	return r.reconcileCandidate(ctx, &dm, eng, params, candidate, digest, cacheDegraded, apiKey)
}

// applyRetryToken handles the decisionmodel.io/retry annotation. A new token
// (!= status.lastRetryToken) is the user's "I fixed the cause, try again" signal:
// it clears a failed revision AND resets an exhausted stable-store recovery (the
// in-memory latch), and is consumed once (lastRetryToken recorded). A stale or
// absent token clears a failed revision only when the spec has returned to the
// stable revision (existing failed-revision semantics).
func (r *DecisionModelReconciler) applyRetryToken(dm *decisionmodelv1alpha1.DecisionModel, isStable bool) {
	tok := dm.Annotations[decisionmodelv1alpha1.AnnotationRetry]
	newRetry := tok != "" && tok != dm.Status.LastRetryToken
	if dm.Status.FailedRevision != nil {
		if newRetry {
			dm.Status.FailedRevision = nil
			dm.Status.LastRetryToken = tok
		} else if isStable {
			dm.Status.FailedRevision = nil
		}
	}
	// A new token while the stable store recovery is exhausted
	// (Degraded=StorePrefetchFailed) clears the latch so the stable path starts a
	// fresh bounded set of prefetch attempts (the failed Job is deleted and
	// recreated as usual).
	if newRetry && r.storeRecoverExhausted(dm.UID) {
		r.clearStoreRecover(dm.UID)
		dm.Status.LastRetryToken = tok
	}
}

// reconcileFailedRevision is the authoritative path for a candidate revision
// that already failed and must not be retried automatically. It never
// runs the caching/candidate flow: it drops stale conditions, reflects the
// stable revision's readiness (or Failed when there is no stable), preserves the
// original failure reason in Degraded=True, and GCs stale revisions.
func (r *DecisionModelReconciler) reconcileFailedRevision(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
	cacheDegraded bool,
) (ctrl.Result, error) {
	dm.Status.CandidateRevision = nil
	// A revision that never cached/evaluated should not advertise those
	// conditions; drop stale "prefetching"/eval conditions.
	meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionCached)
	meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated)
	// Remember the original failure reason so it survives applyStableReadiness
	// (which would otherwise flip Degraded to False for a healthy stable).
	failReason, failMsg := "RevisionFailed",
		fmt.Sprintf("revision %s failed and will not be retried", dm.Status.FailedRevision.Hash)
	if c := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded); c != nil && c.Status == metav1.ConditionTrue {
		failReason, failMsg = c.Reason, c.Message
	}
	if stable != nil {
		ready, err := r.reconcileStable(ctx, dm, eng, stable, apiKey)
		if err != nil {
			return r.finish(ctx, dm, ctrl.Result{}, err)
		}
		// Reflect the stable revision's readiness, then override phase to
		// RolledBack (a failed candidate was rolled back to this stable).
		r.applyStableReadiness(ctx, dm, ready, desiredReplicas(dm), cacheDegraded)
		r.setReplicaStatus(dm, ready)
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseRolledBack)
	} else {
		r.setReplicaStatus(dm, 0)
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
	}
	// Preserve the original failure reason in Degraded=True.
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  failReason,
		Message: failMsg,
	})
	// GC stale revisions on this path too: the failed candidate's
	// workloads and any other non-stable/non-candidate revisions.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	return r.finish(ctx, dm, ctrl.Result{}, nil)
}

// reconcileResolve resolves the model digest and maintains the Resolved
// condition. done=true means the caller should return (res, err) immediately.
func (r *DecisionModelReconciler) reconcileResolve(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
) (digest string, done bool, res ctrl.Result, err error) {
	digest, err = r.resolveDigest(ctx, dm, eng)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			setStatusCondition(dm, metav1.Condition{
				Type:    decisionmodelv1alpha1.ConditionResolved,
				Status:  metav1.ConditionFalse,
				Reason:  reasonModelNotFound,
				Message: fmt.Sprintf("model %q not found", dm.Spec.Model),
			})
			r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
			res, err = r.finish(ctx, dm, ctrl.Result{RequeueAfter: notFoundRequeue}, nil)
			return "", true, res, err
		}
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionResolved,
			Status:  metav1.ConditionFalse,
			Reason:  reasonResolveFailed,
			Message: err.Error(),
		})
		if dm.Status.Phase == "" {
			r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseResolving)
		}
		res, err = r.finish(ctx, dm, ctrl.Result{}, err)
		return "", true, res, err
	}
	if !meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionResolved) {
		r.event(ctx, dm, corev1.EventTypeNormal, eventResolved,
			"resolved %q to digest %s", dm.Spec.Model, digest)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionResolved,
		Status:  metav1.ConditionTrue,
		Reason:  reasonResolved,
		Message: fmt.Sprintf("resolved %q to digest %s", dm.Spec.Model, digest),
	})
	return digest, false, ctrl.Result{}, nil
}

// reconcileStablePath keeps the stable revision in sync and re-probes its Pods.
func (r *DecisionModelReconciler) reconcileStablePath(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
	cacheDegraded bool,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	// Render the stable revision from its own recorded fields (not the live spec,
	// which may already describe a pending candidate) and mount its own store
	// claim (per-revision PVC). Fall back to the live Deployment's
	// image/resources for a legacy stable whose status predates these fields.
	claim, legacy := r.storeClaimForStable(ctx, dm, stable)

	// Recover a lost stable store before (re)starting Pods: if the store PVC is
	// missing, re-create it and prefetch the stable's recorded identity into it,
	// staying Degraded meanwhile. Serving on an empty store would make the stable
	// silently un-ready. The legacy shared store is recreated under its
	// own name (never switched to a per-revision PVC).
	recovering, storeTerminating, rres, rerr := r.recoverStableStore(ctx, dm, eng, stable, claim, legacy)
	if recovering {
		return rres, rerr
	}

	stableParams := r.stableParams(ctx, dm, stable, claim)
	if err := r.ensureDeployment(ctx, dm, eng, stableParams, stable.Hash, true, apiKeyChecksum(apiKey), storeTerminating); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if err := r.ensureService(ctx, dm, eng, stable.Hash); err != nil {
		if errors.Is(err, errResourceConflict) {
			// A foreign Service occupies our name: conflictIfNotOwned already set
			// Degraded=ResourceConflict. The model is not reachable through our
			// Service, so it is not Ready; requeue to re-check with nil err.
			setStatusCondition(dm, metav1.Condition{
				Type:    decisionmodelv1alpha1.ConditionReady,
				Status:  metav1.ConditionFalse,
				Reason:  reasonResourceConflict,
				Message: fmt.Sprintf("Service %q is not owned by this DecisionModel; traffic would not reach our Pods", dm.Name),
			})
			return r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
		}
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if err := r.ensurePDB(ctx, dm, stable.Hash); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	dm.Status.CandidateRevision = nil
	ready, precision, probeErr := r.probePods(ctx, dm, eng, stable, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		// Could not even list the Pods: abort and let the workqueue back off. It
		// must not be mistaken for "0 ready" (false Degraded / timeout).
		return r.finish(ctx, dm, ctrl.Result{}, probeErr)
	}
	if probeErr != nil {
		log.V(1).Info("probe error (stable)", "error", probeErr)
	}
	if precision != "" {
		stable.Precision = precision
	}
	r.setReplicaStatus(dm, ready)
	// Honest readiness: reflect how many stable Pods actually report the
	// expected digest/device, not an unconditional Ready. A Terminating-but-mounted
	// store keeps the stable serving while preserving Degraded=StoreTerminating,
	// just like a cache-sharing Degraded.
	r.applyStableReadiness(ctx, dm, ready, desiredReplicas(dm), cacheDegraded || storeTerminating)

	// GC every stale revision: keeps stable + candidate, and the demoted
	// previous revision only within its grace window.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	res := r.requeueIfShort(ready, desiredReplicas(dm))
	// While the previous revision is still within its grace window, requeue so it
	// gets collected once the window elapses.
	if res.RequeueAfter == 0 && dm.Status.PreviousRevision != nil {
		res.RequeueAfter = promoteGrace
	}
	// All Pods ready: also come back to re-inspect them, but never
	// later than an already pending requeue. This only requeues; a reconcile that
	// changes nothing writes nothing.
	if ready > 0 {
		res = soonest(res, r.regateRequeue())
	}
	return r.finish(ctx, dm, res, nil)
}

// maintainStable keeps the recorded stable revision healthy while a candidate is
// mid-rollout. It restores a deleted stable Deployment, applies an
// API-key rotation (checksum), keeps the PDB, and regates the stable Pods — all
// rendered from status.stableRevision, never the live spec (which describes the
// candidate). It owns no phase/Ready/Degraded conditions and does not switch the
// Service or GC: the candidate flow owns those.
//
// Store recovery is intentionally NOT run here (it drives the stable path once
// the rollout concludes). If the stable store PVC is missing or terminating the
// Deployment is left untouched — never re-rendered against a missing/dying
// volume — and regating is skipped for this round.
func (r *DecisionModelReconciler) maintainStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) error {
	claim, _ := r.storeClaimForStable(ctx, dm, stable)
	serveable, terminating, err := r.stableStoreServeable(ctx, dm, claim)
	if err != nil {
		return err
	}
	if !serveable {
		// Missing/recovering store: do not render the Deployment against it. The
		// stable path will recover it after the rollout concludes.
		return nil
	}
	stableParams := r.stableParams(ctx, dm, stable, claim)
	if err := r.ensureDeployment(ctx, dm, eng, stableParams, stable.Hash, true, apiKeyChecksum(apiKey), terminating); err != nil {
		return err
	}
	if err := r.ensurePDB(ctx, dm, stable.Hash); err != nil {
		return err
	}
	// Regate the stable Pods: a stable Pod that lost the model during
	// the rollout must flip its gate False. The readiness count is not used here;
	// the candidate flow owns phase/Ready.
	_, _, probeErr := r.probePods(ctx, dm, eng, stable, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return probeErr
	}
	return nil
}

// stableStoreServeable reports whether the stable store PVC named claim is
// present, owned, not terminating and not marked recovering — i.e. safe to
// render the stable Deployment against. terminating is true when the PVC is
// being deleted but still present (freeze the template). A missing,
// foreign, or recovering store is not serveable.
func (r *DecisionModelReconciler) stableStoreServeable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) (serveable, terminating bool, err error) {
	pvc := &corev1.PersistentVolumeClaim{}
	gerr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc)
	switch {
	case apierrors.IsNotFound(gerr):
		return false, false, nil
	case gerr != nil:
		return false, false, gerr
	case !ownedBy(pvc, dm):
		return false, false, nil
	case pvc.Annotations[storeRecoveringAnnotation] == annotationTrue:
		return false, false, nil
	case pvc.DeletionTimestamp != nil:
		return true, true, nil
	default:
		return true, false, nil
	}
}

// recoverStableStore ensures the stable revision's store PVC exists and is
// populated before the stable Pods are (re)started. It returns handled=true when
// the store is missing or still being (re)prefetched: in that case it has set
// the result/err the caller must return and the stable path stops for this round
// (the Deployment is not re-rendered against a missing/empty volume).
// It returns handled=false (serve) for a healthy store and also for a PVC that is
// Terminating but still mounted, reporting storeTerminating=true so the caller
// keeps the stable serving and preserves Degraded=StoreTerminating.
//
// Recovery is level-triggered from cluster state: a store PVC recreated
// by recovery carries the storeRecoveringAnnotation, set at Create time, so the
// store counts as unpopulated until a fresh prefetch completes — and this
// survives an operator restart (no in-memory "recovering" marker). The store is
// treated as populated once a Complete prefetch Job created at or after the PVC
// exists (job.CreationTimestamp >= pvc.CreationTimestamp); a Complete Job older
// than the PVC filled a previous volume and is stale. The annotation is then
// removed. A PVC without the annotation (normal rollout, or legacy/pruned) is
// healthy.
//
// A failed recovery prefetch is deleted and recreated, bounded by
// maxStoreRecoverAttempts (counted per failed Job UID so a stale cache read is not
// double-counted); past the bound it stops with Degraded=StorePrefetchFailed. To
// avoid reading the informer cache for a Job just deleted in the same reconcile,
// every delete returns immediately with a short requeue.
func (r *DecisionModelReconciler) recoverStableStore(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	legacy bool,
) (handled, storeTerminating bool, res ctrl.Result, err error) {
	// --- 1-2. Inspect the store PVC and the stable prefetch Job. ---
	st, handled, res, err := r.inspectRecoveryState(ctx, dm, stable, claim)
	if handled {
		return true, false, res, err
	}

	// --- 3. Healthy (serve)? A present PVC that is not marked recovering is
	// populated: either a normal rollout or a legacy/pruned store. A PVC still
	// mounted but Terminating also keeps serving; the caller preserves
	// Degraded=StoreTerminating through applyStableReadiness. ---
	if !st.pvcMissing && !st.recovering {
		if !st.terminating {
			r.clearStoreRecover(dm.UID)
		}
		return false, st.terminating, ctrl.Result{}, nil
	}

	// --- 4. Recovering (PVC missing, or present with the recovering annotation):
	// hold Degraded and keep the store being (re)filled. ---
	// Once recovery is exhausted, StorePrefetchFailed owns the Degraded condition;
	// do not overwrite it with StoreLost on every reconcile (no status churn).
	if !r.storeRecoverExhausted(dm.UID) {
		r.degradeStoreLost(ctx, dm, claim)
	}

	// Recreate the PVC if it is gone, carrying the recovering annotation, then
	// return (next reconcile observes the new PVC and (re)creates the prefetch;
	// never Get a resource we just wrote in the same reconcile from the cache).
	if st.pvcMissing {
		if cerr := r.createRecoveryPVC(ctx, dm, stable, claim, legacy); cerr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, cerr)
			return true, false, res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return true, false, res, ferr
	}

	res, err = r.runRecoveryPrefetch(ctx, dm, eng, stable, claim, st)
	return true, false, res, err
}

// storeRecoveryState is the inspected PVC/Job state the recovery flow needs.
type storeRecoveryState struct {
	pvc          *corev1.PersistentVolumeClaim
	job          *batchv1.Job
	jobExists    bool
	pvcMissing   bool
	jobOngoing   bool
	jobFailedNow bool
	// recovering is true when the present PVC carries the storeRecoveringAnnotation
	// (recreated by recovery and not yet repopulated).
	recovering bool
	// terminating is true when the present PVC is being deleted but is still
	// mounted by the stable Pods (pvc-protection). The stable keeps serving and
	// surfaces Degraded=StoreTerminating.
	terminating bool
	// populated is true when the PVC is present and a Complete prefetch Job was
	// created at or after it (so it filled THIS volume, not a previous one).
	populated bool
	// staleComplete is true when a Complete Job predates the PVC (it filled an
	// older volume): it must be deleted before a fresh prefetch runs.
	staleComplete bool
}

// inspectRecoveryState reads the stable store PVC and prefetch Job and classifies
// them for recoverStableStore. It returns handled=true (with the result/err the
// caller must return) for the terminal cases — a foreign/terminating PVC, a
// foreign Job, or a non-NotFound Get error.
func (r *DecisionModelReconciler) inspectRecoveryState(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
) (st storeRecoveryState, handled bool, res ctrl.Result, err error) {
	pvc := &corev1.PersistentVolumeClaim{}
	pvcErr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc)
	switch {
	case pvcErr == nil:
		if !ownedBy(pvc, dm) {
			// Persist the ResourceConflict condition/Event and back off via
			// RequeueAfter with a nil error (controller-runtime ignores RequeueAfter
			// when err != nil).
			_ = r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
			res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
			return st, true, res, ferr
		}
		if pvc.DeletionTimestamp != nil {
			// Policy: a stable store PVC stuck Terminating is still
			// mounted by the stable Pods (pvc-protection). Do NOT stop them
			// automatically — surface Degraded=StoreTerminating and let the stable
			// path keep running (regate, key rotation). Once the PVC is actually
			// gone, recovery (pvcMissing) recreates and reprefetches it. handled is
			// false so reconcileStablePath proceeds; the condition is persisted by
			// the stable path's own finish().
			r.degradeStoreTerminating(ctx, dm, claim)
			st.pvc = pvc
			st.recovering = false // present and mounted: keep serving, do not treat as unpopulated
			st.terminating = true
			return st, false, ctrl.Result{}, nil
		}
		st.pvc = pvc
		st.recovering = pvc.Annotations[storeRecoveringAnnotation] == annotationTrue
	case apierrors.IsNotFound(pvcErr):
		st.pvcMissing = true
	default:
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, pvcErr)
		return st, true, res, ferr
	}

	st.job = &batchv1.Job{}
	jobErr := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, stable.Hash)}, st.job)
	st.jobExists = jobErr == nil
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, jobErr)
		return st, true, res, ferr
	}
	if st.jobExists && !ownedBy(st.job, dm) {
		// Same as the PVC conflict: persist the condition and requeue with nil err.
		_ = r.conflictIfNotOwned(ctx, dm, st.job, "Job")
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
		return st, true, res, ferr
	}
	live := st.jobExists && st.job.DeletionTimestamp == nil
	st.jobOngoing = live && !jobComplete(st.job) && !jobFailed(st.job)
	st.jobFailedNow = live && jobFailed(st.job)
	if live && jobComplete(st.job) {
		// Complete AND created at/after the PVC => it filled this (recovering)
		// volume; otherwise it filled an older volume and is stale.
		if !st.pvcMissing && !st.job.CreationTimestamp.Before(&st.pvc.CreationTimestamp) {
			st.populated = true
		} else {
			st.staleComplete = true
		}
	}
	return st, false, ctrl.Result{}, nil
}

// runRecoveryPrefetch manages the recovery prefetch Job once the PVC exists: it
// deletes a failed Job (bounded by maxStoreRecoverAttempts, per Job UID, then
// StorePrefetchFailed) or a stale Complete Job, returning immediately after any
// delete (never reading the just-deleted Job from the cache), and otherwise
// (re)creates the prefetch and holds until it completes.
func (r *DecisionModelReconciler) runRecoveryPrefetch(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	st storeRecoveryState,
) (res ctrl.Result, err error) {
	bg := metav1.DeletePropagationBackground
	delJob := func() error {
		return r.deleteIfOwned(ctx, dm,
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: prefetchName(dm, stable.Hash)}},
			&client.DeleteOptions{PropagationPolicy: &bg})
	}

	// Recovery complete: a fresh Complete prefetch Job filled this PVC. Clear the
	// recovering annotation (so the next reconcile serves) and the failure counter.
	if st.populated {
		if cerr := r.clearStoreRecoveringAnnotation(ctx, st.pvc); cerr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, cerr)
			return res, ferr
		}
		r.clearStoreRecover(dm.UID)
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A failed recovery prefetch: count it once per Job UID, then delete and
	// requeue (recreate next reconcile). Past the bound, give up.
	if st.jobFailedNow {
		if r.countFailedRecovery(dm.UID, st.job.UID) {
			r.degradeStorePrefetchFailed(ctx, dm, claim)
			// Stop churning: slow requeue, no further counting or writes.
			res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
			return res, ferr
		}
		if derr := delJob(); derr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, derr)
			return res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A stale Complete Job (older than the PVC): delete it and requeue; a fresh
	// prefetch is created on the next reconcile, after the delete has settled.
	if st.staleComplete {
		if derr := delJob(); derr != nil {
			res, ferr := r.finish(ctx, dm, ctrl.Result{}, derr)
			return res, ferr
		}
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
		return res, ferr
	}

	// A live ongoing prefetch: hold until it completes.
	if st.jobOngoing {
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		return res, ferr
	}

	// No Job present (PVC exists, recovery active): create a fresh prefetch.
	_, done, failed, perr := r.ensurePrefetchJob(ctx, dm, eng, r.stableParams(ctx, dm, stable, claim), stable.Hash)
	if perr != nil {
		res, ferr := r.finish(ctx, dm, ctrl.Result{}, perr)
		return res, ferr
	}
	if failed || !done {
		res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
		return res, ferr
	}
	// Freshly created and already Complete (unusual): populated next reconcile.
	res, ferr := r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
	return res, ferr
}

// countFailedRecovery records a failed recovery prefetch Job identified by jobUID.
// It counts a given Job UID at most once (so a stale cache read of the same failed
// Job is not double-counted) and returns whether the bounded retries are now
// exhausted.
func (r *DecisionModelReconciler) countFailedRecovery(dmUID, jobUID types.UID) (exhausted bool) {
	r.storeRecoverMu.Lock()
	defer r.storeRecoverMu.Unlock()
	if r.storeRecover == nil {
		r.storeRecover = map[types.UID]*storeRecoverState{}
	}
	s := r.storeRecover[dmUID]
	if s == nil {
		s = &storeRecoverState{}
		r.storeRecover[dmUID] = s
	}
	if s.exhausted {
		return true
	}
	if s.lastFailedJob != jobUID {
		s.lastFailedJob = jobUID
		s.attempts++
	}
	if s.attempts >= maxStoreRecoverAttempts {
		s.exhausted = true
	}
	return s.exhausted
}

// clearStoreRecoveringAnnotation removes the storeRecoveringAnnotation from a now
// repopulated store PVC via a Patch, so later reconciles treat it as healthy.
// A no-op if the annotation is already gone.
func (r *DecisionModelReconciler) clearStoreRecoveringAnnotation(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc == nil || pvc.Annotations[storeRecoveringAnnotation] == "" {
		return nil
	}
	patched := pvc.DeepCopy()
	delete(patched.Annotations, storeRecoveringAnnotation)
	return r.Patch(ctx, patched, client.MergeFrom(pvc))
}

// storeRecoverExhausted reports whether the bounded recovery retries for a
// DecisionModel are exhausted (StorePrefetchFailed owns the Degraded condition).
func (r *DecisionModelReconciler) storeRecoverExhausted(uid types.UID) bool {
	r.storeRecoverMu.Lock()
	defer r.storeRecoverMu.Unlock()
	s := r.storeRecover[uid]
	return s != nil && s.exhausted
}

// clearStoreRecover resets the (in-memory, bound-only) failure counter for a
// DecisionModel. The restart-safe "recovering" signal lives on the PVC annotation.
func (r *DecisionModelReconciler) clearStoreRecover(uid types.UID) {
	r.storeRecoverMu.Lock()
	defer r.storeRecoverMu.Unlock()
	delete(r.storeRecover, uid)
}

// degradeStorePrefetchFailed marks a lost-store recovery as exhausted after
// attempts failed prefetch Jobs.
func (r *DecisionModelReconciler) degradeStorePrefetchFailed(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("stable store %q recovery failed after %d prefetch attempts; "+
		"the store could not be repopulated (manual intervention required)", claim, maxStoreRecoverAttempts)
	// Emit the Event only on the transition into StorePrefetchFailed, and write the
	// condition with a fixed message so repeated reconciles cause no status churn.
	already := meta.IsStatusConditionPresentAndEqual(dm.Status.Conditions,
		decisionmodelv1alpha1.ConditionDegraded, metav1.ConditionTrue) &&
		meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded).Reason == reasonStorePrefetchFailed
	if already {
		return
	}
	r.event(ctx, dm, corev1.EventTypeWarning, eventStorePrefetchFail, "%s", msg)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStorePrefetchFailed,
		Message: msg,
	})
}

// degradeStoreLost sets Degraded=True/StoreLost and emits one Event.
func (r *DecisionModelReconciler) degradeStoreLost(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("stable store PVC %q is missing; recreating and prefetching before serving", claim)
	if meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded) == nil ||
		!meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded) {
		r.event(ctx, dm, corev1.EventTypeWarning, eventStoreLost, "%s", msg)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStoreLost,
		Message: msg,
	})
}

// degradeStoreTerminating sets Degraded=True/StoreTerminating while the stable
// store PVC is being deleted but is still mounted by the stable Pods.
// The operator does not stop the stable automatically (a PVC delete cannot be
// undone); the user must delete the Pods to let recovery proceed. The Event fires
// only on the transition into StoreTerminating and the condition carries a fixed
// message, so repeated reconciles on the regate interval cause no status churn.
func (r *DecisionModelReconciler) degradeStoreTerminating(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) {
	msg := fmt.Sprintf("the stable store PVC %q is being deleted; it is still mounted by the stable Pods "+
		"— delete them to let recovery proceed", claim)
	deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
	already := deg != nil && deg.Status == metav1.ConditionTrue && deg.Reason == reasonStoreTerminating
	if !already {
		r.event(ctx, dm, corev1.EventTypeWarning, eventStoreTerminating, "%s", msg)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reasonStoreTerminating,
		Message: msg,
	})
}

// storePVCSpec returns the model-store PVC spec (size/class/access modes) from
// the DecisionModel's cache spec, with the operator defaults when unset.
func storePVCSpec(dm *decisionmodelv1alpha1.DecisionModel) corev1.PersistentVolumeClaimSpec {
	size := resource.MustParse("10Gi")
	var storageClass *string
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if dm.Spec.Cache != nil {
		if !dm.Spec.Cache.Size.IsZero() {
			size = dm.Spec.Cache.Size
		}
		storageClass = dm.Spec.Cache.StorageClassName
		if len(dm.Spec.Cache.AccessModes) > 0 {
			accessModes = dm.Spec.Cache.AccessModes
		}
	}
	return corev1.PersistentVolumeClaimSpec{
		AccessModes:      accessModes,
		StorageClassName: storageClass,
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: size},
		},
	}
}

// createRecoveryPVC creates the store PVC for a lost-store recovery, carrying the
// storeRecoveringAnnotation so the store counts as unpopulated (restart-safe)
// until a fresh prefetch completes. legacy uses the shared <dm>-store
// name and labels; otherwise the per-revision name/labels. No-op if it exists.
func (r *DecisionModelReconciler) createRecoveryPVC(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
	claim string,
	legacy bool,
) error {
	existing := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	labels := map[string]string{decisionmodelv1alpha1.LabelName: dm.Name}
	if !legacy {
		labels = revisionLabels(dm, stable.Hash)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   dm.Namespace,
			Name:        claim,
			Labels:      labels,
			Annotations: map[string]string{storeRecoveringAnnotation: annotationTrue},
		},
		Spec: storePVCSpec(dm),
	}
	if err := controllerutil.SetControllerReference(dm, pvc, r.Scheme); err != nil {
		return err
	}
	return r.createOrAdopt(ctx, dm, pvc, "PersistentVolumeClaim")
}

// soonest returns the result with the earliest non-zero RequeueAfter (zero means
// "no requeue" and never wins).
func soonest(a, b ctrl.Result) ctrl.Result {
	switch {
	case a.RequeueAfter == 0:
		return b
	case b.RequeueAfter == 0:
		return a
	case b.RequeueAfter < a.RequeueAfter:
		return b
	}
	return a
}

// reconcileCandidate drives Caching -> Starting -> Promoting -> Ready for a
// candidate revision (or Failed / RolledBack on failure).
func (r *DecisionModelReconciler) reconcileCandidate(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	params engine.Params,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	digest string,
	cacheDegraded bool,
	apiKey string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	rev := candidate.Hash
	// awaiting is true when a previous reconcile already parked exactly this
	// candidate in AwaitingPromotion (manual promotion). It must be read before
	// status.candidateRevision is overwritten below. It lets a parked candidate
	// skip re-evaluation (which would flip Evaluated/phase back and forth every
	// reconcile and re-run the eval after a manager restart).
	awaiting := manualPromotion(dm) &&
		dm.Status.Phase == decisionmodelv1alpha1.PhaseAwaitingPromotion &&
		dm.Status.CandidateRevision != nil && dm.Status.CandidateRevision.Hash == rev
	// A policy change while parked (thresholds, datasetRef, or maxCases) makes the
	// recorded result stale: leave AwaitingPromotion and re-evaluate rather than
	// promote on the old result — even if an approval annotation was set in the
	// same edit. The re-run routes through evaluateOrPromote, which
	// (for a dataset/maxCases change) uses a different evalKey and runs afresh;
	// a thresholds-only change re-applies the gates to the re-read result. The
	// approval is NOT honoured on the re-eval cycle (policyChanged); the candidate
	// re-parks under the new policy hash and the SAME approval then promotes it on
	// the next reconcile, now on the freshly recorded result.
	policyChanged := false
	if awaiting {
		if evalSpec := evaluationSpec(dm); evalSpec != nil {
			ev := dm.Status.Evaluation
			if ev == nil || ev.PolicyHash != evalPolicyHash(evalSpec) {
				awaiting = false
				policyChanged = true
			}
		}
	}
	if !awaiting {
		// A different (or no longer parked) candidate must not keep advertising
		// the previous candidate's pending approval.
		meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionPromoted)
	}
	dm.Status.CandidateRevision = candidate

	// While a candidate is being rolled out the stable revision must still be
	// maintained: restore its Deployment if deleted, apply a key
	// rotation, and regate its Pods — rendered from status.stableRevision, never
	// the live spec. The Service re-assert below keeps traffic on the stable.
	if stable := dm.Status.StableRevision; stable != nil {
		if err := r.maintainStable(ctx, dm, eng, stable, apiKey); err != nil {
			return r.finish(ctx, dm, ctrl.Result{}, err)
		}
		if err := r.ensureService(ctx, dm, eng, stable.Hash); err != nil {
			if errors.Is(err, errResourceConflict) {
				// A foreign Service holds our name: do not report Ready; the
				// candidate flow still runs but traffic cannot reach the stable.
				setStatusCondition(dm, metav1.Condition{
					Type:    decisionmodelv1alpha1.ConditionReady,
					Status:  metav1.ConditionFalse,
					Reason:  reasonResourceConflict,
					Message: fmt.Sprintf("Service %q is not owned by this DecisionModel; traffic would not reach our Pods", dm.Name),
				})
				return r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
			}
			return r.finish(ctx, dm, ctrl.Result{}, err)
		}
	}

	// GC stale revisions on the candidate path too: a quick successive
	// rollout must not leak the old stable Deployment while the new candidate is
	// still Starting. Keeps stable + this candidate; the demoted previous
	// revision only within its grace window.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}

	// 5. Prefetch Job.
	jobCreated, jobDone, jobFailed, err := r.ensurePrefetchJob(ctx, dm, eng, params, rev)
	if err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if jobCreated {
		r.event(ctx, dm, corev1.EventTypeNormal, eventPrefetchStarted,
			"prefetching %q (%s) into store", dm.Spec.Model, digest)
	}
	if jobFailed {
		return r.rollbackOrFail(ctx, dm, candidate, reasonPrefetchFailed, "prefetch Job failed")
	}
	if !jobDone {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseCaching)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionCached,
			Status:  metav1.ConditionFalse,
			Reason:  reasonCaching,
			Message: "prefetching model into store",
		})
		if r.phaseExceeded(dm, cachingTimeout(dm)) {
			return r.rollbackOrFail(ctx, dm, candidate, reasonCacheTimeout, "caching exceeded timeout")
		}
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
	}
	if !meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionCached) {
		r.event(ctx, dm, corev1.EventTypeNormal, eventCached, "model %q present in store", dm.Spec.Model)
	}
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionCached,
		Status:  metav1.ConditionTrue,
		Reason:  reasonCached,
		Message: "model present in store",
	})

	// 6. Serving Deployment.
	depExisted, err := r.deploymentExists(ctx, dm, rev)
	if err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if err := r.ensureDeployment(ctx, dm, eng, params, rev, false, apiKeyChecksum(apiKey), false); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if !depExisted {
		r.event(ctx, dm, corev1.EventTypeNormal, eventRevisionStarting, "starting revision %s", rev)
	}

	// 7. Probe candidate Pods.
	ready, precision, probeErr := r.probePods(ctx, dm, eng, candidate, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return r.finish(ctx, dm, ctrl.Result{}, probeErr)
	}
	if probeErr != nil {
		log.V(1).Info("probe error", "error", probeErr)
	}
	r.setReplicaStatus(dm, ready)

	// 8. Promote or keep starting.
	if ready >= desiredReplicas(dm) {
		if awaiting {
			return r.promoteOrAwait(ctx, dm, eng, candidate, precision, cacheDegraded, false)
		}
		return r.evaluateOrPromote(ctx, dm, eng, candidate, precision, cacheDegraded, apiKey, policyChanged)
	}
	// The parked candidate lost model-readiness: it is no longer awaiting
	// approval, it is Starting again (and the normal start timeout applies).
	meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionPromoted)
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseStarting)
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionModelReady,
		Status:  metav1.ConditionFalse,
		Reason:  reasonStarting,
		Message: fmt.Sprintf("%d/%d Pods model-ready", ready, desiredReplicas(dm)),
	})
	if r.phaseExceeded(dm, startingTimeout(dm)) {
		return r.rollbackOrFail(ctx, dm, candidate, reasonStartTimeout, "starting exceeded timeout")
	}
	return r.finish(ctx, dm, ctrl.Result{RequeueAfter: probeRequeue}, nil)
}

// promote marks the candidate as the stable revision and moves the Service to it.
//
// Order matters and is the crash-safety invariant: status.stableRevision
// is persisted FIRST, and only when that write is durable is the Service switched.
// The Service selector therefore only ever names a revision that the persisted
// status already records as stable. If the process dies, or the status write
// conflicts or fails, before the switch, the Service still points at the previous
// stable (which is still recorded as stable), and the next reconcile simply
// promotes again. The opposite order left a window in which the Service pointed at
// a revision that status did not know about; a spec change in that window started
// a new candidate whose garbage collection deleted the revision the Service was
// selecting, leaving it with no endpoints.
func (r *DecisionModelReconciler) promote(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	precision string,
	cacheDegraded bool,
) (ctrl.Result, error) {
	rev := candidate.Hash
	prevStable := dm.Status.StableRevision
	// Do not promote onto a foreign Service: traffic would go to someone else's
	// Service, not our Pods. Check through the uncached APIReader (a foreign
	// unlabelled Service is invisible to the cache) BEFORE persisting Ready.
	// The candidate keeps running; this is not a rollout failure.
	if blocked, berr := r.foreignServiceBlocks(ctx, dm); berr != nil {
		return r.finish(ctx, dm, ctrl.Result{}, berr)
	} else if blocked {
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: resourceConflictRequeue}, nil)
	}
	switching := prevStable == nil || prevStable.Hash != rev
	fromRev := "<none>"
	if prevStable != nil {
		fromRev = prevStable.Hash
	}
	candidate.Precision = precision
	dm.Status.StableRevision = candidate
	dm.Status.CandidateRevision = nil
	dm.Status.FailedRevision = nil // successful rollout clears any prior failure

	if switching {
		// Record the demoted revision so its workloads linger for promoteGrace
		// (endpoints of the new revision must populate first), then emit Promoted
		// exactly once for this transition.
		if prevStable != nil {
			now := metav1.NewTime(r.now())
			dm.Status.PreviousRevision = &decisionmodelv1alpha1.PreviousRevisionStatus{
				Hash: prevStable.Hash, PromotedAt: &now,
			}
		}
		r.event(ctx, dm, corev1.EventTypeNormal, eventPromoted, "promoted revision %s -> %s", fromRev, rev)
		bufferRollout(ctx, rolloutPromoted)
	}
	// GC now: everything except the new stable, the just-demoted previous revision
	// (protected within its grace window) and whichever revision the live Service
	// still selects (see gcRevisions): the Service has not moved yet.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseReady)
	setReadyConditions(dm, cacheDegraded)

	// Persist first. Events and metrics are flushed by this write, only on success.
	persisted, conflict, err := r.persistStatus(ctx, dm)
	if conflict {
		// Lost an optimistic-lock race: nothing was stored and the Service was not
		// touched. The next reconcile promotes again from fresh state.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !persisted {
		return ctrl.Result{}, nil
	}
	// Only now, with the new stable durably recorded, move the Service. If this
	// fails the error requeues; every reconcile path that has a stable re-asserts
	// the Service to it, so it converges.
	if err := r.ensureService(ctx, dm, eng, rev); err != nil {
		return ctrl.Result{}, err
	}
	// Requeue after the grace so the previous revision is collected even if
	// nothing else triggers a reconcile.
	if dm.Status.PreviousRevision != nil {
		return ctrl.Result{RequeueAfter: promoteGrace}, nil
	}
	return ctrl.Result{}, nil
}

// resolveDigest returns the digest to pin this reconcile to. An explicit
// spec.digest wins. Otherwise it reuses the digest already recorded for the
// same spec.model (stable or candidate) — so unrelated spec changes (replicas,
// scheduling, cache) do not re-resolve the tag and cannot silently start a new
// revision or block during a registry outage. The tag is only re-resolved
// when spec.model itself changes (no prior digest for it).
func (r *DecisionModelReconciler) resolveDigest(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
) (string, error) {
	if dm.Spec.Digest != "" {
		return dm.Spec.Digest, nil
	}
	for _, rev := range []*decisionmodelv1alpha1.RevisionStatus{dm.Status.StableRevision, dm.Status.CandidateRevision} {
		if rev != nil && rev.Model == dm.Spec.Model && rev.Digest != "" {
			return rev.Digest, nil
		}
	}
	start := time.Now()
	ref, err := eng.Resolve(ctx, dm.Spec.Model)
	observeRegistryResolve(time.Since(start))
	if err != nil {
		return "", err
	}
	return ref.Digest, nil
}

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

// errStoreTerminating reports that the revision's store PVC is being deleted and
// cannot be reused yet. It is a normal, transient condition, not a failure.
var errStoreTerminating = errors.New("model store PVC is terminating")

// errStoreLostRecovering reports that a stable revision's store PVC is missing or
// terminating, so the stable path must enter recovery (recoverStableStore) rather
// than render Pods against a missing/empty/dying volume.
var errStoreLostRecovering = errors.New("stable store lost; recovering")

// storeTerminatingRequeue is how long to wait for a terminating store PVC to go.
const storeTerminatingRequeue = 5 * time.Second

// resourceConflictRequeue backs off a reconcile that found a name collision with
// an object this DM does not own, so it does not hot-loop while it waits for the
// conflicting object to be removed or re-owned.
const resourceConflictRequeue = 30 * time.Second

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

// ensurePVC creates the per-revision model-store PVC and returns it. On
// an existing PVC it does not mutate access modes (immutable). Each revision
// owns its store (storeNameRev) so a blue-green rollout on RWO storage does not
// deadlock on Multi-Attach when the candidate lands on a different node than the
// stable. The PVC carries revisionLabels so gcRevisions can collect it by the
// same rules as the revision's Deployment/Job.
func (r *DecisionModelReconciler) ensurePVC(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: storeNameRev(dm, rev)}
	err := r.Get(ctx, key, pvc)
	if err == nil {
		if !ownedBy(pvc, dm) {
			return nil, r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
		}
		// A claim that is being deleted must not be reused: Pods would mount a
		// volume that is about to disappear, and the prefetch would write into it.
		// This happens after fast churn (a revision is recreated while its store from
		// the previous round is still terminating). Wait until it is gone, then
		// create a fresh one.
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreTerminating
		}
		return pvc, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      storeNameRev(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		Spec: storePVCSpec(dm),
	}
	if err := controllerutil.SetControllerReference(dm, pvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.createOrAdopt(ctx, dm, pvc, "PersistentVolumeClaim"); err != nil {
		return nil, err
	}
	return pvc, nil
}

// storeClaimForStable resolves the store PVC claim name a recorded stable
// revision should mount. A legacy stable (promoted before per-revision stores) still mounts
// the shared <dm>-store; its live Deployment is authoritative, so we read the
// claim from it and keep using it until the next promotion (legacy store migration).
// Otherwise the per-revision PVC storeNameRev(dm, rev.Hash) is used. The second
// return reports whether the resolved claim is the legacy shared PVC.
func (r *DecisionModelReconciler) storeClaimForStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev *decisionmodelv1alpha1.RevisionStatus,
) (claimName string, legacy bool) {
	perRev := storeNameRev(dm, rev.Hash)
	dep := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev.Hash)}
	if err := r.Get(ctx, key, dep); err == nil {
		if live := claimNameFromPodSpec(&dep.Spec.Template.Spec); live != "" {
			return live, live == storeName(dm)
		}
	}
	// The live Deployment is gone (e.g. deleted out of band, or lost before a
	// restart). Do not blindly fall back to a fresh per-revision PVC: a legacy
	// stable's populated shared <dm>-store may still exist, and switching to a new
	// empty per-revision claim would strand the full volume and serve from an
	// empty one. If the legacy shared store exists and is ours, keep
	// using it; otherwise use the per-revision claim.
	legacyPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: storeName(dm)}, legacyPVC); err == nil &&
		ownedBy(legacyPVC, dm) && legacyPVC.DeletionTimestamp == nil {
		return storeName(dm), true
	}
	return perRev, false
}

// claimNameFromPodSpec returns the ClaimName of the first PVC-backed volume in a
// PodSpec (the model store), or "" if none. Used to read a live Deployment's
// store claim for legacy migration.
func claimNameFromPodSpec(spec *corev1.PodSpec) string {
	for i := range spec.Volumes {
		if src := spec.Volumes[i].PersistentVolumeClaim; src != nil {
			return src.ClaimName
		}
	}
	return ""
}

// ensureStoreForStable returns the store PVC the stable revision uses, for the
// cache-sharing guard. It never recreates a missing store here: a lost stable
// store must go through recoverStableStore (annotated create + prefetch) so the
// stable is not served from an empty volume. It therefore returns
// errStoreLostRecovering when the stable store PVC is missing or terminating —
// both for a per-revision PVC and for a legacy shared store — and the caller
// routes straight to reconcileStablePath. For a healthy legacy stable it Gets
// the shared <dm>-store (never recreates it); otherwise it ensures the stable
// revision's own per-revision PVC exists.
func (r *DecisionModelReconciler) ensureStoreForStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	stable *decisionmodelv1alpha1.RevisionStatus,
) (*corev1.PersistentVolumeClaim, error) {
	claim, legacy := r.storeClaimForStable(ctx, dm, stable)
	if legacy {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				// Legacy shared store gone: recover it (recoverStableStore recreates
				// the shared PVC annotated and reprefetches) instead of returning a
				// bare NotFound before the stable path.
				return nil, errStoreLostRecovering
			}
			return nil, err
		}
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreLostRecovering
		}
		return pvc, nil
	}
	// Per-revision stable store. A missing or terminating PVC is not re-created
	// here (that would be an unannotated, empty volume); hand it to recovery.
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: storeNameRev(dm, stable.Hash)}, pvc)
	switch {
	case err == nil:
		if !ownedBy(pvc, dm) {
			return nil, r.conflictIfNotOwned(ctx, dm, pvc, "PersistentVolumeClaim")
		}
		if pvc.DeletionTimestamp != nil {
			return nil, errStoreLostRecovering
		}
		return pvc, nil
	case apierrors.IsNotFound(err):
		return nil, errStoreLostRecovering
	default:
		return nil, err
	}
}

// guardCacheSharing sets the Degraded condition when the model-store PVC cannot
// be shared across the requested replicas, or when the spec asks to change an
// immutable PVC's access modes/storage class, and applies an allowed size grow.
// It never blocks progress for a policy reason (single-node works). Returns
// (true, nil) when it set Degraded=True, and (false, err) for a transient error
// (e.g. an expansion Patch that failed for a non-policy reason) so the caller
// backs off via the workqueue instead of parking a misleading condition.
func (r *DecisionModelReconciler) guardCacheSharing(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pvc *corev1.PersistentVolumeClaim,
) (bool, error) {
	if dm.Spec.Cache != nil {
		// StorageClassName and access modes are immutable on an existing PVC.
		if dm.Spec.Cache.StorageClassName != nil &&
			!storageClassEqual(dm.Spec.Cache.StorageClassName, pvc.Spec.StorageClassName) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: "spec.cache.storageClassName differs from the existing PVC; a PVC's " +
					"storage class is immutable. Delete the PVC to apply a new storage class.",
			})
			return true, nil
		}
		if len(dm.Spec.Cache.AccessModes) > 0 &&
			!accessModesEqual(dm.Spec.Cache.AccessModes, pvc.Spec.AccessModes) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: "spec.cache.accessModes differs from the existing PVC; PVC access modes are " +
					"immutable and will not be changed. Delete the PVC to apply new access modes.",
			})
			return true, nil
		}
		// Size: grow via an expansion Patch (an API Forbidden/Invalid rejection ->
		// CacheSpecImmutable); a shrink is CacheSpecImmutable; a transient Patch
		// error is returned so the caller backs off.
		degraded, serr := r.guardCacheSize(ctx, dm, pvc)
		if serr != nil {
			return false, serr
		}
		if degraded {
			return true, nil
		}
	}

	if desiredReplicas(dm) > 1 && !accessModesShareable(pvc.Spec.AccessModes) {
		setStatusCondition(dm, metav1.Condition{
			Type:   decisionmodelv1alpha1.ConditionDegraded,
			Status: metav1.ConditionTrue,
			Reason: reasonCacheNotShareable,
			Message: "replicas>1 but the model-store PVC is not ReadWriteMany/ReadOnlyMany; " +
				"Pods on different nodes will be stuck in ContainerCreating (Multi-Attach). " +
				"Set spec.cache.accessModes to a shared mode with RWX-capable storage.",
		})
		return true, nil
	}
	return false, nil
}

// guardCacheSize reconciles a spec.cache.size change against an existing PVC: a
// grow is attempted as a volume expansion patch, and the API server's own
// rejection (the StorageClass forbids expansion, or the request is otherwise
// Invalid/Forbidden) is surfaced as CacheSpecImmutable. A shrink is rejected
// without a call. A transient Patch error (anything other than Forbidden/Invalid)
// is returned so the caller backs off via the workqueue instead of parking a
// misleading immutable condition. This never reads the
// (cluster-scoped) StorageClass, so it works unchanged in namespace-scoped mode.
// Returns (true, nil) when it set Degraded.
func (r *DecisionModelReconciler) guardCacheSize(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pvc *corev1.PersistentVolumeClaim,
) (bool, error) {
	want := dm.Spec.Cache.Size
	if want.IsZero() {
		return false, nil
	}
	have := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	cmp := want.Cmp(have)
	if cmp == 0 {
		return false, nil
	}
	if cmp < 0 {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonCacheSpecImmutable,
			Message: "spec.cache.size is smaller than the existing PVC; a PVC cannot be shrunk.",
		})
		return true, nil
	}
	// Grow: attempt the expansion patch and let the API server decide. A
	// StorageClass that forbids expansion rejects it with Forbidden/Invalid.
	patched := pvc.DeepCopy()
	if patched.Spec.Resources.Requests == nil {
		patched.Spec.Resources.Requests = corev1.ResourceList{}
	}
	patched.Spec.Resources.Requests[corev1.ResourceStorage] = want
	if err := r.Patch(ctx, patched, client.MergeFrom(pvc)); err != nil {
		if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
			setStatusCondition(dm, metav1.Condition{
				Type:   decisionmodelv1alpha1.ConditionDegraded,
				Status: metav1.ConditionTrue,
				Reason: reasonCacheSpecImmutable,
				Message: fmt.Sprintf("spec.cache.size increase was rejected (the StorageClass may not "+
					"allow volume expansion); the PVC size is unchanged: %v", err),
			})
			return true, nil
		}
		// A transient error (conflict, server unavailable, ...): return it so the
		// workqueue retries with backoff; do not misreport it as immutable.
		return false, fmt.Errorf("expanding store PVC %q: %w", pvc.Name, err)
	}
	return false, nil
}

// ensurePrefetchJob creates the prefetch Job for a revision and reports its state.
func (r *DecisionModelReconciler) ensurePrefetchJob(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	params engine.Params,
	rev string,
) (created, done, failed bool, err error) {
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}
	getErr := r.Get(ctx, key, job)
	if getErr == nil {
		if !ownedBy(job, dm) {
			return false, false, false, r.conflictIfNotOwned(ctx, dm, job, "Job")
		}
		if job.DeletionTimestamp != nil {
			// Being deleted (e.g. a lost-store recovery is replacing it): report it
			// as neither done nor failed so the caller waits and recreates it once
			// it is gone.
			return false, false, false, nil
		}
		return false, jobComplete(job), jobFailed(job), nil
	}
	if !apierrors.IsNotFound(getErr) {
		return false, false, false, getErr
	}

	// Prefetch only needs to pull weights, never to run inference, so it must not
	// use the resolved serving image. For device: cuda that image is the ~1.7 GB
	// :0.7.3-cuda tag, which forces the prefetch Pod onto a CUDA-capable node and
	// adds a long pull (seen on EKS). Pass only the user's explicit spec.image
	// override; with none, the engine picks its CPU default. The
	// serving Deployment still renders from the resolved image in params.
	prefetchParams := params
	prefetchParams.Image = dm.Spec.Image

	job = &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dm.Namespace,
			Name:      prefetchName(dm, rev),
			Labels:    revisionLabels(dm, rev),
		},
		Spec: eng.PrefetchJobSpec(prefetchParams),
	}
	// The prefetch Pod must land where serving Pods may run: on tainted /
	// dedicated (e.g. GPU) pools, and — with WaitForFirstConsumer storage — it
	// is the first consumer that pins the PVC's zone.
	applyScheduling(&job.Spec.Template.Spec, dm.Spec.Scheduling)
	applyProxyEnv(&job.Spec.Template.Spec, r.PrefetchProxyEnv)
	// A large model needs a longer pull than the engine's default Job deadline:
	// follow an explicit spec.rollout.timeouts.caching. Left alone when
	// unset so the engine's own default stays in force.
	if t := rolloutTimeouts(dm); t != nil && t.Caching != nil {
		secs := int64(t.Caching.Duration / time.Second)
		job.Spec.ActiveDeadlineSeconds = &secs
	}
	applySELinuxLevel(&job.Spec.Template.Spec, dm)
	if err := controllerutil.SetControllerReference(dm, job, r.Scheme); err != nil {
		return false, false, false, err
	}
	if err := r.createOrAdopt(ctx, dm, job, "Job"); err != nil {
		return false, false, false, err
	}
	return true, false, false, nil
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

// storeAccessModes returns the access modes of the store claim a Deployment
// mounts: the PVC's own (they are immutable and authoritative), else what the
// spec asks for, else ReadWriteOnce (the default the PVC is created with).
func (r *DecisionModelReconciler) storeAccessModes(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	claim string,
) []corev1.PersistentVolumeAccessMode {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: claim}, pvc); err == nil &&
		len(pvc.Spec.AccessModes) > 0 {
		return pvc.Spec.AccessModes
	}
	if dm.Spec.Cache != nil && len(dm.Spec.Cache.AccessModes) > 0 {
		return dm.Spec.Cache.AccessModes
	}
	return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
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

// probePods probes candidate Pods and patches their readiness gate condition.
// It returns the number of Pods with the gate True, the observed precision, and
// the last probe transport error (if any).
func (r *DecisionModelReconciler) probePods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	rev *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) (ready int32, precision string, probeErr error) {
	pods, err := r.revisionPods(ctx, dm, rev.Hash)
	if err != nil {
		return 0, "", err
	}
	prober := r.prober()
	reinsp, canReinspect := r.canReinspect()
	// Bound each background warmup by spec.rollout.timeouts.starting when set.
	probeCtx := withWarmupTimeout(ctx, dm)

	var counts probeCounts

	for i := range pods {
		pod := &pods[i]
		// A terminating Pod never counts and is not probed: it is on its way out
		// and must not be promoted onto.
		if pod.DeletionTimestamp != nil {
			continue
		}
		// A Pod already gated True is only probed again when needed:
		//  - its containers restarted and became ready again since it was last
		//    probed (the pinned model may be gone): full Probe, which also warms up;
		//  - or regateInterval elapsed: an Inspect-only re-check, which catches a
		//    model the runtime lost without a container restart.
		// Otherwise it only has to still be serving-ready to be counted.
		recheck := false
		if gateTrue(pod) {
			if !r.restartedSinceProbe(pod) {
				if canReinspect && r.regateDue(pod.UID, r.now()) {
					recheck = true
				} else {
					if servingReady(pod) {
						ready++
						if pr := r.knownPrecision(pod.UID); pr != "" {
							precision = pr
						}
					}
					continue
				}
			}
		} else if pod.Status.PodIP == "" || !containersReady(pod) {
			continue
		}
		// The prober warms the model asynchronously (keyed by Pod UID) and does a
		// short synchronous inspect; while warmup is still running it returns
		// errWarmupInProgress, which is neither ready nor a mismatch.
		var (
			loaded engine.Loaded
			perr   error
		)
		if recheck {
			loaded, perr = reinsp.Reinspect(probeCtx, pod, eng, apiKey, rev.Model)
			r.regateMark(pod.UID, r.now())
		} else {
			loaded, perr = prober.Probe(probeCtx, pod, eng, apiKey, rev.Model)
		}
		if errors.Is(perr, errWarmupInProgress) {
			continue
		}
		if perr != nil {
			probeErr = perr
			// A re-inspect of an already-healthy (gate True, PodReady) Pod that
			// hits a *transport/auth* error — e.g. a 401 in the window right after
			// an API key rotation, before the new-key Pods are up — must NOT flip
			// the Pod out of the Service on the first error: that would drop all
			// serving capacity on a transient blip. Keep it counted for a bounded
			// number of consecutive re-inspect failures; past that, a stuck/hung or
			// mis-keyed runtime is flipped False. A definitive model
			// loss (errModelNotLoaded) flips the gate False at once, and a
			// first-time probe error (gate not yet True) is handled as before.
			if recheck && gateTrue(pod) && !errors.Is(perr, errModelNotLoaded) &&
				r.transientReinspectFail(pod.UID) <= maxTransientReinspectFailures {
				if servingReady(pod) {
					ready++
					if pr := r.knownPrecision(pod.UID); pr != "" {
						precision = pr
					}
				}
				continue
			}
			if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonProbeError, perr.Error()); transitioned {
				counts.nErr++
				r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
					"pod %s probe error: %s", pod.Name, perr.Error())
			}
			r.clearTransientReinspectFail(pod.UID)
			continue
		}
		// A conclusive probe/re-inspect succeeded: clear any transient-failure run.
		r.clearTransientReinspectFail(pod.UID)
		// The restart (if any) has been looked at conclusively: stop re-probing it.
		r.markHandled(pod, loaded.Precision)
		if pr := r.applyProbeResult(ctx, dm, pod, rev, loaded, recheck, &counts); pr != "" {
			precision = pr
		}
		if counts.lastReady {
			ready++
		}
	}
	// Count probe outcomes only on a gate transition, so a steady-state Pod is
	// not re-counted on every requeue. Buffered to flush after the status write.
	bufferProbeResult(ctx, probeReady, counts.nReady)
	bufferProbeResult(ctx, probeDigestMismatch, counts.nDigest)
	bufferProbeResult(ctx, probeDeviceMismatch, counts.nDevice)
	bufferProbeResult(ctx, probeNotPinned, counts.nPin)
	bufferProbeResult(ctx, probeError, counts.nErr)
	return ready, precision, probeErr
}

// probeCounts accumulates per-reconcile probe outcome counters.
type probeCounts struct {
	nReady, nDigest, nDevice, nPin, nErr int
	lastReady                            bool // set by applyProbeResult to add one ready
}

// applyProbeResult patches the readiness gate for one successfully-probed Pod
// based on digest/device/pin match, and records the outcome counters. It returns
// a non-empty precision when the Pod matched (to surface to status) and whether
// the Pod should be counted ready this reconcile (via counts.lastReady, read by
// the caller). Extracted from probePods to keep its cyclomatic complexity in
// check.
func (r *DecisionModelReconciler) applyProbeResult(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	pod *corev1.Pod,
	rev *decisionmodelv1alpha1.RevisionStatus,
	loaded engine.Loaded,
	recheck bool,
	counts *probeCounts,
) (precision string) {
	counts.lastReady = false
	switch {
	case loaded.Digest != rev.Digest:
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonDigestMismatch,
			fmt.Sprintf("loaded digest %s != expected %s", loaded.Digest, rev.Digest)); transitioned {
			counts.nDigest++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (loaded %s, expected %s)", pod.Name, reasonDigestMismatch, loaded.Digest, rev.Digest)
		}
	case loaded.Device != rev.Device:
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonDeviceMismatch,
			fmt.Sprintf("loaded on device %s, expected %s", loaded.Device, rev.Device)); transitioned {
			counts.nDevice++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (loaded %s, expected %s)", pod.Name, reasonDeviceMismatch, loaded.Device, rev.Device)
		}
	case !loaded.Pinned:
		// The right model on the right device, but the runtime may evict it
		// (keep_alive expiry): it would silently stop serving. Not ready.
		if transitioned, _ := r.patchGate(ctx, pod, metav1.ConditionFalse, reasonNotPinned,
			"model is loaded but not pinned (keep_alive); the runtime may evict it"); transitioned {
			counts.nPin++
			r.event(ctx, dm, corev1.EventTypeWarning, eventProbeMismatch,
				"pod %s: %s (model %s is not pinned in the runtime)", pod.Name, reasonNotPinned, rev.Model)
		}
	default:
		if recheck {
			// Still loaded, on the expected device, pinned: the gate is already
			// True, so write nothing (no Pod patch, no status churn).
			if servingReady(pod) {
				counts.lastReady = true
			}
			return loaded.Precision
		}
		// Set the gate, but do not count the Pod in this same reconcile: the
		// Service must only move to Pods that Kubernetes already reports Ready
		// (and that are therefore in the EndpointSlice). PodReady follows the
		// gate asynchronously; the Pod update event brings the next reconcile.
		// An already-True gate (a probe after a restart) counts if PodReady holds.
		transitioned, ok := r.patchGate(ctx, pod, metav1.ConditionTrue, reasonModelReady, "model loaded on expected device")
		if !ok {
			return loaded.Precision
		}
		r.regateMark(pod.UID, r.now())
		if transitioned {
			counts.nReady++
		}
		if !transitioned && servingReady(pod) {
			counts.lastReady = true
		}
		return loaded.Precision
	}
	return ""
}

// afterGatePatch, when non-nil, is called with the Pod after a successful
// model-ready gate patch. It is a seam for tests only: envtest has no kubelet, and
// the kubelet is what sets Pod condition Ready=True once ContainersReady and every
// readiness gate are True. Production never sets it.
var afterGatePatch func(ctx context.Context, c client.Client, pod *corev1.Pod)

// patchGate sets the model-ready Pod condition via a status merge-patch of just
// that condition (not a full-object update, avoiding conflicts with the
// kubelet). It returns whether the status transitioned and whether the write
// succeeded; callers count a Pod ready only when the gate write succeeds.
func (r *DecisionModelReconciler) patchGate(
	ctx context.Context,
	pod *corev1.Pod,
	status metav1.ConditionStatus,
	reason, message string,
) (transitioned, ok bool) {
	log := logf.FromContext(ctx)
	transitioned = !podConditionHasStatus(pod, corev1.ConditionStatus(status))
	base := pod.DeepCopy()
	cond := corev1.PodCondition{
		Type:               corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate),
		Status:             corev1.ConditionStatus(status),
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(r.now()),
	}
	setPodCondition(pod, cond)
	// A JSON merge patch (client.MergeFrom) would send the whole status.conditions
	// array and could revert a condition the kubelet set between our Get and this
	// Patch (e.g. Ready/ContainersReady). Pod conditions have a strategic-merge
	// key of `type`, so a strategic merge patches only the ModelReady gate by type
	// and leaves the kubelet's conditions untouched.
	if err := r.Status().Patch(ctx, pod, client.StrategicMergeFrom(base)); err != nil {
		log.V(1).Info("failed to patch pod readiness gate", "pod", pod.Name, "error", err)
		return transitioned, false
	}
	if afterGatePatch != nil && status == metav1.ConditionTrue {
		afterGatePatch(ctx, r.Client, pod)
	}
	return transitioned, true
}

// reconcileStable keeps the stable revision's workloads in sync and re-probes
// its Pods without touching candidate/failed state. Returns model-ready count.
func (r *DecisionModelReconciler) reconcileStable(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	apiKey string,
) (int32, error) {
	log := logf.FromContext(ctx)
	// Render the stable revision from its own recorded fields, not
	// the live spec which describes the failed candidate.
	claim, _ := r.storeClaimForStable(ctx, dm, stable)
	params := r.stableParams(ctx, dm, stable, claim)
	if err := r.ensureDeployment(ctx, dm, eng, params, stable.Hash, true, apiKeyChecksum(apiKey), false); err != nil {
		return 0, err
	}
	if err := r.ensureService(ctx, dm, eng, stable.Hash); err != nil {
		return 0, err
	}
	if err := r.ensurePDB(ctx, dm, stable.Hash); err != nil {
		return 0, err
	}
	ready, precision, probeErr := r.probePods(ctx, dm, eng, stable, apiKey)
	if errors.Is(probeErr, errPodListFailed) {
		return 0, probeErr
	}
	if probeErr != nil {
		// Probe transport errors are non-fatal; a mismatch sets the gate False.
		log.V(1).Info("probe error (stable)", "error", probeErr)
	}
	if precision != "" {
		stable.Precision = precision
	}
	return ready, nil
}

// revisionPods lists Pods for a DecisionModel revision.
func (r *DecisionModelReconciler) revisionPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(dm.Namespace),
		client.MatchingLabels{
			decisionmodelv1alpha1.LabelName:     dm.Name,
			decisionmodelv1alpha1.LabelRevision: rev,
		},
	); err != nil {
		return nil, fmt.Errorf("%w: %v", errPodListFailed, err)
	}
	return r.maybeFilterOwnedPods(ctx, dm, rev, list.Items)
}

// enforcePodOwnership gates the controller-chain ownership check in
// revisionPods. It defaults to true (production always enforces); the envtest
// suite relaxes it for legacy fixtures that create bare labelled Pods, and the
// ownership specs switch it back on. This mirrors the kubelet seam used
// for.
var enforcePodOwnership atomic.Bool

func init() { enforcePodOwnership.Store(true) }

// maybeFilterOwnedPods applies filterOwnedPods only when ownership enforcement
// is on; otherwise it trusts the label selector (envtest legacy fixtures).
func (r *DecisionModelReconciler) maybeFilterOwnedPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
	pods []corev1.Pod,
) ([]corev1.Pod, error) {
	if !enforcePodOwnership.Load() {
		return pods, nil
	}
	return r.filterOwnedPods(ctx, dm, rev, pods)
}

// filterOwnedPods keeps only Pods whose controller chain leads to this DM's
// revision Deployment: Pod -> ReplicaSet (controlled by the expected Deployment
// UID) -> Deployment (controlled by dm.UID). Labels are attacker-controllable
// (anyone who can create Pods in the namespace can set them and, with them, make
// the operator send the API key to a foreign Pod), so selection by label alone
// is not enough. The namespace is the real trust boundary — a Pod creator can
// already mount any Secret in it — so this is defence in depth, not a boundary
// fix.
//
// A genuine lookup error (a non-NotFound Get on the Deployment or a ReplicaSet)
// is returned (wrapped as errPodListFailed) so the caller aborts and the
// workqueue backs off — it must NOT be mistaken for "0 owned Pods", which would
// wrongly flip Degraded / trip progress timeouts. A NotFound (the
// Deployment or RS is simply gone) legitimately drops the Pod(s).
func (r *DecisionModelReconciler) filterOwnedPods(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
	pods []corev1.Pod,
) ([]corev1.Pod, error) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: revisionName(dm, rev)}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil // no Deployment => no owned Pods (not an error)
		}
		return nil, fmt.Errorf("%w: get Deployment: %v", errPodListFailed, err)
	}
	if c := metav1.GetControllerOf(&dep); c == nil || c.UID != dm.UID {
		return nil, nil
	}
	rsOK := map[types.UID]bool{}
	owned := pods[:0]
	for i := range pods {
		pod := &pods[i]
		rsRef := metav1.GetControllerOf(pod)
		if rsRef == nil || rsRef.Kind != "ReplicaSet" {
			continue
		}
		ok, seen := rsOK[rsRef.UID]
		if !seen {
			var rs appsv1.ReplicaSet
			if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: rsRef.Name}, &rs); err != nil {
				if !apierrors.IsNotFound(err) {
					// A transient RS read failure must abort, not silently drop the
					// Pod (which would look like "0 Pods").
					return nil, fmt.Errorf("%w: get ReplicaSet %s: %v", errPodListFailed, rsRef.Name, err)
				}
				rsOK[rsRef.UID] = false
			} else {
				// The fetched RS must be exactly the one the Pod references (a
				// name can be reused with a different UID) and controlled by this
				// revision's Deployment.
				rc := metav1.GetControllerOf(&rs)
				rsOK[rsRef.UID] = rs.UID == rsRef.UID && rc != nil && rc.UID == dep.UID
			}
			ok = rsOK[rsRef.UID]
		}
		if ok {
			owned = append(owned, *pod)
		}
	}
	return owned, nil
}

// garbageCollectRevisions deletes Deployments and Jobs of other revisions.
// gcRevisions deletes Deployments and Jobs of revisions that are neither the
// current stable nor the current candidate. The single revision demoted by the
// most recent promotion (status.previousRevision) is spared for promoteGrace
// after its promotedAt so the new revision's endpoints can populate; it never
// blocks GC of any other stale revision.
func (r *DecisionModelReconciler) gcRevisions(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) error {
	keep := map[string]struct{}{}
	if dm.Status.StableRevision != nil {
		keep[dm.Status.StableRevision.Hash] = struct{}{}
	}
	if dm.Status.CandidateRevision != nil {
		keep[dm.Status.CandidateRevision.Hash] = struct{}{}
	}
	// The just-demoted previous revision lingers only within the grace window.
	prevHash := ""
	if p := dm.Status.PreviousRevision; p != nil && p.Hash != "" {
		within := p.PromotedAt != nil && r.now().Sub(p.PromotedAt.Time) < promoteGrace
		if within {
			keep[p.Hash] = struct{}{}
			prevHash = p.Hash
		}
	}

	// Never delete the revision the live Service is selecting, whatever status
	// says: it is what clients are being served from right now. Read from the
	// Service itself so it holds even if a code path forgot to keep status and
	// Service in step.
	svc := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: dm.Name}, svc); err == nil {
		if rev := svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]; rev != "" {
			keep[rev] = struct{}{}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	shouldDelete := func(rev string) bool {
		if rev == "" {
			return false
		}
		_, kept := keep[rev]
		return !kept
	}

	// Delete owned Deployments, Jobs, per-revision store PVCs and PDBs whose
	// revision is no longer kept. Jobs need background
	// propagation so their Pods are collected; the rest use foreground defaults.
	jobPolicy := metav1.DeletePropagationBackground
	if err := r.gcStaleByRevision(ctx, dm, &appsv1.DeploymentList{}, shouldDelete); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &batchv1.JobList{}, shouldDelete,
		&client.DeleteOptions{PropagationPolicy: &jobPolicy}); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &corev1.PersistentVolumeClaimList{}, shouldDelete); err != nil {
		return err
	}
	if err := r.gcStaleByRevision(ctx, dm, &policyv1.PodDisruptionBudgetList{}, shouldDelete); err != nil {
		return err
	}

	// Legacy migration: the shared <dm>-store PVC carries only LabelName
	// (no revision label), so the loop above never selects it. Delete it once no
	// live Deployment of this DM still references it (i.e. the legacy stable has
	// been superseded by a per-revision revision). Never delete it while a
	// workload still mounts it.
	if err := r.gcLegacyStore(ctx, dm); err != nil {
		return err
	}

	// Once the previous revision's grace has elapsed and it has been removed,
	// clear it so it no longer appears in status.
	if dm.Status.PreviousRevision != nil && prevHash == "" {
		dm.Status.PreviousRevision = nil
	}
	return nil
}

// gcStaleByRevision lists the owned objects of one kind for a DecisionModel and
// deletes those whose decisionmodel.io/revision label is no longer kept. It
// unifies the per-kind GC loops (Deployments, Jobs, PVCs, PDBs) so gcRevisions
// stays simple. A missing object on delete is ignored (already collected).
func (r *DecisionModelReconciler) gcStaleByRevision(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	list client.ObjectList,
	shouldDelete func(rev string) bool,
	deleteOpts ...client.DeleteOption,
) error {
	if err := r.List(ctx, list, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return err
	}
	var delErr error
	if err := meta.EachListItem(list, func(o runtime.Object) error {
		obj, ok := o.(client.Object)
		if !ok {
			return nil
		}
		// Defence in depth: only delete objects this DM actually owns.
		// Labels alone are attacker-settable, so a foreign object with matching
		// labels (same name/revision) must survive GC; delete only when the
		// controller OwnerReference UID is this DM's.
		if c := metav1.GetControllerOf(obj); c == nil || c.UID != dm.UID {
			return nil
		}
		if !shouldDelete(obj.GetLabels()[decisionmodelv1alpha1.LabelRevision]) {
			return nil
		}
		if err := r.Delete(ctx, obj, deleteOpts...); err != nil && !apierrors.IsNotFound(err) {
			delErr = err
		}
		return nil
	}); err != nil {
		return err
	}
	return delErr
}

// gcLegacyStore deletes the legacy shared <dm>-store PVC once no live Deployment
// of this DM still mounts it (legacy store migration). It re-lists Deployments so a
// Deployment just deleted earlier in the same gcRevisions pass is not counted as
// a live reference. It is a no-op when the PVC does not exist or is still
// referenced.
func (r *DecisionModelReconciler) gcLegacyStore(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) error {
	legacy := storeName(dm)
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: legacy}, pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pvc.DeletionTimestamp != nil {
		return nil // already being deleted
	}
	// Only ever delete the shared store if this DM owns it. The operator always set a
	// controller OwnerReference on <dm>-store, so a missing/foreign owner means
	// the PVC is not ours — never delete it by name alone.
	if !ownedBy(pvc, dm) {
		return nil
	}
	var deps appsv1.DeploymentList
	if err := r.List(ctx, &deps, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return err
	}
	for i := range deps.Items {
		if deps.Items[i].DeletionTimestamp != nil {
			continue // being deleted, not a live reference
		}
		if claimNameFromPodSpec(&deps.Items[i].Spec.Template.Spec) == legacy {
			return nil
		}
	}
	if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteRevisionWorkloads removes the Deployment and Job of a specific revision
// (used on rollback of a failed candidate).
func (r *DecisionModelReconciler) deleteRevisionWorkloads(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	rev string,
) error {
	policy := metav1.DeletePropagationBackground
	// Each object is deleted only if it exists AND is controlled by this DM: a
	// name collision with a user's object must never be deleted.
	if err := r.deleteIfOwned(ctx, dm,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: revisionName(dm, rev)}}); err != nil {
		return err
	}
	if err := r.deleteIfOwned(ctx, dm,
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: prefetchName(dm, rev)}},
		&client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return err
	}
	// A failed revision's per-revision store PVC is deleted with its workloads.
	// The legacy shared <dm>-store is never per-revision, so this only
	// removes storeNameRev; the live Pod spec no longer references it after the
	// Deployment delete above.
	if err := r.deleteIfOwned(ctx, dm,
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: storeNameRev(dm, rev)}}); err != nil {
		return err
	}
	// A failed candidate never gets a PDB (only the stable revision does), but
	// delete any that might exist for symmetry with the other owned objects.
	if err := r.deleteIfOwned(ctx, dm,
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Namespace: dm.Namespace, Name: pdbName(dm, rev)}}); err != nil {
		return err
	}
	return nil
}

// deleteIfOwned Gets the object by its name/namespace and deletes it only when
// it exists and is controlled by this DM. A NotFound is ignored (already gone);
// a foreign object with the same name is left untouched.
func (r *DecisionModelReconciler) deleteIfOwned(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	obj client.Object,
	opts ...client.DeleteOption,
) error {
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !ownedBy(obj, dm) {
		return nil
	}
	if err := r.Delete(ctx, obj, opts...); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// rollbackOrFail handles a failed candidate: RolledBack if a stable revision
// exists (which keeps serving), else Failed. The failed revision is recorded so
// it is not automatically retried; a spec change (new hash) clears it.
func (r *DecisionModelReconciler) rollbackOrFail(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	failed *decisionmodelv1alpha1.RevisionStatus,
	reason, message string,
) (ctrl.Result, error) {
	if err := r.deleteRevisionWorkloads(ctx, dm, failed.Hash); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	dm.Status.CandidateRevision = nil
	dm.Status.FailedRevision = failed
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message,
	})
	if dm.Status.StableRevision != nil {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseRolledBack)
		r.event(ctx, dm, corev1.EventTypeWarning, eventRolledBack,
			"revision %s rolled back (%s): %s", failed.Hash, reason, message)
		bufferRollout(ctx, rolloutRolledBack)
	} else {
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseFailed)
		r.event(ctx, dm, corev1.EventTypeWarning, eventFailed,
			"revision %s failed (%s): %s", failed.Hash, reason, message)
		bufferRollout(ctx, rolloutFailed)
	}
	return r.finish(ctx, dm, ctrl.Result{}, nil)
}

// permanentReasons are failure reasons that retrying the same spec cannot fix;
// after the status write the reconciler returns a reconcile.TerminalError so the
// queue does not keep retrying.
var permanentReasons = map[string]struct{}{
	reasonInvalidModelName:        {},
	reasonRegistryNotAllowed:      {},
	reasonImageOverrideNotAllowed: {},
	// reasonSecretNotAllowed is intentionally NOT terminal: there is no Secret
	// watch, so adding the opt-in label must be picked up by a requeue rather than
	// wedged by a TerminalError. The API-key path requeues at the regate interval;
	// RegistryNotAllowed / ImageOverrideNotAllowed stay terminal (they need a spec
	// change, which retriggers).
	reasonDatasetInvalid:        {},
	reasonEvaluationUnsupported: {},
}

// rollbackOrFailPermanent is rollbackOrFail for a permanent failure: after the
// status write it returns a TerminalError so the workqueue stops retrying.
func (r *DecisionModelReconciler) rollbackOrFailPermanent(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	failed *decisionmodelv1alpha1.RevisionStatus,
	reason, message string,
) (ctrl.Result, error) {
	res, err := r.rollbackOrFail(ctx, dm, failed, reason, message)
	if err != nil {
		return res, err // status write failed; let the caller requeue
	}
	if _, ok := permanentReasons[reason]; ok {
		return res, reconcile.TerminalError(fmt.Errorf("%s: %s", reason, message))
	}
	return res, nil
}

// apiKey reads the engine API key from the referenced Secret, if any.
func (r *DecisionModelReconciler) apiKey(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) (string, error) {
	if dm.Spec.Auth == nil || dm.Spec.Auth.APIKeySecretRef == nil {
		return "", nil
	}
	ref := dm.Spec.Auth.APIKeySecretRef
	rdr, err := r.reader()
	if err != nil {
		return "", err
	}
	var secret corev1.Secret
	if err := rdr.Get(ctx, types.NamespacedName{Namespace: dm.Namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) && ref.Optional != nil && *ref.Optional {
			return "", nil
		}
		return "", err
	}
	if err := requireAPIKeyLabel(secret.Labels); err != nil {
		return "", err
	}
	return string(secret.Data[ref.Key]), nil
}

// finish updates status (with observedGeneration + endpoint) and returns.
// reconcileState is per-reconcile context carrying the status-patch merge base
// and buffered Events (flushed only after a successful status write, so a
// conflicting/failed write never emits duplicate Events —.
type reconcileState struct {
	base   *decisionmodelv1alpha1.DecisionModel
	events []bufferedEvent
	// metrics are counter increments / histogram observations accumulated during
	// the reconcile and flushed only after a successful status write, so a
	// conflicting/failed write never double-counts (same rule as Events).
	metrics []bufferedMetric
}

type bufferedEvent struct {
	eventType, reason, note string
	args                    []interface{}
}

// bufferedMetric is a pending metric mutation. Exactly one of the counter/
// histogram roles applies, selected by kind.
type bufferedMetric struct {
	kind   metricKind
	label  string        // result label (rollout/probe) or phase (duration)
	amount float64       // counter increment (default 1)
	dur    time.Duration // observation for phase-duration histogram
}

type metricKind int

const (
	metricKindRollout metricKind = iota
	metricKindProbe
	metricKindPhaseDuration
)

type reconcileStateKey struct{}

// withPatchBase returns a context carrying base as the status-patch merge base
// and an empty Event buffer.
func withPatchBase(ctx context.Context, base *decisionmodelv1alpha1.DecisionModel) context.Context {
	return context.WithValue(ctx, reconcileStateKey{}, &reconcileState{base: base})
}

// reconcileStateFrom returns the per-reconcile state, or nil when absent.
func reconcileStateFrom(ctx context.Context) *reconcileState {
	if v, ok := ctx.Value(reconcileStateKey{}).(*reconcileState); ok {
		return v
	}
	return nil
}

// finish patches status (observedGeneration + endpoint) with an optimistic-lock
// merge from the reconcile's captured base, then flushes buffered Events. On a
// conflict it requeues without emitting Events or persisting a stale status.
func (r *DecisionModelReconciler) finish(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	res ctrl.Result,
	reconcileErr error,
) (ctrl.Result, error) {
	persisted, conflict, err := r.persistStatus(ctx, dm)
	switch {
	case conflict:
		// Stale object: events and metrics were dropped; requeue for a fresh read.
		return ctrl.Result{RequeueAfter: time.Second}, reconcileErr
	case err != nil:
		if reconcileErr == nil {
			return res, err
		}
		return res, reconcileErr
	case !persisted:
		return res, reconcileErr
	}
	return res, reconcileErr
}

// persistStatus writes dm.Status (observedGeneration + endpoint included) with an
// optimistic-lock merge from the reconcile's captured base and, only on success,
// flushes the buffered Events and metrics. persisted reports whether the
// status is now durably stored; conflict is true when the write lost an
// optimistic-lock race (nothing was stored and nothing was emitted).
//
// promote() relies on persisted: the Service may only be moved to a revision that
// is already recorded as status.stableRevision, so a failed or conflicting write
// can never leave the Service on a revision the cluster state does not know is
// stable.
func (r *DecisionModelReconciler) persistStatus(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
) (persisted, conflict bool, err error) {
	dm.Status.ObservedGeneration = dm.Generation
	dm.Status.Endpoint = r.endpoint(dm)
	st := reconcileStateFrom(ctx)
	if st != nil && st.base != nil {
		if perr := r.Status().Patch(ctx, dm, client.MergeFromWithOptions(st.base, client.MergeFromWithOptimisticLock{})); perr != nil {
			return false, apierrors.IsConflict(perr), perr
		}
		r.flushEvents(dm, st)
		flushMetrics(dm, st)
		return true, false, nil
	}
	if uerr := r.Status().Update(ctx, dm); uerr != nil {
		return false, false, uerr
	}
	if st != nil {
		r.flushEvents(dm, st)
		flushMetrics(dm, st)
	}
	return true, false, nil
}

// flushEvents emits all buffered Events after a successful status write.
func (r *DecisionModelReconciler) flushEvents(dm *decisionmodelv1alpha1.DecisionModel, st *reconcileState) {
	if r.Recorder == nil {
		st.events = nil
		return
	}
	for _, e := range st.events {
		r.Recorder.Eventf(dm, nil, e.eventType, e.reason, e.reason, e.note, e.args...)
	}
	st.events = nil
}

// flushMetrics records the just-persisted status into the gauges and applies
// all buffered counter/histogram mutations. Called from finish only after a
// successful status write, so metrics stay consistent with accepted status.
func flushMetrics(dm *decisionmodelv1alpha1.DecisionModel, st *reconcileState) {
	recordStatusMetrics(dm)
	ns, name := dm.Namespace, dm.Name
	for _, m := range st.metrics {
		switch m.kind {
		case metricKindRollout:
			rolloutsTotal.WithLabelValues(ns, name, m.label).Inc()
		case metricKindProbe:
			probeResultsTotal.WithLabelValues(ns, name, m.label).Add(m.amount)
		case metricKindPhaseDuration:
			phaseDurationSeconds.WithLabelValues(ns, name, m.label).Observe(m.dur.Seconds())
		}
	}
	st.metrics = nil
}

// bufferRollout records a rollout outcome to flush after the status write.
func bufferRollout(ctx context.Context, result string) {
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics, bufferedMetric{kind: metricKindRollout, label: result, amount: 1})
	}
}

// bufferProbeResult records n probe outcomes of a given result to flush after
// the status write.
func bufferProbeResult(ctx context.Context, result string, n int) {
	if n <= 0 {
		return
	}
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics,
			bufferedMetric{kind: metricKindProbe, label: result, amount: float64(n)})
	}
}

// bufferPhaseDuration records the time spent in a phase that is now being left,
// to flush after the status write.
func bufferPhaseDuration(ctx context.Context, phase decisionmodelv1alpha1.DecisionModelPhase, d time.Duration) {
	if phase == "" || d < 0 {
		return
	}
	if st := reconcileStateFrom(ctx); st != nil {
		st.metrics = append(st.metrics,
			bufferedMetric{kind: metricKindPhaseDuration, label: string(phase), dur: d})
	}
}

// setReplicaStatus records desired/modelReady counts.
func (r *DecisionModelReconciler) setReplicaStatus(dm *decisionmodelv1alpha1.DecisionModel, ready int32) {
	dm.Status.Replicas = decisionmodelv1alpha1.ReplicaStatus{
		Desired:    desiredReplicas(dm),
		ModelReady: ready,
	}
}

// applyStableReadiness sets phase + Ready/ModelReady/Degraded conditions for the
// stable revision honestly:
//   - ready == desired      -> Ready, Ready=True, ModelReady=True
//   - 0 < ready < desired   -> Degraded, Ready=True, ModelReady=False, Degraded=True (ReplicasNotModelReady)
//   - ready == 0            -> Degraded, Ready=False (NoModelReadyPods), ModelReady=False
//
// A cache-sharing Degraded (cacheDegraded) is always preserved.
func (r *DecisionModelReconciler) applyStableReadiness(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	ready, desired int32,
	cacheDegraded bool,
) {
	switch {
	case ready >= desired:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseReady)
		setReadyConditions(dm, cacheDegraded)
	case ready > 0:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseDegraded)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  reasonReady,
			Message: "serving on a subset of replicas",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionModelReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonReplicasNotModelReady,
			Message: fmt.Sprintf("%d/%d replicas model-ready", ready, desired),
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonReplicasNotModelReady,
			Message: fmt.Sprintf("%d/%d replicas model-ready", ready, desired),
		})
	default:
		r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseDegraded)
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoModelReadyPods,
			Message: "no serving Pod reports the expected digest on the expected device",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionModelReady,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNoModelReadyPods,
			Message: "0 replicas model-ready",
		})
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reasonNoModelReadyPods,
			Message: "0 replicas model-ready",
		})
	}
}

// requeueIfShort requeues while fewer Pods are ready than desired.
func (r *DecisionModelReconciler) requeueIfShort(ready, desired int32) ctrl.Result {
	if ready < desired {
		return ctrl.Result{RequeueAfter: probeRequeue}
	}
	return ctrl.Result{}
}

// setPhase updates the phase and records the transition time when it changes.
// When the phase actually changes, it buffers the duration the DecisionModel
// spent in the previous phase for the phase-duration histogram (flushed after
// the status write).
func (r *DecisionModelReconciler) setPhase(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	phase decisionmodelv1alpha1.DecisionModelPhase,
) {
	if dm.Status.Phase == phase && dm.Status.PhaseTransitionTime != nil {
		return
	}
	if dm.Status.Phase != phase {
		// Observe how long we stayed in the phase we are now leaving.
		if dm.Status.Phase != "" && dm.Status.PhaseTransitionTime != nil {
			bufferPhaseDuration(ctx, dm.Status.Phase, r.now().Sub(dm.Status.PhaseTransitionTime.Time))
		}
		t := metav1.NewTime(r.now())
		dm.Status.PhaseTransitionTime = &t
	}
	dm.Status.Phase = phase
}

// phaseExceeded reports whether the current phase has lasted longer than d,
// measured from the explicit phaseTransitionTime.
func (r *DecisionModelReconciler) phaseExceeded(
	dm *decisionmodelv1alpha1.DecisionModel,
	d time.Duration,
) bool {
	if dm.Status.PhaseTransitionTime == nil {
		return false
	}
	return r.now().Sub(dm.Status.PhaseTransitionTime.Time) > d
}

func (r *DecisionModelReconciler) endpoint(dm *decisionmodelv1alpha1.DecisionModel) string {
	port := int32(0)
	if eng, ok := r.Engines[engineOrDefault(dm.Spec.Engine)]; ok {
		port = eng.ServicePort()
	}
	return fmt.Sprintf("http://%s.%s.svc:%d/v1/systemone", dm.Name, dm.Namespace, port)
}

func (r *DecisionModelReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// bgContext returns the base context for background evaluation goroutines,
// so they are cancelled on manager shutdown. Falls back to Background.
func (r *DecisionModelReconciler) bgContext() context.Context {
	if r.BaseContext != nil {
		return r.BaseContext
	}
	return context.Background()
}

// event emits an events.k8s.io/v1 Event if a Recorder is configured (nil-safe).
// The reason doubles as the action (the verb describing what happened to the DM).
// event buffers a Kubernetes Event to be emitted only after a successful status
// write. When no reconcile state is present (e.g. some tests), it emits
// immediately.
func (r *DecisionModelReconciler) event(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eventType, reason, noteFmt string,
	args ...interface{},
) {
	if st := reconcileStateFrom(ctx); st != nil {
		st.events = append(st.events, bufferedEvent{eventType: eventType, reason: reason, note: noteFmt, args: args})
		return
	}
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(dm, nil, eventType, reason, reason, noteFmt, args...)
}

func (r *DecisionModelReconciler) prober() Prober {
	if r.Prober != nil {
		return r.Prober
	}
	return NewProber(r.bgContext)
}

// pruneProberWarm drops the prober's per-Pod warm-cache entries for Pods of this
// DecisionModel that no longer exist, so the map does not leak across rollouts.
// Best-effort: a list failure skips pruning (never aborts the
// reconcile), and a Prober without the warmPruner capability is a no-op.
func (r *DecisionModelReconciler) pruneProberWarm(ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel) {
	pruner, ok := r.prober().(warmPruner)
	if !ok {
		return
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(dm.Namespace),
		client.MatchingLabels{decisionmodelv1alpha1.LabelName: dm.Name}); err != nil {
		return
	}
	live := make(map[types.UID]bool, len(pods.Items))
	for i := range pods.Items {
		live[pods.Items[i].UID] = true
	}
	pruner.PruneWarm(dm.Namespace+"/"+dm.Name, live)
}

// reader returns the uncached APIReader for Secrets/ConfigMaps. It is a hard
// error to use it unset: the manager must inject mgr.GetAPIReader() so the
// operator never starts cluster-wide Secret/ConfigMap informers.
func (r *DecisionModelReconciler) reader() (client.Reader, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("APIReader is not configured")
	}
	return r.APIReader, nil
}

// SetupWithManager wires the controller: DecisionModel (generation-changed),
// owned workloads, and Pods mapped back to their owning DecisionModel by label.
func (r *DecisionModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Prober == nil {
		r.Prober = NewProber(r.bgContext)
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("decisionmodel-controller")
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrent
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&decisionmodelv1alpha1.DecisionModel{},
			builder.WithPredicates(predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.AnnotationChangedPredicate{},
			))).
		Owns(&appsv1.Deployment{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToDecisionModel),
			builder.WithPredicates(podEnqueuePredicate())).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrent}).
		Named("decisionmodel").
		Complete(r)
}

// podToDecisionModel maps a Pod carrying the LabelName back to its DecisionModel.
func (r *DecisionModelReconciler) podToDecisionModel(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetLabels()[decisionmodelv1alpha1.LabelName]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name},
	}}
}

// podEnqueuePredicate enqueues on Pod create/delete, and on update only when a
// field that affects model readiness changed: PodIP, ContainersReady status,
// any container restartCount, or deletionTimestamp. It ignores our own
// gate patch and other status noise so a gate write does not re-trigger a
// reconcile.
func podEnqueuePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
			newPod, ok2 := e.ObjectNew.(*corev1.Pod)
			if !ok1 || !ok2 {
				return true
			}
			return podReadinessInputsChanged(oldPod, newPod)
		},
	}
}

// podReadinessInputsChanged reports whether a Pod update touched a field the
// controller's readiness logic depends on.
func podReadinessInputsChanged(oldPod, newPod *corev1.Pod) bool {
	if oldPod.Status.PodIP != newPod.Status.PodIP {
		return true
	}
	if (oldPod.DeletionTimestamp == nil) != (newPod.DeletionTimestamp == nil) {
		return true
	}
	if containersReady(oldPod) != containersReady(newPod) {
		return true
	}
	// PodReady follows the model-ready gate asynchronously and is what puts the Pod
	// behind the Service. A candidate is only promoted onto Pods that are Ready, so
	// this transition is the event that lets the next reconcile count the Pod.
	if podReady(oldPod) != podReady(newPod) {
		return true
	}
	return totalRestarts(oldPod) != totalRestarts(newPod)
}

// totalRestarts sums container restart counts for a Pod.
func totalRestarts(pod *corev1.Pod) int32 {
	var n int32
	for _, cs := range pod.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}
