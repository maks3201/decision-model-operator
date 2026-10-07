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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
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
	// stabilizationWindow is the default post-promotion window during which the
	// previous revision is kept running (out of the Service) so traffic can be
	// switched back if the new stable turns out unhealthy. Overridable per
	// DecisionModel via spec.rollout.stabilization (0 disables it).
	stabilizationWindow = 5 * time.Minute
	// postPromotionDebounce is how long the new stable must stay below the
	// model-ready quorum during the stabilization window before an automatic
	// rollback (a brief dip during a rolling restart must not trip it). A gate
	// digest/device mismatch rolls back immediately, without this debounce.
	postPromotionDebounce = 30 * time.Second
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

	reasonEvaluationUnsupported  = "EvaluationUnsupported"
	reasonDatasetInvalid         = "DatasetInvalid"
	reasonDatasetNotFound        = "DatasetNotFound"
	reasonDatasetKeyNotFound     = "DatasetKeyNotFound"
	reasonEvaluationTimeout      = "EvaluationTimeout"
	reasonEvaluationFailed       = "EvaluationFailed"
	reasonBaselineUnavailable    = "BaselineUnavailable"
	reasonCalibrationUnavailable = "CalibrationUnavailable"
	reasonDatasetChanged         = "DatasetChanged"

	// Evaluated-condition reasons that tell the rollout story.
	reasonEvaluationRunning = "EvaluationRunning"
	reasonEvaluationPassed  = "EvaluationPassed"
	reasonEvaluationSkipped = "EvaluationSkipped"

	// Ready-condition reason on a rollback: the candidate was rejected and the
	// stable revision keeps serving.
	reasonCandidateRejected = "CandidateRejected"

	reasonReplicasNotModelReady = "ReplicasNotModelReady"
	reasonNoModelReadyPods      = "NoModelReadyPods"

	reasonInvalidModelName          = "InvalidModelName"
	reasonRegistryNotAllowed        = "RegistryNotAllowed"
	reasonImageOverrideNotAllowed   = "ImageOverrideNotAllowed"
	reasonSecretNotAllowed          = "SecretNotAllowed"
	reasonResourceConflict          = "ResourceConflict"
	reasonStoreLost                 = "StoreLost"
	reasonStorePrefetchFailed       = "StorePrefetchFailed"
	reasonStoreTerminating          = "StoreTerminating"
	reasonDownloadTokenInvalid      = "DownloadTokenInvalid"
	reasonInvalidRuntimeVersion     = "InvalidRuntimeVersion"
	reasonRuntimeVersionUnsupported = "RuntimeVersionUnsupported"
	reasonRolloutQueued             = "RolloutQueued"
	reasonRuntimeUpdateAvailable    = "RuntimeUpdateAvailable"
	reasonAPIKeyInvalid             = "APIKeyInvalid"
	reasonStabilizing               = "Stabilizing"
	reasonStabilized                = "Stabilized"
	reasonPostPromotionUnhealthy    = "PostPromotionUnhealthy"
	// reasonStableRolling marks the Stabilizing condition True while a quorum
	// shortfall is being ignored because the stable Deployment is mid-rollout (an
	// intentional in-place change: replicas scale-up, API-key rotation, scheduling
	// or a manual restart), not a model-health failure.
	reasonStableRolling = "StableRolling"
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
	eventResolved              = "Resolved"
	eventPrefetchStarted       = "PrefetchStarted"
	eventCached                = "Cached"
	eventRevisionStarting      = "RevisionStarting"
	eventPromoted              = "Promoted"
	eventRolledBack            = "RolledBack"
	eventFailed                = "Failed"
	eventProbeMismatch         = "ProbeMismatch"
	eventEvaluationStarted     = "EvaluationStarted"
	eventEvaluationPassed      = "EvaluationPassed"
	eventEvaluationFailed      = "EvaluationFailed"
	eventEvaluationOnHold      = "EvaluationOnHold"
	eventDeprecatedSecretLabel = "DeprecatedSecretLabel"
	eventDatasetChanged        = "DatasetChanged"
	eventResourceConflict      = "ResourceConflict"
	eventStoreLost             = "StoreLost"
	eventStorePrefetchFail     = "StorePrefetchFailed"
	eventStoreTerminating      = "StoreTerminating"
	eventRuntimeUpdate         = "RuntimeUpdateAvailable"
	eventStabilized            = "Stabilized"
	eventRolledBackPromo       = "RolledBackAfterPromotion"
	eventCandidateRejected     = "CandidateRejected"
	eventCandidateSuperseded   = "CandidateSuperseded"
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
	// RuntimeVersionPolicy selects how an unset spec.runtimeVersion resolves:
	// "Pinned" (default) reuses the stable revision's recorded version so an
	// operator upgrade does not start a rollout; "FollowOperator" uses the engine
	// default. Empty means Pinned.
	RuntimeVersionPolicy string
	// MaxConcurrentRollouts bounds how many DecisionModels in the watched scope
	// may have a live candidate at once. 0 means unlimited. Others wait in phase
	// Pending (reason RolloutQueued), FIFO by phaseTransitionTime.
	MaxConcurrentRollouts int
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
	// budgetMu serializes the fleet rollout-budget "count + decide + reserve" so
	// two DecisionModels reconciled concurrently (MaxConcurrentReconciles > 1)
	// cannot both see a free slot and both admit. budgetReservations holds
	// admitted-but-not-yet-visible candidates (ns/name -> rev) until the cached DM
	// shows status.candidateRevision.hash == rev; they count as active. In memory
	// only (a restart re-derives admissions from candidateRevision in the API);
	// cleaned when the candidate becomes visible, ends, or the DM is deleted.
	budgetMu           sync.Mutex
	budgetReservations map[string]string
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
			// release any rollout-budget reservation it held, and delete its
			// exported metric series so they stop being scraped.
			r.evalStoreOrInit().forgetExcept(req.Namespace, req.Name, "")
			r.releaseReservation(req.Namespace, req.Name)
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

	// Candidate preflight (security guards, resolve, runtime version) validates
	// the LIVE spec, which describes the next candidate. When a stable revision is
	// already serving, a preflight failure must never stop maintaining it: the
	// stable is rendered from status.stableRevision (independent of the broken
	// spec), so the failure is surfaced as a condition/Event and the stable is
	// kept alive. With no stable, preflight failures behave as before (Failed /
	// Resolving / Terminal).
	digest, done, res, err := r.reconcilePreflight(ctx, &dm, eng)
	if done {
		return res, err
	}

	// 3. Compute the revision and candidate identity.
	stable := dm.Status.StableRevision
	effVer := r.effectiveRuntimeVersion(eng, &dm, stable)
	image := servingImage(eng, r.paramsForVersion(&dm, digest, "", "", effVer))
	rev := RevisionHash(dm.Spec, digest, image)
	params := r.paramsForVersion(&dm, digest, image, rev, effVer)
	candidate := &decisionmodelv1alpha1.RevisionStatus{
		Hash:   rev,
		Engine: engineOrDefault(dm.Spec.Engine),
		Model:  dm.Spec.Model,
		Digest: digest,
		Device: deviceOrDefault(dm.Spec.Device),
		// Record the model-affecting render inputs so the stable revision is later
		// rendered from its own state, not a (possibly newer) spec.
		Image:          image,
		RuntimeVersion: recordedCandidateRuntimeVersion(eng, &dm, effVer),
		Resources:      dm.Spec.Resources,
		Placement:      placementRecord(dm.Spec.Scheduling),
	}
	// A stable created before placement was recorded keeps its old hash name and
	// workloads; just record the placement it is running with. This runs
	// on the first reconcile after an operator upgrade, so no candidate is started
	// and nothing is rolled. From then on a change of scheduling differs from the
	// recorded placement and starts a new revision.
	adoptLegacyStable(stable, candidate, legacyRevisionHash(dm.Spec, digest, image))
	isStable := stable != nil && (stable.Hash == rev || sameIdentity(stable, candidate))

	// Surface whether a newer engine runtime default exists than the version this
	// DecisionModel runs (informational; never blocks serving). An explicit
	// spec.runtimeVersion or spec.image opts out of the nudge.
	r.reconcileRuntimeUpdate(ctx, eng, &dm, stable, candidate)

	// Cancel any evaluations running for a revision of this DM that is neither the
	// current candidate nor the current stable (e.g. after a spec/model change).
	keepRevs := []string{rev}
	if stable != nil {
		keepRevs = append(keepRevs, stable.Hash)
	}
	r.evalStoreOrInit().forgetExcept(dm.Namespace, dm.Name, keepRevs...)

	// 4. Store/cache handling.
	//   - Stable path: ensure (or recover) the stable revision's existing claim
	//     and run the cache-sharing guard against it, here, before serving.
	//   - Candidate path: the per-revision PVC must NOT be created until the
	//     rollout budget admits the candidate (a queued rollout must not provision
	//     a volume while it waits). So the candidate's ensurePVC + cache guard are
	//     deferred into reconcileCandidatePath, after the gate.
	var (
		pvc              *corev1.PersistentVolumeClaim
		storeTerminating bool
		storeLost        bool
		cacheDegraded    bool
	)
	if isStable {
		pvc, err = r.ensureStoreForStable(ctx, &dm, stable)
		storeTerminating = errors.Is(err, errStoreTerminating)
		storeLost = errors.Is(err, errStoreLostRecovering)
		if err != nil && !storeTerminating && !storeLost {
			return r.finish(ctx, &dm, ctrl.Result{}, err)
		}
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
	}

	apiKey, err := r.apiKey(ctx, &dm)
	if err != nil {
		if errors.Is(err, errSecretNotAllowed) {
			return r.degradeSecretNotAllowed(ctx, &dm, err)
		}
		if errors.Is(err, errAPIKeyInvalid) {
			return r.degradeSecretReason(ctx, &dm, reasonAPIKeyInvalid, err)
		}
		return r.finish(ctx, &dm, ctrl.Result{}, err)
	}

	// Validate the optional download-token Secret before any prefetch Job (same
	// confused-deputy guard as the API key; a cache field, so this runs on both
	// the candidate and stable-store-recovery paths). The value is not read here;
	// the engine injects it into the Job from the Secret ref.
	if err := r.validateDownloadToken(ctx, &dm); err != nil {
		switch {
		case errors.Is(err, errSecretNotAllowed):
			return r.degradeSecretNotAllowed(ctx, &dm, err)
		case errors.Is(err, errDownloadTokenInvalid):
			return r.degradeSecretReason(ctx, &dm, reasonDownloadTokenInvalid, err)
		default:
			return r.finish(ctx, &dm, ctrl.Result{}, err)
		}
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

	// Candidate revision (new spec or first creation).
	return r.reconcileCandidatePath(ctx, &dm, eng, params, candidate, digest, apiKey, rev)
}

// reconcileCandidatePath handles a non-stable (candidate) revision: it waits
// while this revision's store claim is still terminating, applies the fleet
// rollout budget, then runs the candidate rollout.
func (r *DecisionModelReconciler) reconcileCandidatePath(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	params engine.Params,
	candidate *decisionmodelv1alpha1.RevisionStatus,
	digest string,
	apiKey, rev string,
) (ctrl.Result, error) {
	// Fleet rollout budget FIRST — before any allocation AND before ending the
	// current stabilization window. A brand-new candidate waits in
	// Pending/RolloutQueued when the watched scope is already at
	// --max-concurrent-rollouts; a queued candidate must not create its
	// per-revision PVC (provision a volume) while it waits, and must keep its
	// previous revision + Stabilizing condition so the current window still
	// protects it (an unhealthy stable can still roll back while queued — the
	// stable path owns that, reached on the next reconcile for a DM with a stable
	// revision). An already-admitted candidate, or one reusing its own window's
	// slot, bypasses the gate so an in-flight rollout cannot deadlock.
	if queued, qres, qerr := r.gateRolloutBudget(ctx, dm, rev); qerr != nil {
		return r.finish(ctx, dm, ctrl.Result{}, qerr)
	} else if queued {
		return qres, nil
	}

	// Admitted: only now end any in-flight post-promotion stabilization window, so
	// a fresh rollout is not entangled with the last one's rollback target. Only
	// when the window is actually enabled (>0) — with stabilization disabled the
	// previous revision keeps its short endpoint-gap grace, collected by GC as
	// before. Clearing it before admission would drop the rollback protection of a
	// DM that then sits queued for minutes.
	if dm.Status.PreviousRevision != nil && stabilizationFor(dm) > 0 {
		dm.Status.PreviousRevision = nil
		meta.RemoveStatusCondition(&dm.Status.Conditions, decisionmodelv1alpha1.ConditionStabilizing)
	}

	// Create the candidate's per-revision store PVC and run the cache-sharing
	// guard against it. It builds on this revision's store claim, so it waits
	// while a previous claim of the same revision is still terminating; once that
	// is NotFound the next reconcile creates a fresh one.
	pvc, err := r.ensurePVC(ctx, dm, rev)
	storeTerminating := errors.Is(err, errStoreTerminating)
	if err != nil && !storeTerminating {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	if storeTerminating {
		return r.finish(ctx, dm, ctrl.Result{RequeueAfter: storeTerminatingRequeue}, nil)
	}
	cacheDegraded, cacheErr := r.guardCacheSharing(ctx, dm, pvc)
	if cacheErr != nil {
		return r.finish(ctx, dm, ctrl.Result{}, cacheErr)
	}
	return r.reconcileCandidate(ctx, dm, eng, params, candidate, digest, cacheDegraded, apiKey)
}

// degradeSecretNotAllowed sets Degraded + Ready=False/SecretNotAllowed and
// requeues at the regate interval. Used for both the API-key and download-token
// confused-deputy guard: there is no Secret watch, so adding the label must be
// picked up by a requeue within ~60s.
func (r *DecisionModelReconciler) degradeSecretNotAllowed(
	ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel, err error,
) (ctrl.Result, error) {
	return r.degradeSecretReason(ctx, dm, reasonSecretNotAllowed, err)
}

// degradeSecretReason sets Degraded + Ready=False with the given reason and
// requeues at the regate interval (no Secret watch, so a fix must be polled).
func (r *DecisionModelReconciler) degradeSecretReason(
	ctx context.Context, dm *decisionmodelv1alpha1.DecisionModel, reason string, err error,
) (ctrl.Result, error) {
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	})
	r.setPhase(ctx, dm, decisionmodelv1alpha1.PhaseDegraded)
	return r.finish(ctx, dm, ctrl.Result{RequeueAfter: regateInterval}, nil)
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
	if newRetry && storeRecoverExhausted(dm) {
		clearStoreRecover(dm)
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

// reconcilePreflight runs the candidate preflight checks (security guards,
// resolve, runtime version) against the live spec. It returns the resolved
// digest and done=false to continue the normal flow. On a preflight failure it
// returns done=true with the result: when a stable revision is serving the
// failure is surfaced and the stable is maintained (never failing the object);
// with no stable the checks behave as before (Failed / Resolving / Terminal).
func (r *DecisionModelReconciler) reconcilePreflight(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
) (digest string, done bool, res ctrl.Result, err error) {
	hasStable := dm.Status.StableRevision != nil
	stable := dm.Status.StableRevision

	// Security guards: model name, registry allow-list, image-override policy.
	if reason, msg, bad, sdone, sres, serr := r.preflightSecurity(ctx, dm, eng, hasStable); sdone {
		return "", true, sres, serr
	} else if bad {
		res, err = r.maintainStableAfterPreflight(ctx, dm, eng, stable, reason, msg, false)
		return "", true, res, err
	}

	// Resolve the model to a digest.
	digest, rdone, bad, transient, reason, msg, rres, rerr := r.preflightResolve(ctx, dm, eng, hasStable)
	if rdone {
		return "", true, rres, rerr
	}
	if bad {
		res, err = r.maintainStableAfterPreflight(ctx, dm, eng, stable, reason, msg, transient)
		return "", true, res, err
	}

	// Validate an explicit spec.runtimeVersion against the engine (CEL rejects
	// malformed strings; this catches a too-old version or an engine that cannot
	// pin versions). An invalid/unsupported version changes no workloads — unless
	// a stable is serving, in which case it keeps running.
	if verr := validateRuntimeVersion(eng, dm.Spec.RuntimeVersion); verr != nil {
		reason := reasonInvalidRuntimeVersion
		if errors.Is(verr, errRuntimeVersionUnsupported) {
			reason = reasonRuntimeVersionUnsupported
		}
		if hasStable {
			res, err = r.maintainStableAfterPreflight(ctx, dm, eng, stable, reason, verr.Error(), false)
			return "", true, res, err
		}
		res, err = r.degradeSecretReason(ctx, dm, reason, verr)
		return "", true, res, err
	}
	return digest, false, ctrl.Result{}, nil
}

// preflightSecurity runs the candidate security guards. When no stable revision
// is serving it behaves like guardSecurity (sets Failed + Terminal and finishes,
// done=true). When a stable is serving a violation is reported as bad=true with
// the reason/message so the caller keeps maintaining the stable instead of
// failing the whole object; the guard is not weakened — the candidate is still
// refused.
func (r *DecisionModelReconciler) preflightSecurity(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	hasStable bool,
) (reason, msg string, bad, done bool, res ctrl.Result, err error) {
	if !hasStable {
		res, err, done = r.guardSecurity(ctx, dm, eng)
		return "", "", false, done, res, err
	}
	// A stable is serving: evaluate the same checks but do not fail the object.
	if verr := validateModelName(eng, dm.Spec.Model); verr != nil {
		return reasonInvalidModelName, verr.Error(), true, false, ctrl.Result{}, nil
	}
	if rsn, m := r.checkRegistryAllowed(eng, dm.Spec.Model); rsn != "" {
		return rsn, m, true, false, ctrl.Result{}, nil
	}
	if dm.Spec.Image != "" && !r.AllowImageOverride {
		return reasonImageOverrideNotAllowed,
			"spec.image override is not allowed (set --allow-image-override)", true, false, ctrl.Result{}, nil
	}
	return "", "", false, false, ctrl.Result{}, nil
}

// preflightResolve resolves the digest. With no stable it delegates to
// reconcileResolve (done=true on failure). With a stable it never fails the
// object: it reports bad=true and the caller routes to the stable path. A
// permanent model-not-found is bad=true/ModelNotFound (transient=false); a
// transient resolve error (e.g. registry 5xx) is bad=true/ResolveFailed with
// transient=true, so the caller surfaces only Resolved=False (not Degraded) and
// keeps the stable serving. Nothing is marked failed in either case.
func (r *DecisionModelReconciler) preflightResolve(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	hasStable bool,
) (digest string, done, bad, transient bool, reason, msg string, res ctrl.Result, err error) {
	if !hasStable {
		digest, done, res, err = r.reconcileResolve(ctx, dm, eng)
		return digest, done, false, false, "", "", res, err
	}
	digest, rerr := r.resolveDigest(ctx, dm, eng)
	if rerr == nil {
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
		return digest, false, false, false, "", "", ctrl.Result{}, nil
	}
	if errors.Is(rerr, engine.ErrNotFound) {
		// Permanent for the candidate: surface it and keep the stable serving.
		return "", false, true, false, reasonModelNotFound,
			fmt.Sprintf("model %q not found", dm.Spec.Model), ctrl.Result{}, nil
	}
	// Transient (e.g. registry 5xx): nothing is marked failed. Surface it as a
	// candidate failure (transient) and keep the stable serving; resolve retries
	// on the next reconcile via the stable path's requeue.
	return "", false, true, true, reasonResolveFailed, rerr.Error(), ctrl.Result{}, nil
}

// maintainStableAfterPreflight records a candidate-preflight failure and runs
// the full stable maintenance path so a bad candidate never stops serving the
// stable. The candidate failure is surfaced on the Resolved condition
// (Resolved=False with the specific reason); the Degraded condition is left to
// the stable path so a real stable-side problem (CacheNotShareable,
// PostPromotionUnhealthy, replica shortfall) is never hidden by the candidate
// reason. A Warning Event is emitted only when the Resolved reason/message
// changes, so a persistently bad spec does not spam one per reconcile; a
// transient failure (registry 5xx) stays quiet (no Degraded, Event only on
// change). A candidate that was in flight for the previous spec is abandoned so
// it stops holding a rollout slot / GPU / disk.
func (r *DecisionModelReconciler) maintainStableAfterPreflight(
	ctx context.Context,
	dm *decisionmodelv1alpha1.DecisionModel,
	eng engine.Engine,
	stable *decisionmodelv1alpha1.RevisionStatus,
	reason, msg string,
	transient bool,
) (ctrl.Result, error) {
	// Dedupe the Event: emit only when the candidate-failure signal changes.
	prev := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionResolved)
	changed := prev == nil || prev.Status != metav1.ConditionFalse ||
		prev.Reason != reason || prev.Message != msg

	setStatusCondition(dm, metav1.Condition{
		Type: decisionmodelv1alpha1.ConditionResolved, Status: metav1.ConditionFalse,
		Reason: reason, Message: msg,
	})
	// Emit a Warning only for a permanent rejection (bad model name, missing tag,
	// disallowed registry, invalid runtimeVersion) and only when the signal
	// changed. A transient resolve blip (registry 5xx) is a retriable outage, not
	// a user error: it stays quiet (Resolved=False carries it) so a flapping
	// registry does not spam Events.
	if changed && !transient {
		r.event(ctx, dm, corev1.EventTypeWarning, eventCandidateRejected,
			"candidate spec rejected (%s): %s; the current stable %s keeps serving", reason, msg, stable.Hash)
	}

	// Abandon a candidate started for a now-superseded spec: the spec no longer
	// asks for it, and leaving it holds its Deployment/Job/PVC (GPU, disk) and a
	// rollout slot until the spec is fixed. Clearing candidateRevision lets
	// gcRevisions (run by the stable path) collect it. Emit once.
	if c := dm.Status.CandidateRevision; c != nil {
		r.event(ctx, dm, corev1.EventTypeNormal, eventCandidateSuperseded,
			"abandoning candidate %s: the spec changed to a revision that cannot start", c.Hash)
		dm.Status.CandidateRevision = nil
	}

	// The stable's API key may itself be unreadable (e.g. the Secret lost its
	// label). That must not stop maintaining a running stable: continue with an
	// empty key. ensureDeployment preserves the existing key checksum on an empty
	// key, so this never rolls the running stable; the prober may get a transient
	// 401 (handled by Reinspect) rather than a template change.
	apiKey, kerr := r.apiKey(ctx, dm)
	if kerr != nil {
		apiKey = ""
	}
	// cacheDegraded=false: the stable path owns Degraded and sets it from the
	// stable's own health; the candidate failure lives on Resolved above.
	return r.reconcileStablePath(ctx, dm, eng, stable, apiKey, false)
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
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, stableParams, stable.Hash, true, keyChecksum, legacyChecksum, storeTerminating); err != nil {
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

	// Post-promotion stabilization window: while the previous revision is still
	// kept, watch the new stable; roll back to the previous revision if it turns
	// unhealthy, or announce Stabilized and let GC collect the previous once the
	// window passes healthy.
	if sr := r.reconcileStabilization(ctx, dm, eng, stable, ready); sr.rolledBack || sr.err != nil {
		return sr.res, sr.err
	}

	// GC every stale revision: keeps stable + candidate, and the demoted
	// previous revision only within its grace window.
	if err := r.gcRevisions(ctx, dm); err != nil {
		return r.finish(ctx, dm, ctrl.Result{}, err)
	}
	res := r.requeueIfShort(ready, desiredReplicas(dm))
	// While the previous revision is still kept (stabilization window), requeue so
	// the new stable's health is re-checked and the previous revision is collected
	// once the window elapses.
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
	// A policy OR dataset-content change while parked makes the recorded result
	// stale: leave AwaitingPromotion and re-evaluate rather than promote on the
	// old result — even if an approval annotation was set in the same edit. The
	// re-run routes through evaluateOrPromote. The approval is NOT honoured on the
	// re-eval cycle (policyChanged); the candidate re-parks under the new
	// policyHash/datasetDigest with a new approvalID, which the operator must then
	// be approved against. A dataset edited in place (same ConfigMap/Secret) is
	// detected by re-reading its digest here, bounded by the regate requeue.
	policyChanged := false
	if awaiting {
		if evalSpec := evaluationSpec(dm); evalSpec != nil {
			stale, datasetChanged := r.evalIdentityStale(ctx, dm, evalSpec)
			if stale {
				awaiting = false
				policyChanged = true
				if datasetChanged {
					r.event(ctx, dm, corev1.EventTypeWarning, eventDatasetChanged,
						"golden dataset changed while awaiting promotion; re-evaluating revision %s", rev)
					setStatusCondition(dm, metav1.Condition{
						Type:    decisionmodelv1alpha1.ConditionEvaluated,
						Status:  metav1.ConditionFalse,
						Reason:  reasonDatasetChanged,
						Message: "golden dataset content changed; re-evaluating",
					})
				}
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
		msg := "prefetch Job failed"
		if permanent, detail := r.prefetchFailureReason(ctx, dm, eng, rev); detail != "" {
			msg = "prefetch failed: " + detail
			if permanent {
				msg += " (permanent)"
			}
		}
		return r.rollbackOrFail(ctx, dm, candidate, reasonPrefetchFailed, msg)
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
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, params, rev, false, keyChecksum, legacyChecksum, false); err != nil {
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

// resourceConflictRequeue backs off a reconcile that found a name collision with
// an object this DM does not own, so it does not hot-loop while it waits for the
// conflicting object to be removed or re-owned.
const resourceConflictRequeue = 30 * time.Second

// afterGatePatch, when non-nil, is called with the Pod after a successful
// model-ready gate patch. It is a seam for tests only: envtest has no kubelet, and
// the kubelet is what sets Pod condition Ready=True once ContainersReady and every
// readiness gate are True. Production never sets it.
var afterGatePatch func(ctx context.Context, c client.Client, pod *corev1.Pod)

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
	keyChecksum, legacyChecksum := r.apiKeyTrigger(ctx, dm, apiKey)
	if err := r.ensureDeployment(ctx, dm, eng, params, stable.Hash, true, keyChecksum, legacyChecksum, false); err != nil {
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
