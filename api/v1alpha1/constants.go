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

package v1alpha1

// Phase constants for DecisionModelStatus.Phase.
const (
	PhasePending    DecisionModelPhase = "Pending"
	PhaseResolving  DecisionModelPhase = "Resolving"
	PhaseCaching    DecisionModelPhase = "Caching"
	PhaseStarting   DecisionModelPhase = "Starting"
	PhaseEvaluating DecisionModelPhase = "Evaluating"
	PhaseDegraded   DecisionModelPhase = "Degraded"
	// PhasePromoting is reserved and never set. Promotion is a single reconcile that
	// persists status.stableRevision first and only then moves the Service, so there
	// is no observable intermediate phase. It is kept, not removed, because the
	// documented phase set and copied health checks (e.g. GitOps/Argo Lua) name it.
	PhasePromoting DecisionModelPhase = "Promoting"
	PhaseReady     DecisionModelPhase = "Ready"
	// PhaseAwaitingPromotion means the candidate passed its gate and waits for a
	// human to approve it (spec.rollout.manualPromotion). The stable keeps serving.
	PhaseAwaitingPromotion DecisionModelPhase = "AwaitingPromotion"
	PhaseFailed            DecisionModelPhase = "Failed"
	PhaseRolledBack        DecisionModelPhase = "RolledBack"
)

// Condition type constants for DecisionModelStatus.Conditions.
const (
	ConditionResolved   = "Resolved"
	ConditionCached     = "Cached"
	ConditionModelReady = "ModelReady"
	ConditionEvaluated  = "Evaluated"
	ConditionReady      = "Ready"
	ConditionDegraded   = "Degraded"
	// ConditionPromoted is only present once manual promotion is in play: False
	// (AwaitingApproval) while a candidate waits, True after it was promoted.
	ConditionPromoted = "Promoted"
	// ConditionRuntimeUpdateAvailable is True when a newer engine runtime default
	// is available than the version this DecisionModel is pinned to (set
	// spec.runtimeVersion to adopt it). Informational: it never blocks serving.
	ConditionRuntimeUpdateAvailable = "RuntimeUpdateAvailable"
	// ConditionStabilizing is present during the post-promotion stabilization
	// window: True while the new stable is being watched, then removed once the
	// window passes healthy (the previous revision is collected). It flips to
	// False (PostPromotionUnhealthy) just before an automatic rollback.
	ConditionStabilizing = "Stabilizing"
)

// ModelReadyGate is the Pod readiness gate condition type set to True only when
// /api/ps shows the expected digest on the expected device.
const ModelReadyGate = "decisionmodel.io/model-ready"

// Label keys applied to owned objects.
const (
	// LabelName identifies the owning DecisionModel by name.
	LabelName = "decisionmodel.io/name"
	// LabelRevision identifies the revision hash of an owned object. It is the
	// selector of a revision's Service, PDB and Deployment, so it is applied only
	// to serving Pods — never to a prefetch or other non-serving Pod, which would
	// otherwise be selected as a Service endpoint with nothing listening.
	LabelRevision = "decisionmodel.io/revision"
	// LabelPrefetchRevision identifies the revision whose store a prefetch Pod is
	// populating. It deliberately differs from LabelRevision so prefetch Pods stay
	// out of every serving selector while remaining in the operator's
	// name-scoped Pod cache (via LabelName) for failure classification.
	LabelPrefetchRevision = "decisionmodel.io/prefetch-revision"
	// LabelAPIKey must be set to "true" on a Secret before the operator will read
	// it as the engine API key, guarding against confused-deputy use.
	LabelAPIKey = "decisionmodel.io/api-key"
	// LabelEvalDataset must be set to "true" on a Secret before the operator will
	// read it as an eval golden dataset. One label grants one capability; a
	// dataset Secret labelled only with api-key still works for one release
	// (deprecated) and emits a Warning Event.
	LabelEvalDataset = "decisionmodel.io/eval-dataset"
	// LabelDownloadToken must be set to "true" on a Secret before the operator
	// will read it as a download credential (e.g. Hugging Face token).
	LabelDownloadToken = "decisionmodel.io/download-token"
)

// MaxNameLength is the longest metadata.name a DecisionModel may have. The
// operator derives Service (<dm>), Job (<dm>-prefetch-<rev> or <dm>-pf-<rev>),
// PVC (<dm>-store-<rev>), Deployment/PDB (<dm>-<rev>) names and the
// decisionmodel.io/name label value from it; the binding limit is the prefetch
// Job name (a job-name Pod label value, <= 63). A new revision's 16-hex hash uses
// the short "-pf-" prefix (43 + 4 + 16 = 63); a legacy 10-hex revision keeps
// "-prefetch-" (43 + 10 + 10 = 63). The CEL rule on DecisionModel repeats this
// number as a literal (markers cannot reference constants); a unit test and an
// envtest keep the two in sync.
const MaxNameLength = 43

// AnnotationRetry, when its value differs from status.lastRetryToken, clears a
// failed revision once so a transient failure can be retried without a spec
// change.
const AnnotationRetry = "decisionmodel.io/retry"

// AnnotationPromote approves a candidate held for manual promotion. When
// spec.rollout.evaluation is set its value must equal status.evaluation.approvalId
// (which binds the approval to the evaluated revision, policy and dataset); a bare
// revision hash is then ignored. Without evaluation the value may be the candidate
// revision hash, accepted for one release with a Warning Event. An approval for any
// other value is ignored. The controller removes the annotation after the promotion
// is persisted.
const AnnotationPromote = "decisionmodel.io/promote"
