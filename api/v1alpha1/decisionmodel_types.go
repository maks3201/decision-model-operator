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

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DecisionModelSpec defines the desired state of DecisionModel.
type DecisionModelSpec struct {
	// Engine is the serving runtime for the model.
	// +kubebuilder:validation:Enum=ollaya
	// +kubebuilder:default=ollaya
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Engine",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	Engine string `json:"engine,omitempty"`

	// Model is the model name in engine terms, e.g. "laya:en". The last path
	// segment must include an explicit ":tag" (a bare name resolves to :latest,
	// a different and heavier artifact — see spike 001 §9).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self.split('/')[size(self.split('/')) - 1].contains(':')",message="model must include an explicit tag, e.g. laya:en"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Model",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	Model string `json:"model"`

	// Digest optionally pins the model to an immutable digest (bare hex sha256).
	// When empty, the operator resolves the tag and records the digest in status.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Digest",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	Digest string `json:"digest,omitempty"`

	// Replicas is the number of serving Pods for the stable revision (default 1).
	// With replicas > 1 spread across nodes the model store must be shareable —
	// see cache.accessModes; otherwise the operator reports Degraded
	// (CacheNotShareable).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Replicas",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:podCount"}
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Device selects the target compute device for serving. "cuda" also requests
	// an nvidia.com/gpu and uses the engine's CUDA image.
	// +kubebuilder:validation:Enum=cpu;cuda
	// +kubebuilder:default=cpu
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Device",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:select:cpu","urn:alm:descriptor:com.tectonic.ui:select:cuda"}
	// +optional
	Device string `json:"device,omitempty"`

	// Image overrides the engine's default container image. Rejected unless the
	// operator is started with --allow-image-override (a DecisionModel editor
	// could otherwise run an arbitrary image under the operator's Pod template).
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image Override",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	Image string `json:"image,omitempty"`

	// Resources are the compute resource requirements for the serving container.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resources",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:resourceRequirements"}
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Scheduling is a passthrough of nodeSelector/tolerations/affinity for serving Pods.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Scheduling",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	// +optional
	Scheduling *SchedulingSpec `json:"scheduling,omitempty"`

	// Cache configures the model store PVC.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Cache",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	// +optional
	Cache *CacheSpec `json:"cache,omitempty"`

	// Auth configures the engine API key.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Auth",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// Rollout configures rollout behaviour, including eval-gated promotion.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Rollout",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	// +optional
	Rollout *RolloutSpec `json:"rollout,omitempty"`
}

// SchedulingSpec is a passthrough of standard Pod scheduling controls.
type SchedulingSpec struct {
	// NodeSelector is a selector which must be true for the Pod to fit on a node.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow the Pod to schedule onto nodes with matching taints.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity constrains Pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// RuntimeClassName selects the RuntimeClass for serving Pods, e.g. "nvidia"
	// when the NVIDIA GPU Operator does not make it the default runtime. It is
	// applied to serving Pods only (the prefetch Job needs no GPU runtime).
	// Because it is part of spec.scheduling, changing it changes the revision's
	// placement hash and therefore starts a new revision (blue-green), like any
	// other scheduling change.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`
}

// CacheSpec configures the model store PVC.
type CacheSpec struct {
	// Size is the requested PVC size for the model store.
	// +kubebuilder:default="10Gi"
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClassName selects the PVC storage class. RWX is required for
	// replicas>1 spread across nodes.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// AccessModes for the model store PVC. Defaults to ReadWriteOnce. Set
	// ReadWriteMany when running replicas>1 spread across nodes. ReadOnlyMany is
	// rejected: the prefetch Job must write the store. PVC access modes are
	// immutable once created.
	// +kubebuilder:validation:XValidation:rule="self.all(m, m != 'ReadOnlyMany')",message="ReadOnlyMany is not allowed: the prefetch Job writes the model store"
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`

	// DownloadTokenSecretRef references a Secret key holding a credential for
	// weight downloads (e.g. a Hugging Face token for private or gated repositories,
	// or a mirror). It is injected into the prefetch Job only, never into serving
	// Pods. The Secret must carry the label decisionmodel.io/download-token: "true"
	// (opt-in guard), or the operator reports Degraded (SecretNotAllowed) and does
	// not create a prefetch Job. The optional field is not supported: when a ref is
	// set the Secret must exist and contain the referenced key, or the operator
	// reports Degraded (DownloadTokenInvalid).
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Download Token Secret",xDescriptors={"urn:alm:descriptor:io.kubernetes:Secret"}
	// +optional
	DownloadTokenSecretRef *corev1.SecretKeySelector `json:"downloadTokenSecretRef,omitempty"`
}

// AuthSpec configures the engine API key.
type AuthSpec struct {
	// APIKeySecretRef references a Secret key holding the engine API key. The
	// Secret must carry the label decisionmodel.io/api-key: "true" (opt-in), or
	// the operator reports Degraded (SecretNotAllowed) and creates no workloads.
	// The same key is used by the operator's prober and evaluator.
	// +optional
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef,omitempty"`
}

// PromotionPolicy selects how a model-ready candidate is promoted.
type PromotionPolicy string

const (
	// PromotionAutomatic promotes a candidate as soon as it is model-ready (and,
	// when evaluation is configured, has passed the gate).
	PromotionAutomatic PromotionPolicy = "Automatic"
	// PromotionEvaluationGated requires rollout.evaluation and lets the gate
	// decide promotion. Rejected by CEL when evaluation is unset.
	PromotionEvaluationGated PromotionPolicy = "EvaluationGated"
	// PromotionManual holds a passed candidate in AwaitingPromotion until a human
	// approves it via the decisionmodel.io/promote annotation.
	PromotionManual PromotionPolicy = "Manual"
)

// RolloutSpec configures rollout behaviour.
// +kubebuilder:validation:XValidation:rule="!(has(self.promotion) && self.promotion == 'EvaluationGated') || has(self.evaluation)",message="promotion: EvaluationGated requires rollout.evaluation to be set"
// +kubebuilder:validation:XValidation:rule="!(has(self.promotion) && has(self.manualPromotion) && self.manualPromotion) || self.promotion == 'Manual'",message="manualPromotion: true conflicts with promotion; use promotion: Manual"
type RolloutSpec struct {
	// Evaluation gates promotion on a golden-dataset accuracy check. When unset,
	// a candidate is promoted as soon as all its Pods are model-ready.
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Evaluation",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:advanced"}
	// +optional
	Evaluation *EvaluationSpec `json:"evaluation,omitempty"`

	// Promotion selects how a candidate that passed ModelReady is promoted:
	//   - Automatic: promote as soon as the candidate is model-ready (and, when
	//     evaluation is configured, has passed the gate).
	//   - EvaluationGated: like Automatic but requires rollout.evaluation to be
	//     set (rejected by CEL otherwise); the gate decides promotion.
	//   - Manual: hold the candidate in AwaitingPromotion until a human sets the
	//     annotation decisionmodel.io/promote to the candidate's revision hash
	//     (evaluation still runs when configured).
	// When unset the effective policy is EvaluationGated if rollout.evaluation is
	// set, else Automatic. The deprecated manualPromotion:true is an alias for
	// Manual; setting both promotion and manualPromotion:true to disagreeing
	// values is rejected by CEL.
	// +kubebuilder:validation:Enum=Automatic;EvaluationGated;Manual
	// +optional
	Promotion PromotionPolicy `json:"promotion,omitempty"`

	// ManualPromotion holds a candidate that passed its gate (model-ready, plus
	// evaluation when configured) in phase AwaitingPromotion until a human
	// approves it by setting the annotation decisionmodel.io/promote to the
	// candidate's revision hash. The stable revision keeps serving meanwhile.
	// There is no progress timeout while waiting. The very first revision of a
	// DecisionModel (no stable revision yet) is promoted without approval, since
	// there is no traffic to protect. The approval annotation is removed once the
	// promotion has been persisted.
	//
	// Deprecated: use promotion: Manual. manualPromotion:true keeps working as an
	// alias for promotion: Manual.
	// +kubebuilder:default=false
	// +optional
	ManualPromotion bool `json:"manualPromotion,omitempty"`

	// Timeouts overrides the progress timeouts of a rollout. Unset fields keep
	// the built-in defaults. Large models (tens of GB) need more than the
	// defaults on a cold node. Not part of the revision hash; a change applies to
	// the phase timeouts immediately, but a prefetch Job that already exists keeps
	// the deadline it was created with.
	// +optional
	Timeouts *RolloutTimeouts `json:"timeouts,omitempty"`
}

// RolloutTimeouts overrides the progress timeouts of a rollout. Each value must
// be between 1m and 24h.
type RolloutTimeouts struct {
	// Caching bounds the Caching phase and is also used as the prefetch Job's
	// activeDeadlineSeconds. Default 30m.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('24h')",message="must be between 1m and 24h"
	// +optional
	Caching *metav1.Duration `json:"caching,omitempty"`

	// Starting bounds the Starting phase (candidate Pods becoming model-ready)
	// and, when set, also the background model warmup on each Pod. When unset the
	// phase timeout is 10m and the warmup bound stays 2m. Default 10m.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('24h')",message="must be between 1m and 24h"
	// +optional
	Starting *metav1.Duration `json:"starting,omitempty"`

	// Evaluating bounds the Evaluating phase and each golden-dataset run.
	// Default 10m.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('24h')",message="must be between 1m and 24h"
	// +optional
	Evaluating *metav1.Duration `json:"evaluating,omitempty"`
}

// DatasetRef references a golden dataset (JSONL) in a ConfigMap or Secret.
// Exactly one of configMapRef / secretRef must be set.
// +kubebuilder:validation:XValidation:rule="(has(self.configMapRef) ? 1 : 0) + (has(self.secretRef) ? 1 : 0) == 1",message="exactly one of configMapRef or secretRef must be set"
type DatasetRef struct {
	// ConfigMapRef selects a key in a ConfigMap holding the JSONL dataset.
	// +optional
	ConfigMapRef *DatasetKeyRef `json:"configMapRef,omitempty"`
	// SecretRef selects a key in a Secret holding the JSONL dataset. The Secret
	// must carry the label decisionmodel.io/api-key: "true" (opt-in guard).
	// +optional
	SecretRef *DatasetKeyRef `json:"secretRef,omitempty"`
}

// DatasetKeyRef names an object and a key within it.
type DatasetKeyRef struct {
	// Name of the ConfigMap or Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Key holding the JSONL content.
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// EvaluationSpec configures the eval-gated rollout.
type EvaluationSpec struct {
	// DatasetRef points at the golden dataset (JSONL).
	DatasetRef DatasetRef `json:"datasetRef"`

	// MinAccuracy is the minimum accuracy (decimal string in [0,1], e.g. "0.90")
	// the candidate must reach to be promoted.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	MinAccuracy string `json:"minAccuracy"`

	// MaxAccuracyDrop is the maximum tolerated accuracy drop vs the stable
	// baseline (decimal string, e.g. "0.02"). Only enforced when a stable
	// revision exists to provide a baseline; empty means no drop constraint.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	MaxAccuracyDrop string `json:"maxAccuracyDrop,omitempty"`

	// MaxCases caps how many dataset cases are used (first N).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5000
	// +kubebuilder:default=500
	// +optional
	MaxCases int32 `json:"maxCases,omitempty"`

	// MaxECE is the maximum tolerated Expected Calibration Error (decimal string
	// in [0,1], e.g. "0.10"). Empty disables the absolute ECE gate.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	MaxECE string `json:"maxECE,omitempty"`

	// MaxECEIncrease is the maximum tolerated ECE increase vs the stable baseline
	// (decimal string). Only enforced when a stable revision exists to provide a
	// baseline; empty disables the relative ECE gate.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	MaxECEIncrease string `json:"maxECEIncrease,omitempty"`
}

// DecisionModelPhase enumerates the high-level lifecycle phase of a DecisionModel.
type DecisionModelPhase string

// RevisionStatus records the resolved, model-affecting identity of a revision.
//
// Model-affecting fields (engine, model, digest, device, image, resources)
// together reproduce the revision hash, so a stable revision can be rendered
// from its own recorded state rather than the current spec (which may already
// describe a different, pending candidate).
type RevisionStatus struct {
	// Hash is the short revision hash of the model-affecting spec fields.
	Hash string `json:"hash,omitempty"`

	// Engine is the serving runtime for this revision.
	// +optional
	Engine string `json:"engine,omitempty"`

	// Model is the model name for this revision.
	Model string `json:"model,omitempty"`

	// Digest is the resolved immutable digest for this revision.
	Digest string `json:"digest,omitempty"`

	// Device is the target device for this revision.
	Device string `json:"device,omitempty"`

	// Image is the resolved serving container image for this revision.
	// +optional
	Image string `json:"image,omitempty"`

	// Resources are the compute resource requirements of this revision's serving
	// container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Precision is the quantization level reported by the engine (e.g. F32, F16).
	Precision string `json:"precision,omitempty"`

	// Placement is a short hash of spec.scheduling (nodeSelector, tolerations,
	// affinity, runtimeClassName) as it was when this revision was created, or
	// "none" when no scheduling was set. Empty only on a revision recorded by an
	// older operator version, which the controller adopts on its first reconcile.
	// Placement is part of a revision's identity: a change of scheduling starts a
	// new revision (blue-green, own store) instead of rolling the running
	// Deployment in place. A hash is recorded, not the spec, because Affinity is a
	// very large schema and this type appears three times in the CRD.
	// +optional
	Placement string `json:"placement,omitempty"`

	// Reason is a short machine reason for why this revision failed. Set only on
	// status.failedRevision; empty on stable/candidate.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable explanation of a failure. Set only on
	// status.failedRevision; empty on stable/candidate.
	// +optional
	Message string `json:"message,omitempty"`

	// FailedAt is when this revision was recorded as failed. Set only on
	// status.failedRevision; nil on stable/candidate.
	// +optional
	FailedAt *metav1.Time `json:"failedAt,omitempty"`
}

// PreviousRevisionStatus records the revision demoted by the latest promotion
// and when the promotion happened, so its workloads may linger for a grace
// period before being garbage-collected.
type PreviousRevisionStatus struct {
	// Hash is the demoted revision's hash.
	Hash string `json:"hash,omitempty"`
	// PromotedAt is when the newer revision was promoted (this one demoted).
	// +optional
	PromotedAt *metav1.Time `json:"promotedAt,omitempty"`
}

// ReplicaStatus reports desired and model-ready replica counts.
type ReplicaStatus struct {
	// Desired is the desired number of serving replicas.
	// +optional
	Desired int32 `json:"desired,omitempty"`

	// ModelReady is the number of replicas that passed the model readiness gate.
	// +optional
	ModelReady int32 `json:"modelReady,omitempty"`
}

// EvaluationResult is the outcome of applying the evaluation gate.
type EvaluationResult string

const (
	// EvaluationPassed means the candidate met every configured gate.
	EvaluationPassed EvaluationResult = "Passed"
	// EvaluationFailed means the candidate failed at least one gate.
	EvaluationFailed EvaluationResult = "Failed"
)

// EvaluationStatus records the outcome of an eval-gated rollout evaluation.
type EvaluationStatus struct {
	// Revision is the revision hash the evaluation was run against.
	Revision string `json:"revision,omitempty"`
	// Accuracy is the candidate accuracy (decimal string).
	Accuracy string `json:"accuracy,omitempty"`
	// BaselineAccuracy is the stable revision's accuracy for this dataset, if known.
	BaselineAccuracy string `json:"baselineAccuracy,omitempty"`
	// Cases is the number of questions scored.
	Cases int32 `json:"cases,omitempty"`
	// FailedCases is the number of questions that were wrong or unanswerable.
	FailedCases int32 `json:"failedCases,omitempty"`
	// ECE is the candidate's expected calibration error (decimal string).
	ECE string `json:"ece,omitempty"`
	// Brier is the candidate's Brier score (decimal string).
	Brier string `json:"brier,omitempty"`
	// BaselineECE is the stable revision's ECE for this dataset, if known.
	BaselineECE string `json:"baselineEce,omitempty"`
	// MinAccuracy echoes the accuracy floor the gate applied, if any.
	// +optional
	MinAccuracy string `json:"minAccuracy,omitempty"`
	// MaxAccuracyDrop echoes the accuracy-drop limit the gate applied, if any.
	// +optional
	MaxAccuracyDrop string `json:"maxAccuracyDrop,omitempty"`
	// MaxECE echoes the absolute ECE limit the gate applied, if any.
	// +optional
	MaxECE string `json:"maxEce,omitempty"`
	// MaxECEIncrease echoes the relative ECE-increase limit the gate applied, if any.
	// +optional
	MaxECEIncrease string `json:"maxEceIncrease,omitempty"`
	// Result is the gate outcome: Passed or Failed.
	// +kubebuilder:validation:Enum=Passed;Failed
	// +optional
	Result EvaluationResult `json:"result,omitempty"`
	// Reason is the gate message (e.g. the failing comparison), human-readable.
	// +optional
	Reason string `json:"reason,omitempty"`
	// PolicyHash is a hash of the effective evaluation policy (thresholds,
	// datasetRef, maxCases) this result was produced under. A parked candidate in
	// AwaitingPromotion whose current policy hash differs is re-evaluated rather
	// than promoted on the stale result.
	// +optional
	PolicyHash string `json:"policyHash,omitempty"`
	// CompletedAt is when the evaluation finished.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// DecisionModelStatus defines the observed state of DecisionModel.
type DecisionModelStatus struct {
	// Phase is the high-level lifecycle phase.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Phase",xDescriptors={"urn:alm:descriptor:io.kubernetes.phase"}
	// +optional
	Phase DecisionModelPhase `json:"phase,omitempty"`

	// PhaseTransitionTime is when Phase last changed. Used for progress timeouts.
	// +optional
	PhaseTransitionTime *metav1.Time `json:"phaseTransitionTime,omitempty"`

	// LastPromotionTime is when a candidate was most recently promoted to stable.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Last Promotion Time",xDescriptors={"urn:alm:descriptor:text"}
	// +optional
	LastPromotionTime *metav1.Time `json:"lastPromotionTime,omitempty"`

	// ObservedGeneration is the generation last processed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Endpoint is the in-cluster serving endpoint URL.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Endpoint",xDescriptors={"urn:alm:descriptor:org.w3:link"}
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// StableRevision is the revision currently receiving traffic.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Stable Revision",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	StableRevision *RevisionStatus `json:"stableRevision,omitempty"`

	// CandidateRevision is the revision being rolled out, if any.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Candidate Revision",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	CandidateRevision *RevisionStatus `json:"candidateRevision,omitempty"`

	// FailedRevision is a revision that failed to roll out. The controller does
	// not automatically retry it; a spec change (new revision) is required, or
	// setting the decisionmodel.io/retry annotation to a new token re-attempts
	// the same revision.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Failed Revision",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	FailedRevision *RevisionStatus `json:"failedRevision,omitempty"`

	// PreviousRevision is the revision that was stable immediately before the
	// most recent promotion. It may linger for a short grace period after
	// promotedAt so the new revision's endpoints populate before it is removed.
	// +optional
	PreviousRevision *PreviousRevisionStatus `json:"previousRevision,omitempty"`

	// LastRetryToken is the value of the decisionmodel.io/retry annotation the
	// controller last consumed to clear a failed revision.
	// +optional
	LastRetryToken string `json:"lastRetryToken,omitempty"`

	// Replicas reports desired and model-ready replica counts.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Replicas",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	Replicas ReplicaStatus `json:"replicas,omitempty"`

	// Evaluation records the most recent eval-gated rollout result.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Evaluation",xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	// +optional
	Evaluation *EvaluationStatus `json:"evaluation,omitempty"`

	// Conditions represent the latest available observations of the object's state.
	// +listType=map
	// +listMapKey=type
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Conditions",xDescriptors={"urn:alm:descriptor:io.kubernetes.conditions"}
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dm
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.stableRevision.model`
// +kubebuilder:printcolumn:name="Candidate",type=string,JSONPath=`.status.candidateRevision.model`
// +kubebuilder:printcolumn:name="Accuracy",type=string,JSONPath=`.status.evaluation.accuracy`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Digest",type=string,JSONPath=`.status.stableRevision.digest`,priority=1
// +kubebuilder:printcolumn:name="Device",type=string,JSONPath=`.status.stableRevision.device`,priority=1
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.replicas.modelReady`,priority=1
// +kubebuilder:printcolumn:name="Promoted",type=date,JSONPath=`.status.lastPromotionTime`,priority=1

// DecisionModel is the Schema for the decisionmodels API.
//
// metadata.name must be a DNS-1035 label of at most 43 characters (see
// MaxNameLength): the operator derives a Service named after it (no dots) and
// Job/PVC names that must stay within 63 characters.
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="metadata.name must be a DNS-1035 label: lowercase letters, digits and '-', starting with a letter and ending with a letter or digit (no dots)"
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 43",message="metadata.name must be at most 43 characters (derived Job and PVC names must fit in 63)"
// +operator-sdk:csv:customresourcedefinitions:displayName="Decision Model",resources={{Deployment,v1,""},{Service,v1,""},{Job,v1,""},{PersistentVolumeClaim,v1,""},{PodDisruptionBudget,v1,""}}
type DecisionModel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DecisionModelSpec   `json:"spec,omitempty"`
	Status DecisionModelStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DecisionModelList contains a list of DecisionModel.
type DecisionModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DecisionModel `json:"items"`
}
