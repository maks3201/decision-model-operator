# API Reference

## Packages
- [decisionmodel.io/v1alpha1](#decisionmodeliov1alpha1)


## decisionmodel.io/v1alpha1

Package v1alpha1 contains API Schema definitions for the decisionmodel v1alpha1 API group.

### Resource Types
- [DecisionModel](#decisionmodel)



#### AuthSpec



AuthSpec configures the engine API key.



_Appears in:_
- [DecisionModelSpec](#decisionmodelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiKeySecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ | APIKeySecretRef references a Secret key holding the engine API key. The<br />Secret must carry the label decisionmodel.io/api-key: "true" (opt-in), or<br />the operator reports Degraded (SecretNotAllowed) and creates no workloads.<br />The same key is used by the operator's prober and evaluator. |  | Optional: \{\} <br /> |


#### CacheSpec



CacheSpec configures the model store PVC.



_Appears in:_
- [DecisionModelSpec](#decisionmodelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `size` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#quantity-resource-api)_ | Size is the requested PVC size for the model store. | 10Gi | Optional: \{\} <br /> |
| `storageClassName` _string_ | StorageClassName selects the PVC storage class. RWX is required for<br />replicas>1 spread across nodes. |  | Optional: \{\} <br /> |
| `accessModes` _[PersistentVolumeAccessMode](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#persistentvolumeaccessmode-v1-core) array_ | AccessModes for the model store PVC. Defaults to ReadWriteOnce. Set<br />ReadWriteMany when running replicas>1 spread across nodes. ReadOnlyMany is<br />rejected: the prefetch Job must write the store. PVC access modes are<br />immutable once created. |  | Optional: \{\} <br /> |
| `downloadTokenSecretRef` _[SecretKeySelector](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#secretkeyselector-v1-core)_ | DownloadTokenSecretRef references a Secret key holding a credential for<br />weight downloads (e.g. a Hugging Face token for private or gated repositories,<br />or a mirror). It is injected into the prefetch Job only, never into serving<br />Pods. The Secret must carry the label decisionmodel.io/download-token: "true"<br />(opt-in guard), or the operator reports Degraded (SecretNotAllowed) and does<br />not create a prefetch Job. The optional field is not supported: when a ref is<br />set the Secret must exist and contain the referenced key, or the operator<br />reports Degraded (DownloadTokenInvalid). |  | Optional: \{\} <br /> |


#### DatasetKeyRef



DatasetKeyRef names an object and a key within it.



_Appears in:_
- [DatasetRef](#datasetref)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name of the ConfigMap or Secret. |  | MinLength: 1 <br /> |
| `key` _string_ | Key holding the JSONL content. |  | MinLength: 1 <br /> |


#### DatasetRef



DatasetRef references a golden dataset (JSONL) in a ConfigMap or Secret.
Exactly one of configMapRef / secretRef must be set.



_Appears in:_
- [EvaluationSpec](#evaluationspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `configMapRef` _[DatasetKeyRef](#datasetkeyref)_ | ConfigMapRef selects a key in a ConfigMap holding the JSONL dataset. |  | Optional: \{\} <br /> |
| `secretRef` _[DatasetKeyRef](#datasetkeyref)_ | SecretRef selects a key in a Secret holding the JSONL dataset. The Secret<br />must carry the label decisionmodel.io/api-key: "true" (opt-in guard). |  | Optional: \{\} <br /> |


#### DecisionModel



DecisionModel is the Schema for the decisionmodels API.

metadata.name must be a DNS-1035 label of at most 43 characters (see
MaxNameLength): the operator derives a Service named after it (no dots) and
Job/PVC names that must stay within 63 characters.





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `decisionmodel.io/v1alpha1` | | |
| `kind` _string_ | `DecisionModel` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[DecisionModelSpec](#decisionmodelspec)_ |  |  |  |
| `status` _[DecisionModelStatus](#decisionmodelstatus)_ |  |  |  |


#### DecisionModelPhase

_Underlying type:_ _string_

DecisionModelPhase enumerates the high-level lifecycle phase of a DecisionModel.



_Appears in:_
- [DecisionModelStatus](#decisionmodelstatus)

| Field | Description |
| --- | --- |
| `Pending` |  |
| `Resolving` |  |
| `Caching` |  |
| `Starting` |  |
| `Evaluating` |  |
| `Degraded` |  |
| `Promoting` | PhasePromoting is reserved and never set. Promotion is a single reconcile that<br />persists status.stableRevision first and only then moves the Service, so there<br />is no observable intermediate phase. It is kept, not removed, because the<br />documented phase set and copied health checks (e.g. GitOps/Argo Lua) name it.<br /> |
| `Ready` |  |
| `AwaitingPromotion` | PhaseAwaitingPromotion means the candidate passed its gate and waits for a<br />human to approve it (spec.rollout.manualPromotion). The stable keeps serving.<br /> |
| `Failed` |  |
| `RolledBack` |  |


#### DecisionModelSpec



DecisionModelSpec defines the desired state of DecisionModel.



_Appears in:_
- [DecisionModel](#decisionmodel)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `engine` _string_ | Engine is the serving runtime for the model. | ollaya | Enum: [ollaya] <br />Optional: \{\} <br /> |
| `model` _string_ | Model is the model name in engine terms, e.g. "laya:en". The last path<br />segment must include an explicit ":tag" (a bare name resolves to :latest,<br />a different and heavier artifact — see spike 001 §9). |  | MinLength: 1 <br />Required: \{\} <br /> |
| `digest` _string_ | Digest optionally pins the model to an immutable digest (bare hex sha256).<br />When empty, the operator resolves the tag and records the digest in status. |  | Pattern: `^[a-f0-9]\{64\}$` <br />Optional: \{\} <br /> |
| `replicas` _integer_ | Replicas is the number of serving Pods for the stable revision (default 1).<br />With replicas > 1 spread across nodes the model store must be shareable —<br />see cache.accessModes; otherwise the operator reports Degraded<br />(CacheNotShareable). | 1 | Minimum: 1 <br />Optional: \{\} <br /> |
| `device` _string_ | Device selects the target compute device for serving. "cuda" also requests<br />an nvidia.com/gpu and uses the engine's CUDA image. | cpu | Enum: [cpu cuda] <br />Optional: \{\} <br /> |
| `image` _string_ | Image overrides the engine's default container image. Rejected unless the<br />operator is started with --allow-image-override (a DecisionModel editor<br />could otherwise run an arbitrary image under the operator's Pod template). |  | Optional: \{\} <br /> |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#resourcerequirements-v1-core)_ | Resources are the compute resource requirements for the serving container. |  | Optional: \{\} <br /> |
| `scheduling` _[SchedulingSpec](#schedulingspec)_ | Scheduling is a passthrough of nodeSelector/tolerations/affinity for serving Pods. |  | Optional: \{\} <br /> |
| `cache` _[CacheSpec](#cachespec)_ | Cache configures the model store PVC. |  | Optional: \{\} <br /> |
| `auth` _[AuthSpec](#authspec)_ | Auth configures the engine API key. |  | Optional: \{\} <br /> |
| `rollout` _[RolloutSpec](#rolloutspec)_ | Rollout configures rollout behaviour, including eval-gated promotion. |  | Optional: \{\} <br /> |


#### DecisionModelStatus



DecisionModelStatus defines the observed state of DecisionModel.



_Appears in:_
- [DecisionModel](#decisionmodel)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `phase` _[DecisionModelPhase](#decisionmodelphase)_ | Phase is the high-level lifecycle phase. |  | Optional: \{\} <br /> |
| `phaseTransitionTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | PhaseTransitionTime is when Phase last changed. Used for progress timeouts. |  | Optional: \{\} <br /> |
| `lastPromotionTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | LastPromotionTime is when a candidate was most recently promoted to stable. |  | Optional: \{\} <br /> |
| `observedGeneration` _integer_ | ObservedGeneration is the generation last processed by the controller. |  | Optional: \{\} <br /> |
| `endpoint` _string_ | Endpoint is the in-cluster serving endpoint URL. |  | Optional: \{\} <br /> |
| `stableRevision` _[RevisionStatus](#revisionstatus)_ | StableRevision is the revision currently receiving traffic. |  | Optional: \{\} <br /> |
| `candidateRevision` _[RevisionStatus](#revisionstatus)_ | CandidateRevision is the revision being rolled out, if any. |  | Optional: \{\} <br /> |
| `failedRevision` _[RevisionStatus](#revisionstatus)_ | FailedRevision is a revision that failed to roll out. The controller does<br />not automatically retry it; a spec change (new revision) is required, or<br />setting the decisionmodel.io/retry annotation to a new token re-attempts<br />the same revision. |  | Optional: \{\} <br /> |
| `previousRevision` _[PreviousRevisionStatus](#previousrevisionstatus)_ | PreviousRevision is the revision that was stable immediately before the<br />most recent promotion. It may linger for a short grace period after<br />promotedAt so the new revision's endpoints populate before it is removed. |  | Optional: \{\} <br /> |
| `lastRetryToken` _string_ | LastRetryToken is the value of the decisionmodel.io/retry annotation the<br />controller last consumed to clear a failed revision. |  | Optional: \{\} <br /> |
| `replicas` _[ReplicaStatus](#replicastatus)_ | Replicas reports desired and model-ready replica counts. |  | Optional: \{\} <br /> |
| `evaluation` _[EvaluationStatus](#evaluationstatus)_ | Evaluation records the most recent eval-gated rollout result. |  | Optional: \{\} <br /> |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#condition-v1-meta) array_ | Conditions represent the latest available observations of the object's state. |  | Optional: \{\} <br /> |


#### EvaluationResult

_Underlying type:_ _string_

EvaluationResult is the outcome of applying the evaluation gate.



_Appears in:_
- [EvaluationStatus](#evaluationstatus)

| Field | Description |
| --- | --- |
| `Passed` | EvaluationPassed means the candidate met every configured gate.<br /> |
| `Failed` | EvaluationFailed means the candidate failed at least one gate.<br /> |


#### EvaluationSpec



EvaluationSpec configures the eval-gated rollout.



_Appears in:_
- [RolloutSpec](#rolloutspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `datasetRef` _[DatasetRef](#datasetref)_ | DatasetRef points at the golden dataset (JSONL). |  |  |
| `minAccuracy` _string_ | MinAccuracy is the minimum accuracy (decimal string in [0,1], e.g. "0.90")<br />the candidate must reach to be promoted. |  | Pattern: `^(0(\.[0-9]+)?\|1(\.0+)?)$` <br /> |
| `maxAccuracyDrop` _string_ | MaxAccuracyDrop is the maximum tolerated accuracy drop vs the stable<br />baseline (decimal string, e.g. "0.02"). Only enforced when a stable<br />revision exists to provide a baseline; empty means no drop constraint. |  | Pattern: `^(0(\.[0-9]+)?\|1(\.0+)?)$` <br />Optional: \{\} <br /> |
| `maxCases` _integer_ | MaxCases caps how many dataset cases are used (first N). | 500 | Maximum: 5000 <br />Minimum: 1 <br />Optional: \{\} <br /> |
| `maxECE` _string_ | MaxECE is the maximum tolerated Expected Calibration Error (decimal string<br />in [0,1], e.g. "0.10"). Empty disables the absolute ECE gate. |  | Pattern: `^(0(\.[0-9]+)?\|1(\.0+)?)$` <br />Optional: \{\} <br /> |
| `maxECEIncrease` _string_ | MaxECEIncrease is the maximum tolerated ECE increase vs the stable baseline<br />(decimal string). Only enforced when a stable revision exists to provide a<br />baseline; empty disables the relative ECE gate. |  | Pattern: `^(0(\.[0-9]+)?\|1(\.0+)?)$` <br />Optional: \{\} <br /> |


#### EvaluationStatus



EvaluationStatus records the outcome of an eval-gated rollout evaluation.



_Appears in:_
- [DecisionModelStatus](#decisionmodelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `revision` _string_ | Revision is the revision hash the evaluation was run against. |  |  |
| `accuracy` _string_ | Accuracy is the candidate accuracy (decimal string). |  |  |
| `baselineAccuracy` _string_ | BaselineAccuracy is the stable revision's accuracy for this dataset, if known. |  |  |
| `cases` _integer_ | Cases is the number of questions scored. |  |  |
| `failedCases` _integer_ | FailedCases is the number of questions that were wrong or unanswerable. |  |  |
| `ece` _string_ | ECE is the candidate's expected calibration error (decimal string). |  |  |
| `brier` _string_ | Brier is the candidate's Brier score (decimal string). |  |  |
| `baselineEce` _string_ | BaselineECE is the stable revision's ECE for this dataset, if known. |  |  |
| `minAccuracy` _string_ | MinAccuracy echoes the accuracy floor the gate applied, if any. |  | Optional: \{\} <br /> |
| `maxAccuracyDrop` _string_ | MaxAccuracyDrop echoes the accuracy-drop limit the gate applied, if any. |  | Optional: \{\} <br /> |
| `maxEce` _string_ | MaxECE echoes the absolute ECE limit the gate applied, if any. |  | Optional: \{\} <br /> |
| `maxEceIncrease` _string_ | MaxECEIncrease echoes the relative ECE-increase limit the gate applied, if any. |  | Optional: \{\} <br /> |
| `result` _[EvaluationResult](#evaluationresult)_ | Result is the gate outcome: Passed or Failed. |  | Enum: [Passed Failed] <br />Optional: \{\} <br /> |
| `reason` _string_ | Reason is the gate message (e.g. the failing comparison), human-readable. |  | Optional: \{\} <br /> |
| `policyHash` _string_ | PolicyHash is a hash of the effective evaluation policy (thresholds,<br />datasetRef, maxCases) this result was produced under. A parked candidate in<br />AwaitingPromotion whose current policy hash differs is re-evaluated rather<br />than promoted on the stale result. |  | Optional: \{\} <br /> |
| `completedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | CompletedAt is when the evaluation finished. |  | Optional: \{\} <br /> |


#### PreviousRevisionStatus



PreviousRevisionStatus records the revision demoted by the latest promotion
and when the promotion happened, so its workloads may linger for a grace
period before being garbage-collected.



_Appears in:_
- [DecisionModelStatus](#decisionmodelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `hash` _string_ | Hash is the demoted revision's hash. |  |  |
| `promotedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | PromotedAt is when the newer revision was promoted (this one demoted). |  | Optional: \{\} <br /> |


#### PromotionPolicy

_Underlying type:_ _string_

PromotionPolicy selects how a model-ready candidate is promoted.



_Appears in:_
- [RolloutSpec](#rolloutspec)

| Field | Description |
| --- | --- |
| `Automatic` | PromotionAutomatic promotes a candidate as soon as it is model-ready (and,<br />when evaluation is configured, has passed the gate).<br /> |
| `EvaluationGated` | PromotionEvaluationGated requires rollout.evaluation and lets the gate<br />decide promotion. Rejected by CEL when evaluation is unset.<br /> |
| `Manual` | PromotionManual holds a passed candidate in AwaitingPromotion until a human<br />approves it via the decisionmodel.io/promote annotation.<br /> |


#### ReplicaStatus



ReplicaStatus reports desired and model-ready replica counts.



_Appears in:_
- [DecisionModelStatus](#decisionmodelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `desired` _integer_ | Desired is the desired number of serving replicas. |  | Optional: \{\} <br /> |
| `modelReady` _integer_ | ModelReady is the number of replicas that passed the model readiness gate. |  | Optional: \{\} <br /> |


#### RevisionStatus



RevisionStatus records the resolved, model-affecting identity of a revision.

Model-affecting fields (engine, model, digest, device, image, resources)
together reproduce the revision hash, so a stable revision can be rendered
from its own recorded state rather than the current spec (which may already
describe a different, pending candidate).



_Appears in:_
- [DecisionModelStatus](#decisionmodelstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `hash` _string_ | Hash is the short revision hash of the model-affecting spec fields. |  |  |
| `engine` _string_ | Engine is the serving runtime for this revision. |  | Optional: \{\} <br /> |
| `model` _string_ | Model is the model name for this revision. |  |  |
| `digest` _string_ | Digest is the resolved immutable digest for this revision. |  |  |
| `device` _string_ | Device is the target device for this revision. |  |  |
| `image` _string_ | Image is the resolved serving container image for this revision. |  | Optional: \{\} <br /> |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#resourcerequirements-v1-core)_ | Resources are the compute resource requirements of this revision's serving<br />container. |  | Optional: \{\} <br /> |
| `precision` _string_ | Precision is the quantization level reported by the engine (e.g. F32, F16). |  |  |
| `placement` _string_ | Placement is a short hash of spec.scheduling (nodeSelector, tolerations,<br />affinity, runtimeClassName) as it was when this revision was created, or<br />"none" when no scheduling was set. Empty only on a revision recorded by an<br />older operator version, which the controller adopts on its first reconcile.<br />Placement is part of a revision's identity: a change of scheduling starts a<br />new revision (blue-green, own store) instead of rolling the running<br />Deployment in place. A hash is recorded, not the spec, because Affinity is a<br />very large schema and this type appears three times in the CRD. |  | Optional: \{\} <br /> |
| `reason` _string_ | Reason is a short machine reason for why this revision failed. Set only on<br />status.failedRevision; empty on stable/candidate. |  | Optional: \{\} <br /> |
| `message` _string_ | Message is a human-readable explanation of a failure. Set only on<br />status.failedRevision; empty on stable/candidate. |  | Optional: \{\} <br /> |
| `failedAt` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#time-v1-meta)_ | FailedAt is when this revision was recorded as failed. Set only on<br />status.failedRevision; nil on stable/candidate. |  | Optional: \{\} <br /> |


#### RolloutSpec



RolloutSpec configures rollout behaviour.



_Appears in:_
- [DecisionModelSpec](#decisionmodelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `evaluation` _[EvaluationSpec](#evaluationspec)_ | Evaluation gates promotion on a golden-dataset accuracy check. When unset,<br />a candidate is promoted as soon as all its Pods are model-ready. |  | Optional: \{\} <br /> |
| `promotion` _[PromotionPolicy](#promotionpolicy)_ | Promotion selects how a candidate that passed ModelReady is promoted:<br />  - Automatic: promote as soon as the candidate is model-ready (and, when<br />    evaluation is configured, has passed the gate).<br />  - EvaluationGated: like Automatic but requires rollout.evaluation to be<br />    set (rejected by CEL otherwise); the gate decides promotion.<br />  - Manual: hold the candidate in AwaitingPromotion until a human sets the<br />    annotation decisionmodel.io/promote to the candidate's revision hash<br />    (evaluation still runs when configured).<br />When unset the effective policy is EvaluationGated if rollout.evaluation is<br />set, else Automatic. The deprecated manualPromotion:true is an alias for<br />Manual; setting both promotion and manualPromotion:true to disagreeing<br />values is rejected by CEL. |  | Enum: [Automatic EvaluationGated Manual] <br />Optional: \{\} <br /> |
| `manualPromotion` _boolean_ | ManualPromotion holds a candidate that passed its gate (model-ready, plus<br />evaluation when configured) in phase AwaitingPromotion until a human<br />approves it by setting the annotation decisionmodel.io/promote to the<br />candidate's revision hash. The stable revision keeps serving meanwhile.<br />There is no progress timeout while waiting. The very first revision of a<br />DecisionModel (no stable revision yet) is promoted without approval, since<br />there is no traffic to protect. The approval annotation is removed once the<br />promotion has been persisted.<br />Deprecated: use promotion: Manual. manualPromotion:true keeps working as an<br />alias for promotion: Manual. | false | Optional: \{\} <br /> |
| `timeouts` _[RolloutTimeouts](#rollouttimeouts)_ | Timeouts overrides the progress timeouts of a rollout. Unset fields keep<br />the built-in defaults. Large models (tens of GB) need more than the<br />defaults on a cold node. Not part of the revision hash; a change applies to<br />the phase timeouts immediately, but a prefetch Job that already exists keeps<br />the deadline it was created with. |  | Optional: \{\} <br /> |


#### RolloutTimeouts



RolloutTimeouts overrides the progress timeouts of a rollout. Each value must
be between 1m and 24h.



_Appears in:_
- [RolloutSpec](#rolloutspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `caching` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#duration-v1-meta)_ | Caching bounds the Caching phase and is also used as the prefetch Job's<br />activeDeadlineSeconds. Default 30m. |  | MaxLength: 32 <br />Pattern: `^([0-9]+(\.[0-9]+)?(ns\|us\|ms\|s\|m\|h))+$` <br />Type: string <br />Optional: \{\} <br /> |
| `starting` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#duration-v1-meta)_ | Starting bounds the Starting phase (candidate Pods becoming model-ready)<br />and, when set, also the background model warmup on each Pod. When unset the<br />phase timeout is 10m and the warmup bound stays 2m. Default 10m. |  | MaxLength: 32 <br />Pattern: `^([0-9]+(\.[0-9]+)?(ns\|us\|ms\|s\|m\|h))+$` <br />Type: string <br />Optional: \{\} <br /> |
| `evaluating` _[Duration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#duration-v1-meta)_ | Evaluating bounds the Evaluating phase and each golden-dataset run.<br />Default 10m. |  | MaxLength: 32 <br />Pattern: `^([0-9]+(\.[0-9]+)?(ns\|us\|ms\|s\|m\|h))+$` <br />Type: string <br />Optional: \{\} <br /> |


#### SchedulingSpec



SchedulingSpec is a passthrough of standard Pod scheduling controls.



_Appears in:_
- [DecisionModelSpec](#decisionmodelspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `nodeSelector` _object (keys:string, values:string)_ | NodeSelector is a selector which must be true for the Pod to fit on a node. |  | Optional: \{\} <br /> |
| `tolerations` _[Toleration](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#toleration-v1-core) array_ | Tolerations allow the Pod to schedule onto nodes with matching taints. |  | Optional: \{\} <br /> |
| `affinity` _[Affinity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#affinity-v1-core)_ | Affinity constrains Pod scheduling. |  | Optional: \{\} <br /> |
| `runtimeClassName` _string_ | RuntimeClassName selects the RuntimeClass for serving Pods, e.g. "nvidia"<br />when the NVIDIA GPU Operator does not make it the default runtime. It is<br />applied to serving Pods only (the prefetch Job needs no GPU runtime).<br />Because it is part of spec.scheduling, changing it changes the revision's<br />placement hash and therefore starts a new revision (blue-green), like any<br />other scheduling change. |  | MaxLength: 253 <br />MinLength: 1 <br />Pattern: `^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$` <br />Optional: \{\} <br /> |


