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
	"errors"
	"fmt"
	"net/url"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// engineOrDefault returns the configured engine or the "ollaya" default.
func engineOrDefault(e string) string {
	if e == "" {
		return "ollaya"
	}
	return e
}

// deviceOrDefault returns the configured device or the "cpu" default.
func deviceOrDefault(d string) string {
	if d == "" {
		return engine.DeviceCPU
	}
	return d
}

// desiredReplicas returns spec.replicas, defaulting to 1 when unset.
func desiredReplicas(dm *decisionmodelv1alpha1.DecisionModel) int32 {
	if dm.Spec.Replicas != nil {
		return *dm.Spec.Replicas
	}
	return 1
}

// storeName is the legacy shared model-store PVC name. It is still
// read for migration of a stable revision created before per-revision PVCs.
func storeName(dm *decisionmodelv1alpha1.DecisionModel) string {
	return dm.Name + "-store"
}

// storeNameRev is the per-revision model-store PVC name. Each revision
// owns its store so a blue-green rollout on RWO storage does not deadlock on
// Multi-Attach (the candidate lands on another node than the stable). It is
// shorter than prefetchName (<dm>-prefetch-<rev>), so any dm.Name that keeps the
// prefetch Job name within 63 chars keeps this PVC name within 63 chars too.
func storeNameRev(dm *decisionmodelv1alpha1.DecisionModel, rev string) string {
	return dm.Name + "-store-" + rev
}

// revisionName is the serving Deployment name for a revision.
func revisionName(dm *decisionmodelv1alpha1.DecisionModel, rev string) string {
	return dm.Name + "-" + rev
}

// prefetchName is the prefetch Job name for a revision.
func prefetchName(dm *decisionmodelv1alpha1.DecisionModel, rev string) string {
	return dm.Name + "-prefetch-" + rev
}

// pdbName is the PodDisruptionBudget name for a revision.
func pdbName(dm *decisionmodelv1alpha1.DecisionModel, rev string) string {
	return dm.Name + "-" + rev
}

// revisionLabels are the identifying labels for a revision's owned objects.
func revisionLabels(dm *decisionmodelv1alpha1.DecisionModel, rev string) map[string]string {
	return map[string]string{
		decisionmodelv1alpha1.LabelName:     dm.Name,
		decisionmodelv1alpha1.LabelRevision: rev,
	}
}

// servingImage returns the image the engine uses for the serving container,
// honouring an explicit spec override.
func servingImage(eng engine.Engine, p engine.Params) string {
	if p.Image != "" {
		return p.Image
	}
	spec := eng.ServingPodSpec(p)
	if len(spec.Containers) > 0 {
		return spec.Containers[0].Image
	}
	return p.Image
}

// intstrFromInt32 builds an IntOrString from an int32 port.
func intstrFromInt32(p int32) intstr.IntOrString {
	return intstr.FromInt32(p)
}

// applyScheduling applies nodeSelector/tolerations/affinity passthrough to a PodSpec.
func applyScheduling(spec *corev1.PodSpec, s *decisionmodelv1alpha1.SchedulingSpec) {
	if s == nil {
		return
	}
	if s.NodeSelector != nil {
		spec.NodeSelector = s.NodeSelector
	}
	if s.Tolerations != nil {
		spec.Tolerations = s.Tolerations
	}
	if s.Affinity != nil {
		spec.Affinity = s.Affinity
	}
}

// gpuTaintKey is the taint key GPU node pools commonly carry (EKS managed GPU
// nodegroups, AKS) so only GPU workloads schedule there.
const gpuTaintKey = "nvidia.com/gpu"

// applyGPUToleration adds a toleration for the nvidia.com/gpu:NoSchedule taint to
// serving Pods when device is cuda, so they can land on tainted/dedicated GPU
// pools. It is a no-op for cpu, and never overrides a user toleration that
// already covers the key (so an explicit value/effect from spec.scheduling
// wins). Not applied to the prefetch Job, which no longer uses the GPU image.
func applyGPUToleration(spec *corev1.PodSpec, device string) {
	if device != engine.DeviceCUDA {
		return
	}
	// Skip only when an existing toleration really tolerates the GPU taint, in
	// Kubernetes' own semantics: a wildcard (empty key + Exists), an empty effect,
	// or an exact match all count; a toleration for the same key but another
	// effect (e.g. NoExecute only) does not.
	gpuTaint := corev1.Taint{Key: gpuTaintKey, Effect: corev1.TaintEffectNoSchedule}
	for i := range spec.Tolerations {
		// enableComparisonOperators=false: a Lt/Gt toleration is treated as not
		// matching, so we add our default; a redundant toleration is harmless,
		// wrongly omitting it would leave the Pod unschedulable.
		if spec.Tolerations[i].ToleratesTaint(logf.Log.WithName("gpu-toleration"), &gpuTaint, false) {
			return
		}
	}
	// Copy before appending: spec.Tolerations may alias the DecisionModel's own
	// slice (applyScheduling assigns it), and append could write into its backing
	// array.
	tols := make([]corev1.Toleration, 0, len(spec.Tolerations)+1)
	tols = append(tols, spec.Tolerations...)
	spec.Tolerations = append(tols, corev1.Toleration{
		Key:      gpuTaintKey,
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	})
}

// archLabel is the well-known node label for the CPU architecture.
const archLabel = "kubernetes.io/arch"

// archAmd64 is the only architecture the CUDA image is published for (Ollaya
// ships :<version>-cuda for amd64 only; arm64 GPU nodes such as Graviton/Grace
// would pull an image that cannot run).
const archAmd64 = "amd64"

// hasArchAffinity reports whether any node-affinity term (required or
// preferred) already constrains kubernetes.io/arch.
func hasArchAffinity(a *corev1.Affinity) bool {
	if a == nil || a.NodeAffinity == nil {
		return false
	}
	onArch := func(reqs []corev1.NodeSelectorRequirement) bool {
		for _, r := range reqs {
			if r.Key == archLabel {
				return true
			}
		}
		return false
	}
	na := a.NodeAffinity
	if req := na.RequiredDuringSchedulingIgnoredDuringExecution; req != nil {
		for _, t := range req.NodeSelectorTerms {
			if onArch(t.MatchExpressions) || onArch(t.MatchFields) {
				return true
			}
		}
	}
	for _, p := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		if onArch(p.Preference.MatchExpressions) || onArch(p.Preference.MatchFields) {
			return true
		}
	}
	return false
}

// applyCUDAArch pins serving Pods of a cuda DecisionModel to amd64 nodes, since
// the CUDA image is amd64 only. It yields to the user: a kubernetes.io/arch key
// in scheduling.nodeSelector or any node-affinity term on that key is left
// alone. No-op for cpu (the CPU image is multi-arch) and never applied to the
// prefetch Job (it uses the CPU image). Call after applyScheduling so the user's
// settings are already in the PodSpec.
func applyCUDAArch(spec *corev1.PodSpec, device string) {
	if device != engine.DeviceCUDA {
		return
	}
	if _, ok := spec.NodeSelector[archLabel]; ok || hasArchAffinity(spec.Affinity) {
		return
	}
	// Copy: spec.NodeSelector may alias the DecisionModel's own map.
	sel := make(map[string]string, len(spec.NodeSelector)+1)
	for k, v := range spec.NodeSelector {
		sel[k] = v
	}
	sel[archLabel] = archAmd64
	spec.NodeSelector = sel
}

// applyRuntimeClass sets spec.scheduling.runtimeClassName on a serving PodSpec.
// It is deliberately separate from applyScheduling: the prefetch Job only pulls
// weights and needs no GPU runtime, so it must not get the RuntimeClass.
func applyRuntimeClass(spec *corev1.PodSpec, s *decisionmodelv1alpha1.SchedulingSpec) {
	if s == nil || s.RuntimeClassName == nil {
		return
	}
	spec.RuntimeClassName = s.RuntimeClassName
}

// proxyEnvKeys are the only variables copied from the operator's environment to
// the prefetch container (upper- and lower-case variants, as Go's
// http.ProxyFromEnvironment and most CLIs read both).
var proxyEnvKeys = []string{
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
	"http_proxy", "https_proxy", "no_proxy",
}

// parseProxySetting parses a proxy environment value. A value without "://" is
// a scheme-less "host:port" (very common) and is parsed as "http://"+value;
// plain url.Parse would read "host:3128" as scheme "host". A value that already
// has a scheme is parsed as is and is NOT retried with a prefix: net/http's
// retry turns "http://a b:p@host" into "http://http://a b:p@host", which parses
// "successfully" with the credentials buried in the path.
func parseProxySetting(v string) (*url.URL, error) {
	if !strings.Contains(v, "://") {
		v = "http://" + v // NOSONAR: scheme-less proxy value, same default as net/http
	}
	return url.Parse(v)
}

// proxyValueShareable reports whether a proxy env value may be copied into a
// prefetch Job. Prefetch Jobs live in tenant namespaces and their spec is readable
// by anyone who can get Jobs there, so a value carrying credentials must not be
// copied.
//
// The credential test does not depend on how the value parses: credentials in a
// proxy URL need userinfo syntax, which needs an "@", so any value containing one
// is refused. (A parser-based test alone was fooled by a value such as
// "http://a b:p@host:1", which parses "successfully" with the userinfo hidden in
// the path.) A proxy URL with an "@" that is not userinfo is not a thing worth
// supporting: skipping it fails closed.
//
// A value that cannot be parsed is not copied either: it cannot be proven free of
// credentials, the operator's own HTTP client cannot use it, and skipping costs a
// visible PrefetchFailed instead of an invisible leak. NO_PROXY/no_proxy is a host
// list, not a URL, and is always shareable.
func proxyValueShareable(name, value string) bool {
	if strings.EqualFold(name, "NO_PROXY") {
		return true
	}
	if strings.Contains(value, "@") {
		return false
	}
	u, err := parseProxySetting(value)
	return err == nil && u.User == nil
}

// ProxyEnvFromEnviron builds the proxy env list for prefetch Jobs from the
// operator's own environment via getenv (os.Getenv in production). Only the keys
// in proxyEnvKeys that are set to a non-empty value are considered, in a fixed
// order. It is read once at startup and injected into the reconciler, so a tenant
// can never influence it through a DecisionModel and tests do not touch the
// process environment.
//
// Values that carry credentials (or cannot be parsed) are NOT returned: the
// operator itself still uses them, but they are not written into tenant Jobs.
// skipped lists the key names that were left out, so the caller can warn
// once at startup. It holds names only: never the value, and never a parse error,
// because net/url errors embed the input string.
func ProxyEnvFromEnviron(getenv func(string) string) (env []corev1.EnvVar, skipped []string) {
	for _, k := range proxyEnvKeys {
		v := getenv(k)
		if v == "" {
			continue
		}
		if !proxyValueShareable(k, v) {
			skipped = append(skipped, k)
			continue
		}
		env = append(env, corev1.EnvVar{Name: k, Value: v})
	}
	return env, skipped
}

// applyProxyEnv adds the given proxy variables to every container of a PodSpec
// (the prefetch Job has one) unless the container already defines that variable.
// The upper- and lower-case spellings of one variable (HTTP_PROXY / http_proxy)
// count as the same key: if the container already sets either, neither is added,
// so a container-level choice is never half-overridden by the operator's other
// spelling. Only prefetch Pods use it; serving Pods never talk to the registry.
func applyProxyEnv(spec *corev1.PodSpec, env []corev1.EnvVar) {
	if len(env) == 0 {
		return
	}
	for i := range spec.Containers {
		c := &spec.Containers[i]
		// Computed before adding, so when neither spelling is present both of the
		// operator's spellings are added together.
		present := make(map[string]struct{}, len(c.Env))
		for _, e := range c.Env {
			present[strings.ToLower(e.Name)] = struct{}{}
		}
		for _, e := range env {
			if _, ok := present[strings.ToLower(e.Name)]; !ok {
				c.Env = append(c.Env, e)
			}
		}
	}
}

// jobComplete reports whether a Job has a Complete condition True.
func jobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// jobFailed reports whether a Job has a Failed condition True.
func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// containersReady reports whether the Pod's ContainersReady condition is True.
func containersReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podReady reports whether the Pod's Ready condition is True. With a readiness
// gate, Ready is True only once ContainersReady AND every gate are True, and it is
// the condition the EndpointSlice controller uses to put the Pod behind the
// Service. The gate alone is not enough to switch traffic.
func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// servingReady reports whether a Pod can take traffic right now: its model-ready
// gate is True, Kubernetes has set PodReady (so it is in the EndpointSlice), and
// it is not terminating. A Pod whose gate this reconcile has just patched does
// NOT count yet: PodReady follows the gate asynchronously, and the Pod update
// event brings the next reconcile.
func servingReady(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && gateTrue(pod) && podReady(pod)
}

// gateTrue reports whether the model-ready readiness gate condition is True.
func gateTrue(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podConditionHasStatus reports whether the model-ready gate currently has the
// given status. A missing condition counts as "not that status".
func podConditionHasStatus(pod *corev1.Pod, status corev1.ConditionStatus) bool {
	for _, c := range pod.Status.Conditions {
		if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
			return c.Status == status
		}
	}
	return false
}

// errSecretNotAllowed is returned when a referenced Secret lacks the required
// opt-in label (confused-deputy guard).
var errSecretNotAllowed = errors.New("secret not allowed")

// errResourceConflict reports that an object with the name this DM wants to
// manage exists but is NOT controlled by this DM (different/absent controller
// owner UID). The reconciler must never update or delete such an object; it
// surfaces Degraded=ResourceConflict and waits for an event.
var errResourceConflict = errors.New("resource not owned by this DecisionModel")

// errPodListFailed wraps a failure to List (or resolve the ownership of) the
// Pods of a revision. It must abort the reconcile (workqueue backoff) rather
// than be mistaken for "0 ready Pods", which would wrongly flip Degraded /
// progress timeouts.
var errPodListFailed = errors.New("listing revision pods failed")

// ownedBy reports whether obj is controlled by dm (controller OwnerReference UID
// == dm.UID). Labels are attacker-settable, so every write/delete path checks
// the owner UID before touching an object it located by name.
func ownedBy(obj metav1.Object, dm *decisionmodelv1alpha1.DecisionModel) bool {
	c := metav1.GetControllerOf(obj)
	return c != nil && c.UID == dm.UID
}

// apiKeyChecksum returns a short hash of the API key value, or "" for an empty
// key (no auth). Only the hash is placed on the Pod template, never the value.
func apiKeyChecksum(key string) string {
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:16]
}

// setStatusCondition sets a condition on dm.Status.Conditions, stamping
// ObservedGeneration with dm.Generation first (tech.md rule: every condition
// carries observedGeneration). Use this instead of meta.SetStatusCondition
// directly so no condition is ever written without it.
func setStatusCondition(dm *decisionmodelv1alpha1.DecisionModel, cond metav1.Condition) {
	cond.ObservedGeneration = dm.Generation
	meta.SetStatusCondition(&dm.Status.Conditions, cond)
}

// requireAPIKeyLabel returns errSecretNotAllowed unless the Secret carries the
// decisionmodel.io/api-key=true label.
func requireAPIKeyLabel(labels map[string]string) error {
	if labels[decisionmodelv1alpha1.LabelAPIKey] != annotationTrue {
		return fmt.Errorf("%w: missing label %s=true", errSecretNotAllowed, decisionmodelv1alpha1.LabelAPIKey)
	}
	return nil
}

// labelsEqual reports whether two label maps are equal.
func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// accessModesShareable reports whether the access modes allow a PVC to be
// mounted by Pods on multiple nodes (RWX or ROX).
func accessModesShareable(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == corev1.ReadWriteMany || m == corev1.ReadOnlyMany {
			return true
		}
	}
	return false
}

// storageClassEqual compares a desired storage-class name (spec, a *string) with
// a PVC's live StorageClassName (*string), treating nil and "" as "no class set"
// so they compare equal.
func storageClassEqual(want, have *string) bool {
	w, h := "", ""
	if want != nil {
		w = *want
	}
	if have != nil {
		h = *have
	}
	return w == h
}

// accessModesEqual compares two access-mode sets ignoring order and duplicates.
func accessModesEqual(a, b []corev1.PersistentVolumeAccessMode) bool {
	set := func(ms []corev1.PersistentVolumeAccessMode) map[corev1.PersistentVolumeAccessMode]struct{} {
		s := make(map[corev1.PersistentVolumeAccessMode]struct{}, len(ms))
		for _, m := range ms {
			s[m] = struct{}{}
		}
		return s
	}
	sa, sb := set(a), set(b)
	if len(sa) != len(sb) {
		return false
	}
	for m := range sa {
		if _, ok := sb[m]; !ok {
			return false
		}
	}
	return true
}

// containersReadyTime returns the LastTransitionTime of the Pod's ContainersReady
// condition.
func containersReadyTime(pod *corev1.Pod) (metav1.Time, bool) {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.LastTransitionTime, true
		}
	}
	return metav1.Time{}, false
}

// containersReadyNewerThanGate reports whether the Pod's ContainersReady
// condition transitioned more recently than the model-ready gate — i.e. the
// containers restarted and became ready again after the gate was last set, so
// the pinned model may have been lost and the Pod must be re-probed.
func containersReadyNewerThanGate(pod *corev1.Pod) bool {
	var crLTT, gateLTT metav1.Time
	var haveCR, haveGate bool
	for _, c := range pod.Status.Conditions {
		switch {
		case c.Type == corev1.ContainersReady:
			crLTT, haveCR = c.LastTransitionTime, true
		case string(c.Type) == decisionmodelv1alpha1.ModelReadyGate:
			gateLTT, haveGate = c.LastTransitionTime, true
		}
	}
	if !haveCR || !haveGate {
		return false
	}
	return crLTT.After(gateLTT.Time)
}

// deploymentSpecHash hashes the replica count and serving PodSpec so no-op
// reconciles can skip the Deployment Update.
func deploymentSpecHash(replicas int32, podSpec corev1.PodSpec, strategy appsv1.DeploymentStrategy, tmplAnnotations map[string]string) string {
	payload := struct {
		Replicas    int32                     `json:"replicas"`
		Pod         corev1.PodSpec            `json:"pod"`
		Strategy    appsv1.DeploymentStrategy `json:"strategy"`
		Annotations map[string]string         `json:"tmplAnnotations"`
	}{Replicas: replicas, Pod: podSpec, Strategy: strategy, Annotations: tmplAnnotations}
	b, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16]
}

// deploymentMatchesDesired reports whether the live Deployment still matches the
// desired state for the fields the operator owns: spec.replicas, the pod
// template labels, and the serving PodSpec. The PodSpec is compared with
// DeepDerivative (desired is a subset of live) because the API server defaults
// many fields the operator never sets; a plain DeepEqual would always differ.
// A false result means the live object drifted (e.g. manual scale to 0 or a
// removed readiness gate) and must be reconciled back.
func deploymentMatchesDesired(
	dep *appsv1.Deployment, replicas int32, labels map[string]string, podSpec corev1.PodSpec,
	strategy appsv1.DeploymentStrategy, tmplAnnotations map[string]string,
) bool {
	live := int32(1)
	if dep.Spec.Replicas != nil {
		live = *dep.Spec.Replicas
	}
	if live != replicas {
		return false
	}
	if !strategyMatches(dep.Spec.Strategy, strategy) {
		return false
	}
	if !labelsEqual(dep.Spec.Template.Labels, labels) {
		return false
	}
	// Pod template annotations are owned too: an injected annotation (e.g.
	// sidecar.istio.io/inject) must be reverted, but a user rollout-restart
	// (kubectl.kubernetes.io/restartedAt) is tolerated so it is neither undone nor
	// looped.
	if !templateAnnotationsMatch(dep.Spec.Template.Annotations, tmplAnnotations) {
		return false
	}
	// DeepDerivative only checks that desired ⊆ live, so an *added* container
	// (an injected sidecar), initContainer, or volume in the live template is not
	// detected. Those are fields the operator owns, so an extra one is drift to be
	// reverted: compare the names exactly.
	if !sameNamedSet(containerNames(podSpec.Containers), containerNames(dep.Spec.Template.Spec.Containers)) ||
		!sameNamedSet(containerNames(podSpec.InitContainers), containerNames(dep.Spec.Template.Spec.InitContainers)) ||
		!sameNamedSet(volumeNames(podSpec.Volumes), volumeNames(dep.Spec.Template.Spec.Volumes)) {
		return false
	}
	return equality.Semantic.DeepDerivative(podSpec, dep.Spec.Template.Spec)
}

// containerNames / volumeNames extract the names from a slice, for exact-set
// drift comparison (an added sidecar / volume must be reverted —.
func containerNames(cs []corev1.Container) []string {
	out := make([]string, len(cs))
	for i := range cs {
		out[i] = cs[i].Name
	}
	return out
}

func volumeNames(vs []corev1.Volume) []string {
	out := make([]string, len(vs))
	for i := range vs {
		out[i] = vs[i].Name
	}
	return out
}

// restartedAtAnnotation is the annotation `kubectl rollout restart` stamps on a
// Deployment's Pod template. The operator tolerates it (never writes it, never
// strips it) so a user restart is neither undone nor loops.
const restartedAtAnnotation = "kubectl.kubernetes.io/restartedAt"

// templateAnnotationsMatch reports whether the live Pod-template annotations
// equal the desired set, ignoring the tolerated restartedAt key. Any extra key
// (an injected sidecar annotation) or a missing/changed desired key is drift.
func templateAnnotationsMatch(live, desired map[string]string) bool {
	// Every desired key must be present with the desired value.
	for k, v := range desired {
		if live[k] != v {
			return false
		}
	}
	// No extra keys beyond the desired set and the tolerated restartedAt.
	for k := range live {
		if k == restartedAtAnnotation {
			continue
		}
		if _, ok := desired[k]; !ok {
			return false
		}
	}
	return true
}

// mergeTemplateAnnotations returns the desired template annotations, preserving a
// live restartedAt (so restoring drift does not undo a user rollout restart). It
// drops every other live annotation — those are drift to be reverted.
func mergeTemplateAnnotations(desired, live map[string]string) map[string]string {
	var out map[string]string
	if len(desired) > 0 {
		out = make(map[string]string, len(desired)+1)
		for k, v := range desired {
			out[k] = v
		}
	}
	if v, ok := live[restartedAtAnnotation]; ok {
		if out == nil {
			out = make(map[string]string, 1)
		}
		out[restartedAtAnnotation] = v
	}
	return out
}

// sameNamedSet reports whether a and b contain the same set of names.
func sameNamedSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, n := range a {
		seen[n] = struct{}{}
	}
	for _, n := range b {
		if _, ok := seen[n]; !ok {
			return false
		}
	}
	return true
}

// setPodCondition inserts or updates a Pod status condition in place,
// preserving LastTransitionTime when the status has not changed.
func setPodCondition(pod *corev1.Pod, cond corev1.PodCondition) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == cond.Type {
			if pod.Status.Conditions[i].Status == cond.Status {
				cond.LastTransitionTime = pod.Status.Conditions[i].LastTransitionTime
			}
			pod.Status.Conditions[i] = cond
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, cond)
}

// setReadyConditions marks a DecisionModel fully ready. When cacheDegraded is
// true, the Degraded condition set by the cache-sharing guard is preserved
// rather than cleared.
func setReadyConditions(dm *decisionmodelv1alpha1.DecisionModel, cacheDegraded bool) {
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionModelReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonModelReady,
		Message: "all serving Pods report the expected digest on the expected device",
	})
	setStatusCondition(dm, metav1.Condition{
		Type:    decisionmodelv1alpha1.ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  reasonReady,
		Message: "revision promoted and serving",
	})
	if !cacheDegraded {
		setStatusCondition(dm, metav1.Condition{
			Type:    decisionmodelv1alpha1.ConditionDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  reasonReady,
			Message: "healthy",
		})
	}
}

// selinuxLevel returns a stable SELinux MCS level ("s0:cX,cY", X<Y, 0..1023)
// derived from the DecisionModel's namespace/name.
//
// On SELinux-enforcing nodes (e.g. Bottlerocket) whose CSI driver does not
// support SELinux mount options, the runtime relabels the store volume to the
// MCS level of the Pod that mounts it. Without a shared level the prefetch Pod
// labels the weights with its own random categories and the serving Pods
// (different random categories) get EACCES. All Pods of one DecisionModel share
// its store PVC, so they share one level. Ignored on non-SELinux nodes.
func selinuxLevel(dm *decisionmodelv1alpha1.DecisionModel) string {
	sum := sha256.Sum256([]byte(dm.Namespace + "/" + dm.Name))
	a := (int(sum[0])<<8 | int(sum[1])) % 1024
	b := (int(sum[2])<<8 | int(sum[3])) % 1023
	if b >= a {
		b++ // distinct from a
	}
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("s0:c%d,c%d", a, b)
}

// applySELinuxLevel sets the DecisionModel's shared SELinux level on a PodSpec
// unless the engine already chose SELinux options.
func applySELinuxLevel(spec *corev1.PodSpec, dm *decisionmodelv1alpha1.DecisionModel) {
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.SecurityContext.SELinuxOptions != nil {
		return
	}
	spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{Level: selinuxLevel(dm)}
}

// deploymentStrategy picks the update strategy for a serving Deployment so that an
// in-place template change can never deadlock on the model store or on GPUs.
// The default RollingUpdate starts the new Pod before stopping the old
// one; with a ReadWriteOnce store the new Pod can land on another node and hang
// on Multi-Attach while the old Pod keeps the volume, and with one GPU per node
// it cannot be scheduled at all. Either way the Deployment never completes.
//
//   - Store not shareable (RWO): Recreate. RollingUpdate with maxSurge 0 is not
//     enough, because with replicas > 1 the new Pod can still land on another node
//     while another old Pod holds the volume.
//   - Store shareable (RWX/ROX) and device cuda: RollingUpdate with maxSurge 0 and
//     maxUnavailable 1, so no spare GPU is needed.
//   - Otherwise: the API server's own default (25% / 25%), written explicitly so
//     it can be compared.
//
// For replicas: 1 the first two mean a short outage during an in-place template
// change (the old Pod stops first). Template changes that come from the spec
// (model, resources, scheduling, ...) do not go through here: they are new
// revisions.
func deploymentStrategy(accessModes []corev1.PersistentVolumeAccessMode, device string) appsv1.DeploymentStrategy {
	switch {
	case !accessModesShareable(accessModes):
		return appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	case device == engine.DeviceCUDA:
		zero, one := intstr.FromInt32(0), intstr.FromInt32(1)
		return appsv1.DeploymentStrategy{
			Type:          appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &zero, MaxUnavailable: &one},
		}
	default:
		quarter := intstr.FromString("25%")
		return appsv1.DeploymentStrategy{
			Type:          appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &quarter, MaxUnavailable: &quarter},
		}
	}
}

// strategyMatches compares a live Deployment strategy with the desired one. An
// unset live type is the API default, RollingUpdate.
func strategyMatches(live, want appsv1.DeploymentStrategy) bool {
	liveType := live.Type
	if liveType == "" {
		liveType = appsv1.RollingUpdateDeploymentStrategyType
	}
	if liveType != want.Type {
		return false
	}
	if want.Type == appsv1.RecreateDeploymentStrategyType {
		return live.RollingUpdate == nil
	}
	if live.RollingUpdate == nil || want.RollingUpdate == nil {
		return false
	}
	return equality.Semantic.DeepEqual(live.RollingUpdate.MaxSurge, want.RollingUpdate.MaxSurge) &&
		equality.Semantic.DeepEqual(live.RollingUpdate.MaxUnavailable, want.RollingUpdate.MaxUnavailable)
}

// freezeFromLive copies the parts of a serving Pod template that are decided when
// a revision is created from the live Deployment onto a freshly rendered PodSpec:
// placement (nodeSelector, tolerations, affinity, runtimeClassName) and the
// serving container's resources. They only change through a new revision. This is
// what keeps an operator upgrade (the GPU toleration, the arch selector, new
// engine resource defaults) from rolling every running stable in place, and what
// keeps a pending or failed candidate's scheduling from leaking into the stable.
// Containers are matched by name.
func freezeFromLive(podSpec *corev1.PodSpec, live *corev1.PodSpec) {
	podSpec.NodeSelector = live.NodeSelector
	podSpec.Tolerations = live.Tolerations
	podSpec.Affinity = live.Affinity
	podSpec.RuntimeClassName = live.RuntimeClassName
	for i := range podSpec.Containers {
		for j := range live.Containers {
			if live.Containers[j].Name == podSpec.Containers[i].Name {
				podSpec.Containers[i].Resources = *live.Containers[j].Resources.DeepCopy()
			}
		}
	}
}
