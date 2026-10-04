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
	"encoding/json"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// ---------------------------------------------------------------- unit tests

func TestDeploymentStrategy(t *testing.T) {
	rwo := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	rwx := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	rox := []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
	tests := []struct {
		name         string
		modes        []corev1.PersistentVolumeAccessMode
		device       string
		wantType     appsv1.DeploymentStrategyType
		wantSurge    string
		wantUnavail  string
		wantRolling  bool
		wantExplicit bool
	}{
		{"RWO cpu: Recreate", rwo, engine.DeviceCPU, appsv1.RecreateDeploymentStrategyType, "", "", false, false},
		{"RWO cuda: Recreate", rwo, engine.DeviceCUDA, appsv1.RecreateDeploymentStrategyType, "", "", false, false},
		{"no modes recorded counts as not shareable: Recreate", nil, engine.DeviceCPU, appsv1.RecreateDeploymentStrategyType, "", "", false, false},
		{"RWX cuda: no surge, one unavailable", rwx, engine.DeviceCUDA, appsv1.RollingUpdateDeploymentStrategyType, "0", "1", true, true},
		{"ROX cuda: no surge, one unavailable", rox, engine.DeviceCUDA, appsv1.RollingUpdateDeploymentStrategyType, "0", "1", true, true},
		{"RWX cpu: default 25%", rwx, engine.DeviceCPU, appsv1.RollingUpdateDeploymentStrategyType, "25%", "25%", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deploymentStrategy(tt.modes, tt.device)
			if got.Type != tt.wantType {
				t.Fatalf("type = %q, want %q", got.Type, tt.wantType)
			}
			if !tt.wantRolling {
				if got.RollingUpdate != nil {
					t.Errorf("Recreate must not carry rollingUpdate: %+v", got.RollingUpdate)
				}
				return
			}
			if got.RollingUpdate.MaxSurge.String() != tt.wantSurge || got.RollingUpdate.MaxUnavailable.String() != tt.wantUnavail {
				t.Errorf("surge/unavailable = %s/%s, want %s/%s", got.RollingUpdate.MaxSurge,
					got.RollingUpdate.MaxUnavailable, tt.wantSurge, tt.wantUnavail)
			}
		})
	}
}

func TestStrategyMatches(t *testing.T) {
	recreate := appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	noSurge := deploymentStrategy([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, engine.DeviceCUDA)
	def := deploymentStrategy([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, engine.DeviceCPU)
	// What the API server stores for an unset strategy: type defaulted, 25%/25%.
	q := intstr.FromString("25%")
	apiDefault := appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &q, MaxUnavailable: &q}}
	tests := []struct {
		name       string
		live, want appsv1.DeploymentStrategy
		match      bool
	}{
		{"recreate = recreate", recreate, recreate, true},
		{"API default = our explicit 25%", apiDefault, def, true},
		{"empty live type counts as RollingUpdate", appsv1.DeploymentStrategy{RollingUpdate: apiDefault.RollingUpdate}, def, true},
		{"rolling where recreate is wanted", apiDefault, recreate, false},
		{"recreate where rolling is wanted", recreate, def, false},
		{"25% where no-surge is wanted", apiDefault, noSurge, false},
		{"rolling without parameters never matches", appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType}, def, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strategyMatches(tt.live, tt.want); got != tt.match {
				t.Errorf("strategyMatches = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestFreezeFromLive(t *testing.T) {
	live := &corev1.PodSpec{
		NodeSelector:     map[string]string{"pool": "old"},
		Tolerations:      []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
		RuntimeClassName: ptrTo("nvidia"),
		Containers: []corev1.Container{
			{Name: "ollaya", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}}},
			{Name: "sidecar"},
		},
	}
	fresh := &corev1.PodSpec{
		NodeSelector: map[string]string{"pool": "new", archLabel: "amd64"},
		Tolerations: []corev1.Toleration{{Key: gpuTaintKey, Operator: corev1.TolerationOpExists},
			{Key: "dedicated", Operator: corev1.TolerationOpExists}},
		Affinity: &corev1.Affinity{},
		Containers: []corev1.Container{
			{Name: "ollaya", Image: "fresh-image", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}},
		},
		ServiceAccountName: "fresh-sa",
	}
	freezeFromLive(fresh, live)

	if fresh.NodeSelector["pool"] != "old" || len(fresh.NodeSelector) != 1 {
		t.Errorf("nodeSelector = %v, want the live one only", fresh.NodeSelector)
	}
	if len(fresh.Tolerations) != 1 || fresh.Tolerations[0].Key != "dedicated" {
		t.Errorf("tolerations = %v, want the live ones (no operator-added GPU toleration)", fresh.Tolerations)
	}
	if fresh.Affinity != nil {
		t.Errorf("affinity = %v, want the live (nil)", fresh.Affinity)
	}
	if fresh.RuntimeClassName == nil || *fresh.RuntimeClassName != "nvidia" {
		t.Errorf("runtimeClassName = %v, want nvidia", fresh.RuntimeClassName)
	}
	c := fresh.Containers[0]
	if _, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
		t.Errorf("resources = %+v, want the live ones (no re-defaulted request)", c.Resources)
	}
	if got := c.Resources.Limits[corev1.ResourceMemory]; got.String() != "4Gi" {
		t.Errorf("limit = %s, want 4Gi", got.String())
	}
	// Everything else is still freshly rendered.
	if c.Image != "fresh-image" || fresh.ServiceAccountName != "fresh-sa" {
		t.Errorf("non-frozen fields must stay freshly rendered: image=%q sa=%q", c.Image, fresh.ServiceAccountName)
	}
	// The copy must not alias the live object.
	fresh.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("9Gi")
	if live.Containers[0].Resources.Limits[corev1.ResourceMemory] == resource.MustParse("9Gi") {
		t.Error("resources alias the live PodSpec")
	}
}

func TestSameIdentity(t *testing.T) {
	base := func() *decisionmodelv1alpha1.RevisionStatus {
		return &decisionmodelv1alpha1.RevisionStatus{
			Hash: "h", Engine: "ollaya", Model: "laya:en", Digest: "d", Device: "cpu", Image: "img",
			Placement: placementNone,
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
		}
	}
	mut := func(f func(*decisionmodelv1alpha1.RevisionStatus)) *decisionmodelv1alpha1.RevisionStatus {
		r := base()
		f(r)
		return r
	}
	tests := []struct {
		name string
		a, b *decisionmodelv1alpha1.RevisionStatus
		want bool
	}{
		{"equal", base(), base(), true},
		{"hash name is ignored (adopted legacy keeps its name)", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Hash = "old" }), base(), true},
		{"precision is not identity", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Precision = "F16" }), base(), true},
		{"equivalent quantities", mut(func(r *decisionmodelv1alpha1.RevisionStatus) {
			r.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1024Mi")
		}), base(), true},
		{"model", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Model = "kev:en" }), base(), false},
		{"digest", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Digest = "x" }), base(), false},
		{"device", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Device = "cuda" }), base(), false},
		{"image", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Image = "x" }), base(), false},
		{"placement", mut(func(r *decisionmodelv1alpha1.RevisionStatus) { r.Placement = "abc" }), base(), false},
		{"resources", mut(func(r *decisionmodelv1alpha1.RevisionStatus) {
			r.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
		}), base(), false},
		{"nil", nil, base(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameIdentity(tt.a, tt.b); got != tt.want {
				t.Errorf("sameIdentity = %v, want %v", got, tt.want)
			}
		})
	}
}

// ------------------------------------------------------------ envtest specs

var _ = Describe("in-place updates", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }
	rwx := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}

	newR := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(128),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		Expect(err).NotTo(HaveOccurred())
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	getDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	jobCount := func(name string) int {
		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(namespace),
			client.MatchingLabels{decisionmodelv1alpha1.LabelName: name})).To(Succeed())
		return len(jobs.Items)
	}
	markJob := func(dmName, rev string, failed bool) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		if failed {
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
			}
		} else {
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	createGatedPod := func(dmName, rev string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: dmName + "-pod-" + rev,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.80"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	newDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}
	// stableNow drives a fresh DM (created by the current operator) to a stable
	// revision with a gated Pod and returns the revision hash.
	stableNow := func(r *DecisionModelReconciler, name string, image string) string {
		rec(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, image)
		markJob(name, rev, false)
		rec(r, name)
		createGatedPod(name, rev)
		rec(r, name)
		rec(r, name)
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		return rev
	}

	// legacyStable fabricates what an OLDER operator left behind for a cuda DM
	// with scheduling: a stable recorded without placement under the OLD hash, and
	// a Deployment rendered without the GPU toleration / arch selector that later
	// versions add (and without a strategy).
	legacyStable := func(r *DecisionModelReconciler, name string) (h1 string, before *appsv1.Deployment) {
		newDM(name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Device = "cuda"
			dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{AccessModes: rwx}
			dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{NodeSelector: map[string]string{"pool": "gpu"}}
		})
		dm := getDM(name)
		h1 = legacyRevisionHash(dm.Spec, defaultDigest, fakeImageCUDA)
		Expect(h1).NotTo(Equal(RevisionHash(dm.Spec, defaultDigest, fakeImageCUDA)), "scenario needs the hash to have moved")
		Expect(updateDMStatus(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.Phase = decisionmodelv1alpha1.PhaseReady
			d.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
				Hash: h1, Engine: "ollaya", Model: dm.Spec.Model, Digest: defaultDigest, Device: "cuda",
				Image: fakeImageCUDA, // recorded since per-revision stores; no Placement (older operator)
			}
		})).To(Succeed())

		dm = getDM(name)
		params := r.paramsForRevision(dm, dm.Status.StableRevision, storeNameRev(dm, h1))
		podSpec := r.Engines["ollaya"].ServingPodSpec(params)
		podSpec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: decisionmodelv1alpha1.ModelReadyGate}}
		podSpec.NodeSelector = map[string]string{"pool": "gpu"} // user scheduling only
		applySELinuxLevel(&podSpec, dm)                         // already rendered by older operator versions
		labels := revisionLabels(dm, h1)
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name + "-" + h1, Labels: labels,
				Annotations: map[string]string{specHashAnnotation: "written-by-an-older-operator"}},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: podSpec},
			},
		}
		Expect(controllerutil.SetControllerReference(dm, dep, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		createGatedPod(name, h1)
		return h1, getDep(name, h1)
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("inplace-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	Describe("deployment strategy", func() {
		It("is Recreate on an RWO store, no-surge rolling on RWX+cuda, and is repaired on drift", func() {
			r := newR()
			newDM("st-rwo", nil) // default cache = ReadWriteOnce, cpu
			revRWO := stableNow(r, "st-rwo", fakeImage)
			dep := getDep("st-rwo", revRWO)
			Expect(dep.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			Expect(dep.Spec.Strategy.RollingUpdate).To(BeNil())

			newDM("st-cuda", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Device = "cuda"
				dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{AccessModes: rwx}
			})
			revCuda := stableNow(r, "st-cuda", fakeImageCUDA)
			cuda := getDep("st-cuda", revCuda)
			Expect(cuda.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
			Expect(cuda.Spec.Strategy.RollingUpdate.MaxSurge.IntValue()).To(BeZero())
			Expect(cuda.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(1))

			// Someone puts the default rolling strategy back: the next reconcile restores ours.
			Expect(retryUpdateDep(ctx, namespace, "st-rwo-"+revRWO, func(d *appsv1.Deployment) {
				q := intstr.FromString("25%")
				d.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
					RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &q, MaxUnavailable: &q}}
			})).To(Succeed())
			rec(r, "st-rwo")
			Expect(getDep("st-rwo", revRWO).Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))

			// And a steady state writes nothing.
			rv := getDep("st-rwo", revRWO).ResourceVersion
			rec(r, "st-rwo")
			rec(r, "st-rwo")
			Expect(getDep("st-rwo", revRWO).ResourceVersion).To(Equal(rv))
		})
	})

	Describe("upgrade safety", func() {
		It("adopts a legacy stable without a new revision and without touching its template", func() {
			r := newR()
			h1, before := legacyStable(r, "up1")

			rec(r, "up1")

			dm := getDM("up1")
			Expect(dm.Status.CandidateRevision).To(BeNil(), "an operator upgrade must not start a candidate")
			Expect(dm.Status.StableRevision.Hash).To(Equal(h1), "the stable keeps its hash name")
			Expect(dm.Status.StableRevision.Placement).To(Equal(placementRecord(dm.Spec.Scheduling)), "placement is now recorded")
			Expect(dm.Status.StableRevision.Placement).NotTo(Or(BeEmpty(), Equal(placementNone)))
			Expect(jobCount("up1")).To(BeZero(), "nothing is prefetched again")
			Expect(dm.Status.FailedRevision).To(BeNil())

			after := getDep("up1", h1)
			Expect(equality.Semantic.DeepEqual(after.Spec.Template, before.Spec.Template)).To(BeTrue(),
				"the pod template is byte-for-byte unchanged, so no new ReplicaSet and no Pod roll:\nbefore=%s\nafter=%s",
				mustJSON(before.Spec.Template), mustJSON(after.Spec.Template))
			Expect(after.Spec.Template.Spec.Tolerations).To(BeEmpty(), "the operator-added GPU toleration is NOT applied to a running stable")
			Expect(after.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{"pool": "gpu"}), "no arch selector added either")
			// The one allowed change is the strategy (not part of the template).
			Expect(after.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
			Expect(after.Spec.Strategy.RollingUpdate.MaxSurge.IntValue()).To(BeZero())

			// Idempotent afterwards.
			rv := after.ResourceVersion
			rec(r, "up1")
			rec(r, "up1")
			Expect(getDep("up1", h1).ResourceVersion).To(Equal(rv))
			Expect(getDM("up1").Status.CandidateRevision).To(BeNil())
		})

		It("starts a new revision when scheduling really changes after the upgrade, and leaves the stable alone", func() {
			r := newR()
			h1, _ := legacyStable(r, "up2")
			rec(r, "up2") // adoption
			stableRV := getDep("up2", h1).ResourceVersion

			Expect(updateDM(ctx, namespace, "up2", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Scheduling.NodeSelector = map[string]string{"pool": "gpu-b"}
			})).To(Succeed())
			rec(r, "up2")

			dm := getDM("up2")
			Expect(dm.Status.StableRevision.Hash).To(Equal(h1))
			Expect(dm.Status.CandidateRevision).NotTo(BeNil())
			newRev := dm.Status.CandidateRevision.Hash
			Expect(newRev).NotTo(Equal(h1))
			Expect(dm.Status.CandidateRevision.Placement).NotTo(Equal(dm.Status.StableRevision.Placement))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "up2-prefetch-" + newRev}, &batchv1.Job{})).To(Succeed())
			stable := getDep("up2", h1)
			Expect(stable.ResourceVersion).To(Equal(stableRV), "the running stable is not touched")
			Expect(stable.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{"pool": "gpu"}))
		})

		It("does not mistake a modern stable for a legacy one when scheduling is added later", func() {
			r := newR()
			newDM("up3", nil) // no scheduling
			rev := stableNow(r, "up3", fakeImage)
			Expect(getDM("up3").Status.StableRevision.Placement).To(Equal(placementNone))

			Expect(updateDM(ctx, namespace, "up3", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{NodeSelector: map[string]string{"pool": "a"}}
			})).To(Succeed())
			rec(r, "up3")

			dm := getDM("up3")
			Expect(dm.Status.CandidateRevision).NotTo(BeNil(), "adding scheduling is a new revision, not an adoption")
			Expect(dm.Status.StableRevision.Hash).To(Equal(rev))
			Expect(dm.Status.StableRevision.Placement).To(Equal(placementNone))
			Expect(getDep("up3", rev).Spec.Template.Spec.NodeSelector).To(BeEmpty())
		})

		It("keeps the hash of a DecisionModel without scheduling (no candidate after the upgrade)", func() {
			r := newR()
			newDM("up4", nil)
			rev := stableNow(r, "up4", fakeImage)
			// Make the stable look like one recorded by an older operator.
			Expect(updateDMStatus(ctx, namespace, "up4", func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Status.StableRevision.Placement = ""
			})).To(Succeed())
			rec(r, "up4")
			dm := getDM("up4")
			Expect(dm.Status.CandidateRevision).To(BeNil())
			Expect(dm.Status.StableRevision.Hash).To(Equal(rev))
			Expect(dm.Status.StableRevision.Placement).To(Equal(placementNone))
		})
	})

	Describe("frozen placement", func() {
		It("keeps a failed candidate's scheduling out of the running stable", func() {
			r := newR()
			newDM("fz1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{NodeSelector: map[string]string{"pool": "a"}}
			})
			rev := stableNow(r, "fz1", fakeImage)
			stableRV := getDep("fz1", rev).ResourceVersion

			Expect(updateDM(ctx, namespace, "fz1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Scheduling.NodeSelector = map[string]string{"pool": "b"}
			})).To(Succeed())
			rec(r, "fz1") // candidate, Caching
			newRev := getDM("fz1").Status.CandidateRevision.Hash
			markJob("fz1", newRev, true)
			rec(r, "fz1") // rolled back; the stable is reconciled with the (failed) new spec in hand
			rec(r, "fz1")

			dm := getDM("fz1")
			Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))
			Expect(dm.Status.FailedRevision.Hash).To(Equal(newRev))
			stable := getDep("fz1", rev)
			Expect(stable.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{"pool": "a"}),
				"the failed candidate's scheduling must not reach the running stable")
			Expect(stable.ResourceVersion).To(Equal(stableRV), "and the stable Deployment is not rewritten")
		})
	})
})

func retryUpdateDep(ctx context.Context, ns, name string, mutate func(*appsv1.Deployment)) error {
	for i := 0; i < 5; i++ {
		dep := &appsv1.Deployment{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, dep); err != nil {
			return err
		}
		mutate(dep)
		if err := k8sClient.Update(ctx, dep); err == nil {
			return nil
		}
	}
	return fmt.Errorf("could not update deployment %s/%s", ns, name)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	return string(b)
}
