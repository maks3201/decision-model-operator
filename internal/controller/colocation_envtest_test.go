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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// With a non-shareable (RWO) store and replicas > 1, the serving Deployment
// co-locates its replicas on the node holding the volume (required host
// pod-affinity) instead of hanging on Multi-Attach — merged with any user
// affinity, and never a new revision when only replicas changed.
var _ = Describe("replicas co-location on a ReadWriteOnce store", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(64),
		}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	getDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	// markJob completes the revision's prefetch Job so the reconcile advances
	// from Caching to Starting (where the serving Deployment is created).
	markJob := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	// drive reconciles until the revision's serving Deployment exists.
	driveToDeployment := func(r *DecisionModelReconciler, name string) (string, *appsv1.Deployment) {
		rec(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJob(name, rev)
		rec(r, name)
		dep := &appsv1.Deployment{}
		Eventually(func() error {
			rec(r, name)
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)
		}, "5s", "50ms").Should(Succeed())
		return rev, dep
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "coloc-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("adds a host co-location affinity merged with the user affinity on RWO + replicas 2", func() {
		eng := newFakeEngine()
		r := newReconciler(eng)
		userAff := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"serving"},
					}},
				}},
			},
		}}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "co"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(2),
				Scheduling: &decisionmodelv1alpha1.SchedulingSpec{Affinity: userAff},
			},
		})).To(Succeed())
		rev, dep := driveToDeployment(r, "co")
		dm := getDM("co")

		aff := dep.Spec.Template.Spec.Affinity
		Expect(aff).NotTo(BeNil())
		Expect(aff.NodeAffinity).NotTo(BeNil(), "user node affinity preserved")
		Expect(hasColocationTerm(aff, revisionLabels(dm, rev))).To(BeTrue(), "co-location term added")
		// No stale CacheNotShareable Degraded.
		d := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		if d != nil {
			Expect(d.Status).NotTo(Equal(metav1.ConditionTrue))
		}
	})

	It("does not co-locate on an RWX store", func() {
		eng := newFakeEngine()
		r := newReconciler(eng)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rwx"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(2),
				Cache: &decisionmodelv1alpha1.CacheSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			},
		})).To(Succeed())
		rev, dep := driveToDeployment(r, "rwx")
		Expect(hasColocationTerm(dep.Spec.Template.Spec.Affinity, revisionLabels(getDM("rwx"), rev))).To(BeFalse())
	})

	It("does not co-locate at replicas 1", func() {
		eng := newFakeEngine()
		r := newReconciler(eng)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "one"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rev, dep := driveToDeployment(r, "one")
		Expect(hasColocationTerm(dep.Spec.Template.Spec.Affinity, revisionLabels(getDM("one"), rev))).To(BeFalse())
	})

	It("treats replicas 1 -> 2 as an in-place update, not a new revision, and adds the affinity", func() {
		eng := newFakeEngine()
		r := newReconciler(eng)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "scale"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rev1, dep1 := driveToDeployment(r, "scale")
		Expect(hasColocationTerm(dep1.Spec.Template.Spec.Affinity, revisionLabels(getDM("scale"), rev1))).To(BeFalse())

		// Scale up to 2: the revision hash is unchanged (replicas is in-place).
		Expect(updateDM(ctx, namespace, "scale", func(d *decisionmodelv1alpha1.DecisionModel) {
			two := int32(2)
			d.Spec.Replicas = &two
		})).To(Succeed())
		rec(r, "scale")
		dm := getDM("scale")
		rev2 := RevisionHash(dm.Spec, defaultDigest, fakeImage)
		Expect(rev2).To(Equal(rev1), "scaling replicas must not change the revision hash")

		dep := getDep("scale", rev1)
		Expect(*dep.Spec.Replicas).To(Equal(int32(2)))
		Expect(hasColocationTerm(dep.Spec.Template.Spec.Affinity, revisionLabels(dm, rev1))).To(BeTrue(),
			"co-location affinity added in place on the scale-up")

		// The ReplicasCoLocated Event fired on the transition.
		fr, _ := r.Recorder.(*events.FakeRecorder)
		sawCoLocated := false
		for drained := false; !drained; {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, eventReplicasCoLocated) {
					sawCoLocated = true
				}
			default:
				drained = true
			}
		}
		Expect(sawCoLocated).To(BeTrue(), "ReplicasCoLocated Event emitted on the transition into co-location")
	})
})
