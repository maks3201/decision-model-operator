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

// A repeated annotation write on a Ready stable must not churn the serving
// Deployment's Pod template. If it did, each reconcile would bump the Deployment
// generation and roll a new ReplicaSet while the old one's Pods are still Ready,
// which would leave more ready endpoints in the Service than desired replicas.
// The DM's own annotations are control signals only (promote/retry) and never
// feed the Pod template or any template checksum, so annotating must be a no-op
// for the Deployment.
var _ = Describe("annotating a stable does not roll its Deployment", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	newRec := func(pr Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   pr,
			Recorder: events.NewFakeRecorder(256),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}
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
	// gatedPods makes `n` model-ready Pods for a revision so a multi-replica stable
	// reaches Ready.
	gatedPods := func(name, rev string, n int) {
		for i := 0; i < n; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: fmt.Sprintf("%s-pod-%s-%d", name, rev, i),
					Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status.PodIP = fmt.Sprintf("10.1.0.%d", i+1)
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
	}
	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("annot-noroll-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("keeps the Deployment generation and Pod template unchanged across repeated annotation writes (replicas 3)", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "ann"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(3)},
		})).To(Succeed())
		rev := RevisionHash(getDM("ann").Spec, defaultDigest, fakeImage)
		rec(r, "ann")
		markJob("ann", rev)
		rec(r, "ann")
		gatedPods("ann", rev, 3)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "ann") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))

		depBefore := stableDep("ann", rev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()
		rvBefore := depBefore.ResourceVersion

		By("annotating the DM 10 times, reconciling each time")
		for i := 0; i < 10; i++ {
			Expect(updateDM(ctx, namespace, "ann", func(d *decisionmodelv1alpha1.DecisionModel) {
				if d.Annotations == nil {
					d.Annotations = map[string]string{}
				}
				d.Annotations["example.com/touch"] = fmt.Sprintf("v%d", i)
			})).To(Succeed())
			rec(r, "ann")
		}

		By("the serving Deployment is byte-identical: no generation bump, no template change, no write")
		depAfter := stableDep("ann", rev)
		Expect(depAfter.Generation).To(Equal(genBefore), "annotating the DM must not bump the Deployment generation")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the Pod template must be unchanged (no new ReplicaSet)")
		Expect(depAfter.ResourceVersion).To(Equal(rvBefore), "the Deployment must not be written at all on an annotation-only change")
		Expect(*depAfter.Spec.Replicas).To(Equal(int32(3)), "replicas unchanged")
	})
})
