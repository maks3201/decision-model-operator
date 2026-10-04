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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// after a container restart the Pod is re-probed ONCE, not on every
// reconcile. (Reproduced first against the unfixed code; see the report.)
var _ = Describe("restart re-probe", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("reprobe-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("re-probes a restarted Pod exactly once, and again only after a further restart", func() {
		prober := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(128),
		}
		rec := func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "rp"}})
			Expect(err).NotTo(HaveOccurred())
		}
		one := int32(1)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rp"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: &one},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{}
		rec()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rp"}, dm)).To(Succeed())
		rev := RevisionHash(dm.Spec, defaultDigest, fakeImage)
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rp-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		rec()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rp-pod",
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName: "rp", decisionmodelv1alpha1.LabelRevision: rev}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodConditions := func(crLTT time.Time) {
			cur := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rp-pod"}, cur)).To(Succeed())
			cur.Status.PodIP = "10.0.0.90"
			cur.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(crLTT)},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(crLTT)},
				{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(crLTT.Add(-time.Hour))},
			}
			Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
		}
		base := time.Now().Add(-2 * time.Hour)
		setPodConditions(base) // gate set an hour before containers became ready
		// Reach a stable revision with the Pod counted.
		for i := 0; i < 4; i++ {
			rec()
		}
		Expect(prober.callCount()).To(BeNumerically(">=", 0))

		// Container restart #1: ContainersReady turns ready again, after the gate.
		callsBefore := prober.callCount()
		setPodConditions(time.Now())
		for i := 0; i < 5; i++ {
			rec()
		}
		delta1 := prober.callCount() - callsBefore
		Expect(delta1).To(Equal(1), "a restarted Pod must be re-probed once, not on every reconcile (got %d probes over 5 reconciles)", delta1)

		// Container restart #2 is a new event and probes again, once.
		callsBefore = prober.callCount()
		setPodConditions(time.Now().Add(time.Minute))
		for i := 0; i < 5; i++ {
			rec()
		}
		Expect(prober.callCount()-callsBefore).To(Equal(1), "a second restart probes once more")
	})
})
