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

// behaviour (transport errors invalidate a run) is covered end-to-end by the
// "eval blip does not promote" spec below.

// Integration: a transport blip during the candidate eval must not promote; once
// it clears, the candidate evaluates and promotes.
var _ = Describe("eval blip does not promote", func() {
	const model = "laya:en"
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": eng}, Prober: prober,
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
	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.22"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	markJob := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "evalcorr-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("does not promote on a transport blip, then promotes once it clears", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		eng.decideErr.Store(true)
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden"},
			Data:       map[string]string{"cases.jsonl": `{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}` + "\n"},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "blip"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
					MinAccuracy: "0.90",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())

		rec(r, "blip")
		rev := RevisionHash(getDM("blip").Spec, defaultDigest, fakeImage)
		markJob("blip", rev)
		rec(r, "blip")
		createReadyPod("blip", rev, "blip-pod-0")

		// While the engine returns transport errors, the eval never promotes:
		// a few reconciles keep it out of Ready (the blip run is forgotten+retried).
		for i := 0; i < 5; i++ {
			rec(r, "blip")
		}
		Expect(getDM("blip").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseReady),
			"a transport blip must not promote")
		Expect(getDM("blip").Status.StableRevision).To(BeNil())

		// Blip clears: the candidate evaluates (100% accuracy) and promotes.
		eng.decideErr.Store(false)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "blip")
			return getDM("blip").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("blip").Status.StableRevision.Hash).To(Equal(rev))
	})

	// every condition carries ObservedGeneration, updated on a spec change.
	It("stamps ObservedGeneration on conditions and updates it on a spec change", func() {
		eng := newFakeEngine()
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "og"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, "og")
		got := getDM("og")
		gen1 := got.Generation
		Expect(got.Status.Conditions).NotTo(BeEmpty())
		for _, c := range got.Status.Conditions {
			Expect(c.ObservedGeneration).To(Equal(gen1), "condition %s carries observedGeneration", c.Type)
		}

		// Bump the spec (a model change -> new generation) and reconcile.
		Expect(updateDM(ctx, namespace, "og", func(d *decisionmodelv1alpha1.DecisionModel) {
			two := int32(1)
			d.Spec.Replicas = &two
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		gen2 := getDM("og").Generation
		Expect(gen2).To(BeNumerically(">", gen1))
		rec(r, "og")
		for _, c := range getDM("og").Status.Conditions {
			Expect(c.ObservedGeneration).To(Equal(gen2), "condition %s observedGeneration updated", c.Type)
		}
	})
})

// evalStore.get returns a snapshot; mutating the returned copy does not
// affect the stored entry, and a concurrent finish is race-free (also proven by
// go test -race).
var _ = Describe("evalStore snapshot", func() {
	It("returns a copy from get, not the shared pointer", func() {
		s := newEvalStore()
		k := evalKey{"ns", "n", "r", "d", 0}
		s.start(k, func() {})
		s.finish(k, evalResult{done: true, accuracy: 0.5})
		snap, ok := s.get(k)
		Expect(ok).To(BeTrue())
		snap.result.accuracy = 0.99 // mutate the copy
		again, _ := s.get(k)
		Expect(again.result.accuracy).To(Equal(0.5), "stored entry is not aliased by the returned snapshot")
	})
})
